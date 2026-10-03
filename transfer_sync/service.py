"""跨网点批次同步服务。

持久化边界（每笔转账的提交流程，每步独立 commit）：
1. 完整批次先持久保存到源网点账本（batches 表），再产生任何账务效果。
2. 逐项执行，当前项终结（success/failed 落盘 + 进度推进）后才执行下一项：
   a. 源账本：余额不足 -> 记录 failed 结果（无分录）；足够 -> 借记分录落盘。
   b. 目标账本：贷记分录落盘（tx_id 唯一，幂等）。
   c. 源账本：记录 success 结果。
   d. 源账本：推进批次进度。
任一边界中断后，recover() 依据分录与结果的持久状态幂等续跑，
结果与无中断顺序执行一次相同。
"""
from __future__ import annotations

import json
from typing import Callable, Dict, List, Optional

from .errors import ConflictError, TransferSyncError, UnknownAccountError
from .ledger import Ledger

# 崩溃注入检查点名称（测试用）
CHECKPOINTS = (
    "after_debit_commit",    # 源分录已落盘、目标未入账
    "after_credit_commit",   # 两侧分录已落盘、结果未记录
    "after_result_commit",   # 结果已落盘、批次进度未推进
    "after_advance_commit",  # 进度已推进、sync 尚未返回
)


class SyncService:
    """管理若干网点账本，提供 sync/recover/inspect。"""

    def __init__(self, ledger_paths: Dict[str, str],
                 crash_hook: Optional[Callable[[str], None]] = None) -> None:
        if len(ledger_paths) < 2:
            raise ValueError("至少需要两个网点账本")
        self._ledgers: Dict[str, Ledger] = {
            site: Ledger(path) for site, path in ledger_paths.items()
        }
        self._crash_hook = crash_hook

    def close(self) -> None:
        for ledger in self._ledgers.values():
            ledger.close()

    def __enter__(self) -> "SyncService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- 初始化 --
    def init_site(self, site: str, accounts: Dict[str, int]) -> None:
        self._ledger(site).init_accounts(accounts)

    # -- 核心操作 --
    def sync(self, batch_id: str, source: str,
             items: List[dict]) -> List[dict]:
        """持久接收并执行有序批次，返回逐项结果。"""
        src_ledger = self._ledger_of_account(source)
        self._validate_items(items)

        # 1. 完整批次先持久保存；同 batch_id 重试须与原请求一致。
        existing = src_ledger.get_batch(batch_id)
        if existing is None:
            src_ledger.save_batch(batch_id, source, items)
            batch = src_ledger.get_batch(batch_id)
        else:
            if existing["source"] != source or existing["items"] != items:
                raise ConflictError(
                    f"batch_id={batch_id!r} 已被不同的请求使用")
            batch = existing

        self._run_batch(src_ledger, batch)
        return self._batch_results(src_ledger, batch["batch_id"])

    def recover(self) -> List[dict]:
        """重新打开账本后，续跑所有已持久接收但未终结的批次。"""
        recovered: List[dict] = []
        for ledger in self._ledgers.values():
            for batch in ledger.pending_batches():
                self._run_batch(ledger, batch)
                recovered.append({
                    "batch_id": batch["batch_id"],
                    "results": self._batch_results(ledger, batch["batch_id"]),
                })
        return recovered

    def inspect(self) -> dict:
        """只读查看账户余额及批次逐项状态。"""
        accounts: Dict[str, Dict[str, int]] = {}
        batches: List[dict] = []
        for site, ledger in self._ledgers.items():
            accounts[site] = ledger.list_accounts()
            results = {r["tx_id"]: r for r in ledger.list_tx_results()}
            for batch in ledger.list_batches():
                item_states = []
                for item in batch["items"]:
                    result = results.get(item["tx_id"])
                    item_states.append({
                        "tx_id": item["tx_id"],
                        "dest": item["dest"],
                        "amount": item["amount"],
                        "status": result["status"] if result else "pending",
                    })
                batches.append({
                    "site": site,
                    "batch_id": batch["batch_id"],
                    "source": batch["source"],
                    "done": batch["done"],
                    "next_index": batch["next_index"],
                    "items": item_states,
                })
        return {"accounts": accounts, "batches": batches}

    # -- 内部：批次执行 --
    def _run_batch(self, src_ledger: Ledger, batch: dict) -> None:
        items = batch["items"]
        for index in range(batch["next_index"], len(items)):
            self._run_item(src_ledger, batch["batch_id"], batch["source"],
                           items[index])
            src_ledger.advance_batch(batch["batch_id"], index + 1,
                                     done=(index + 1 == len(items)))
            self._checkpoint("after_advance_commit")

    def _run_item(self, src_ledger: Ledger, batch_id: str, source: str,
                  item: dict) -> None:
        tx_id = item["tx_id"]
        dest = item["dest"]
        amount = item["amount"]

        # 已终结交易：返回首次结果，不重复记账、不改判。
        prior = src_ledger.get_tx_result(tx_id)
        if prior is not None:
            if (prior["source"], prior["dest"], prior["amount"]) != (source, dest, amount):
                raise ConflictError(
                    f"tx_id={tx_id!r} 已以不同参数终结")
            return

        dst_ledger = self._ledger_of_account(dest)
        if dst_ledger is src_ledger:
            raise TransferSyncError("仅处理跨网点转账")

        debited = src_ledger.has_entry(tx_id)
        credited = dst_ledger.has_entry(tx_id)

        if not debited:
            # 源分录尚未落盘：余额不足则该项失败（无分录），继续后项。
            if src_ledger.get_balance(source) < amount:
                src_ledger.save_tx_result(tx_id, batch_id, source, dest,
                                          amount, "failed")
                self._checkpoint("after_result_commit")
                return
            src_ledger.post_entry(tx_id, source, -amount)
            self._checkpoint("after_debit_commit")

        if not credited:
            dst_ledger.post_entry(tx_id, dest, amount)
            self._checkpoint("after_credit_commit")

        src_ledger.save_tx_result(tx_id, batch_id, source, dest, amount,
                                  "success")
        self._checkpoint("after_result_commit")

    # -- 内部：工具 --
    def _batch_results(self, src_ledger: Ledger, batch_id: str) -> List[dict]:
        batch = src_ledger.get_batch(batch_id)
        results = []
        for item in batch["items"]:
            record = src_ledger.get_tx_result(item["tx_id"])
            results.append({
                "tx_id": item["tx_id"],
                "status": record["status"] if record else "pending",
            })
        return results

    def _ledger(self, site: str) -> Ledger:
        try:
            return self._ledgers[site]
        except KeyError:
            raise TransferSyncError(f"未知网点: {site!r}") from None

    def _ledger_of_account(self, account_id: str) -> Ledger:
        for ledger in self._ledgers.values():
            if ledger.has_account(account_id):
                return ledger
        raise UnknownAccountError(f"账户不属于任何网点: {account_id!r}")

    def _checkpoint(self, name: str) -> None:
        if self._crash_hook is not None:
            self._crash_hook(name)

    @staticmethod
    def _validate_items(items: List[dict]) -> None:
        for item in items:
            amount = item.get("amount")
            if not isinstance(amount, int) or isinstance(amount, bool) or amount <= 0:
                raise ValueError(f"金额必须为正整数分: {item!r}")
            if not item.get("tx_id") or not item.get("dest"):
                raise ValueError(f"项缺少 tx_id 或 dest: {item!r}")
