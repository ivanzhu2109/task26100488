"""离线跨网点转账批次同步库（仅标准库 + SQLite）。

两个网点各持有一个独立的 SQLite 账本文件。账户唯一属于一个网点。
仅处理跨网点转账，金额为正整数（分）。

持久化与恢复模型（单批次内逐项两阶段落盘）：
1. 产生任何账务效果前，完整批次（有序条目）先持久化到源网点的 batches 表。
2. 每个条目：源网点事务内扣款 + 源分录 + txs(pending) 落盘并提交；
   随后目标网点事务内 inbox 去重 + 入账 + 目标分录落盘并提交；
   最后源网点事务内将 txs 置为 success/failed 并推进批次游标。
3. 任意边界中断后，recover() 依据已落盘状态幂等地续做，已终结结果不可改判。
"""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Optional, Union


class LedgerSyncError(Exception):
    """库基础异常。"""


class BatchConflictError(LedgerSyncError):
    """相同 batch_id 对应了不同的请求内容。"""


class TxConflictError(LedgerSyncError):
    """相同 tx_id 对应了不同的交易参数。"""


@dataclass(frozen=True)
class TransferItem:
    tx_id: str
    to: str
    amount: int


@dataclass(frozen=True)
class ItemResult:
    tx_id: str
    status: str  # "success" | "failed"
    reason: Optional[str] = None


ItemLike = Union[TransferItem, Mapping[str, object], tuple]
CrashHook = Callable[[str], None]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    balance INTEGER NOT NULL CHECK (balance >= 0)
);
CREATE TABLE IF NOT EXISTS entries (
    tx_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    account TEXT NOT NULL,
    delta INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    items_json TEXT NOT NULL,
    next_index INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'done'))
);
CREATE TABLE IF NOT EXISTS txs (
    tx_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL,
    dest TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'success', 'failed')),
    reason TEXT
);
CREATE TABLE IF NOT EXISTS inbox (
    tx_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL,
    amount INTEGER NOT NULL
);
"""


def _normalize_item(raw: ItemLike) -> TransferItem:
    if isinstance(raw, TransferItem):
        item = raw
    elif isinstance(raw, Mapping):
        item = TransferItem(
            tx_id=raw["tx_id"], to=raw["to"], amount=raw["amount"]  # type: ignore[arg-type]
        )
    else:
        tx_id, to, amount = raw  # type: ignore[misc]
        item = TransferItem(tx_id=tx_id, to=to, amount=amount)
    if not isinstance(item.tx_id, str) or not item.tx_id:
        raise LedgerSyncError(f"非法 tx_id: {item.tx_id!r}")
    if not isinstance(item.to, str) or not item.to:
        raise LedgerSyncError(f"非法收款账户: {item.to!r}")
    if isinstance(item.amount, bool) or not isinstance(item.amount, int) or item.amount <= 0:
        raise LedgerSyncError(f"金额必须为正整数分: {item.amount!r}")
    return item


class LedgerSync:
    """管理两个网点的独立 SQLite 账本，提供 sync/recover/inspect。

    branches: {网点名: SQLite 文件路径}，恰好两个网点。
    initial_accounts: 仅首次初始化生效，{网点名: {账户: 非负余额}}。
    crash_hook: 测试用钩子，在持久化边界被调用，可抛出异常模拟宕机。
    """

    def __init__(
        self,
        branches: Mapping[str, str],
        initial_accounts: Optional[Mapping[str, Mapping[str, int]]] = None,
        crash_hook: Optional[CrashHook] = None,
    ) -> None:
        if len(branches) != 2:
            raise LedgerSyncError("必须恰好提供两个网点")
        self._paths = dict(branches)
        self._hook = crash_hook
        self._conns: dict[str, sqlite3.Connection] = {}
        for name, path in self._paths.items():
            conn = sqlite3.connect(path)
            conn.execute("PRAGMA busy_timeout = 5000")
            conn.execute("PRAGMA foreign_keys = ON")
            self._conns[name] = conn
            self._init_branch(name, conn, (initial_accounts or {}).get(name, {}))

    def _init_branch(
        self, name: str, conn: sqlite3.Connection, accounts: Mapping[str, int]
    ) -> None:
        with conn:
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('branch', ?)", (name,)
            )
            row = conn.execute(
                "SELECT value FROM meta WHERE key = 'seeded'"
            ).fetchone()
            if row is None:
                for acct, balance in accounts.items():
                    if isinstance(balance, bool) or not isinstance(balance, int) or balance < 0:
                        raise LedgerSyncError(f"初始余额必须为非负整数: {acct}={balance!r}")
                    conn.execute(
                        "INSERT INTO accounts(id, balance) VALUES (?, ?)", (acct, balance)
                    )
                conn.execute("INSERT INTO meta(key, value) VALUES ('seeded', '1')")

    def close(self) -> None:
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()

    def __enter__(self) -> "LedgerSync":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 只读视图
    # ------------------------------------------------------------------
    def inspect(self) -> dict:
        """只读查看账户余额及批次逐项状态。"""
        accounts: dict[str, dict[str, int]] = {}
        batches: list[dict] = []
        for name, conn in self._conns.items():
            accounts[name] = {
                row[0]: row[1]
                for row in conn.execute("SELECT id, balance FROM accounts ORDER BY id")
            }
            tx_status = {
                row[0]: (row[1], row[2])
                for row in conn.execute("SELECT tx_id, status, reason FROM txs")
            }
            for batch_id, source, items_json, next_index, status in conn.execute(
                "SELECT batch_id, source, items_json, next_index, status "
                "FROM batches ORDER BY rowid"
            ):
                items = []
                for index, raw in enumerate(json.loads(items_json)):
                    tx_id = raw["tx_id"]
                    tx_status_row = tx_status.get(tx_id)
                    if tx_status_row is not None and tx_status_row[0] in ("success", "failed"):
                        item_status, reason = tx_status_row
                    elif index < next_index:
                        item_status, reason = "success", None
                    else:
                        item_status, reason = "pending", None
                    items.append(
                        {
                            "tx_id": tx_id,
                            "to": raw["to"],
                            "amount": raw["amount"],
                            "status": item_status,
                            "reason": reason,
                        }
                    )
                batches.append(
                    {
                        "branch": name,
                        "batch_id": batch_id,
                        "source": source,
                        "status": status,
                        "next_index": next_index,
                        "items": items,
                    }
                )
        return {"accounts": accounts, "batches": batches}

    # ------------------------------------------------------------------
    # 批次接收、执行与恢复
    # ------------------------------------------------------------------
    def _branch_of(self, account: str) -> Optional[str]:
        for name, conn in self._conns.items():
            row = conn.execute(
                "SELECT 1 FROM accounts WHERE id = ?", (account,)
            ).fetchone()
            if row is not None:
                return name
        return None

    def sync(self, batch_id: str, source: str, items: Iterable[ItemLike]) -> list[ItemResult]:
        """持久接收并有序执行一个批次，返回逐项结果。

        相同 batch_id 重试同一完整有序请求时，继续原批次或返回已有结果；
        tx_id 级别的重试返回首次结果，不重复记账。
        """
        normalized = [_normalize_item(raw) for raw in items]
        src_branch = self._branch_of(source)
        if src_branch is None:
            raise LedgerSyncError(f"源账户不存在: {source!r}")
        for item in normalized:
            dst_branch = self._branch_of(item.to)
            if dst_branch is None:
                raise LedgerSyncError(f"收款账户不存在: {item.to!r}")
            if dst_branch == src_branch:
                raise LedgerSyncError(f"仅支持跨网点转账: {source!r} -> {item.to!r}")

        items_json = json.dumps(
            [{"tx_id": i.tx_id, "to": i.to, "amount": i.amount} for i in normalized],
            ensure_ascii=False,
        )
        conn = self._conns[src_branch]
        # 产生任何账务效果前，完整批次先持久化。
        with conn:
            row = conn.execute(
                "SELECT source, items_json FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO batches(batch_id, source, items_json, next_index, status) "
                    "VALUES (?, ?, ?, 0, 'open')",
                    (batch_id, source, items_json),
                )
            elif row[0] != source or row[1] != items_json:
                raise BatchConflictError(f"batch_id 已存在且内容不同: {batch_id!r}")
        self._fire("batch_persisted")

        self._run_batch(src_branch, batch_id)
        return self._batch_results(src_branch, batch_id)

    def recover(self) -> list[str]:
        """重新打开账本后，续做所有已持久接收但未终结的批次。返回恢复的 batch_id。"""
        resumed: list[str] = []
        for name, conn in self._conns.items():
            open_batches = [
                row[0]
                for row in conn.execute(
                    "SELECT batch_id FROM batches WHERE status = 'open' ORDER BY rowid"
                )
            ]
            for batch_id in open_batches:
                self._run_batch(name, batch_id)
                resumed.append(batch_id)
        return resumed

    # ------------------------------------------------------------------
    # 内部：批次推进与逐项两阶段执行
    # ------------------------------------------------------------------
    def _fire(self, event: str) -> None:
        if self._hook is not None:
            self._hook(event)

    def _run_batch(self, src_branch: str, batch_id: str) -> None:
        conn = self._conns[src_branch]
        row = conn.execute(
            "SELECT source, items_json, next_index, status FROM batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        if row is None:
            raise LedgerSyncError(f"批次不存在: {batch_id!r}")
        source, items_json, next_index, status = row
        if status == "done":
            return
        items = [TransferItem(raw["tx_id"], raw["to"], raw["amount"]) for raw in json.loads(items_json)]
        for index in range(next_index, len(items)):
            self._execute_item(src_branch, batch_id, source, index, items[index], len(items))

    def _execute_item(
        self,
        src_branch: str,
        batch_id: str,
        source: str,
        index: int,
        item: TransferItem,
        batch_len: int,
    ) -> None:
        src = self._conns[src_branch]
        dst_branch = self._branch_of(item.to)
        assert dst_branch is not None
        dst = self._conns[dst_branch]

        existing = src.execute(
            "SELECT source, dest, amount, status FROM txs WHERE tx_id = ?", (item.tx_id,)
        ).fetchone()
        if existing is not None:
            if existing[0] != source or existing[1] != item.to or existing[2] != item.amount:
                raise TxConflictError(f"tx_id 已存在且参数不同: {item.tx_id!r}")
            if existing[3] in ("success", "failed"):
                # 已终结（含换批次重传）：只推进游标，不重复记账。
                self._finalize_item(src, batch_id, index, batch_len)
                return
            # status == 'pending'：中断恢复，续做目标侧入账。
        else:
            # 阶段一：源网点事务内扣款 + 源分录 + txs 落盘，一次提交。
            with src:
                cursor = src.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?",
                    (item.amount, source, item.amount),
                )
                sufficient = cursor.rowcount == 1
                if sufficient:
                    src.execute(
                        "INSERT INTO entries(tx_id, batch_id, account, delta) VALUES (?, ?, ?, ?)",
                        (item.tx_id, batch_id, source, -item.amount),
                    )
                    src.execute(
                        "INSERT INTO txs(tx_id, batch_id, source, dest, amount, status) "
                        "VALUES (?, ?, ?, ?, ?, 'pending')",
                        (item.tx_id, batch_id, source, item.to, item.amount),
                    )
                else:
                    src.execute(
                        "INSERT INTO txs(tx_id, batch_id, source, dest, amount, status, reason) "
                        "VALUES (?, ?, ?, ?, ?, 'failed', 'insufficient_funds')",
                        (item.tx_id, batch_id, source, item.to, item.amount),
                    )
                    self._advance_cursor(src, batch_id, index, batch_len)
            if not sufficient:
                self._fire("item_finalized")
                return
            self._fire("source_committed")

        # 阶段二：目标网点事务内 inbox 去重 + 入账 + 目标分录，幂等。
        with dst:
            cursor = dst.execute(
                "INSERT OR IGNORE INTO inbox(tx_id, batch_id, source, amount) VALUES (?, ?, ?, ?)",
                (item.tx_id, batch_id, source, item.amount),
            )
            if cursor.rowcount == 1:
                dst.execute(
                    "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                    (item.amount, item.to),
                )
                dst.execute(
                    "INSERT INTO entries(tx_id, batch_id, account, delta) VALUES (?, ?, ?, ?)",
                    (item.tx_id, batch_id, item.to, item.amount),
                )
        self._fire("dest_committed")

        # 阶段三：源网点事务内终态 + 推进游标，一次提交。
        with src:
            src.execute(
                "UPDATE txs SET status = 'success' WHERE tx_id = ? AND status = 'pending'",
                (item.tx_id,),
            )
            self._advance_cursor(src, batch_id, index, batch_len)
        self._fire("item_finalized")

    def _advance_cursor(
        self, conn: sqlite3.Connection, batch_id: str, index: int, batch_len: int
    ) -> None:
        new_index = index + 1
        new_status = "done" if new_index >= batch_len else "open"
        conn.execute(
            "UPDATE batches SET next_index = ?, status = ? WHERE batch_id = ?",
            (new_index, new_status, batch_id),
        )

    def _finalize_item(
        self, conn: sqlite3.Connection, batch_id: str, index: int, batch_len: int
    ) -> None:
        with conn:
            self._advance_cursor(conn, batch_id, index, batch_len)
        self._fire("item_finalized")

    def _batch_results(self, src_branch: str, batch_id: str) -> list[ItemResult]:
        conn = self._conns[src_branch]
        items_json = conn.execute(
            "SELECT items_json FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()[0]
        results = []
        for raw in json.loads(items_json):
            row = conn.execute(
                "SELECT status, reason FROM txs WHERE tx_id = ?", (raw["tx_id"],)
            ).fetchone()
            results.append(ItemResult(tx_id=raw["tx_id"], status=row[0], reason=row[1]))
        return results
