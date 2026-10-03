"""ledger_sync 自动化测试。

运行：python3 -m unittest test_ledger_sync -v
"""
import os
import shutil
import sqlite3
import tempfile
import unittest

from ledger_sync import (
    BatchConflictError,
    LedgerSync,
    LedgerSyncError,
    TxConflictError,
)


class SimulatedCrash(Exception):
    """测试用：在持久化边界模拟进程宕机。"""


def crash_after(event_name):
    """返回一个 crash_hook：在指定事件首次触发时抛出 SimulatedCrash。"""
    state = {"fired": False}

    def hook(event):
        if event == event_name and not state["fired"]:
            state["fired"] = True
            raise SimulatedCrash(event)

    return hook


class LedgerSyncTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="ledger_sync_test_")
        self.paths = {"A": os.path.join(self.dir, "branch_a.db"),
                      "B": os.path.join(self.dir, "branch_b.db")}
        self.initial = {"A": {"alice": 10000}, "B": {"bob": 0}}

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def open_sync(self, crash_hook=None):
        return LedgerSync(self.paths, self.initial, crash_hook=crash_hook)

    # -- 直接读库断言 -------------------------------------------------
    def entries_of(self, branch):
        conn = sqlite3.connect(self.paths[branch])
        try:
            return conn.execute(
                "SELECT tx_id, account, delta FROM entries ORDER BY rowid"
            ).fetchall()
        finally:
            conn.close()

    def total_balance(self):
        total = 0
        for path in self.paths.values():
            conn = sqlite3.connect(path)
            try:
                total += conn.execute("SELECT COALESCE(SUM(balance),0) FROM accounts").fetchone()[0]
            finally:
                conn.close()
        return total


class FunctionalTest(LedgerSyncTestBase):
    def test_partial_failure_retry_and_final_failure(self):
        ls = self.open_sync()
        items = [("tx1", "bob", 8000), ("tx2", "bob", 3000), ("tx3", "bob", 1000)]
        results = ls.sync("batch-1", "alice", items)
        # 中间失败不阻断后项
        self.assertEqual([r.status for r in results], ["success", "failed", "success"])
        self.assertEqual(results[1].reason, "insufficient_funds")
        view = ls.inspect()
        self.assertEqual(view["accounts"]["A"]["alice"], 1000)
        self.assertEqual(view["accounts"]["B"]["bob"], 9000)
        ls.close()

        # 重试原批次：返回已有结果，不重复记账
        ls = self.open_sync()
        again = ls.sync("batch-1", "alice", items)
        self.assertEqual([r.status for r in again], ["success", "failed", "success"])
        self.assertEqual(ls.inspect()["accounts"]["A"]["alice"], 1000)
        self.assertEqual(ls.inspect()["accounts"]["B"]["bob"], 9000)

        # 换批次重传同样的交易：tx_id 级幂等，不重复记账
        again2 = ls.sync("batch-2", "alice", items)
        self.assertEqual([r.status for r in again2], ["success", "failed", "success"])
        self.assertEqual(ls.inspect()["accounts"]["A"]["alice"], 1000)
        self.assertEqual(ls.inspect()["accounts"]["B"]["bob"], 9000)
        self.assertEqual(len(self.entries_of("A")), 2)   # tx1, tx3 各一条借方分录
        self.assertEqual(len(self.entries_of("B")), 2)   # tx1, tx3 各一条贷方分录

        # 反向转账补足源资金
        back = ls.sync("batch-3", "bob", [("tx-back", "alice", 5000)])
        self.assertEqual(back[0].status, "success")
        self.assertEqual(ls.inspect()["accounts"]["A"]["alice"], 6000)

        # 源资金已足够，但原失败项首次结果不可改判
        retry_failed = ls.sync("batch-4", "alice", [("tx2", "bob", 3000)])
        self.assertEqual(retry_failed[0].status, "failed")
        self.assertEqual(ls.inspect()["accounts"]["A"]["alice"], 6000)
        self.assertEqual(ls.inspect()["accounts"]["B"]["bob"], 4000)
        ls.close()

    def test_validation_and_conflicts(self):
        ls = self.open_sync()
        with self.assertRaises(LedgerSyncError):
            ls.sync("b0", "alice", [("tx0", "alice", 100)])      # 同网点
        with self.assertRaises(LedgerSyncError):
            ls.sync("b0", "alice", [("tx0", "bob", 0)])          # 非正金额
        with self.assertRaises(LedgerSyncError):
            ls.sync("b0", "nobody", [("tx0", "bob", 1)])         # 源账户不存在
        ls.sync("b1", "alice", [("tx1", "bob", 100)])
        with self.assertRaises(BatchConflictError):
            ls.sync("b1", "alice", [("tx9", "bob", 100)])        # batch 内容不同
        with self.assertRaises(TxConflictError):
            ls.sync("b2", "alice", [("tx1", "bob", 200)])        # tx_id 参数不同
        # 冲突不影响已有记账
        self.assertEqual(ls.inspect()["accounts"]["A"]["alice"], 9900)
        ls.close()


class CrashRecoveryTest(LedgerSyncTestBase):
    ITEMS = [("tx1", "bob", 8000), ("tx2", "bob", 3000), ("tx3", "bob", 1000)]
    EXPECTED = ["success", "failed", "success"]

    def _crash_sync(self, event):
        """在指定持久化边界中断 sync，返回异常已被触发。"""
        ls = self.open_sync(crash_hook=crash_after(event))
        with self.assertRaises(SimulatedCrash):
            ls.sync("batch-1", "alice", self.ITEMS)
        ls.close()

    def _assert_recovered_invariants(self, ls):
        results = ls.sync("batch-1", "alice", self.ITEMS)  # 重试原批次
        self.assertEqual([r.status for r in results], self.EXPECTED)
        view = ls.inspect()
        self.assertEqual(view["accounts"]["A"]["alice"], 1000)
        self.assertEqual(view["accounts"]["B"]["bob"], 9000)
        # 每笔成功转账在两个账本中各有且仅有一条分录
        a_entries = self.entries_of("A")
        b_entries = self.entries_of("B")
        self.assertEqual(sorted(a_entries), [("tx1", "alice", -8000), ("tx3", "alice", -1000)])
        self.assertEqual(sorted(b_entries), [("tx1", "bob", 8000), ("tx3", "bob", 1000)])
        # 总额守恒
        self.assertEqual(self.total_balance(), 10000)
        # 批次顺序与逐项状态
        batch = view["batches"][0]
        self.assertEqual(batch["status"], "done")
        self.assertEqual([i["status"] for i in batch["items"]], self.EXPECTED)
        self.assertEqual([i["tx_id"] for i in batch["items"]], ["tx1", "tx2", "tx3"])

    def test_crash_after_batch_persisted(self):
        self._crash_sync("batch_persisted")
        ls = self.open_sync()
        self.assertEqual(ls.recover(), ["batch-1"])
        self._assert_recovered_invariants(ls)
        ls.close()

    def test_crash_after_source_committed(self):
        # 源分录已落盘而目标未入账
        self._crash_sync("source_committed")
        self.assertEqual(self.entries_of("A"), [("tx1", "alice", -8000)])
        self.assertEqual(self.entries_of("B"), [])
        ls = self.open_sync()
        self.assertEqual(ls.recover(), ["batch-1"])
        self._assert_recovered_invariants(ls)
        ls.close()

    def test_crash_after_dest_committed(self):
        # 两侧已落盘但 sync 未返回（终态未推进）
        self._crash_sync("dest_committed")
        self.assertEqual(self.entries_of("B"), [("tx1", "bob", 8000)])
        ls = self.open_sync()
        self.assertEqual(ls.recover(), ["batch-1"])
        self._assert_recovered_invariants(ls)
        ls.close()

    def test_crash_after_item_finalized(self):
        self._crash_sync("item_finalized")
        ls = self.open_sync()
        self.assertEqual(ls.recover(), ["batch-1"])
        self._assert_recovered_invariants(ls)
        ls.close()

    def test_recover_without_crash_is_noop(self):
        ls = self.open_sync()
        ls.sync("batch-1", "alice", self.ITEMS)
        self.assertEqual(ls.recover(), [])
        ls.close()
        ls = self.open_sync()
        self.assertEqual(ls.recover(), [])
        self._assert_recovered_invariants(ls)
        ls.close()


if __name__ == "__main__":
    unittest.main()
