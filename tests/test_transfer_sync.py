"""离线转账批次同步库的自动化测试（仅标准库 unittest）。"""
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfer_sync import ConflictError, SyncService  # noqa: E402


class CrashInjected(Exception):
    """模拟进程在持久化边界崩溃。"""


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.paths = {
            "S1": os.path.join(self.dir, "site1.db"),
            "S2": os.path.join(self.dir, "site2.db"),
        }
        self.services = []

    def open_service(self, crash_hook=None):
        svc = SyncService(self.paths, crash_hook=crash_hook)
        self.services.append(svc)
        return svc

    def close_all(self):
        for svc in self.services:
            svc.close()
        self.services.clear()

    def tearDown(self):
        self.close_all()

    def init_accounts(self, svc, source=10000, dest=0):
        svc.init_site("S1", {"A": source})
        svc.init_site("S2", {"B": dest})

    def balances(self, svc):
        accounts = svc.inspect()["accounts"]
        return accounts["S1"]["A"], accounts["S2"]["B"]

    def entry_counts(self, svc, tx_id):
        return [ledger.entry_count(tx_id) for ledger in svc._ledgers.values()]


ITEMS = [
    {"tx_id": "t1", "dest": "B", "amount": 8000},
    {"tx_id": "t2", "dest": "B", "amount": 3000},
    {"tx_id": "t3", "dest": "B", "amount": 1000},
]
EXPECTED = ["success", "failed", "success"]


class TestBasicBatch(BaseCase):
    """场景一：中间失败不阻断后项；重试不重复记账；补足资金后原失败不改判。"""

    def test_partial_failure_and_idempotent_retry(self):
        svc = self.open_service()
        self.init_accounts(svc)

        results = svc.sync("b1", "A", ITEMS)
        self.assertEqual([r["status"] for r in results], EXPECTED)
        # 中间失败不阻断后项，最终源余额 1000
        self.assertEqual(self.balances(svc), (1000, 9000))

        # 重试原批次（同 batch_id 同请求）：返回已有结果，不重复执行
        again = svc.sync("b1", "A", ITEMS)
        self.assertEqual(again, results)
        self.assertEqual(self.balances(svc), (1000, 9000))

        # 换批次重传相同交易：tx_id 唯一，返回首次结果，不重复记账
        replay = svc.sync("b2", "A", ITEMS)
        self.assertEqual([r["status"] for r in replay], EXPECTED)
        self.assertEqual(self.balances(svc), (1000, 9000))
        for tx_id in ("t1", "t3"):
            self.assertEqual(self.entry_counts(svc, tx_id), [1, 1])
        self.assertEqual(sum(self.entry_counts(svc, "t2")), 0)

        # 反向转账补足源资金
        back = svc.sync("b3", "B", [{"tx_id": "t4", "dest": "A", "amount": 9000}])
        self.assertEqual(back[0]["status"], "success")
        self.assertEqual(self.balances(svc), (10000, 0))

        # 原失败项仍失败：已确定结果不可改判
        retry_failed = svc.sync("b4", "A",
                                [{"tx_id": "t2", "dest": "B", "amount": 3000}])
        self.assertEqual(retry_failed[0]["status"], "failed")
        self.assertEqual(self.balances(svc), (10000, 0))

    def test_conflicting_reuse_rejected(self):
        svc = self.open_service()
        self.init_accounts(svc)
        svc.sync("b1", "A", ITEMS)
        # 同 batch_id 不同请求
        with self.assertRaises(ConflictError):
            svc.sync("b1", "A", [{"tx_id": "x", "dest": "B", "amount": 1}])
        # 同 tx_id 不同金额
        with self.assertRaises(ConflictError):
            svc.sync("b5", "A", [{"tx_id": "t1", "dest": "B", "amount": 1}])

    def test_inspect_readonly(self):
        svc = self.open_service()
        self.init_accounts(svc)
        svc.sync("b1", "A", ITEMS)
        before = self.balances(svc)
        view = svc.inspect()
        batch = next(b for b in view["batches"] if b["batch_id"] == "b1")
        self.assertTrue(batch["done"])
        self.assertEqual([i["status"] for i in batch["items"]], EXPECTED)
        self.assertEqual(self.balances(svc), before)


class TestCrashRecovery(BaseCase):
    """场景二：在持久化边界中断，恢复后双边分录恰好一次、守恒、结果不变。"""

    def _crash_hook(self, checkpoint, state, fire_on):
        """在第 fire_on 次经过 checkpoint 时模拟崩溃。"""
        def hook(name):
            if name == checkpoint:
                state["seen"] += 1
                if state["seen"] == fire_on:
                    raise CrashInjected(checkpoint)
        return hook

    def _run_crash_case(self, checkpoint, fire_on=1):
        state = {"seen": 0}
        svc = self.open_service(crash_hook=self._crash_hook(checkpoint, state, fire_on))
        self.init_accounts(svc)
        with self.assertRaises(CrashInjected):
            svc.sync("b1", "A", ITEMS)
        self.close_all()  # 模拟进程退出后重新打开账本

        # 重新打开账本并恢复（不依赖客户端重传）
        svc2 = self.open_service()
        recovered = svc2.recover()
        # 崩溃若发生在最后一项进度推进之后，批次已终结，无需恢复
        self.assertLessEqual(len(recovered), 1)
        if recovered:
            self.assertEqual(recovered[0]["batch_id"], "b1")
            self.assertEqual([r["status"] for r in recovered[0]["results"]],
                             EXPECTED)

        # 恢复后余额与无中断顺序执行一致，总额守恒
        self.assertEqual(self.balances(svc2), (1000, 9000))

        # 恢复后重试原批次：返回首次结果，不重复执行
        retry = svc2.sync("b1", "A", ITEMS)
        self.assertEqual([r["status"] for r in retry], EXPECTED)
        self.assertEqual(self.balances(svc2), (1000, 9000))

        # 每笔成功交易在两个账本中各有且仅有一条分录；失败项无分录
        for tx_id in ("t1", "t3"):
            self.assertEqual(self.entry_counts(svc2, tx_id), [1, 1])
        self.assertEqual(sum(self.entry_counts(svc2, "t2")), 0)

        # 批次顺序与逐项状态正确
        view = svc2.inspect()
        batch = next(b for b in view["batches"] if b["batch_id"] == "b1")
        self.assertTrue(batch["done"])
        self.assertEqual([i["tx_id"] for i in batch["items"]],
                         ["t1", "t2", "t3"])
        self.assertEqual([i["status"] for i in batch["items"]], EXPECTED)

    def test_crash_after_debit_before_credit(self):
        # 源分录已落盘而目标未入账
        self._run_crash_case("after_debit_commit")

    def test_crash_after_credit_before_result(self):
        # 两侧分录已落盘但交易结果未记录
        self._run_crash_case("after_credit_commit")

    def test_crash_after_result_before_advance(self):
        # 结果已落盘但批次进度未推进
        self._run_crash_case("after_result_commit")

    def test_crash_after_advance_before_return(self):
        # 两侧已落盘、进度已推进，但 sync 未返回
        self._run_crash_case("after_advance_commit")

    def test_crash_at_every_item_and_checkpoint(self):
        # 每一项的每个持久化边界逐一崩溃并恢复，最终结果恒定。
        # 失败项 t2 无分录，因此 debit/credit 检查点只在第 1、3 项出现。
        cases = {
            "after_debit_commit": (1, 2),
            "after_credit_commit": (1, 2),
            "after_result_commit": (1, 2, 3),
            "after_advance_commit": (1, 2, 3),
        }
        for checkpoint, fire_ons in cases.items():
            for fire_on in fire_ons:
                with self.subTest(checkpoint=checkpoint, occurrence=fire_on):
                    self.setUp()  # 每个子场景使用全新账本目录
                    self._run_crash_case(checkpoint, fire_on)
                    self.close_all()


class TestConservation(BaseCase):
    """总额守恒与余额非负。"""

    def test_conservation_across_batches(self):
        svc = self.open_service()
        svc.init_site("S1", {"A": 5000})
        svc.init_site("S2", {"B": 7000})
        total_before = sum(sum(v.values())
                           for v in svc.inspect()["accounts"].values())

        svc.sync("b1", "A", [{"tx_id": "t1", "dest": "B", "amount": 2000}])
        svc.sync("b2", "B", [{"tx_id": "t2", "dest": "A", "amount": 1500}])
        svc.sync("b3", "A", [{"tx_id": "t3", "dest": "B", "amount": 99999}])

        accounts = svc.inspect()["accounts"]
        total_after = sum(sum(v.values()) for v in accounts.values())
        self.assertEqual(total_before, total_after)
        for site_accounts in accounts.values():
            for balance in site_accounts.values():
                self.assertGreaterEqual(balance, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
