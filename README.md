# task26100488

离线跨网点转账批次同步库：仅 Python 标准库 + SQLite，无外部服务、无网络端口。

## 文件

- `ledger_sync.py` — 库实现（`LedgerSync`：`sync` / `recover` / `inspect`）
- `test_ledger_sync.py` — 自动化测试（功能场景 + 崩溃恢复场景）

## 运行测试

```bash
python3 -m unittest test_ledger_sync -v
```

## 使用示例

```python
from ledger_sync import LedgerSync

# 两个网点各一个 SQLite 账本文件；初始账户仅在首次初始化时生效
ls = LedgerSync(
    {"A": "branch_a.db", "B": "branch_b.db"},
    initial_accounts={"A": {"alice": 10000}, "B": {"bob": 0}},
)

# 有序批次：源账户 alice，逐项执行，失败项不阻断后项、不回滚整批
results = ls.sync("batch-1", "alice", [
    ("tx1", "bob", 8000),
    ("tx2", "bob", 3000),   # 余额不足 -> failed，继续后续项
    ("tx3", "bob", 1000),
])

ls.close()

# 重新打开账本后：不依赖客户端重传，续做未终结批次
ls = LedgerSync({"A": "branch_a.db", "B": "branch_b.db"})
ls.recover()
view = ls.inspect()   # 只读查看余额与批次逐项状态
ls.close()
```

## 持久化与恢复模型

1. 产生任何账务效果前，完整批次（有序条目）先持久化到源网点 `batches` 表。
2. 每项两阶段落盘：源网点事务内「扣款 + 源分录 + txs(pending)」一次提交；
   目标网点事务内「inbox 去重 + 入账 + 目标分录」一次提交（幂等）；
   最后源网点事务内「txs 置终态 + 推进批次游标」一次提交。
3. 任意边界中断后，`recover()` 依据已落盘状态幂等续做；
   已确定的成功/失败结果不可改判，`tx_id` 全局唯一标识交易，重试不重复记账。
