# 离线转账批次同步库

仅用 Python 标准库 + SQLite 实现的跨网点离线转账批次同步。两个网点各有独立的
SQLite 账本文件，账户只属于一个网点，只处理跨网点转账，金额为正整数分。

## 结构

- `transfer_sync/ledger.py` — 单网点 SQLite 账本：账户、分录、批次、交易结果
- `transfer_sync/service.py` — `SyncService`：`sync` / `recover` / `inspect`
- `transfer_sync/errors.py` — `ConflictError` / `UnknownAccountError` 等异常
- `tests/test_transfer_sync.py` — 自动化测试（标准库 unittest）

## 使用

```python
from transfer_sync import SyncService

svc = SyncService({"S1": "site1.db", "S2": "site2.db"})
svc.init_site("S1", {"A": 10000})   # 首次初始化：账户及非负余额（分）
svc.init_site("S2", {"B": 0})

# 持久接收并执行有序批次；当前项终结后才执行下一项，失败项不阻断后项
results = svc.sync("batch-1", "A", [
    {"tx_id": "t1", "dest": "B", "amount": 8000},
    {"tx_id": "t2", "dest": "B", "amount": 3000},
    {"tx_id": "t3", "dest": "B", "amount": 1000},
])
# -> t1 success, t2 failed（余额不足）, t3 success；源余额 1000

svc.recover()          # 重新打开账本后续跑未终结批次（不依赖客户端重传）
view = svc.inspect()   # 只读查看余额与批次逐项状态
svc.close()
```

## 持久化与记账规则

- 产生账务效果前，完整批次已持久保存到源网点账本（`batches` 表）。
- 每笔转账按「源借记分录 → 目标贷记分录 → 交易结果 → 批次进度」逐步独立
  提交；分录以 `tx_id` 唯一，重放幂等，中断后 `recover()` 续跑结果与无中断
  顺序执行一次相同。
- `tx_id` 全局唯一标识交易：同参数重试返回首次结果，不重复记账；成功与失败
  均不可改判；参数不一致的复用抛 `ConflictError`。
- 同 `batch_id` 重试同一完整有序请求时继续原批次或返回已有结果。
- 每笔成功转账在两个账本各有且仅有一条分录；余额非负、总额守恒、失败项无分录。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖：

- 源 10000 / 目标 0，依次转 8000、3000、1000：中间失败不阻断，最终源 1000；
  原批次重试与换批次重传不重复记账；反向补足资金后原失败项仍失败。
- 在借记落盘后、贷记落盘后、结果落盘后、进度推进后（sync 未返回）等持久化
  边界逐项注入崩溃，重新打开账本恢复并重试，验证双边分录恰好一次、余额守恒、
  批次顺序与首次结果不变。
