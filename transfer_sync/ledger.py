"""单个网点的 SQLite 账本封装。

每个网点一个独立的 SQLite 文件，包含四张表：
- accounts:    账户及非负余额
- entries:     持久分录（tx_id 唯一，delta 有符号，借负贷正）
- batches:     已持久接收的批次（有序项的 JSON、执行进度、是否终结）
- tx_results:  已终结交易的首次结果（成功/失败均不可改判）
"""
from __future__ import annotations

import json
import sqlite3
from typing import Dict, List, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    balance    INTEGER NOT NULL CHECK (balance >= 0)
);
CREATE TABLE IF NOT EXISTS entries (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id      TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    delta      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id    TEXT PRIMARY KEY,
    source      TEXT NOT NULL,
    items_json  TEXT NOT NULL,
    next_index  INTEGER NOT NULL,
    done        INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tx_results (
    tx_id    TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    source   TEXT NOT NULL,
    dest     TEXT NOT NULL,
    amount   INTEGER NOT NULL,
    status   TEXT NOT NULL CHECK (status IN ('success', 'failed'))
);
"""


class Ledger:
    """一个网点账本文件：账户、分录、批次与交易结果。"""

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- 账户 --
    def init_accounts(self, accounts: Dict[str, int]) -> None:
        for account_id, balance in accounts.items():
            if not isinstance(balance, int) or balance < 0:
                raise ValueError(f"初始余额必须为非负整数分: {account_id}={balance!r}")
        with self._conn:
            for account_id, balance in accounts.items():
                self._conn.execute(
                    "INSERT OR IGNORE INTO accounts(account_id, balance) VALUES (?, ?)",
                    (account_id, balance),
                )

    def has_account(self, account_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)
        ).fetchone()
        return row is not None

    def get_balance(self, account_id: str) -> int:
        row = self._conn.execute(
            "SELECT balance FROM accounts WHERE account_id = ?", (account_id,)
        ).fetchone()
        if row is None:
            raise KeyError(account_id)
        return row["balance"]

    def list_accounts(self) -> Dict[str, int]:
        rows = self._conn.execute(
            "SELECT account_id, balance FROM accounts ORDER BY account_id"
        ).fetchall()
        return {row["account_id"]: row["balance"] for row in rows}

    # -- 分录 --
    def has_entry(self, tx_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM entries WHERE tx_id = ?", (tx_id,)
        ).fetchone()
        return row is not None

    def post_entry(self, tx_id: str, account_id: str, delta: int) -> None:
        """落一条分录并更新余额（同一事务提交）。

        tx_id 已存在分录时为空操作（幂等），保证恢复重放不重复记账。
        """
        with self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO entries(tx_id, account_id, delta) VALUES (?, ?, ?)",
                (tx_id, account_id, delta),
            )
            if cur.rowcount == 0:
                return
            new_balance = self.get_balance(account_id) + delta
            if new_balance < 0:
                raise ValueError(f"余额不足: {account_id}")
            self._conn.execute(
                "UPDATE accounts SET balance = ? WHERE account_id = ?",
                (new_balance, account_id),
            )

    def entry_count(self, tx_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM entries WHERE tx_id = ?", (tx_id,)
        ).fetchone()
        return row["n"]

    def list_entries(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT tx_id, account_id, delta FROM entries ORDER BY id"
        ).fetchall()
        return [dict(row) for row in rows]

    # -- 批次与交易结果 --
    def save_batch(self, batch_id: str, source: str, items: List[dict]) -> None:
        """完整批次在产生任何账务效果前持久保存。"""
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO batches(batch_id, source, items_json, next_index, done)"
                " VALUES (?, ?, ?, 0, 0)",
                (batch_id, source, json.dumps(items, ensure_ascii=False)),
            )

    def get_batch(self, batch_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return self._row_to_batch(row) if row else None

    def pending_batches(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM batches WHERE done = 0 ORDER BY rowid"
        ).fetchall()
        return [self._row_to_batch(row) for row in rows]

    def list_batches(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM batches ORDER BY rowid"
        ).fetchall()
        return [self._row_to_batch(row) for row in rows]

    def advance_batch(self, batch_id: str, next_index: int, done: bool) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE batches SET next_index = ?, done = ? WHERE batch_id = ?",
                (next_index, 1 if done else 0, batch_id),
            )

    def get_tx_result(self, tx_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM tx_results WHERE tx_id = ?", (tx_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_tx_results(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM tx_results ORDER BY rowid"
        ).fetchall()
        return [dict(row) for row in rows]

    def save_tx_result(self, tx_id: str, batch_id: str, source: str,
                       dest: str, amount: int, status: str) -> None:
        """记录交易首次结果；已存在则不改判（幂等）。"""
        with self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO tx_results(tx_id, batch_id, source, dest, amount, status)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (tx_id, batch_id, source, dest, amount, status),
            )

    @staticmethod
    def _row_to_batch(row: sqlite3.Row) -> dict:
        return {
            "batch_id": row["batch_id"],
            "source": row["source"],
            "items": json.loads(row["items_json"]),
            "next_index": row["next_index"],
            "done": bool(row["done"]),
        }
