"""协调者的持久化预写日志（WAL）。

模拟真实系统里的 fsync 磁盘：领导者在发放租约前必须把新的栅栏令牌
计数和租约状态写进 WAL，写入完成前不返回成功。协调者进程重启或
更换领导者后，从 WAL 恢复，保证令牌计数永不回退。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class WalRecord:
    term: int
    token_counter: int
    leases: dict[str, dict[str, Any]]


class DurableWAL:
    """文件支持的 WAL；同目录文件由本类独占管理。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._latched: WalRecord | None = None
        if os.path.exists(path):
            self._load()

    # ---- 持久化原语 ------------------------------------------------
    def write(self, record: WalRecord) -> None:
        """一次 fsync 语义的原子写（临时文件 + rename）。"""
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "term": record.term,
                    "token_counter": record.token_counter,
                    "leases": record.leases,
                },
                f,
                ensure_ascii=False,
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        self._latched = record

    def _load(self) -> None:
        with open(self.path, encoding="utf-8") as f:
            data = json.load(f)
        self._latched = WalRecord(
            term=data["term"],
            token_counter=data["token_counter"],
            leases=data["leases"],
        )

    def latest(self) -> WalRecord | None:
        if self._latched is not None:
            return self._latched
        if os.path.exists(self.path):
            self._load()
        return self._latched


class InMemoryWAL:
    """不落地的 WAL，用于单元测试与临时演示。"""

    def __init__(self) -> None:
        self._record: WalRecord | None = None

    def write(self, record: WalRecord) -> None:
        self._record = record

    def latest(self) -> WalRecord | None:
        return self._record
