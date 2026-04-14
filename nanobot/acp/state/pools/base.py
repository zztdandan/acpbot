"""池抽象基类：统一封装接收、死手刷新与终态约束。"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod

from nanobot.acp.state.models import ACPBucketType, FlushResult


class ACPPoolBase(ABC):
    """单请求池基类：池自己维护死手时间，router 只负责调度 timeout handle。"""

    bucket_type: ACPBucketType
    idle_timeout_seconds: float | None = None

    def __init__(self, *, bucket_key: str) -> None:
        self.bucket_key = bucket_key
        self._terminal = False
        self.deadline_monotonic: float | None = None

    def accept(self, payload: object) -> None:
        if self._terminal:
            return
        consumed = self._accept(payload)
        if consumed:
            self._refresh_deadline()

    @abstractmethod
    def _accept(self, payload: object) -> bool:
        """处理单条输入并更新池内事实；返回是否实际消费该输入。"""

    @abstractmethod
    def flush(self) -> FlushResult | None:
        """提取当前可发布片段；返回空值表示当前没有可见增量。"""

    def is_terminal(self) -> bool:
        return self._terminal

    def mark_terminal(self) -> None:
        self._terminal = True

    def _refresh_deadline(self) -> None:
        if self.idle_timeout_seconds is None:
            self.deadline_monotonic = None
            return
        self.deadline_monotonic = time.monotonic() + self.idle_timeout_seconds
