"""状态处理器协议定义。"""

from __future__ import annotations

from typing import Protocol


class StateHandler(Protocol):
    """状态处理器最小行为约束。"""

    def consume(self, payload: object) -> None:
        """消费输入并将事实写入对应状态池。"""
        ...
