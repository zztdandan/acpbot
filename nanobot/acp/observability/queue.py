"""观测事件队列模型。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap, ObservabilityEventName, ObservabilityScopeName


@dataclass(slots=True)
class ObservabilityEvent:
    """结构化观测事件。"""

    scope: ObservabilityScopeName
    # 事件归属域。
    event: ObservabilityEventName
    # 事件名称。
    request_key: str | None = None
    # 关联请求键。
    nanobot_side_session_key: str | None = None
    # 关联业务侧会话主键。
    acp_side_session_id: str | None = None
    # 关联协议侧会话标识。
    payload: JSONMap = field(default_factory=dict)
    # 扩展负载。


class ObservabilityQueue:
    """观测事件异步队列封装。"""

    def __init__(self) -> None:
        """初始化内部异步队列。"""
        self._queue: asyncio.Queue[ObservabilityEvent] = asyncio.Queue()

    async def push(self, event: ObservabilityEvent) -> None:
        """写入一条观测事件。"""
        await self._queue.put(event)

    async def consume(self) -> ObservabilityEvent:
        """消费并返回一条观测事件。"""
        return await self._queue.get()
