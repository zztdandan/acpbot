"""结构化观测事件记录层。"""

from __future__ import annotations

import asyncio

from nanobot.acp.observability.audit import write_audit_event
from nanobot.acp.observability.queue import ObservabilityEvent, ObservabilityQueue
from nanobot.acp.observability.tooling import emit_tooling_event


class ObservabilityManager:
    """负责对应领域状态与流程编排。"""

    def __init__(self, *, queue: ObservabilityQueue | None = None) -> None:
        """初始化当前对象并建立必要状态。"""
        self.queue = queue or ObservabilityQueue()
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        if self._running:
            return
        self._running = True
        self._consumer_task = asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
        """请求停止主循环。"""
        self._running = False
        task = self._consumer_task
        self._consumer_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def push(self, event: ObservabilityEvent) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        await self.queue.push(event)

    async def _consume_loop(self) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        while self._running:
            event = await self.queue.consume()
            write_audit_event(event)
            emit_tooling_event(event)
