"""ACP observability manager.

Runtime owns queue creation; business modules only push structured events here.
The manager consumes the queue and fan-outs to audit/tooling sinks.
"""

from __future__ import annotations

import asyncio

from nanobot.acp.observability.audit import write_audit_event
from nanobot.acp.observability.queue import ObservabilityEvent, ObservabilityQueue
from nanobot.acp.observability.tooling import emit_tooling_event


class ObservabilityManager:
    """Central observability sink used by runtime, process manager, and state."""

    def __init__(self, *, queue: ObservabilityQueue | None = None) -> None:
        self.queue = queue or ObservabilityQueue()
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._running:
            return
        # 中文注释：observability 不是调试时临时附加的 logger，
        # 而是 runtime/process/state 共用的结构化事件 owner，所以 direct/bus 两条路径都会主动启动它。
        self._running = True
        self._consumer_task = asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
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
        # 中文注释：业务模块只 push 事件，不关心落盘/展示细节；
        # 这样审计与工具化输出才能与业务 owner 解耦。
        await self.queue.push(event)

    async def _consume_loop(self) -> None:
        while self._running:
            event = await self.queue.consume()
            write_audit_event(event)
            emit_tooling_event(event)
