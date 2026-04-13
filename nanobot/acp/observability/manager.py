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
        # Observability is not a temporary debug logger. It is the shared structured-
        # event owner for runtime, process, and state, so both direct and bus paths start it.
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
        # Business modules only push events here and stay unaware of persistence or
        # presentation details so audit/tooling sinks remain decoupled from owners.
        await self.queue.push(event)

    async def _consume_loop(self) -> None:
        while self._running:
            event = await self.queue.consume()
            write_audit_event(event)
            emit_tooling_event(event)
