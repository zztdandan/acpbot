"""结构化观测事件记录层。"""

from __future__ import annotations

import asyncio
from pathlib import Path

from nanobot.acp.observability.audit import (
    LOCAL_FILE_AUDIT_BACKEND,
    initialize_audit_backend,
    write_audit_event,
)
from nanobot.acp.observability.logging import emit_logging_event
from nanobot.acp.observability.queue import ObservabilityEvent, ObservabilityQueue


class ObservabilityManager:
    """观测事件管理器：串行消费队列并把事件分发到 audit/logging 双通道。

    职责：
        - 持有 `ObservabilityQueue` 作为唯一事件缓冲
        - 管理消费协程生命周期（start/stop）
        - 对每条事件执行固定分发：先 audit，再 logging
    """

    def __init__(
        self,
        *,
        queue: ObservabilityQueue | None = None,
        audit_backend: str = LOCAL_FILE_AUDIT_BACKEND,
        audit_base_dir: Path | None = None,
    ) -> None:
        """初始化观测管理器；未传 queue 时创建默认内存队列。"""
        self.queue = queue or ObservabilityQueue()
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None
        self._audit_backend = audit_backend
        self._audit_base_dir = audit_base_dir
        self._audit_run_dir: Path | None = None

    async def start(self) -> None:
        """启动观测消费循环；重复调用保持幂等不重复起协程。"""
        if self._running:
            return
        if self._audit_run_dir is None and self._audit_base_dir is not None:
            self._audit_run_dir = initialize_audit_backend(
                backend=self._audit_backend,
                base_dir=self._audit_base_dir,
            )
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
        """推送一条结构化事件到观测队列；业务层只负责写入不直接落日志。"""
        await self.queue.push(event)

    async def _consume_loop(self) -> None:
        """持续消费观测队列并执行双通道分发，直到收到 stop 信号。"""
        while self._running:
            event = await self.queue.consume()
            write_audit_event(
                event,
                backend=self._audit_backend,
                run_dir=self._audit_run_dir,
            )
            emit_logging_event(event)
