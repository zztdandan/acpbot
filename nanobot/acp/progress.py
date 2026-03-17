"""ACP progress 聚合工具。

本模块只负责流式进度聚合，不关心业务分发与 ACP 协议细节。
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine, Literal


class _ProgressAccumulator:
    """按类型聚合 progress/tool-hint，并支持 2 秒死手机制。"""

    def __init__(
        self,
        *,
        idle_seconds: float,
        publish: Callable[[str, bool], Coroutine[Any, Any, None]],
    ) -> None:
        self._idle_seconds = idle_seconds
        self._publish = publish
        self._kind: Literal["text", "tool"] | None = None
        self._text_parts: list[str] = []
        self._tool_hints: list[str] = []
        self._idle_task: asyncio.Task[Any] | None = None
        self._lock = asyncio.Lock()

    async def on_progress(self, content: str, *, tool_hint: bool = False) -> None:
        """收到新进度后，按规则进行类型切换 flush 和死手机制定时。"""
        if not content:
            return

        async with self._lock:
            incoming_kind: Literal["text", "tool"] = "tool" if tool_hint else "text"
            # 中文注释：当类型从 text 切到 tool（或反之）时，必须立刻上抛前一段积累内容。
            if self._kind is not None and self._kind != incoming_kind:
                await self._publish_current_locked()

            self._append_locked(content=content, tool_hint=tool_hint)
            self._arm_idle_timer_locked()

    async def flush(self) -> None:
        """立即 flush 当前缓存。"""
        async with self._lock:
            await self._publish_current_locked()

    async def close(self) -> None:
        """结束本轮会话前，取消定时任务并 flush 遗留内容。"""
        idle_task: asyncio.Task[Any] | None = None
        async with self._lock:
            idle_task = self._idle_task
            self._idle_task = None
        if idle_task is not None:
            idle_task.cancel()
            try:
                await idle_task
            except asyncio.CancelledError:
                pass
        await self.flush()

    def _append_locked(self, *, content: str, tool_hint: bool) -> None:
        self._kind = "tool" if tool_hint else "text"
        if tool_hint:
            self._tool_hints.append(content)
            return
        self._text_parts.append(content)

    def _snapshot_locked(self) -> tuple[str, bool] | None:
        if self._kind == "text":
            merged = "".join(self._text_parts).strip()
            return (merged, False) if merged else None
        if self._kind == "tool":
            # 中文注释：bus 只支持单条字符串，这里将连续 tool hint 聚合后一次性发送。
            merged = "\n".join(part.strip() for part in self._tool_hints if part and part.strip())
            return (merged, True) if merged else None
        return None

    def _clear_locked(self) -> None:
        self._kind = None
        self._text_parts.clear()
        self._tool_hints.clear()

    def _arm_idle_timer_locked(self) -> None:
        if self._idle_task is not None:
            self._idle_task.cancel()
        self._idle_task = asyncio.create_task(self._idle_flush_worker())

    async def _publish_current_locked(self) -> None:
        payload = self._snapshot_locked()
        if payload is None:
            return
        # 中文注释：发布放在锁内串行执行，避免并发 on_progress 场景下顺序颠倒。
        # 中文注释：使用 shield 确保 close() 取消 idle 任务时不会中断正在进行的真实发布。
        publish_task = asyncio.ensure_future(self._publish(payload[0], payload[1]))
        try:
            await asyncio.shield(publish_task)
        except asyncio.CancelledError:
            await publish_task
            raise
        self._clear_locked()

    async def _idle_flush_worker(self) -> None:
        try:
            await asyncio.sleep(self._idle_seconds)
            await self.flush()
        except asyncio.CancelledError:
            return
