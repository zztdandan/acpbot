"""ACP progress 文本分支聚合能力。"""

from __future__ import annotations

import asyncio
from typing import Any


class _ProgressTextMixin:
    """封装 progress 文本缓冲与死手刷出逻辑。"""

    _text_parts: list[str]
    _text_idle_task: asyncio.Task[Any] | None
    _lock: asyncio.Lock
    _idle_seconds: float

    def _append_text_locked(self, *, content: str) -> None:
        """向文本缓冲追加分片。"""
        self._text_parts.append(content)

    def _snapshot_text_locked(self) -> str | None:
        """提取文本缓冲快照；为空时返回 None。"""
        merged = "".join(self._text_parts).strip()
        return merged if merged else None

    def _clear_text_locked(self) -> None:
        """清理文本缓冲。"""
        self._text_parts.clear()

    def _arm_text_idle_timer_locked(self) -> None:
        """重置文本死手。"""
        if self._text_idle_task is not None:
            self._text_idle_task.cancel()
        self._text_idle_task = asyncio.create_task(self._text_idle_flush_worker())

    async def _publish_text_locked(self) -> None:
        payload = self._snapshot_text_locked()
        if payload is None:
            return
        #  发布放在锁内串行执行，避免并发 on_progress 场景下顺序颠倒。
        #  使用 shield 确保 close() 取消 idle 任务时不会中断正在进行的真实发布。
        publish_task = asyncio.ensure_future(self._publish(payload, False))  # type: ignore[attr-defined]
        try:
            await asyncio.shield(publish_task)
        except asyncio.CancelledError:
            await publish_task
            raise
        self._clear_text_locked()

    async def _text_idle_flush_worker(self) -> None:
        try:
            await asyncio.sleep(self._idle_seconds)
            async with self._lock:
                await self._publish_text_locked()
                self._text_idle_task = None
        except asyncio.CancelledError:
            return
