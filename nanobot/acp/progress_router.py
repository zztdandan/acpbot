"""ACP progress 多缓冲路由器。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from nanobot.acp.outbound_content_schema import (
    build_progress_media,
    build_progress_other,
    build_progress_text,
    build_progress_tool,
)
from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.progress_tool_payload_schema import append_history, build_tool_payload


@dataclass
class _ToolBucket:
    latest: dict[str, Any] | None = None
    history: list[dict[str, Any]] = field(default_factory=list)
    history_dropped: int = 0
    idle_task: asyncio.Task[Any] | None = None


class ProgressRouter:
    """按 family/route_key 做分桶缓冲并统一发布 outbound。"""

    TERMINAL_STATUSES = {"completed", "failed", "cancelled", "canceled", "timeout"}

    def __init__(
        self,
        *,
        text_idle_seconds: float,
        text_max_chars: int,
        tool_idle_seconds: float,
        tool_terminal_delay_seconds: float,
        other_idle_seconds: float,
        media_idle_seconds: float,
        publish: Callable[[str, dict[str, Any], str], Awaitable[None]],
    ) -> None:
        self._text_idle_seconds = text_idle_seconds
        self._text_max_chars = text_max_chars
        self._tool_idle_seconds = tool_idle_seconds
        self._tool_terminal_delay_seconds = tool_terminal_delay_seconds
        self._other_idle_seconds = other_idle_seconds
        self._media_idle_seconds = media_idle_seconds
        self._publish = publish

        self._text_parts: list[str] = []
        self._text_session_id: str | None = None
        self._text_route_key: str | None = None
        self._text_idle_task: asyncio.Task[Any] | None = None

        self._tool_buckets: dict[str, _ToolBucket] = {}
        self._other_buckets: dict[str, tuple[ACPProgressEvent, asyncio.Task[Any] | None]] = {}
        self._media_buckets: dict[str, tuple[ACPProgressEvent, asyncio.Task[Any] | None]] = {}
        self._lock = asyncio.Lock()

    async def on_progress_event(self, event: ACPProgressEvent) -> None:
        async with self._lock:
            if event.family == "user":
                # 中文注释：用户输入回放块不做 outbound progress，避免在真实链路提前打断测试收集窗口。
                return
            if event.family == "text":
                await self._on_text_locked(event)
                return
            if event.family == "tool":
                await self._on_tool_locked(event)
                return
            if event.family == "media":
                await self._on_media_locked(event)
                return
            await self._on_other_locked(event)

    async def close(self) -> None:
        async with self._lock:
            await self._flush_text_locked("close")
            for route_key in list(self._tool_buckets.keys()):
                await self._flush_tool_locked(route_key, "close")
            for route_key in list(self._other_buckets.keys()):
                await self._flush_other_locked(route_key, "close")
            for route_key in list(self._media_buckets.keys()):
                await self._flush_media_locked(route_key, "close")

    async def _on_text_locked(self, event: ACPProgressEvent) -> None:
        text = ((event.raw_json or {}).get("content") or {}).get("text")
        if not isinstance(text, str) or not text:
            return
        self._text_parts.append(text)
        self._text_session_id = event.session_id
        self._text_route_key = event.route_key
        if len("".join(self._text_parts)) >= self._text_max_chars:
            await self._flush_text_locked("size")
            return
        self._arm_text_timer_locked()

    async def _on_tool_locked(self, event: ACPProgressEvent) -> None:
        route_key = event.route_key
        bucket = self._tool_buckets.setdefault(route_key, _ToolBucket())
        row = dict(event.raw_json or {})
        row["update_type"] = event.update_type
        row["received_at_ms"] = event.received_at_ms
        row["session_id"] = event.session_id
        status = str((row.get("status") or "")).strip().lower()
        bucket.latest = row
        bucket.history_dropped += append_history(bucket.history, row)
        delay = (
            self._tool_terminal_delay_seconds
            if status in self.TERMINAL_STATUSES
            else self._tool_idle_seconds
        )
        if bucket.idle_task is not None:
            bucket.idle_task.cancel()
        bucket.idle_task = asyncio.create_task(self._tool_idle_flush(route_key, delay))

    async def _on_other_locked(self, event: ACPProgressEvent) -> None:
        route_key = event.route_key
        old = self._other_buckets.get(route_key)
        if old and old[1] is not None:
            old[1].cancel()
        task = asyncio.create_task(self._other_idle_flush(route_key, self._other_idle_seconds))
        self._other_buckets[route_key] = (event, task)

    async def _on_media_locked(self, event: ACPProgressEvent) -> None:
        route_key = event.route_key
        old = self._media_buckets.get(route_key)
        if old and old[1] is not None:
            old[1].cancel()
        task = asyncio.create_task(self._media_idle_flush(route_key, self._media_idle_seconds))
        self._media_buckets[route_key] = (event, task)

    def _arm_text_timer_locked(self) -> None:
        if self._text_idle_task is not None:
            self._text_idle_task.cancel()
        self._text_idle_task = asyncio.create_task(self._text_idle_flush())

    async def _text_idle_flush(self) -> None:
        try:
            await asyncio.sleep(self._text_idle_seconds)
            async with self._lock:
                await self._flush_text_locked("deadman")
        except asyncio.CancelledError:
            return

    async def _tool_idle_flush(self, route_key: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            async with self._lock:
                await self._flush_tool_locked(route_key, "deadman")
        except asyncio.CancelledError:
            return

    async def _other_idle_flush(self, route_key: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            async with self._lock:
                await self._flush_other_locked(route_key, "deadman")
        except asyncio.CancelledError:
            return

    async def _media_idle_flush(self, route_key: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            async with self._lock:
                await self._flush_media_locked(route_key, "deadman")
        except asyncio.CancelledError:
            return

    async def _flush_text_locked(self, reason: str) -> None:
        if not self._text_parts:
            return
        if self._text_idle_task is not None:
            self._text_idle_task.cancel()
            self._text_idle_task = None
        text = "".join(self._text_parts)
        self._text_parts.clear()
        session_id = self._text_session_id or ""
        route_key = self._text_route_key or session_id
        content, metadata = build_progress_text(
            text=text,
            session_id=session_id,
            route_key=route_key,
            flush_reason=reason,
        )
        await self._publish(content, metadata, "progress_text")

    async def _flush_tool_locked(self, route_key: str, reason: str) -> None:
        bucket = self._tool_buckets.pop(route_key, None)
        if bucket is None or bucket.latest is None:
            return
        if bucket.idle_task is not None:
            bucket.idle_task.cancel()
        latest = bucket.latest
        payload = build_tool_payload(
            route_key=route_key,
            latest=latest,
            history=bucket.history,
            history_dropped=bucket.history_dropped,
        )
        content, metadata = build_progress_tool(
            payload=payload,
            session_id=str(latest.get("sessionId") or latest.get("session_id") or ""),
            route_key=route_key,
            update_type=str(latest.get("update_type") or "ToolCallUpdate"),
            flush_reason=reason,
        )
        await self._publish(content, metadata, "progress_tool")

    async def _flush_other_locked(self, route_key: str, reason: str) -> None:
        bucket = self._other_buckets.pop(route_key, None)
        if bucket is None:
            return
        event, idle_task = bucket
        if idle_task is not None:
            idle_task.cancel()
        content, metadata = build_progress_other(
            payload=dict(event.raw_json or {}),
            session_id=event.session_id,
            route_key=event.route_key,
            update_type=event.update_type,
            flush_reason=reason,
        )
        await self._publish(content, metadata, "progress_other")

    async def _flush_media_locked(self, route_key: str, reason: str) -> None:
        bucket = self._media_buckets.pop(route_key, None)
        if bucket is None:
            return
        event, idle_task = bucket
        if idle_task is not None:
            idle_task.cancel()
        content, metadata = build_progress_media(
            payload=dict(event.raw_json or {}),
            session_id=event.session_id,
            route_key=event.route_key,
            update_type=event.update_type,
            flush_reason=reason,
        )
        await self._publish(content, metadata, "progress_media")
