from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Awaitable, Callable
from typing import cast

from nanobot.acp.outbound_content_schema import (
    build_progress_media,
    build_progress_text,
    build_progress_tool,
)
from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state.handlers.agent_message_media import AgentMessageMediaHandler
from nanobot.acp.state.handlers.agent_message_text import AgentMessageTextHandler
from nanobot.acp.state.handlers.base import ACPSessionHandler
from nanobot.acp.state.handlers.other import OtherHandler
from nanobot.acp.state.handlers.tool import ToolHandler, build_tool_payload
from nanobot.acp.state.handlers.user_message import UserMessageHandler
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import MediaPool, MessageTextPool, ToolPool
from nanobot.config.schema import ACPBackendConfig


class HandlerRegistry:
    """Own handler ordering, validation, and fallback resolution."""

    def __init__(
        self,
        *,
        handlers: list[ACPSessionHandler],
        fallback_handler: ACPSessionHandler,
    ) -> None:
        self._handlers_by_type: dict[ACPUpdateType, list[ACPSessionHandler]] = defaultdict(list)
        self._fallback_handler = fallback_handler

        for handler in handlers:
            if not getattr(handler, "auto_dispatch", True):
                continue
            for update_type in handler.supported_update_types:
                self._handlers_by_type[update_type].append(handler)

        for update_type, typed_handlers in self._handlers_by_type.items():
            if update_type != ACPUpdateType.AGENT_MESSAGE_CHUNK and len(typed_handlers) > 1:
                raise ValueError(f"Multiple formal handlers registered for {update_type.value}")

    def handlers_for(self, update_type: ACPUpdateType | str) -> list[ACPSessionHandler]:
        normalized = self._normalize_update_type(update_type)
        if normalized is None:
            return []
        return list(self._handlers_by_type.get(normalized, []))

    def resolve(self, event: ACPProgressEvent) -> ACPSessionHandler:
        candidates = self.handlers_for(event.update_type)
        if not candidates:
            return self._fallback_handler

        matches = [handler for handler in candidates if handler.match(event)]
        if not matches:
            return self._fallback_handler

        if len(matches) > 1:
            update_type = self._normalize_update_type(event.update_type)
            label = update_type.value if update_type is not None else str(event.update_type)
            raise ValueError(f"Multiple handlers matched {label}")

        return matches[0]

    def _normalize_update_type(self, update_type: ACPUpdateType | str) -> ACPUpdateType | None:
        if isinstance(update_type, ACPUpdateType):
            return update_type
        try:
            return ACPUpdateType(str(update_type))
        except ValueError:
            return None


class SessionStateRouter:
    """Thin wrapper that lets callers resolve handlers without embedding rules."""

    TERMINAL_STATUSES = {"completed", "failed", "cancelled", "canceled", "timeout"}

    def __init__(
        self,
        registry: HandlerRegistry,
        *,
        manager: SessionStateManager,
        publish: Callable[[str, dict[str, Any], str], Awaitable[None]],
        text_idle_seconds: float = 1.0,
        text_max_chars: int = 2048,
        media_idle_seconds: float = 0.2,
        tool_idle_seconds: float = 300.0,
        tool_terminal_delay_seconds: float = 1.5,
    ) -> None:
        self._registry = registry
        self._manager = manager
        self._publish = publish
        self._text_idle_seconds = text_idle_seconds
        self._text_max_chars = text_max_chars
        self._media_idle_seconds = media_idle_seconds
        self._tool_idle_seconds = tool_idle_seconds
        self._tool_terminal_delay_seconds = tool_terminal_delay_seconds

    def resolve_handler(self, event: ACPProgressEvent) -> ACPSessionHandler:
        return self._registry.resolve(event)

    async def handle_event(self, event: ACPProgressEvent) -> None:
        handler = self.resolve_handler(event)
        pool = await handler.enqueue(self._manager, event)

        if handler.bucket_type == ACPBucketType.NONE:
            await self._manager.destroy_pool(pool.pool_id)
            return

        if handler.bucket_type == ACPBucketType.TEXT:
            pool = cast(MessageTextPool, pool)
            if pool.idle_task is not None:
                pool.idle_task.cancel()
            text_size = len("".join(pool.parts))
            if text_size >= self._text_max_chars:
                await self._flush_text(event.session_id, pool.route_key, "size")
                return
            pool.idle_task = asyncio.create_task(
                self._text_idle_flush(event.session_id, pool.route_key)
            )
            return

        if handler.bucket_type == ACPBucketType.MEDIA:
            pool = cast(MediaPool, pool)
            if pool.idle_task is not None:
                pool.idle_task.cancel()
            pool.idle_task = asyncio.create_task(
                self._media_idle_flush(event.session_id, pool.route_key)
            )
            return

        if handler.bucket_type == ACPBucketType.TOOL:
            pool = cast(ToolPool, pool)
            if pool.idle_task is not None:
                pool.idle_task.cancel()
            status = str((pool.latest or {}).get("status") or "").strip().lower()
            delay = (
                self._tool_terminal_delay_seconds
                if status in self.TERMINAL_STATUSES
                else self._tool_idle_seconds
            )
            pool.idle_task = asyncio.create_task(
                self._tool_idle_flush(event.session_id, pool.route_key, delay)
            )
            return

    async def close(self, session_id: str) -> None:
        for pool in self._manager.iter_pools_for_session(
            session_id=session_id,
            bucket_type=ACPBucketType.TEXT,
        ):
            pool = cast(MessageTextPool, pool)
            await self._flush_text(session_id, pool.route_key, "close")
        for pool in self._manager.iter_pools_for_session(
            session_id=session_id,
            bucket_type=ACPBucketType.MEDIA,
        ):
            pool = cast(MediaPool, pool)
            await self._flush_media(session_id, pool.route_key, "close")
        for pool in self._manager.iter_pools_for_session(
            session_id=session_id,
            bucket_type=ACPBucketType.TOOL,
        ):
            pool = cast(ToolPool, pool)
            await self._flush_tool(session_id, pool.route_key, "close")

    async def _text_idle_flush(self, session_id: str, bucket_key: str) -> None:
        try:
            await asyncio.sleep(self._text_idle_seconds)
            await self._flush_text(session_id, bucket_key, "deadman")
        except asyncio.CancelledError:
            return

    async def _media_idle_flush(self, session_id: str, bucket_key: str) -> None:
        try:
            await asyncio.sleep(self._media_idle_seconds)
            await self._flush_media(session_id, bucket_key, "deadman")
        except asyncio.CancelledError:
            return

    async def _tool_idle_flush(self, session_id: str, bucket_key: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            await self._flush_tool(session_id, bucket_key, "deadman")
        except asyncio.CancelledError:
            return

    async def _flush_text(self, session_id: str, bucket_key: str, reason: str) -> None:
        pool = self._manager.get_pool_by_key(
            bucket_type=ACPBucketType.TEXT,
            session_id=session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            return
        pool = cast(MessageTextPool, pool)
        if pool.idle_task is not None:
            pool.idle_task.cancel()
            pool.idle_task = None
        payload = await pool.flush(reason)
        if payload.payload:
            content, metadata = build_progress_text(
                text=str(payload.payload),
                session_id=session_id,
                route_key=bucket_key,
                flush_reason=reason,
            )
            await self._publish(content, metadata, "progress_text")
        await self._manager.destroy_pool(pool.pool_id)

    async def _flush_media(self, session_id: str, bucket_key: str, reason: str) -> None:
        pool = self._manager.get_pool_by_key(
            bucket_type=ACPBucketType.MEDIA,
            session_id=session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            return
        pool = cast(MediaPool, pool)
        if pool.idle_task is not None:
            pool.idle_task.cancel()
            pool.idle_task = None
        payload = await pool.flush(reason)
        if payload.payload:
            content, metadata = build_progress_media(
                payload=dict(payload.payload),
                session_id=session_id,
                route_key=bucket_key,
                update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK.value,
                flush_reason=reason,
            )
            await self._publish(content, metadata, "progress_media")
        await self._manager.destroy_pool(pool.pool_id)

    async def _flush_tool(self, session_id: str, bucket_key: str, reason: str) -> None:
        pool = self._manager.get_pool_by_key(
            bucket_type=ACPBucketType.TOOL,
            session_id=session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            return
        pool = cast(ToolPool, pool)
        if pool.idle_task is not None:
            pool.idle_task.cancel()
            pool.idle_task = None
        payload = await pool.flush(reason)
        latest = dict((payload.payload or {}).get("latest") or {})
        history = list((payload.payload or {}).get("history") or [])
        content, metadata = build_progress_tool(
            payload=build_tool_payload(
                route_key=bucket_key,
                latest=latest,
                history=history,
                history_dropped=int((payload.payload or {}).get("history_dropped") or 0),
            ),
            session_id=str(latest.get("sessionId") or latest.get("session_id") or session_id),
            route_key=bucket_key,
            update_type=str(latest.get("update_type") or ACPUpdateType.TOOL_CALL_UPDATE.value),
            flush_reason=reason,
        )
        await self._publish(content, metadata, "progress_tool")
        await self._manager.destroy_pool(pool.pool_id)


class ProgressRouter:
    """Thin coordination layer that owns config and delegates into SessionStateRouter."""

    def __init__(
        self,
        *,
        acp_config: ACPBackendConfig,
        manager: SessionStateManager,
        publish: Callable[[str, dict[str, Any], str], Awaitable[None]],
    ) -> None:
        self._state_router = SessionStateRouter(
            HandlerRegistry(
                handlers=[
                    AgentMessageTextHandler(),
                    AgentMessageMediaHandler(),
                    ToolHandler(),
                    UserMessageHandler(),
                ],
                fallback_handler=OtherHandler(),
            ),
            manager=manager,
            publish=publish,
            text_idle_seconds=float(getattr(acp_config, "progress_text_idle_seconds", 1.0)),
            text_max_chars=int(getattr(acp_config, "progress_text_max_chars", 2048)),
            media_idle_seconds=float(getattr(acp_config, "progress_media_idle_seconds", 0.2)),
            tool_idle_seconds=float(getattr(acp_config, "progress_tool_idle_seconds", 300.0)),
            tool_terminal_delay_seconds=float(
                getattr(acp_config, "progress_tool_terminal_delay_seconds", 1.5)
            ),
        )

    async def on_progress_event(self, event: ACPProgressEvent) -> None:
        await self._state_router.handle_event(event)

    async def close(self, session_id: str) -> None:
        await self._state_router.close(session_id)
