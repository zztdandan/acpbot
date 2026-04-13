"""Request-scoped ACP state manager."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol

from loguru import logger

from nanobot.acp.contracts import (
    ACPCallbackUpdate,
    ACPPermissionKind,
    ACPPermissionOption,
    ACPResourceBlock,
    ACPToolCall,
    JSONMap,
    ObservabilityEventName,
    ObservabilityScopeName,
    build_permission_selected_payload,
)
from nanobot.acp.observability import ObservabilityEvent
from nanobot.acp.runtime_models import ProgressCallback
from nanobot.acp.state.models import RequestScopeState
from nanobot.acp.state.permission_events import PendingPermissionRequest
from nanobot.acp.state.pools import MediaPool, MessageTextPool, PermissionPool, ToolPool
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.state.router import ProgressRouter


class _RuntimeObservabilityOwner(Protocol):
    async def push_observability(self, event: ObservabilityEvent) -> None: ...


class SessionStateManager:
    """Owns all request-scoped state for one active ACP process request."""

    def __init__(
        self,
        *,
        runtime: _RuntimeObservabilityOwner,
        request_key: str,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        channel: str,
        chat_id: str,
        on_progress: ProgressCallback | None,
    ) -> None:
        self._runtime = runtime
        self.request_key = request_key
        self.nanobot_side_session_key = nanobot_side_session_key
        self.acp_side_session_id = acp_side_session_id
        self.channel = channel
        self.chat_id = chat_id
        self.on_progress = on_progress
        self.request_scope = RequestScopeState()
        self.message_text_pool = MessageTextPool()
        self.media_pool = MediaPool()
        self.tool_pool = ToolPool()
        self.permission_pool = PermissionPool()
        self._closed = False
        self._progress_router: ProgressRouter | None = None
        self._pending_permission_future: asyncio.Future[str] | None = None
        self._pending_permission_request: PendingPermissionRequest | None = None

    def is_closed(self) -> bool:
        return self._closed

    def bind_progress_router(self, progress_router: ProgressRouter) -> None:
        """Bind the progress router so state owns its close lifecycle."""

        # Once router is bound to state, state owns the entire shutdown path and callers
        # no longer need to close the router or flush pools separately.
        self._progress_router = progress_router

    async def emit_progress(self, *, content: str, metadata: JSONMap) -> None:
        """Mirror structured progress to the request on_progress sink if present."""

        if not content or self.on_progress is None:
            return
        # State is the source of truth for structured progress. `on_progress` is only a
        # compatibility mirror sink, so signature adaptation is centralized here.
        callback_kwargs: dict[str, object] = {}
        if "tool_hint" in metadata:
            callback_kwargs["tool_hint"] = metadata["tool_hint"]
        if "tool_event" in metadata:
            callback_kwargs["tool_event"] = metadata["tool_event"]
        try:
            await self.on_progress(content, **callback_kwargs)
        except TypeError:
            # Older `on_progress` callbacks accepted fewer keyword arguments, so keep a
            # narrow fallback only at the state exit boundary.
            try:
                if "tool_hint" in callback_kwargs:
                    await self.on_progress(content, tool_hint=callback_kwargs["tool_hint"])
                else:
                    await self.on_progress(content)
            except Exception:
                logger.exception("ACP state on_progress fallback failed")
        except Exception:
            logger.exception("ACP state on_progress emit failed")

    async def consume_session_update(
        self,
        update: ACPCallbackUpdate,
        *,
        progress_router: ProgressRouter,
    ) -> None:
        """Consume ACP callback updates into request-scoped facts and progress pools."""

        if self._closed:
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.LATE_SESSION_UPDATE,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                )
            )
            return

        from acp.schema import (
            AgentMessageChunk,
            EmbeddedResourceContentBlock,
            ImageContentBlock,
            ResourceContentBlock,
            TextContentBlock,
            ToolCallProgress,
            ToolCallStart,
        )

        if isinstance(update, AgentMessageChunk) and isinstance(update.content, TextContentBlock):
            # Text must feed both the progress pool and final-text aggregation. That is
            # exactly why state owns both progress facts and final result materialization.
            text = str(getattr(update.content, "text", "") or "")
            self.message_text_pool.accept(text)
            self.request_scope.final_text = self.message_text_pool.text.strip()
            self.request_scope.partial_text = self.message_text_pool.text.strip()
            await progress_router.emit(self.message_text_pool.flush())
            return

        if isinstance(update, AgentMessageChunk) and isinstance(
            update.content,
            (ImageContentBlock, ResourceContentBlock, EmbeddedResourceContentBlock),
        ):
            # Media paths are aggregated inside request-scoped state instead of flowing
            # back into runtime-level side structures such as old `_session_result_media`.
            media_path = self._extract_media_path(update.content)
            if media_path:
                self.media_pool.accept(media_path)
                if media_path not in self.request_scope.media_paths:
                    self.request_scope.media_paths.append(media_path)
                await progress_router.emit(self.media_pool.flush())
            return

        if isinstance(update, ToolCallStart):
            tool_name = str(
                getattr(update, "tool_name", None) or getattr(update, "toolName", "tool")
            )
            self.tool_pool.accept(f"Running tool: {tool_name}")
            await progress_router.emit(self.tool_pool.flush())
            return

        if isinstance(update, ToolCallProgress):
            text = str(
                getattr(update, "message", None) or getattr(update, "status", "tool progress")
            )
            self.tool_pool.accept(text)
            await progress_router.emit(self.tool_pool.flush())
            return

    @staticmethod
    def _extract_media_path(block: ACPResourceBlock) -> str | None:
        """Best-effort extraction of a usable media path from ACP resource blocks."""

        for attr in ("uri", "path"):
            value = getattr(block, attr, None)
            if isinstance(value, str) and value:
                if value.startswith("file://"):
                    return value[7:]
                return value
        resource = getattr(block, "resource", None)
        if resource is not None:
            uri = getattr(resource, "uri", None)
            if isinstance(uri, str) and uri:
                return uri[7:] if uri.startswith("file://") else uri
        return None

    async def handle_permission_request(
        self,
        *,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall | None = None,
    ) -> object:
        """Wait for an inbound permission reply owned by this request state."""

        # The pending permission waiter belongs to this request state. Inbound only
        # delivers replies and does not own the future or timeout decision.
        if self._closed:
            raise RuntimeError("permission request received after state closed")

        from acp.schema import RequestPermissionResponse

        loop = asyncio.get_running_loop()
        self._pending_permission_future = loop.create_future()
        prompt = self._render_permission_prompt(options)
        self._pending_permission_request = PendingPermissionRequest(
            options=list(options),
            tool_call=tool_call,
            prompt_text=prompt,
        )
        self.permission_pool.accept(prompt)
        if self._progress_router is not None:
            await self._progress_router.emit(self.permission_pool.flush())

        try:
            selected_option_id = await asyncio.wait_for(
                self._pending_permission_future, timeout=300
            )
        except asyncio.TimeoutError as exc:
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.PERMISSION_TIMEOUT,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                )
            )
            raise RuntimeError("permission reply timed out") from exc
        finally:
            self._pending_permission_future = None
            self._pending_permission_request = None

        return RequestPermissionResponse.model_validate(
            build_permission_selected_payload(selected_option_id)
        )

    def has_pending_permission(self) -> bool:
        return (
            self._pending_permission_future is not None
            and not self._pending_permission_future.done()
        )

    def looks_like_permission_reply(self, reply_text: str) -> bool:
        """Return True when the reply can be mapped onto the pending permission options."""

        request = self._pending_permission_request
        if request is None:
            return False
        normalized = reply_text.strip()
        if normalized.startswith("/permission "):
            normalized = normalized[len("/permission ") :]
        elif normalized.startswith("permission "):
            normalized = normalized[len("permission ") :]
        else:
            return False
        return self._select_permission_option(request.options, normalized) is not None

    async def handle_permission_reply(self, *, reply_text: str) -> str:
        """Accept an inbound permission reply and resolve the pending permission waiter."""

        future = self._pending_permission_future
        request = self._pending_permission_request
        if future is None or future.done() or request is None:
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.PERMISSION_REPLY_NOT_FOUND,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                )
            )
            return "No pending permission request for this session."

        normalized_reply = reply_text.strip()
        if normalized_reply.startswith("/permission "):
            normalized_reply = normalized_reply[len("/permission ") :]
        elif normalized_reply.startswith("permission "):
            normalized_reply = normalized_reply[len("permission ") :]
        option_id = self._select_permission_option(request.options, normalized_reply)
        if option_id is None:
            return "Permission reply not understood. Reply with /permission <number>."

        future.set_result(option_id)
        return "Permission reply received."

    @staticmethod
    def _select_permission_option(
        options: list[ACPPermissionOption], reply_text: str
    ) -> str | None:
        """Map simple user replies onto ACP permission option ids."""

        reply = reply_text.strip().lower()
        if not reply:
            return None
        mapping = {str(index + 1): option.option_id for index, option in enumerate(options)}
        if reply in mapping:
            return mapping[reply]
        for option in options:
            kind = str(
                getattr(getattr(option, "kind", None), "value", getattr(option, "kind", "")) or ""
            )
            if reply in {kind.lower(), option.option_id.lower()}:
                return option.option_id
        if reply in {"allow", "yes", "y"}:
            for option in options:
                kind = str(
                    getattr(getattr(option, "kind", None), "value", getattr(option, "kind", ""))
                    or ""
                )
                if kind in {
                    ACPPermissionKind.ALLOW_ONCE.value,
                    ACPPermissionKind.ALLOW_ALWAYS.value,
                }:
                    return option.option_id
        if reply in {"cancel", "deny", "no", "n"}:
            for option in options:
                kind = str(
                    getattr(getattr(option, "kind", None), "value", getattr(option, "kind", ""))
                    or ""
                )
                if kind == ACPPermissionKind.CANCELLED.value:
                    return option.option_id
        return None

    @staticmethod
    def _render_permission_prompt(options: list[ACPPermissionOption]) -> str:
        lines = ["ACP requires permission. Reply with /permission <number>:"]
        for index, option in enumerate(options, start=1):
            kind = getattr(
                getattr(option, "kind", None), "value", getattr(option, "kind", "option")
            )
            label = getattr(option, "label", None) or getattr(option, "title", None) or kind
            lines.append(f"{index}. {label}")
        return "\n".join(lines)

    def materialize_final_outbound(self, *, partial: bool = False) -> OutboundMessage:
        """Materialize the final outbound from request-scoped facts."""

        # State owns final OutboundMessage materialization. ProcessRuntimeManager only
        # consumes the result instead of building final payloads itself.
        content = self.request_scope.partial_text if partial else self.request_scope.final_text
        return OutboundMessage(
            channel=self.channel,
            chat_id=self.chat_id,
            content=content or "",
            media=list(self.request_scope.media_paths),
            metadata=dict(self.request_scope.final_metadata),
        )

    async def close(self) -> None:
        """Close the request state exactly once and flush remaining progress."""

        if self._closed:
            return
        # Close is the hard boundary for one request. After it flips, late writes are
        # only observable events and can no longer mutate final aggregation or progress.
        self._closed = True
        if self._progress_router is not None:
            await self._progress_router.close()
        if (
            self._pending_permission_future is not None
            and not self._pending_permission_future.done()
        ):
            self._pending_permission_future.cancel()
