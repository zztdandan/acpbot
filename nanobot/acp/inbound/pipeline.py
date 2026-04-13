"""Fixed inbound pipeline steps for ACP runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable

from nanobot.acp.inbound.media import build_media_artifacts
from nanobot.acp.runtime_models import InboundContext, ProcessRequest

if TYPE_CHECKING:
    from nanobot.acp.inbound.command_router import CommandRouter
    from nanobot.acp.runtime import ACPRuntime

InboundStep = Callable[[InboundContext], Awaitable[None]]


def build_normalize_step(runtime: ACPRuntime) -> InboundStep:
    async def _normalize(ctx: InboundContext) -> None:
        # Normalize only the inbound shape. It must not absorb command, permission,
        # or state-creation owner logic.
        ctx.content = str(ctx.content or "")
        ctx.media = list(ctx.media or [])
        ctx.metadata = dict(ctx.metadata or {})
        ctx.progress_metadata = dict(ctx.progress_metadata or {})

    return _normalize


def build_permission_inbound_step(runtime: ACPRuntime) -> InboundStep:
    async def _permission_inbound(ctx: InboundContext) -> None:
        # Permission inbound only detects and forwards likely replies. The pending
        # waiter itself remains owned by the target request state.
        active_entry = runtime.process_runtime_manager.find_request_waiting_permission(
            nanobot_side_session_key=ctx.nanobot_side_session_key,
        )
        if active_entry is None:
            return
        if not active_entry.state_manager.looks_like_permission_reply(ctx.content):
            return
        ack = await active_entry.state_manager.handle_permission_reply(reply_text=ctx.content)
        ctx.direct_response = runtime.new_outbound_message(
            channel=ctx.channel,
            chat_id=ctx.chat_id,
            content=ack,
        )

    return _permission_inbound


def build_command_router_step(command_router: CommandRouter) -> InboundStep:
    async def _command_router(ctx: InboundContext) -> None:
        if ctx.direct_response is not None:
            return
        # Slash commands must be intercepted before real prompt execution. Unknown
        # commands also return directly so they are never forwarded to ACP prompt.
        response = await command_router.maybe_handle(ctx)
        if response is not None:
            ctx.direct_response = response

    return _command_router


def build_media_prepare_step(runtime: ACPRuntime) -> InboundStep:
    async def _media_prepare(ctx: InboundContext) -> None:
        # Inbound prepares media artifacts only. Final prompt blocks remain the
        # responsibility of the execution-stage helper.
        media_artifacts = build_media_artifacts(
            workspace=runtime.workspace,
            media=ctx.media,
            nanobot_side_session_key=ctx.nanobot_side_session_key,
            channel=ctx.channel,
        )
        if ctx.media and len(media_artifacts) != len(ctx.media):
            ctx.direct_response = runtime.new_outbound_message(
                channel=ctx.channel,
                chat_id=ctx.chat_id,
                content="Inbound media validation failed. Only existing workspace-local files are allowed.",
            )
            return
        ctx.artifacts["media_artifacts"] = media_artifacts

    return _media_prepare


def build_process_request_step(runtime: ACPRuntime) -> InboundStep:
    async def _build_process_request(ctx: InboundContext) -> None:
        if ctx.direct_response is not None:
            return
        # ProcessRequest keeps only facts that real execution must know. The whole
        # inbound context does not cross into the execution layer unchanged.
        ctx.process_request = ProcessRequest(
            request_key=ctx.request_key,
            nanobot_side_session_key=ctx.nanobot_side_session_key,
            channel=ctx.channel,
            chat_id=ctx.chat_id,
            sender_id=ctx.sender_id,
            content=ctx.content,
            media=list(ctx.media),
            metadata=dict(ctx.metadata),
            on_progress=ctx.on_progress,
            artifacts=dict(ctx.artifacts),
        )

    return _build_process_request
