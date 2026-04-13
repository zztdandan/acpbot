"""Fixed inbound pipeline steps for ACP runtime."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from nanobot.acp.inbound.media import build_media_artifacts
from nanobot.acp.runtime_models import ProcessRequest

InboundStep = Callable[[Any], Awaitable[None]]


def build_normalize_step(runtime: Any) -> InboundStep:
    async def _normalize(ctx: Any) -> None:
        # 中文注释：normalize 只做输入形态归一，不掺杂命令、permission、state 创建等 owner 逻辑。
        ctx.content = str(ctx.content or "")
        ctx.media = list(ctx.media or [])
        ctx.metadata = dict(ctx.metadata or {})
        ctx.progress_metadata = dict(ctx.progress_metadata or {})

    return _normalize


def build_permission_inbound_step(runtime: Any) -> InboundStep:
    async def _permission_inbound(ctx: Any) -> None:
        # 中文注释：permission inbound 只做“像不像 reply”识别与转发，
        # pending waiter 本身仍由目标 request 的 state owner 持有。
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


def build_command_router_step(command_router: Any) -> InboundStep:
    async def _command_router(ctx: Any) -> None:
        if ctx.direct_response is not None:
            return
        # 中文注释：slash 命令必须在真实 prompt 执行前被截获，
        # 未知命令也显式 direct response，避免误透传给 ACP prompt。
        response = await command_router.maybe_handle(ctx)
        if response is not None:
            ctx.direct_response = response

    return _command_router


def build_media_prepare_step(runtime: Any) -> InboundStep:
    async def _media_prepare(ctx: Any) -> None:
        # 中文注释：inbound 只准备 media artifact，不在这里构造最终 prompt blocks；
        # prompt blocks builder 是执行期私有 helper 的职责。
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


def build_process_request_step(runtime: Any) -> InboundStep:
    async def _build_process_request(ctx: Any) -> None:
        if ctx.direct_response is not None:
            return
        # 中文注释：ProcessRequest 只保留真实执行必须知道的事实，
        # 不把整个 inbound context 原样拖进执行层。
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
