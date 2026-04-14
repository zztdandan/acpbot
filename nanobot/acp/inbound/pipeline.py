"""入站步骤工厂：定义固定的 normalize、permission、command、media、build 步骤。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable

from nanobot.acp.inbound.media import build_media_artifacts
from nanobot.acp.runtime_models import InboundContext, ProcessRequest

if TYPE_CHECKING:
    from nanobot.acp.inbound.command_router import CommandRouter
    from nanobot.acp.runtime import ACPRuntime

InboundStep = Callable[[InboundContext], Awaitable[None]]


def _looks_like_permission_reply_text(content: str) -> bool:
    """做轻量 permission reply 识别；用于 pending 查询前的快速分流。"""

    normalized = content.strip().lower()
    return normalized.startswith("/permission ") or normalized.startswith("permission ")


def build_normalize_step(runtime: ACPRuntime) -> InboundStep:
    """构造入站归一化步骤；统一补齐上下文中的空字符串、空列表与空字典。"""

    async def _normalize(ctx: InboundContext) -> None:
        """归一化入站上下文的基础字段；保证后续步骤无需重复判空。"""
        ctx.content = str(ctx.content or "")
        ctx.media = list(ctx.media or [])
        ctx.metadata = dict(ctx.metadata or {})
        ctx.progress_metadata = dict(ctx.progress_metadata or {})

    return _normalize


def build_permission_inbound_step(runtime: ACPRuntime) -> InboundStep:
    """构造权限回复拦截步骤；优先把用户回复路由到等待中的权限请求。"""

    async def _permission_inbound(ctx: InboundContext) -> None:
        """识别并消费权限回复；命中待回复权限时直接生成确认消息。

        处理流程：
            - 先查询当前会话是否有等待权限回复的活跃请求
            - 再判断当前文本是否像权限回复，避免误拦截普通消息
            - 命中时消费回复，并把确认文本写入 `direct_response`
        """
        # 先做文本级 permission reply 判定，避免普通消息误命中 not-found 直返。
        if not _looks_like_permission_reply_text(ctx.content):
            return
        active_entry = runtime.process_runtime_manager.find_request_waiting_permission(
            nanobot_side_session_key=ctx.nanobot_side_session_key,
        )
        if active_entry is None:
            ctx.direct_response = runtime.new_outbound_message(
                channel=ctx.channel,
                chat_id=ctx.chat_id,
                content="No pending permission request.",
            )
            return
        await active_entry.progress_router.handle_permission_reply(reply_text=ctx.content)
        ctx.direct_response = runtime.new_outbound_message(
            channel=ctx.channel,
            chat_id=ctx.chat_id,
            content="Permission accepted.",
        )

    return _permission_inbound


def build_command_router_step(command_router: CommandRouter) -> InboundStep:
    """构造命令路由步骤；把 slash 命令与普通输入在入站阶段分流。"""

    async def _command_router(ctx: InboundContext) -> None:
        """尝试处理 slash 命令；命中时把回复写入 `direct_response`。"""
        if ctx.direct_response is not None:
            return
        response = await command_router.maybe_handle(ctx)
        if response is not None:
            ctx.direct_response = response

    return _command_router


def build_media_prepare_step(runtime: ACPRuntime) -> InboundStep:
    """构造媒体准备步骤；把合法媒体路径转换为可供 ACP 消费的 artifact 列表。"""

    async def _media_prepare(ctx: InboundContext) -> None:
        """校验并构造媒体 artifacts；非法媒体记录错误出站并继续后续流程。

        处理流程：
            - 根据 workspace、session_key 与 channel 校验入站媒体路径
            - 若媒体数量与成功构造的 artifact 数量不一致，追加错误出站到缓冲池
            - 成功时把 artifact 列表写入 `ctx.artifacts`
        """
        media_artifacts = build_media_artifacts(
            workspace=runtime.workspace,
            media=ctx.media,
            nanobot_side_session_key=ctx.nanobot_side_session_key,
            channel=ctx.channel,
        )
        if ctx.media and len(media_artifacts) != len(ctx.media):
            invalid_count = len(ctx.media) - len(media_artifacts)
            # 当前版本暂不消费 `ctx.outbound_messages`，所以全失败时必须直返，避免静默进入执行层。
            if not media_artifacts:
                ctx.direct_response = runtime.new_outbound_message(
                    channel=ctx.channel,
                    chat_id=ctx.chat_id,
                    content="Inbound media validation failed. Only existing workspace-local files are allowed.",
                )
                return
            # 部分成功继续执行，并把 warning 落到 artifacts，供后续观测/调试消费。
            ctx.artifacts["media_validation_warning"] = {
                "message": "Some inbound media paths were rejected.",
                "invalid_count": invalid_count,
                "accepted_count": len(media_artifacts),
            }
        ctx.artifacts["media_artifacts"] = media_artifacts

    return _media_prepare


def build_process_request_step(runtime: ACPRuntime) -> InboundStep:
    """构造 process_request 步骤；把入站上下文收口为运行时可入队的请求对象。"""

    async def _build_process_request(ctx: InboundContext) -> None:
        """构造 `ProcessRequest`；仅在没有直返响应时把上下文固化为执行请求。"""
        if ctx.direct_response is not None:
            return
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
