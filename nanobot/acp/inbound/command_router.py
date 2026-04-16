"""入站命令路由器：在入站阶段处理 ACP slash 命令并生成直返消息。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.contracts import (
    ACP_META_KIND,
    ACP_META_PROGRESS,
    ACP_META_RENDER_AS,
    ACP_META_RENDER_AS_LIST,
    ACP_META_RENDER_AS_NORENDER,
    ACP_META_RENDER_AS_TEXT,
)
from nanobot.acp.runtime_models import InboundContext
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


class CommandRouter:
    """入站命令路由器：负责 slash 命令识别、执行与直返消息生成。

    职责：
        - 在入站阶段拦截 ACP slash 命令，避免普通文本继续进入执行链路
        - 调用 runtime/sessionmap/session manager 完成 `/new`、`/stop`、`/models`、`/agents` 等操作
    """

    HELP_TEXT = (
        "🐈 acpbot acp runtime commands:\n"
        "/new — Start a new conversation\n"
        "/stop — Stop the current task\n"
        "/help — Show available commands\n"
        "/models — List available/current models\n"
        "/set_model <model_id> — Switch model\n"
        "/agents — List available/current agents\n"
        "/set_agent <agent_id> — Switch agent"
    )

    def __init__(self, *, runtime: ACPRuntime) -> None:
        """建立命令路由器并绑定当前 runtime。"""
        self._runtime = runtime

    @staticmethod
    def parse_command(content: str) -> tuple[str, str]:
        """解析命令文本并拆出命令字与参数；普通文本会返回空命令。"""
        raw = content.strip()
        if not raw:
            return "", ""
        parts = raw.split(maxsplit=1)
        return parts[0].lower(), parts[1].strip() if len(parts) > 1 else ""

    async def maybe_handle(self, ctx: InboundContext) -> OutboundMessage | None:
        """尝试处理 slash 命令；命中时返回直返消息，未命中时返回空值。

        处理流程：
            - 先解析入站文本，非 slash 命令直接放行
            - 对已知命令调用对应 runtime 能力，并把结果转成直返消息
            - 未知命令显式回包，避免静默吞掉用户输入
        """
        command, arg = self.parse_command(ctx.content)
        if not command.startswith("/"):
            return None

        if command == "/help":
            return self._reply(ctx, self.HELP_TEXT, ACP_META_RENDER_AS_TEXT)
        if command == "/new":
            await self._runtime.ensure_sessionmap_truth_loaded()
            await self._runtime.stop_session(nanobot_side_session_key=ctx.nanobot_side_session_key)
            old_acp_side_session_id = self._runtime.drop_session_binding_and_runtime_entry(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            if old_acp_side_session_id:
                self._runtime.session_runtime_manager.drop_session_capabilities(
                    acp_side_session_id=old_acp_side_session_id
                )
            return self._reply(ctx, "New session started.", ACP_META_RENDER_AS_TEXT)
        if command == "/models":
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            content = await self._runtime.list_models_command(acp_side_session_id)
            return self._reply(ctx, content, ACP_META_RENDER_AS_LIST)
        if command == "/agents":
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            content = await self._runtime.list_agents_command(acp_side_session_id)
            return self._reply(ctx, content, ACP_META_RENDER_AS_LIST)
        if command == "/set_model":
            if not arg:
                return self._reply(
                    ctx, "invalid input, Usage: /set_model <model_id>", ACP_META_RENDER_AS_TEXT
                )
            result = await self._runtime.session_runtime_manager.set_model_safe(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
                model_id=arg,
            )
            if not result.success:
                return self._reply(
                    ctx,
                    f"Model switch failed: {arg}. reason={result.reason}. Session selection reset and session resumed.",
                    ACP_META_RENDER_AS_TEXT,
                )
            return self._reply(ctx, f"Model switched to: {arg}")
        if command == "/set_agent":
            if not arg:
                return self._reply(
                    ctx, "invalid input, Usage:  /set_agent <agent_id>", ACP_META_RENDER_AS_TEXT
                )
            result = await self._runtime.session_runtime_manager.set_agent_safe(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
                agent_id=arg,
            )
            if not result.success:
                return self._reply(
                    ctx,
                    f"Agent switch failed: {arg}. reason={result.reason}. Session selection reset and session resumed.",
                    ACP_META_RENDER_AS_TEXT,
                )
            return self._reply(ctx, f"Agent switched to: {arg}")
        if command == "/stop":
            result = await self._runtime.stop_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            if result.had_anything_to_stop:
                return self._reply(
                    ctx,
                    f"Stop requested. active_cancel_requested={result.active_cancel_requested} dropped_queued={result.dropped_queued_count}",
                    ACP_META_RENDER_AS_TEXT,
                )
            return self._reply(
                ctx, "Nothing active or queued for this session.", ACP_META_RENDER_AS_TEXT
            )

        return self._reply(ctx, f"Unknown ACP slash command: {command}", ACP_META_RENDER_AS_TEXT)

    @staticmethod
    def _reply(ctx: InboundContext, content: str, render_as=None) -> OutboundMessage:
        """构造命令直返消息；统一复用当前上下文的 channel 与 chat_id。"""
        command_reply_outbound = OutboundMessage(
            channel=ctx.channel,
            chat_id=ctx.chat_id,
            content=content,
        )
        command_reply_outbound.metadata = {
            ACP_META_KIND: "command",
            ACP_META_RENDER_AS: "command",
            ACP_META_PROGRESS: False,
        }
        if render_as is None:
            #  不指定渲染者的话默认建议不渲染这个回调
            command_reply_outbound.metadata[ACP_META_RENDER_AS] = ACP_META_RENDER_AS_NORENDER

        return command_reply_outbound
