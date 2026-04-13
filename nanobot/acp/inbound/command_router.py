"""Inbound slash command router."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.runtime_models import InboundContext
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


class CommandRouter:
    """Handles ACP slash commands as inbound direct responses."""

    HELP_TEXT = (
        "🐈 nanobot commands:\n"
        "/new — Start a new conversation\n"
        "/stop — Stop the current task\n"
        "/help — Show available commands\n"
        "/models — List available/current models\n"
        "/set_model <model_id> — Switch model\n"
        "/agents — List available/current agents\n"
        "/set_agent <agent_id> — Switch agent"
    )

    def __init__(self, *, runtime: ACPRuntime) -> None:
        self._runtime = runtime

    @staticmethod
    def parse_command(content: str) -> tuple[str, str]:
        raw = content.strip()
        if not raw:
            return "", ""
        parts = raw.split(maxsplit=1)
        return parts[0].lower(), parts[1].strip() if len(parts) > 1 else ""

    async def maybe_handle(self, ctx: InboundContext) -> OutboundMessage | None:
        command, arg = self.parse_command(ctx.content)
        if not command.startswith("/"):
            return None

        if command == "/help":
            return self._reply(ctx, self.HELP_TEXT)
        if command == "/new":
            # `/new` is not just a binding delete. It first stops active/queued work for
            # the same session, then drops binding truth and runtime-ready state.
            await self._runtime.ensure_sessionmap_truth_loaded()
            await self._runtime.stop_session(nanobot_side_session_key=ctx.nanobot_side_session_key)
            old_acp_side_session_id = self._runtime.drop_session_binding_and_runtime_entry(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            if old_acp_side_session_id:
                self._runtime.session_runtime_manager.drop_session_capabilities(
                    acp_side_session_id=old_acp_side_session_id
                )
            return self._reply(ctx, "New session started.")
        if command == "/models":
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            content = await self._runtime.list_models_command(acp_side_session_id)
            return self._reply(ctx, content)
        if command == "/agents":
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            content = await self._runtime.list_agents_command(acp_side_session_id)
            return self._reply(ctx, content)
        if command == "/set_model":
            if not arg:
                return self._reply(ctx, "Usage: /set_model <model_id>")
            await self._runtime.ensure_connection()
            # Command-driven selection changes must update the current ready session and
            # the mirrored binding/runtime state so reconnects do not fall back.
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            if self._runtime._acp_client_conn is None:
                raise RuntimeError("ACP connection is not available")
            await self._runtime._acp_client_conn.set_session_model(
                model_id=arg,
                session_id=acp_side_session_id,
            )
            self._runtime.session_runtime_manager.set_current_model(
                acp_side_session_id=acp_side_session_id,
                model_id=arg,
            )
            self._runtime.sessionmap_binding_manager.update_bound_model(
                ctx.nanobot_side_session_key, arg
            )
            self._runtime.session_runtime_manager.update_runtime_selection(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
                bound_model=arg,
            )
            return self._reply(ctx, f"Model switched to: {arg}")
        if command == "/set_agent":
            if not arg:
                return self._reply(ctx, "Usage: /set_agent <agent_id>")
            await self._runtime.ensure_connection()
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            if self._runtime._acp_client_conn is None:
                raise RuntimeError("ACP connection is not available")
            await self._runtime._acp_client_conn.set_session_mode(
                mode_id=arg,
                session_id=acp_side_session_id,
            )
            self._runtime.session_runtime_manager.set_current_agent(
                acp_side_session_id=acp_side_session_id,
                agent_id=arg,
            )
            self._runtime.sessionmap_binding_manager.update_bound_agent(
                ctx.nanobot_side_session_key, arg
            )
            self._runtime.session_runtime_manager.update_runtime_selection(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
                bound_agent=arg,
            )
            return self._reply(ctx, f"Agent switched to: {arg}")
        if command == "/stop":
            # The command layer only produces a user-visible acknowledgement. Real stop
            # decisions and execution remain inside runtime/process manager owners.
            result = await self._runtime.stop_session(
                nanobot_side_session_key=ctx.nanobot_side_session_key,
            )
            if result.had_anything_to_stop:
                return self._reply(
                    ctx,
                    f"Stop requested. active_cancel_requested={result.active_cancel_requested} dropped_queued={result.dropped_queued_count}",
                )
            return self._reply(ctx, "Nothing active or queued for this session.")

        return self._reply(ctx, f"Unknown ACP slash command: {command}")

    @staticmethod
    def _reply(ctx: InboundContext, content: str) -> OutboundMessage:
        return OutboundMessage(
            channel=ctx.channel,
            chat_id=ctx.chat_id,
            content=content,
        )
