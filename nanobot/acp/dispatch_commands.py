"""ACP slash 命令路由辅助。"""

from __future__ import annotations

from typing import Any

from nanobot.acp.dispatcher_ports import _DispatcherCommandPorts
from nanobot.acp.state import _SessionCapabilities
from nanobot.bus.events import OutboundMessage
from nanobot.command.router import CommandContext, CommandRouter

_COMMAND_REASON_METADATA_KEY = "_acp_command_reason"


def _command_outbound(ctx: CommandContext, *, content: str, reason: str) -> OutboundMessage:
    """Build an outbound payload and carry internal debug reason."""
    return OutboundMessage(
        channel=ctx.msg.channel,
        chat_id=ctx.msg.chat_id,
        content=content,
        metadata={_COMMAND_REASON_METADATA_KEY: reason},
    )


def _dispatcher_from_ctx(ctx: CommandContext) -> _DispatcherCommandPorts:
    dispatcher = ctx.loop
    if dispatcher is None:
        raise RuntimeError("ACP command context missing dispatcher")
    return dispatcher


async def _cmd_help(ctx: CommandContext) -> OutboundMessage:
    dispatcher = _dispatcher_from_ctx(ctx)
    return _command_outbound(
        ctx,
        content=dispatcher._HELP_TEXT,
        reason="command_help",
    )


async def _cmd_new(ctx: CommandContext) -> OutboundMessage:
    dispatcher = _dispatcher_from_ctx(ctx)
    session_key = ctx.key
    # 中文注释：/new 仅删除当前映射并同步落盘，保持既有会话重建行为。
    old_session_id = dispatcher._session_map_binding_manager.clear_binding(session_key)
    if old_session_id:
        dispatcher._session_caps.pop(old_session_id, None)
    return _command_outbound(ctx, content="New session started.", reason="command_new")


async def _cmd_models(ctx: CommandContext) -> OutboundMessage:
    dispatcher = _dispatcher_from_ctx(ctx)
    session_id = await dispatcher._ensure_session(ctx.key)
    content = await dispatcher._list_models_command(session_id)
    return _command_outbound(ctx, content=content, reason="command_models")


async def _cmd_agents(ctx: CommandContext) -> OutboundMessage:
    dispatcher = _dispatcher_from_ctx(ctx)
    session_id = await dispatcher._ensure_session(ctx.key)
    content = await dispatcher._list_agents_command(session_id)
    return _command_outbound(ctx, content=content, reason="command_agents")


async def _cmd_set_model(ctx: CommandContext) -> OutboundMessage:
    dispatcher = _dispatcher_from_ctx(ctx)
    arg = ctx.args.strip()
    if not arg:
        return _command_outbound(
            ctx,
            content="Usage: /set_model <model_id>",
            reason="command_set_model_usage",
        )
    await dispatcher._ensure_connection()
    if dispatcher._conn is None:
        raise RuntimeError("ACP connection is not available")
    session_id = await dispatcher._ensure_session(ctx.key)
    await dispatcher._conn.set_session_model(model_id=arg, session_id=session_id)
    caps = dispatcher._session_caps.setdefault(session_id, _SessionCapabilities())
    caps.current_model = arg
    # 中文注释：命令路径只做一次 SDK set；manager 仅更新绑定并持久化，不触发二次 set。
    dispatcher._session_map_binding_manager.update_bound_model(ctx.key, arg)
    return _command_outbound(
        ctx,
        content=f"Model switched to: {arg}",
        reason="command_set_model",
    )


async def _cmd_set_agent(ctx: CommandContext) -> OutboundMessage:
    dispatcher = _dispatcher_from_ctx(ctx)
    arg = ctx.args.strip()
    if not arg:
        return _command_outbound(
            ctx,
            content="Usage: /set_agent <agent_id>",
            reason="command_set_agent_usage",
        )
    await dispatcher._ensure_connection()
    if dispatcher._conn is None:
        raise RuntimeError("ACP connection is not available")
    session_id = await dispatcher._ensure_session(ctx.key)
    await dispatcher._conn.set_session_mode(mode_id=arg, session_id=session_id)
    caps = dispatcher._session_caps.setdefault(session_id, _SessionCapabilities())
    caps.current_agent = arg
    # 中文注释：同 set_model，set_agent 不做后置校验，避免通过 resume/load 触发副作用。
    dispatcher._session_map_binding_manager.update_bound_agent(ctx.key, arg)
    return _command_outbound(
        ctx,
        content=f"Agent switched to: {arg}",
        reason="command_set_agent",
    )


def register_acp_builtin_commands(
    dispatcher: _DispatcherCommandPorts, router: CommandRouter
) -> None:
    """Register ACP command handlers with the shared command router API."""
    del dispatcher  # dispatcher instance is accessed via CommandContext.loop.
    router.exact("/help", _cmd_help)
    router.exact("/new", _cmd_new)
    router.exact("/models", _cmd_models)
    router.exact("/agents", _cmd_agents)
    router.exact("/set_model", _cmd_set_model)
    router.prefix("/set_model ", _cmd_set_model)
    router.exact("/set_agent", _cmd_set_agent)
    router.prefix("/set_agent ", _cmd_set_agent)


def pop_command_reason(metadata: dict[str, Any] | None) -> str:
    """Extract internal command reason tag before message leaves dispatcher."""
    if not metadata:
        return "command"
    reason = metadata.pop(_COMMAND_REASON_METADATA_KEY, None)
    if isinstance(reason, str) and reason:
        return reason
    return "command"
