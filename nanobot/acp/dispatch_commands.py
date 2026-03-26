"""ACP slash 命令路由辅助。"""

from __future__ import annotations

from nanobot.acp.dispatcher_ports import _DispatcherCommandPorts
from nanobot.acp.state import _SessionCapabilities
from nanobot.bus.events import InboundMessage, OutboundMessage


async def _handle_slash_command(
    dispatcher: _DispatcherCommandPorts,
    *,
    msg: InboundMessage,
    session_key: str,
    command: str,
    arg: str,
) -> bool:
    """处理 ACP slash 命令；返回 True 表示命令已消费。"""
    if command == "/help":
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=dispatcher._HELP_TEXT,
            ),
            reason="command_help",
            session_key=session_key,
        )
        return True

    if command == "/new":
        # 中文注释：/new 仅删除当前映射并同步落盘，保持既有会话重建行为。
        old_session_id = dispatcher._session_map.pop(session_key, None)
        if old_session_id:
            dispatcher._session_caps.pop(old_session_id, None)
            dispatcher._persist_session_map()
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content="New session started.",
            ),
            reason="command_new",
            session_key=session_key,
        )
        return True

    if command == "/models":
        session_id = await dispatcher._ensure_session(session_key)
        content = await dispatcher._list_models_command(session_id)
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=content),
            reason="command_models",
            session_key=session_key,
        )
        return True

    if command == "/agents":
        session_id = await dispatcher._ensure_session(session_key)
        content = await dispatcher._list_agents_command(session_id)
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=content),
            reason="command_agents",
            session_key=session_key,
        )
        return True

    if command == "/set_model":
        if not arg:
            await dispatcher._publish_outbound_with_debug(
                msg=OutboundMessage(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    content="Usage: /set_model <model_id>",
                ),
                reason="command_set_model_usage",
                session_key=session_key,
            )
            return True
        await dispatcher._ensure_connection()
        if dispatcher._conn is None:
            raise RuntimeError("ACP connection is not available")
        session_id = await dispatcher._ensure_session(session_key)
        await dispatcher._conn.set_session_model(model_id=arg, session_id=session_id)
        caps = dispatcher._session_caps.setdefault(session_id, _SessionCapabilities())
        caps.current_model = arg
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=f"Model switched to: {arg}",
            ),
            reason="command_set_model",
            session_key=session_key,
        )
        return True

    if command == "/set_agent":
        if not arg:
            await dispatcher._publish_outbound_with_debug(
                msg=OutboundMessage(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    content="Usage: /set_agent <agent_id>",
                ),
                reason="command_set_agent_usage",
                session_key=session_key,
            )
            return True
        await dispatcher._ensure_connection()
        if dispatcher._conn is None:
            raise RuntimeError("ACP connection is not available")
        session_id = await dispatcher._ensure_session(session_key)
        await dispatcher._conn.set_session_mode(mode_id=arg, session_id=session_id)
        caps = dispatcher._session_caps.setdefault(session_id, _SessionCapabilities())
        caps.current_agent = arg
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=f"Agent switched to: {arg}",
            ),
            reason="command_set_agent",
            session_key=session_key,
        )
        return True

    return False
