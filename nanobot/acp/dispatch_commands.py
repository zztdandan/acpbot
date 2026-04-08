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
        old_session_id = dispatcher._session_map_binding_manager.clear_binding(session_key)
        if old_session_id:
            dispatcher._session_caps.pop(old_session_id, None)
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
                    content="no model name Usage: /set_model <model_id>",
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
        # 中文注释：删除 set_model 后置校验：
        # 1) ACP SDK 的 set_session_model 响应不返回当前 modelId；
        # 2) 可用真值仅能通过 resume/load 获取，但这会触发会话激活副作用，
        #    与“命令路径不触发 resume/load”的约束冲突。
        # 因此这里仅记录请求已发送并更新本地缓存，不做额外“伪校验”。
        # 中文注释：命令路径只做一次 SDK set；manager 仅更新绑定并持久化，不触发二次 set。
        dispatcher._session_map_binding_manager.update_bound_model(session_key, arg)
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
        # 中文注释：同 set_model，set_agent 不做后置校验，避免通过 resume/load 触发副作用。
        # 中文注释：命令路径只做一次 SDK set；manager 仅更新绑定并持久化，不触发二次 set。
        dispatcher._session_map_binding_manager.update_bound_agent(session_key, arg)
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
