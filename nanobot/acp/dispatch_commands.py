"""ACP slash 命令路由辅助。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from nanobot.acp.dispatcher_ports import _DispatcherCommandPorts
from nanobot.acp.state import _SessionCapabilities
from nanobot.bus.events import InboundMessage, OutboundMessage


async def _verify_session_selection_with_retry(
    dispatcher: _DispatcherCommandPorts,
    *,
    session_id: str,
    read_current: Callable[[_SessionCapabilities], str | None],
    expected: str,
    attempts: int = 4,
    retry_delay_seconds: float = 0.12,
) -> tuple[bool, str | None, bool]:
    """刷新并校验 session 选择值；返回(是否匹配, 当前值, 是否拿到服务端状态)。"""
    caps = dispatcher._session_caps.setdefault(session_id, _SessionCapabilities())
    saw_server_state = False
    for attempt in range(attempts):
        # 中文注释：set_session_* 在部分 ACP 实现中是“异步生效”，这里做短轮询避免误报失败。
        refreshed = await dispatcher._refresh_session_caps_from_server(session_id)
        saw_server_state = saw_server_state or refreshed
        current = read_current(caps)
        if current == expected:
            return True, current, saw_server_state
        if attempt < attempts - 1:
            await asyncio.sleep(retry_delay_seconds)
    return False, read_current(caps), saw_server_state


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
        # 中文注释：会话重建必须清理上一会话 desired，避免 model/agent 选择跨会话污染。
        dispatcher._session_desired.pop(session_key, None)
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
        # 中文注释：SDK 的 set_session_model 仅确认请求已受理，最终状态需通过会话信息回读确认。
        verified, current_model, saw_server_state = await _verify_session_selection_with_retry(
            dispatcher,
            session_id=session_id,
            read_current=lambda current_caps: current_caps.current_model,
            expected=arg,
        )
        if saw_server_state and not verified and current_model:
            await dispatcher._publish_outbound_with_debug(
                msg=OutboundMessage(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    content=(
                        "Model switch verification failed. "
                        f"Requested: {arg}; ACP current: {current_model}"
                    ),
                ),
                reason="command_set_model_verify_failed",
                session_key=session_key,
            )
            return True
        desired = dispatcher._session_desired.setdefault(session_key, {})
        desired["model"] = current_model or arg
        # 中文注释：/set_model 成功后立即更新并落盘 desired，确保重启或重连后激活可回放目标模型。
        dispatcher._persist_session_map()
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=f"Model switched to: {current_model or arg}",
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
        # 中文注释：mode 切换沿用同一校验策略，降低“已受理但状态尚未刷新”导致的误判。
        verified, current_agent, saw_server_state = await _verify_session_selection_with_retry(
            dispatcher,
            session_id=session_id,
            read_current=lambda current_caps: current_caps.current_agent,
            expected=arg,
        )
        if saw_server_state and not verified and current_agent:
            await dispatcher._publish_outbound_with_debug(
                msg=OutboundMessage(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    content=(
                        "Agent switch verification failed. "
                        f"Requested: {arg}; ACP current: {current_agent}"
                    ),
                ),
                reason="command_set_agent_verify_failed",
                session_key=session_key,
            )
            return True
        desired = dispatcher._session_desired.setdefault(session_key, {})
        desired["agent"] = current_agent or arg
        # 中文注释：/set_agent 成功后立即更新并落盘 desired，确保后续激活会按期望 agent 回放。
        dispatcher._persist_session_map()
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=f"Agent switched to: {current_agent or arg}",
            ),
            reason="command_set_agent",
            session_key=session_key,
        )
        return True

    return False
