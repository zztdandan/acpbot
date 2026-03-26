"""ACP session_update 工具族处理器。"""

from __future__ import annotations

from typing import Any


async def _handle_tool_start(dispatcher: Any, session_id: str, state: Any, update: Any) -> None:
    """处理 tool start：审计、缓存 active tool、透传 tool hint。"""
    title = update.title or "tool"
    tool_name = dispatcher._extract_tool_name(update)
    # 中文注释：tool hint 事件保留完整 tool_call，避免前端只能看到 title 丢失上下文。
    tool_event = {
        "event": "tool_start",
        "session_id": session_id,
        "title": title,
        "status": "pending",
        "tool_name": tool_name,
        "tool_call": dispatcher._to_jsonable(update),
    }
    dispatcher._session_active_tool_name[session_id] = tool_name
    dispatcher._log_acp_json(
        event="acp_session_update_tool_start",
        payload={
            "session_id": session_id,
            "title": title,
            "tool_name": tool_name,
            "tool_call": update,
        },
    )
    await dispatcher._audit_tool_event(
        tool_name=tool_name,
        session_id=session_id,
        event="tool_start",
        payload={
            "title": title,
            "tool_call": update,
        },
    )
    await dispatcher._emit_progress(
        state.on_progress,
        title,
        tool_hint=True,
        tool_event=tool_event,
    )


async def _handle_tool_progress(dispatcher: Any, session_id: str, state: Any, update: Any) -> None:
    """处理 tool progress：状态审计与 tool hint 透传。"""
    status = getattr(update.status, "value", update.status) if update.status else None
    tool_name = dispatcher._extract_tool_name(update)
    if tool_name == "unknown_tool":
        # 中文注释：部分 ACP 实现的 progress 不带 tool_name，需回退到最近一次 tool_start 缓存。
        tool_name = dispatcher._session_active_tool_name.get(session_id, "unknown_tool")
    # 中文注释：progress 同样保留完整 tool_call，供上游按 toolCallId 归并事件。
    tool_event = {
        "event": "tool_progress",
        "session_id": session_id,
        "status": status,
        "tool_name": tool_name,
        "tool_call": dispatcher._to_jsonable(update),
    }
    dispatcher._log_acp_json(
        event="acp_session_update_tool_progress",
        payload={
            "session_id": session_id,
            "status": status,
            "tool_name": tool_name,
            "tool_call": update,
        },
    )
    await dispatcher._audit_tool_event(
        tool_name=tool_name,
        session_id=session_id,
        event="tool_progress",
        payload={
            "status": status,
            "tool_call": update,
        },
    )
    if status:
        await dispatcher._emit_progress(
            state.on_progress,
            status,
            tool_hint=True,
            tool_event=tool_event,
        )
