"""ACP session_update 统一入口。

仅做 update -> ACPProgressEvent 归一化与分发，不在此层做业务拼装。
"""

from __future__ import annotations

from typing import Any

from nanobot.acp.session_update_events import to_progress_event
from nanobot.acp.state import ACPUpdateType, _SessionCapabilities


async def _route_session_update(dispatcher: Any, session_id: str, state: Any, update: Any) -> None:
    """统一把 ACP update 转成事件并交给 state 处理。"""
    event = to_progress_event(session_id=session_id, update=update)
    progress_router = dispatcher._session_state_routers.get(session_id)
    if progress_router is not None:
        event.ext["request_scope_id"] = dispatcher._session_request_scope_ids.get(
            session_id, session_id
        )

    if event.family == "media":
        # 中文注释：媒体落盘仍在 session_update 层兜底执行，保证手工注入 state 的测试路径不退化。
        content = getattr(update, "content", None)
        media_path = dispatcher._extract_agent_media_path(session_id, content)
        if media_path:
            if progress_router is not None:
                event.ext["media_path"] = media_path
            else:
                state.add_media(media_path)

    if event.family == "tool":
        status = str(event.extracted.get("status") or "").strip().lower()
        tool_name = dispatcher._extract_tool_name(update)
        tool_event_name = (
            "tool_start" if event.update_type == ACPUpdateType.TOOL_CALL_START else "tool_progress"
        )
        dispatcher._log_acp_json(
            event=f"acp_session_update_{tool_event_name}",
            payload={
                "session_id": session_id,
                "tool_name": tool_name,
                "status": status,
                "tool_call": update,
            },
        )
        await dispatcher._audit_tool_event(
            tool_name=tool_name,
            session_id=session_id,
            event=tool_event_name,
            payload={
                "status": status,
                "tool_name": tool_name,
                "tool_call": update,
            },
        )
        if tool_name and tool_name != "unknown_tool":
            dispatcher._session_active_tool_name[session_id] = tool_name
        if status == "completed":
            media_path = dispatcher._extract_tool_output_media_path(update)
            if media_path:
                if progress_router is not None:
                    event.ext["tool_output_media_path"] = media_path
                else:
                    state.add_media(media_path)

    if progress_router is None:
        raise RuntimeError(f"Missing session state router for active session {session_id}")
    await progress_router.on_progress_event(event)

    # 中文注释：能力缓存仍在 router 轻量更新，避免影响已有 /models /agents 输出路径。
    current_mode = getattr(update, "current_mode_id", None)
    if isinstance(current_mode, str) and current_mode.strip():
        caps = dispatcher._session_caps.setdefault(session_id, _SessionCapabilities())
        caps.current_agent = current_mode
