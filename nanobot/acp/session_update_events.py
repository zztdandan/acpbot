"""session_update -> ACPProgressEvent 归一化。"""

from __future__ import annotations

import time
from typing import Any

from nanobot.acp.progress_event_types import ACPProgressEvent


def _pick(obj: Any, *names: str) -> Any:
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
        if isinstance(obj, dict) and name in obj:
            return obj[name]
    return None


def _to_jsonable(update: Any) -> Any:
    model_dump = getattr(update, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump(by_alias=True, exclude_none=True, mode="json")
        except Exception:
            pass
    if isinstance(update, dict):
        return update
    return {"raw": str(update)}


def _classify_family(update: Any, update_type: str) -> str:
    from acp.interfaces import AgentMessageChunk, AgentThoughtChunk, UserMessageChunk
    from acp.schema import EmbeddedResourceContentBlock, ImageContentBlock, ResourceContentBlock

    if isinstance(update, UserMessageChunk):
        return "user"
    if isinstance(update, AgentThoughtChunk):
        return "thought"
    if isinstance(update, AgentMessageChunk):
        content = _pick(update, "content")
        if isinstance(
            content, (ImageContentBlock, ResourceContentBlock, EmbeddedResourceContentBlock)
        ):
            return "media"
        text = _pick(content, "text")
        if isinstance(text, str):
            return "text"
        return "other"
    if update_type in {"ToolCallStart", "ToolCallProgress", "ToolCallUpdate"}:
        return "tool"
    if update_type == "UsageUpdate":
        return "usage"
    if update_type == "AgentPlanUpdate":
        return "plan"
    if update_type in {
        "CurrentModeUpdate",
        "ConfigOptionUpdate",
        "AvailableCommandsUpdate",
        "SessionInfoUpdate",
    }:
        return "state"
    return "other"


def _build_route_key(session_id: str, family: str, raw_json: Any) -> str:
    if family == "tool":
        tool_call_id = _pick(raw_json, "toolCallId", "tool_call_id")
        if isinstance(tool_call_id, str) and tool_call_id.strip():
            return tool_call_id.strip()
    if family == "media":
        resource = _pick(raw_json, "content")
        resource_key = _pick(resource, "uri", "name", "resourceUri", "resource_uri")
        if isinstance(resource_key, str) and resource_key.strip():
            return resource_key.strip()
    if family in {"state", "usage", "plan", "thought", "other", "text", "user"}:
        return session_id
    return session_id


def to_progress_event(*, session_id: str, update: Any) -> ACPProgressEvent:
    """将 ACP update 归一化为内部事件。"""

    raw_json = _to_jsonable(update)
    update_type = type(update).__name__
    family = _classify_family(update, update_type)
    route_key = _build_route_key(session_id, family, raw_json)
    extracted = {
        "tool_call_id": _pick(raw_json, "toolCallId", "tool_call_id"),
        "status": _pick(raw_json, "status"),
        "content_type": _pick(_pick(raw_json, "content"), "type"),
    }
    return ACPProgressEvent(
        session_id=session_id,
        raw_update=update,
        raw_json=raw_json,
        update_type=update_type,
        family=family,
        route_key=route_key,
        extracted=extracted,
        received_at_ms=int(time.time() * 1000),
    )
