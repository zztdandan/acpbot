"""ACP outbound content/metadata 组装规范。"""

from __future__ import annotations

import html
from typing import Any


def _base_metadata(
    *,
    kind: str,
    session_id: str,
    route_key: str,
    update_type: str | None,
    flush_reason: str,
    payload: Any,
) -> dict[str, Any]:
    return {
        "_progress": True,
        "_acp_kind": kind,
        "_acp_version": "1",
        "_acp_session_id": session_id,
        "_acp_route_key": route_key,
        "_acp_update_type": update_type,
        "_acp_flush_reason": flush_reason,
        "_acp_payload": payload,
    }


def build_progress_text(
    *, text: str, session_id: str, route_key: str, flush_reason: str
) -> tuple[str, dict[str, Any]]:
    metadata = _base_metadata(
        kind="text",
        session_id=session_id,
        route_key=route_key,
        update_type="AgentMessageChunk",
        flush_reason=flush_reason,
        payload={"text_chars": len(text)},
    )
    return text, metadata


def _xml(summary: str, tag: str, attrs: dict[str, Any], body: str = "") -> str:
    attrs_s = " ".join(
        f'{k}="{html.escape(str(v), quote=True)}"'
        for k, v in attrs.items()
        if v is not None and str(v) != ""
    )
    opening = f"<{tag}{(' ' + attrs_s) if attrs_s else ''}>"
    escaped_body = html.escape(body)
    return f"{summary}\n{opening}{escaped_body}</{tag}>"


def build_progress_tool(
    *,
    payload: dict[str, Any],
    session_id: str,
    route_key: str,
    update_type: str,
    flush_reason: str,
) -> tuple[str, dict[str, Any]]:
    status = payload.get("status")
    title = payload.get("title") or payload.get("tool_name") or "tool"
    summary = f"[tool] {title} status={status or 'unknown'}"
    content = _xml(
        summary,
        "tool-progress",
        {
            "tool_call_id": payload.get("tool_call_id") or route_key,
            "status": status,
            "update_type": update_type,
        },
        body=str(payload),
    )
    metadata = _base_metadata(
        kind="tool",
        session_id=session_id,
        route_key=route_key,
        update_type=update_type,
        flush_reason=flush_reason,
        payload=payload,
    )
    return content, metadata


def build_progress_media(
    *,
    payload: dict[str, Any],
    session_id: str,
    route_key: str,
    update_type: str,
    flush_reason: str,
) -> tuple[str, dict[str, Any]]:
    summary = "[media] received media update"
    content = _xml(summary, "media-progress", {"update_type": update_type}, body=str(payload))
    metadata = _base_metadata(
        kind="media",
        session_id=session_id,
        route_key=route_key,
        update_type=update_type,
        flush_reason=flush_reason,
        payload=payload,
    )
    return content, metadata


def build_progress_other(
    *,
    payload: dict[str, Any],
    session_id: str,
    route_key: str,
    update_type: str,
    flush_reason: str,
) -> tuple[str, dict[str, Any]]:
    summary = f"[state] {update_type}"
    content = _xml(summary, "state-progress", {"update_type": update_type}, body=str(payload))
    metadata = _base_metadata(
        kind="other",
        session_id=session_id,
        route_key=route_key,
        update_type=update_type,
        flush_reason=flush_reason,
        payload=payload,
    )
    return content, metadata


def build_permission_request(
    *,
    request_id: str,
    options: list[dict[str, Any]],
    timeout_seconds: int,
    expires_at: float,
    session_id: str,
    route_key: str,
    flush_reason: str,
) -> tuple[str, dict[str, Any]]:
    summary = "[permission] please reply with option index or option_id"
    content = _xml(
        summary,
        "permission-request",
        {"request_id": request_id, "timeout_seconds": timeout_seconds},
        body=str({"options": options}),
    )
    payload = {
        "request_id": request_id,
        "options_ordered": options,
        "timeout_seconds": timeout_seconds,
        "expires_at": expires_at,
    }
    metadata = _base_metadata(
        kind="permission/request",
        session_id=session_id,
        route_key=route_key,
        update_type=None,
        flush_reason=flush_reason,
        payload=payload,
    )
    metadata.update(payload)
    return content, metadata


def build_permission_ack(
    *,
    request_id: str,
    selected_option_id: str | None,
    decision_source: str,
    session_id: str,
    route_key: str,
    flush_reason: str,
) -> tuple[str, dict[str, Any]]:
    summary = f"[permission] decision received for {request_id}"
    content = _xml(
        summary,
        "permission-ack",
        {
            "request_id": request_id,
            "selected_option_id": selected_option_id,
            "source": decision_source,
        },
        body="",
    )
    payload = {
        "request_id": request_id,
        "decision_source": decision_source,
        "selected_option_id": selected_option_id,
    }
    metadata = _base_metadata(
        kind="permission/ack",
        session_id=session_id,
        route_key=route_key,
        update_type=None,
        flush_reason=flush_reason,
        payload=payload,
    )
    metadata.update(payload)
    return content, metadata
