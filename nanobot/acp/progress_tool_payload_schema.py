"""Tool progress payload 归并与 history 裁剪。"""

from __future__ import annotations

from typing import Any

HISTORY_LIMIT = 100


def append_history(history: list[dict[str, Any]], item: dict[str, Any]) -> int:
    """追加 history，返回累计丢弃数。"""

    history.append(item)
    dropped = 0
    while len(history) > HISTORY_LIMIT:
        history.pop(0)
        dropped += 1
    return dropped


def build_tool_payload(
    *, route_key: str, latest: dict[str, Any], history: list[dict[str, Any]], history_dropped: int
) -> dict[str, Any]:
    """构建 progress/tool outbound payload。"""

    payload = dict(latest)
    payload.setdefault("tool_call_id", latest.get("tool_call_id") or route_key)
    payload["history"] = list(history)
    payload["history_dropped"] = history_dropped
    return payload
