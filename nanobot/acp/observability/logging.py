"""人读观测输出层。"""

from __future__ import annotations

import json

from loguru import logger

from nanobot.acp.observability.queue import ObservabilityEvent


def _mask_value(value: str | None) -> str:
    """把长标识压缩为前四后四，提升控制台可读性。"""

    if not value:
        return "-"
    if len(value) <= 8:
        return value
    return f"{value[:4]}...{value[-4:]}"


def _payload_preview(payload: object) -> str:
    """仅展示 payload 前 30 字符，避免日志噪音。"""

    serialized = json.dumps(payload, ensure_ascii=False)
    if len(serialized) <= 30:
        return serialized
    return serialized[:30] + "..."


def emit_logging_event(event: ObservabilityEvent) -> None:
    """输出三行结构化人读日志：事件头、会话标识、payload 摘要。"""

    header_line = f"{event.scope}|{event.event}|{_mask_value(event.request_key)}"
    session_line = (
        f"nano:{_mask_value(event.nanobot_side_session_key)}"
        f"|acp:{_mask_value(event.acp_side_session_id)}"
    )
    payload_line = f"payload:{_payload_preview(event.payload)}"
    logger.info("ACP\n{}\n{}\n{}", header_line, session_line, payload_line)
