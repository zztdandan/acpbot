"""结构化观测事件记录层。"""

from __future__ import annotations

from loguru import logger

from nanobot.acp.observability.queue import ObservabilityEvent


def write_audit_event(event: ObservabilityEvent) -> None:
    """执行该方法定义的处理流程并返回结果。"""

    logger.debug(
        "ACP audit scope={} event={} request_key={} nanobot_side_session_key={} acp_side_session_id={} payload={}",
        event.scope,
        event.event,
        event.request_key or "-",
        event.nanobot_side_session_key or "-",
        event.acp_side_session_id or "-",
        event.payload,
    )
