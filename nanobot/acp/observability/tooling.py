"""结构化观测事件记录层。"""

from __future__ import annotations

from loguru import logger

from nanobot.acp.observability.queue import ObservabilityEvent


def emit_tooling_event(event: ObservabilityEvent) -> None:
    """执行该方法定义的处理流程并返回结果。"""

    logger.info(
        "ACP event scope={} event={} request_key={} session={} acp_session={} payload_keys={}",
        event.scope,
        event.event,
        event.request_key or "-",
        event.nanobot_side_session_key or "-",
        event.acp_side_session_id or "-",
        sorted(event.payload.keys()),
    )
