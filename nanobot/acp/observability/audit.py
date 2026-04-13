"""Audit sink helpers for ACP observability events."""

from __future__ import annotations

from loguru import logger

from nanobot.acp.observability.queue import ObservabilityEvent


def write_audit_event(event: ObservabilityEvent) -> None:
    """Emit a concise structured audit line for ACP observability."""

    logger.debug(
        "ACP audit scope={} event={} request_key={} nanobot_side_session_key={} acp_side_session_id={} payload={}",
        event.scope,
        event.event,
        event.request_key or "-",
        event.nanobot_side_session_key or "-",
        event.acp_side_session_id or "-",
        event.payload,
    )
