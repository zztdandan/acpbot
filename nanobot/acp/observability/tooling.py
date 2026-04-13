"""Additional tooling hooks for ACP observability events."""

from __future__ import annotations

from loguru import logger

from nanobot.acp.observability.queue import ObservabilityEvent


def emit_tooling_event(event: ObservabilityEvent) -> None:
    """Emit an info-level tooling summary for ACP event consumers."""

    logger.info(
        "ACP event scope={} event={} request_key={} session={} acp_session={} payload_keys={}",
        event.scope,
        event.event,
        event.request_key or "-",
        event.nanobot_side_session_key or "-",
        event.acp_side_session_id or "-",
        sorted(event.payload.keys()),
    )
