"""ACP state package exports and compatibility types."""

from __future__ import annotations

from nanobot.acp.sessionmap.models import _SessionCapabilities
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.models import (
    ACPBucketType,
    ACPOutboundKind,
    ACPUpdateType,
    FlushResult,
    RequestScopeState,
)
from nanobot.acp.state.router import ProgressRouter


class _ACPDispatchError(RuntimeError):
    """Compatibility error used to surface partial ACP output when execution fails."""

    def __init__(self, partial_response: str = "") -> None:
        super().__init__("ACP dispatch failed")
        self.partial_response = partial_response


__all__ = [
    "ACPBucketType",
    "ACPOutboundKind",
    "ACPUpdateType",
    "FlushResult",
    "ProgressRouter",
    "RequestScopeState",
    "SessionStateManager",
    "_ACPDispatchError",
    "_SessionCapabilities",
]
