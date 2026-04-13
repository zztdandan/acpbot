"""ACP state package exports and compatibility types."""

from __future__ import annotations

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


class _SessionCapabilities:
    """Compatibility capability cache preserved for selection commands and replay."""

    def __init__(self) -> None:
        self.available_models: list[str] = []
        self.current_model: str | None = None
        self.available_agents: list[str] = []
        self.current_agent: str | None = None


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
