"""Sessionmap models for binding truth and runtime-ready session entries."""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap


@dataclass(slots=True)
class SessionMapBindingEntry:
    """Persistent binding truth for one nanobot-side session."""

    cwd: str
    nanobot_side_session_key: str
    acp_side_session_id: str
    bound_model: str | None
    bound_agent: str | None
    updated_at: str
    revision: int

    def as_payload(self) -> JSONMap:
        payload: JSONMap = {
            "cwd": self.cwd,
            "nanobotSideSessionKey": self.nanobot_side_session_key,
            "acpSideSessionId": self.acp_side_session_id,
            "updatedAt": self.updated_at,
            "revision": self.revision,
        }
        if self.bound_model:
            payload["boundModel"] = self.bound_model
        if self.bound_agent:
            payload["boundAgent"] = self.bound_agent
        return payload


@dataclass(slots=True)
class SessionRuntimeEntry:
    """Current runtime-owned ready session entry."""

    nanobot_side_session_key: str
    acp_side_session_id: str
    ready: bool
    bound_model: str | None = None
    bound_agent: str | None = None
    capabilities: "_SessionCapabilities" = field(default_factory=lambda: _SessionCapabilities())


class _SessionCapabilities:
    """Runtime-only capability cache for one active ACP session."""

    def __init__(self) -> None:
        self.available_models: list[str] = []
        self.current_model: str | None = None
        self.available_agents: list[str] = []
        self.current_agent: str | None = None
