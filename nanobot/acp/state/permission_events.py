"""Permission event models owned by request-scoped state."""

from __future__ import annotations

from dataclasses import dataclass

from nanobot.acp.contracts import ACPPermissionOption, ACPToolCall


@dataclass(slots=True)
class PendingPermissionRequest:
    """Active permission request waiting for an inbound reply."""

    options: list[ACPPermissionOption]
    tool_call: ACPToolCall | None = None
    prompt_text: str = ""
