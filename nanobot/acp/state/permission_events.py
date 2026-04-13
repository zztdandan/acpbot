"""Permission event models owned by request-scoped state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class PendingPermissionRequest:
    """Active permission request waiting for an inbound reply."""

    options: list[Any]
    tool_call: Any | None = None
    prompt_text: str = ""
