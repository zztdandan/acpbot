from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class PermissionRequestEvent:
    options: list[Any]
    session_id: str
    tool_call: Any


@dataclass(slots=True)
class PermissionReplyEvent:
    session_id: str
    request_id: str
    token: str
    source: str
    session_key: str = ""


@dataclass(slots=True)
class PermissionTimeoutEvent:
    session_id: str
    request_id: str
