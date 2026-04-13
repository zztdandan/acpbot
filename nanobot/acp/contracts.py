"""Shared ACP typing aliases, enums, and wire-shape helpers.

This module centralizes the few places where ACP must stay dynamic because the
installed SDK exposes version-dependent runtime objects that local code cannot
name precisely without creating import cycles or hard SDK coupling.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, TypeAlias

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
JSONMap: TypeAlias = dict[str, JSONValue]
ACPExtPayload: TypeAlias = JSONMap
ACPArtifactMap: TypeAlias = dict[str, object]
ACPArtifactList: TypeAlias = list[ACPArtifactMap]

# ACP callback updates come from SDK-owned schema classes that vary by callback kind
# and SDK version, so callers intentionally narrow them with runtime checks.
ACPCallbackUpdate: TypeAlias = Any
# ACP permission options are SDK-owned objects inspected structurally (`kind`,
# `option_id`, `label`) rather than through a stable local class.
ACPPermissionOption: TypeAlias = Any
# ACP tool-call payloads are only relayed back to the SDK permission response path.
ACPToolCall: TypeAlias = Any
# ACP session payloads can be dataclasses, pydantic models, or plain dicts.
ACPSessionPayload: TypeAlias = Any
# Lazy-imported ACP factory helpers return SDK-owned objects without local stubs.
ACPFactoryValue: TypeAlias = Any
# ACP resource blocks are heterogeneous SDK objects read through attribute probing.
ACPResourceBlock: TypeAlias = Any


class ACPChannelName(StrEnum):
    """Known inbound/outbound channel literals used by ACP runtime."""

    CLI = "cli"
    SYSTEM = "system"


class ACPDirectIdentity(StrEnum):
    """Special direct-entry identity values shared across runtime models."""

    SESSION_KEY = "cli:direct"
    CHAT_ID = "direct"


class ACPPermissionPolicyName(StrEnum):
    """Known ACP permission policy values consumed from config."""

    STRICT = "strict"
    TRUSTED = "trusted"


class ACPPermissionKind(StrEnum):
    """Stable permission option kinds returned by ACP."""

    ALLOW_ALWAYS = "allow_always"
    ALLOW_ONCE = "allow_once"
    CANCELLED = "cancelled"


class ACPPermissionOutcome(StrEnum):
    """Permission outcomes encoded in ACP wire payloads."""

    CANCELLED = "cancelled"
    SELECTED = "selected"


class ObservabilityScopeName(StrEnum):
    """Finite set of owner scopes that emit ACP observability events."""

    PROCESS = "process"
    RUNTIME = "runtime"
    STATE = "state"


class ObservabilityEventName(StrEnum):
    """Current observability event names emitted from ACP runtime owners."""

    CONNECTION_READY = "connection_ready"
    CONNECTION_RESET = "connection_reset"
    DISPATCH_INBOUND_ERROR = "dispatch_inbound_error"
    LATE_SESSION_UPDATE = "late_session_update"
    ORPHAN_PERMISSION_REQUEST = "orphan_permission_request"
    ORPHAN_SESSION_UPDATE = "orphan_session_update"
    PERMISSION_REPLY_NOT_FOUND = "permission_reply_not_found"
    PERMISSION_TIMEOUT = "permission_timeout"
    REQUEST_ACTIVE = "request_active"
    REQUEST_FINISHED = "request_finished"
    REQUEST_QUEUED = "request_queued"


def build_permission_cancelled_payload() -> JSONMap:
    """Return the fixed ACP wire payload for a cancelled permission outcome.

    The nested `outcome` keys intentionally match the ACP schema field names, so
    this helper keeps that protocol-specific string shape in one obvious place.
    """

    return {"outcome": {"outcome": ACPPermissionOutcome.CANCELLED.value}}


def build_permission_selected_payload(option_id: str) -> JSONMap:
    """Return the fixed ACP wire payload for a selected permission option.

    `optionId` intentionally keeps ACP's camelCase wire field instead of a local
    rename because the payload is handed directly to the SDK validator.
    """

    return {
        "outcome": {
            "outcome": ACPPermissionOutcome.SELECTED.value,
            "optionId": option_id,
        }
    }
