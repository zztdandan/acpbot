"""共享类型、枚举与载荷构造工具。"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, TypeAlias

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
JSONMap: TypeAlias = dict[str, JSONValue]
ACPExtPayload: TypeAlias = JSONMap
ACPArtifactMap: TypeAlias = dict[str, object]
ACPArtifactList: TypeAlias = list[ACPArtifactMap]

ACPCallbackUpdate: TypeAlias = Any
ACPPermissionOption: TypeAlias = Any
ACPToolCall: TypeAlias = Any
ACPSessionPayload: TypeAlias = Any
ACPFactoryValue: TypeAlias = Any
ACPResourceBlock: TypeAlias = Any


class ACPChannelName(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

    CLI = "cli"
    SYSTEM = "system"


class ACPDirectIdentity(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

    SESSION_KEY = "cli:direct"
    CHAT_ID = "direct"


class ACPPermissionPolicyName(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

    STRICT = "strict"
    TRUSTED = "trusted"


class ACPPermissionKind(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

    ALLOW_ALWAYS = "allow_always"
    ALLOW_ONCE = "allow_once"
    CANCELLED = "cancelled"


class ACPPermissionOutcome(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

    CANCELLED = "cancelled"
    SELECTED = "selected"


class ObservabilityScopeName(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

    PROCESS = "process"
    RUNTIME = "runtime"
    STATE = "state"


class ObservabilityEventName(StrEnum):
    """负责本对象定义的职责边界与生命周期。"""

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
    """执行该方法定义的处理流程并返回结果。"""

    return {"outcome": {"outcome": ACPPermissionOutcome.CANCELLED.value}}


def build_permission_selected_payload(option_id: str) -> JSONMap:
    """执行该方法定义的处理流程并返回结果。"""

    return {
        "outcome": {
            "outcome": ACPPermissionOutcome.SELECTED.value,
            "optionId": option_id,
        }
    }
