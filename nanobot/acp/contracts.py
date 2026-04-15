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
    """ACP 出站通道名枚举：约束 runtime 回发消息时可用的逻辑通道。"""

    CLI = "cli"
    SYSTEM = "system"


class ACPDirectIdentity(StrEnum):
    """direct 入口身份常量：统一 CLI 直连请求的 session/chat 标识。"""

    SESSION_KEY = "cli:direct"
    CHAT_ID = "direct"


class ACPPermissionPolicyName(StrEnum):
    """权限策略枚举：定义 ACP tool-call 自动决策使用的策略名。"""

    STRICT = "strict"
    TRUSTED = "trusted"


class ACPPermissionKind(StrEnum):
    """权限选项 kind 枚举：统一 allow always/once/cancelled 三种协议值。"""

    ALLOW_ALWAYS = "allow_always"
    ALLOW_ONCE = "allow_once"
    CANCELLED = "cancelled"


class ACPPermissionOutcome(StrEnum):
    """权限结果枚举：约束 permission_reply 载荷里的 outcome 字段。"""

    CANCELLED = "cancelled"
    SELECTED = "selected"


class ObservabilityScopeName(StrEnum):
    """观测域枚举：标记事件由 process/runtime/state 哪个 owner 产生。"""

    PROCESS = "process"
    RUNTIME = "runtime"
    STATE = "state"


class ObservabilityEventName(StrEnum):
    """观测事件名枚举：统一结构化审计与工具日志的事件关键字。"""

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
    """构造取消权限回复载荷；用于 strict 策略或超时拒绝场景。"""

    return {"outcome": {"outcome": ACPPermissionOutcome.CANCELLED.value}}


def build_permission_selected_payload(option_id: str) -> JSONMap:
    """构造选中权限回复载荷；用于把用户选择回传给 ACP 协议层。"""

    return {
        "outcome": {
            "outcome": ACPPermissionOutcome.SELECTED.value,
            "optionId": option_id,
        }
    }
