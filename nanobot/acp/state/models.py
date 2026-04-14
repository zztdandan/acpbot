"""状态共享模型：定义请求级池索引、刷新结果与运行时封装。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from nanobot.acp.contracts import JSONMap

if TYPE_CHECKING:
    from nanobot.acp.state.handlers.base import StateUpdateHandler


class ACPUpdateType(str, Enum):
    """ACP 回流更新类型枚举。"""

    AGENT_MESSAGE_TEXT = "agent_message_text"
    AGENT_MESSAGE_MEDIA = "agent_message_media"
    AGENT_THOUGHT = "agent_thought"
    AVAILABLE_COMMANDS = "available_commands"
    CONFIG_OPTION = "config_option"
    CURRENT_MODE = "current_mode"
    PERMISSION_REQUEST = "permission_request"
    PLAN = "plan"
    SESSION_INFO = "session_info"
    TOOL_PROGRESS = "tool_progress"
    TOOL_START = "tool_start"
    USAGE = "usage"
    USER_MESSAGE = "user_message"
    OTHER = "other"


class ACPBucketType(str, Enum):
    """请求内池类型枚举。"""

    CONSUME_ONLY = "consume_only"
    MEDIA = "media"
    MESSAGE_TEXT = "message_text"
    OTHER = "other"
    PERMISSION = "permission"
    PLAN = "plan"
    THOUGHT = "thought"
    TOOL = "tool"


class ACPOutboundKind(str, Enum):
    """进度片段语义枚举。"""

    MEDIA = "media"
    PERMISSION = "permission"
    PLAN = "plan"
    TEXT = "text"
    THOUGHT = "thought"
    TOOL = "tool"


@dataclass(frozen=True, slots=True)
class PoolKey:
    """池索引主键：在单请求生命周期内唯一定位一个池实例。"""

    bucket_type: ACPBucketType
    bucket_key: str


@dataclass(slots=True)
class FlushResult:
    """池刷新结果：描述一次可对外发布的文本、媒体与元数据。"""

    kind: ACPOutboundKind
    content: str = ""
    media: list[str] = field(default_factory=list)
    metadata: JSONMap = field(default_factory=dict)


@dataclass(slots=True)
class RequestScopeState:
    """单请求聚合事实：保存 final 物化与异常回退共享的状态源。"""

    final_text: str = ""
    media_paths: list[str] = field(default_factory=list)
    message_media_paths: list[str] = field(default_factory=list)
    final_metadata: JSONMap = field(default_factory=dict)
    partial_text: str = ""


class ACPPool(Protocol):
    """状态池统一协议：所有池都遵循同一套 accept/flush/死手约定。"""

    idle_timeout_seconds: float | None
    deadline_monotonic: float | None

    def accept(self, payload: object) -> None: ...

    def flush(self) -> FlushResult | None: ...

    def is_terminal(self) -> bool: ...


@dataclass(slots=True)
class PoolRuntimeEntry:
    """单池运行时封装：绑定池实例、owner handler、结构锁与 timeout handle。"""

    pool_key: PoolKey
    pool: ACPPool
    handler: StateUpdateHandler
    lock: asyncio.Lock
    timeout_handle: asyncio.TimerHandle | None = None
    timeout_token: int = 0
