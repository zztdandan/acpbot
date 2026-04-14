"""state 目录共享模型：统一描述更新类型、池主键与刷新结果。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from nanobot.acp.contracts import JSONMap


class ACPUpdateType(str, Enum):
    """ACP 回流更新类型枚举：供 handler 注册、匹配与 fallback 收口使用。"""

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
    """请求内池类型枚举：用于决定索引键、flush 策略与销毁方式。"""

    CONSUME_ONLY = "consume_only"
    MEDIA = "media"
    MESSAGE_TEXT = "message_text"
    OTHER = "other"
    PERMISSION = "permission"
    PLAN = "plan"
    THOUGHT = "thought"
    TOOL = "tool"


class ACPOutboundKind(str, Enum):
    """进度镜像片段类型：供 ProgressRouter 构造 on_progress 载荷时区分语义。"""

    MEDIA = "media"
    PERMISSION = "permission"
    PLAN = "plan"
    TEXT = "text"
    THOUGHT = "thought"
    TOOL = "tool"


@dataclass(frozen=True, slots=True)
class PoolKey:
    """池索引主键：用 bucket_type + bucket_key 唯一定位单个池实例。"""

    bucket_type: ACPBucketType
    # 池所属的大类。
    bucket_key: str
    # 同类池内的独立实例键；例如 `tool:<tool_call_id>`。


@dataclass(slots=True)
class FlushResult:
    """池刷新结果：描述一次可对外镜像的文本、媒体和元数据片段。"""

    kind: ACPOutboundKind
    # 片段语义类型。
    content: str = ""
    # 要镜像的文本内容。
    media: list[str] = field(default_factory=list)
    # 要镜像的媒体路径列表。
    metadata: JSONMap = field(default_factory=dict)
    # 随片段输出的补充元数据。


@dataclass(slots=True)
class RequestScopeState:
    """单请求聚合事实：供 final materialize 与 late fallback 共享同一份结果源。"""

    final_text: str = ""
    # 当前请求聚合出的最终文本。
    media_paths: list[str] = field(default_factory=list)
    # 当前请求聚合出的媒体路径。
    message_media_paths: list[str] = field(default_factory=list)
    # agent message/update 直接产生的媒体路径；最终结果排在工具附件之前。
    tool_media_paths: list[str] = field(default_factory=list)
    # tool_call/tool_call_update 产生的媒体路径；最终结果排在消息媒体之后。
    final_metadata: JSONMap = field(default_factory=dict)
    # 当前请求显式采纳的最终元数据。
    partial_text: str = ""
    # 异常中断时可回退的部分文本。


class ACPPool(Protocol):
    """状态池统一协议：所有池都必须支持 accept / flush / close / terminal 判定。"""

    def accept(self, payload: object) -> None:
        """接收一条输入并更新池内状态。"""
        ...

    def flush(self) -> FlushResult | None:
        """返回当前可镜像的片段；不销毁池实例。"""
        ...

    def close(self) -> FlushResult | None:
        """执行关闭收尾；供 state close 阶段统一尾刷。"""
        ...

    def is_terminal(self) -> bool:
        """判断池是否已到终态；router 会在终态后销毁索引。"""
        ...
