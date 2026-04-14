"""状态共享模型：定义请求级状态聚合需要的枚举、主键与刷新结果。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from nanobot.acp.contracts import JSONMap


class ACPUpdateType(str, Enum):
    """ACP 回流更新类型枚举：在状态路由阶段区分不同更新语义。

    职责：
        - 为处理器注册表提供稳定的语义标签
        - 约束状态域已知更新的分类边界，避免散落魔法字符串
    """

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
    """请求内池类型枚举：标记单请求范围内每类池的归属语义。

    职责：
        - 区分文本、工具、权限等不同池族，便于路由器决定复用与销毁策略
        - 与 `PoolKey` 一起组成请求内唯一索引，避免不同语义共用同一池
    """

    CONSUME_ONLY = "consume_only"
    MEDIA = "media"
    MESSAGE_TEXT = "message_text"
    OTHER = "other"
    PERMISSION = "permission"
    PLAN = "plan"
    THOUGHT = "thought"
    TOOL = "tool"


class ACPOutboundKind(str, Enum):
    """进度镜像片段类型枚举：描述一次刷新对外呈现的片段语义。

    职责：
        - 告诉 `ProgressRouter` 当前片段属于文本、计划、权限还是工具进度
        - 约束 `build_progress_payload` 的兼容键补齐逻辑只作用在正确片段上
    """

    MEDIA = "media"
    PERMISSION = "permission"
    PLAN = "plan"
    TEXT = "text"
    THOUGHT = "thought"
    TOOL = "tool"


@dataclass(frozen=True, slots=True)
class PoolKey:
    """池索引主键：在单请求生命周期内唯一定位一个池实例。

    职责：
        - 用 `bucket_type` 划分池族，保证不同语义不会互相覆盖
        - 用 `bucket_key` 区分同类池的多个实例，例如不同 `tool_call_id`
    """

    bucket_type: ACPBucketType  # 池所属的大类；决定该键落在哪个池族下。
    bucket_key: str  # 同类池内的实例键；例如 `tool:<tool_call_id>`。


@dataclass(slots=True)
class FlushResult:
    """池刷新结果：描述一次可对外镜像的片段事实。

    职责：
        - 承载某个池在当前时刻对外可见的文本、媒体与元数据增量
        - 作为 `ProgressRouter.emit` 与最终物化链路之间的统一交换格式
    """

    kind: ACPOutboundKind  # 片段语义类型；决定对外镜像时的兼容处理分支。
    content: str = ""  # 要镜像的文本内容；为空时通常只输出媒体或元数据。
    media: list[str] = field(default_factory=list)  # 要镜像的媒体路径列表。
    metadata: JSONMap = field(default_factory=dict)  # 随片段携带的补充事实。


@dataclass(slots=True)
class RequestScopeState:
    """单请求聚合事实：保存最终结果物化与异常回退共享的同一份状态源。

    职责：
        - 聚合当前请求已经确认的最终文本、媒体与元数据
        - 为失败链路保留 `partial_text`，避免中断后完全丢失已生成内容
    """

    final_text: str = ""  # 当前请求确认的最终文本；正常完成时直接用于最终物化。
    media_paths: list[str] = field(default_factory=list)  # 最终输出使用的合并媒体列表。
    message_media_paths: list[str] = field(
        default_factory=list
    )  # 消息正文直接产生的媒体；最终顺序优先。
    tool_media_paths: list[str] = field(
        default_factory=list
    )  # 工具调用产生的媒体；排在消息媒体之后。
    final_metadata: JSONMap = field(default_factory=dict)  # 当前请求显式采纳的最终结构化事实。
    partial_text: str = ""  # 异常中断时用于部分回退的文本快照。


class ACPPool(Protocol):
    """状态池统一协议：约束所有池都遵循同一套请求级生命周期。

    职责：
        - 暴露 `accept`、`flush`、`close`、`is_terminal` 四个统一入口
        - 让路由器能以多态方式驱动不同池，而不关心具体实现细节
    """

    def accept(self, payload: object) -> None:
        """接收一条输入并更新池内事实；供处理器在消费更新时写入。"""
        ...

    def flush(self) -> FlushResult | None:
        """返回当前可镜像片段；适用于普通增量镜像，不负责销毁池实例。"""
        ...

    def close(self) -> FlushResult | None:
        """执行关闭收尾并返回尾部片段；供状态关闭阶段统一尾刷。"""
        ...

    def is_terminal(self) -> bool:
        """判断池是否已进入终态；路由器会据此销毁对应索引。"""
        ...
