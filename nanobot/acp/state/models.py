"""状态层基础模型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from nanobot.acp.contracts import JSONMap


class ACPUpdateType(str, Enum):
    """状态路由识别的更新类型。"""

    AGENT_MESSAGE_TEXT = "agent_message_text"
    AGENT_MESSAGE_MEDIA = "agent_message_media"
    TOOL_START = "tool_start"
    TOOL_PROGRESS = "tool_progress"
    PERMISSION_REQUEST = "permission_request"
    OTHER = "other"


class ACPBucketType(str, Enum):
    """请求内池分桶类型。"""

    MESSAGE_TEXT = "message_text"
    MEDIA = "media"
    TOOL = "tool"
    PERMISSION = "permission"
    OTHER = "other"


class ACPOutboundKind(str, Enum):
    """进度镜像可输出的片段类型。"""

    TEXT = "text"
    MEDIA = "media"
    TOOL = "tool"
    PERMISSION = "permission"


@dataclass(slots=True)
class FlushResult:
    """一次池刷新后的输出结果。"""

    kind: ACPOutboundKind
    # 输出片段类型。
    content: str = ""
    # 文本输出。
    media: list[str] = field(default_factory=list)
    # 媒体输出。
    metadata: JSONMap = field(default_factory=dict)
    # 附带元数据。


@dataclass(slots=True)
class RequestScopeState:
    """单请求聚合事实。"""

    final_text: str = ""
    # 最终文本。
    media_paths: list[str] = field(default_factory=list)
    # 最终媒体路径集合。
    final_metadata: JSONMap = field(default_factory=dict)
    # 最终元数据。
    partial_text: str = ""
    # 异常场景下可回退的部分文本。


class ACPPool(Protocol):
    """状态池统一协议。"""

    def accept(self, payload: object) -> None:
        """接收一条输入并更新池内状态。"""
        ...

    def flush(self) -> FlushResult | None:
        """刷新当前可发布内容，不销毁池。"""
        ...

    def close(self) -> FlushResult | None:
        """关闭池并返回尾部输出。"""
        ...

    def is_terminal(self) -> bool:
        """判断池是否进入终态。"""
        ...
