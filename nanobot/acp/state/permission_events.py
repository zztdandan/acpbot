"""权限事件模型：描述请求内权限等待、回复与 handler 路由所需的最小事实。"""

from __future__ import annotations

from dataclasses import dataclass

from nanobot.acp.contracts import ACPPermissionOption, ACPToolCall


@dataclass(slots=True)
class PendingPermissionRequest:
    """待回复权限请求：保存当前权限等待分支需要的上下文。

    职责：
        - 持有可选权限项，供 reply 解析时做编号或 kind 匹配
        - 持有提示文本与关联工具信息，供进度镜像和审计复用
    """

    options: list[ACPPermissionOption]  # 当前权限请求可供用户选择的全部选项。
    tool_call: ACPToolCall | None = None  # 触发本次权限等待的工具调用上下文，可为空。
    prompt_text: str = ""  # 已渲染的提示文本；供 on_progress 与直接回显复用。


@dataclass(slots=True)
class PermissionRequestEvent:
    """权限请求事件：让 permission 流也能走统一 handler/pool/router 链路。"""

    pending_request: PendingPermissionRequest  # 当前新建的待回复权限请求。


@dataclass(slots=True)
class PermissionReplyEvent:
    """权限回复事件：承载用户原始回复文本，交由 permission handler 消费。"""

    reply_text: str  # 用户回复的原始文本。
