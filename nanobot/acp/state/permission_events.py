"""权限事件模型：描述单请求内等待用户回复的权限事实。"""

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
