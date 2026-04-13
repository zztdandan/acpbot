"""权限事件模型。"""

from __future__ import annotations

from dataclasses import dataclass

from nanobot.acp.contracts import ACPPermissionOption, ACPToolCall


@dataclass(slots=True)
class PendingPermissionRequest:
    """等待用户回复的权限请求。"""

    options: list[ACPPermissionOption]
    # 可选权限项。
    tool_call: ACPToolCall | None = None
    # 关联工具调用信息。
    prompt_text: str = ""
    # 对外提示文本。
