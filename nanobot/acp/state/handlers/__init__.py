"""处理器导出：集中暴露默认处理器链与抽象类型。"""

from __future__ import annotations

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.handlers.consume_only import build_consume_only_handlers
from nanobot.acp.state.handlers.message_media import AgentMessageMediaHandler
from nanobot.acp.state.handlers.message_text import AgentMessageTextHandler
from nanobot.acp.state.handlers.other import OtherUpdateHandler
from nanobot.acp.state.handlers.permission import PermissionHandler
from nanobot.acp.state.handlers.plan import PlanUpdateHandler
from nanobot.acp.state.handlers.thought import AgentThoughtHandler
from nanobot.acp.state.handlers.tool import ToolUpdateHandler


def build_default_handlers() -> list[StateUpdateHandler]:
    """按优先级构造默认处理器链；未知类型固定由末尾兜底分支收口。

    处理流程：
        - 先注册文本、媒体、思考、计划、工具等需要独立进度镜像的 handler
        - 再追加静态事实型 handler，统一收口不需要独立进度的 update
        - 最后追加 `OtherUpdateHandler`，保证任意 update 都能被消费
    """

    handlers: list[StateUpdateHandler] = [
        AgentMessageTextHandler(),
        AgentMessageMediaHandler(),
        AgentThoughtHandler(),
        PlanUpdateHandler(),
        ToolUpdateHandler(),
        PermissionHandler(),
    ]
    handlers.extend(build_consume_only_handlers())
    handlers.append(OtherUpdateHandler())
    return handlers


__all__ = [
    "AgentMessageMediaHandler",
    "AgentMessageTextHandler",
    "AgentThoughtHandler",
    "HandlerConsumeResult",
    "OtherUpdateHandler",
    "PlanUpdateHandler",
    "PermissionHandler",
    "StateUpdateHandler",
    "ToolUpdateHandler",
    "build_default_handlers",
]
