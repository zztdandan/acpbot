"""state handlers 导出：集中暴露默认 handler 集合与抽象类型。"""

from __future__ import annotations

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.handlers.consume_only import build_consume_only_handlers
from nanobot.acp.state.handlers.message_media import AgentMessageMediaHandler
from nanobot.acp.state.handlers.message_text import AgentMessageTextHandler
from nanobot.acp.state.handlers.other import OtherUpdateHandler
from nanobot.acp.state.handlers.plan import PlanUpdateHandler
from nanobot.acp.state.handlers.thought import AgentThoughtHandler
from nanobot.acp.state.handlers.tool import ToolUpdateHandler


def build_default_handlers() -> list[StateUpdateHandler]:
    """按优先级构造默认 handler 链；other handler 固定放在末尾兜底。"""

    handlers: list[StateUpdateHandler] = [
        AgentMessageTextHandler(),
        AgentMessageMediaHandler(),
        AgentThoughtHandler(),
        PlanUpdateHandler(),
        ToolUpdateHandler(),
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
    "StateUpdateHandler",
    "ToolUpdateHandler",
    "build_default_handlers",
]
