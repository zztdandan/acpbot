"""ACP SessionMap 模块：维护 nanobot 会话键与 ACP 会话 ID 的映射（持久化 + 运行时）。"""

from nanobot.acp.sessionmap.binding_manager import SessionMapBindingManager
from nanobot.acp.sessionmap.models import (
    SessionMapBindingEntry,
    SessionRuntimeEntry,
    _SessionCapabilities,
)
from nanobot.acp.sessionmap.runtime_manager import SessionRuntimeManager

# 导出公共 API（内部成员以下划线开头，不导出）
__all__ = [
    "SessionMapBindingEntry",  # 持久化绑定条目
    "SessionMapBindingManager",  # 绑定管理器（持久化）
    "SessionRuntimeEntry",  # 运行时会话条目
    "SessionRuntimeManager",  # 运行时管理器（内存）
    "_SessionCapabilities",  # 会话能力缓存（内部使用）
]
