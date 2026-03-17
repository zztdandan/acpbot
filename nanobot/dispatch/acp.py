"""ACP 兼容导入层。

保留历史导入路径 `nanobot.dispatch.acp`，
内部实现已迁移到 `nanobot.acp`。
"""

from nanobot.acp.dispatcher import ACPDispatcher, _SessionCapabilities
from nanobot.acp.state import _ACPDispatchError

# 旧路径兼容导出，避免现有调用方中断。
__all__ = ["ACPDispatcher", "_SessionCapabilities", "_ACPDispatchError"]
