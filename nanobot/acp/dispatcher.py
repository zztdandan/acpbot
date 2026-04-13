"""ACP dispatcher compatibility export.

The runtime rearchitecture moves the real owner into `nanobot.acp.runtime`, while
this module keeps the historic import path stable for the rest of nanobot.
"""

from nanobot.acp.runtime import ACPDispatcher
from nanobot.acp.state import _SessionCapabilities
from nanobot.config.paths import get_data_dir

# 中文注释：兼容历史导入路径下的 introspection（例如测试或调试输出依赖 __module__），
# 显式对齐到真实实现模块，避免包装层影响行为判断。
ACPDispatcher.__module__ = "nanobot.acp.runtime"

__all__ = ["ACPDispatcher", "_SessionCapabilities", "get_data_dir"]
