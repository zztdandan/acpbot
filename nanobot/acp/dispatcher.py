"""ACP 分发导出层。

该模块仅保留历史导入路径兼容：
- `nanobot.acp.dispatcher.ACPDispatcher`

真实实现已迁移到 `nanobot.acp.dispatcher_core`。
"""

from nanobot.acp.dispatcher_core import ACPDispatcher
from nanobot.acp.state import _SessionCapabilities
from nanobot.config.paths import get_data_dir

# 中文注释：兼容历史导入路径下的 introspection（例如测试或调试输出依赖 __module__），
# 显式对齐到真实实现模块，避免包装层影响行为判断。
ACPDispatcher.__module__ = "nanobot.acp.dispatcher_core"

__all__ = ["ACPDispatcher", "_SessionCapabilities", "get_data_dir"]
