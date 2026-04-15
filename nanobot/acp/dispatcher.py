"""ACP dispatcher 兼容导出层：对外保留旧导入路径并转发到 runtime 实现。"""

from nanobot.acp.runtime import ACPDispatcher
from nanobot.config.paths import get_data_dir

ACPDispatcher.__module__ = "nanobot.acp.runtime"

__all__ = ["ACPDispatcher", "get_data_dir"]
