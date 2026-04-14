"""该模块承接重构后的职责边界。"""

from nanobot.acp.runtime import ACPDispatcher
from nanobot.config.paths import get_data_dir

ACPDispatcher.__module__ = "nanobot.acp.runtime"

__all__ = ["ACPDispatcher", "get_data_dir"]
