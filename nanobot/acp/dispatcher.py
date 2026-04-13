"""ACP dispatcher compatibility export.

The runtime rearchitecture moves the real owner into `nanobot.acp.runtime`, while
this module keeps the historic import path stable for the rest of nanobot.
"""

from nanobot.acp.runtime import ACPDispatcher
from nanobot.acp.state import _SessionCapabilities
from nanobot.config.paths import get_data_dir

# Preserve historical import-path introspection (for example, tests or debug output
# that check `__module__`) by aligning it to the real runtime implementation module.
ACPDispatcher.__module__ = "nanobot.acp.runtime"

__all__ = ["ACPDispatcher", "_SessionCapabilities", "get_data_dir"]
