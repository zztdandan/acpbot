"""ACP session_map family 入口。"""

from __future__ import annotations

from nanobot.acp.session_map_reconcile import _SessionMapReconcileMixin
from nanobot.acp.session_map_storage import _SessionMapStorageMixin


class _SessionMapSupport(_SessionMapStorageMixin, _SessionMapReconcileMixin):
    """为 ACPDispatcher 提供会话映射管理能力的混入类。"""
