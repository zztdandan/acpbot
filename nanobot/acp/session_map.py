"""ACP session_map family 入口。"""

from __future__ import annotations

from nanobot.acp.session_map_reconcile import _SessionMapReconcileMixin
from nanobot.acp.session_map_storage import _SessionMapStorageMixin


class _SessionMapSupport(_SessionMapStorageMixin, _SessionMapReconcileMixin):
    """为 ACPDispatcher 提供会话映射管理能力的混入类。"""

    # 中文注释：统一组合 storage/reconcile 两个子能力；
    # session map 持久化字段扩展（含 desired model/agent）在 storage mixin 内实现。
