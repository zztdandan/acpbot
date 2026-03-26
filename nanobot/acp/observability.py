"""ACP observability family 入口。"""

from __future__ import annotations

from nanobot.acp.observability_audit import _ACPObservabilityAuditMixin
from nanobot.acp.observability_tooling import _ACPObservabilityToolingMixin


class _ACPObservabilityMixin(_ACPObservabilityToolingMixin, _ACPObservabilityAuditMixin):
    """提供 dispatcher 的调试日志与审计文件能力。"""
