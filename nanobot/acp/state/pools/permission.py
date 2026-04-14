"""权限池实现：缓存当前权限提示文案，并对外镜像权限请求文本。"""

from __future__ import annotations

from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class PermissionPool(ACPPoolBase):
    """权限池：承载当前请求内尚未完成的权限提示。"""

    bucket_type = ACPBucketType.PERMISSION

    def __init__(self, *, bucket_key: str) -> None:
        """建立权限池；同一请求通常只保留一个活跃权限池。"""

        super().__init__(bucket_key=bucket_key)
        self.prompt = ""
        self._dirty = False

    def _accept(self, payload: object) -> None:
        """更新当前权限提示文本；仅保留最近一次提示。"""

        prompt = str(payload or "").strip()
        if not prompt:
            return
        self.prompt = prompt
        self._dirty = True

    def flush(self) -> FlushResult | None:
        """在权限提示有更新时输出一条 permission progress。"""

        if not self._dirty or not self.prompt:
            return None
        self._dirty = False
        return FlushResult(kind=ACPOutboundKind.PERMISSION, content=self.prompt)
