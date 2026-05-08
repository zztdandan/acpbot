"""媒体池实现：聚合归一化后的 resource-link 结果，并按新增本地路径镜像。"""

from __future__ import annotations

from nanobot.acp.state.handlers.media_resource import ResolvedMediaResource
from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class MediaPool(ACPPoolBase):
    """媒体池：收集归一化媒体资源，并按新增本地路径刷新给外部。"""

    bucket_type = ACPBucketType.MEDIA
    idle_timeout_seconds: float | None = 1.6

    def __init__(self, *, bucket_key: str) -> None:
        """建立媒体池；池键用于区分不同来源的媒体流。"""

        super().__init__(bucket_key=bucket_key)
        self.resources: list[ResolvedMediaResource] = []
        self._last_flushed_count = 0

    def _accept(self, payload: object) -> bool:
        """写入归一化媒体资源；空路径与重复路径都会被忽略。"""

        if not isinstance(payload, ResolvedMediaResource):
            return False
        if payload.local_path and payload.local_path not in {
            resource.local_path for resource in self.resources
        }:
            self.resources.append(payload)
            return True
        return False

    def flush(self) -> FlushResult | None:
        """返回自上次刷新后新增的媒体路径列表。"""

        if len(self.resources) <= self._last_flushed_count:
            return None
        flushed = self.resources[self._last_flushed_count :]
        self._last_flushed_count = len(self.resources)
        return FlushResult(
            kind=ACPOutboundKind.MEDIA,
            media=[resource.local_path for resource in flushed],
        )
