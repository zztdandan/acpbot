"""媒体池实现：聚合媒体路径并按增量列表镜像。"""

from __future__ import annotations

from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class MediaPool(ACPPoolBase):
    """媒体池：收集单请求内已产生的媒体路径，并支持按新增路径 flush。"""

    bucket_type = ACPBucketType.MEDIA

    def __init__(self, *, bucket_key: str) -> None:
        """建立媒体池；bucket_key 用于区分不同来源的媒体流。"""

        super().__init__(bucket_key=bucket_key)
        self.media_paths: list[str] = []
        self._last_flushed_count = 0

    def _accept(self, payload: object) -> None:
        """写入媒体路径；空值与重复路径都会被忽略。"""

        path = str(payload or "").strip()
        if path and path not in self.media_paths:
            self.media_paths.append(path)

    def flush(self) -> FlushResult | None:
        """返回自上次 flush 后新增的媒体路径列表。"""

        if len(self.media_paths) <= self._last_flushed_count:
            return None
        flushed = self.media_paths[self._last_flushed_count :]
        self._last_flushed_count = len(self.media_paths)
        return FlushResult(kind=ACPOutboundKind.MEDIA, media=list(flushed))
