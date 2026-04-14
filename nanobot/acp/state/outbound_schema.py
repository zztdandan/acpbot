"""progress 出包辅助：把池 flush 结果转换为 on_progress 兼容载荷。"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPOutboundKind, FlushResult


def build_progress_payload(result: FlushResult) -> tuple[str, JSONMap, list[str]]:
    """把 FlushResult 转成统一 progress 载荷；供 ProgressRouter 镜像输出时复用。

    处理流程：
        - 复制 flush metadata，避免下游回调意外修改池内状态
        - tool 片段补齐 `_tool_hint` 兼容键，保持旧 on_progress 行为不回退
        - 返回 `(content, metadata, media)` 三元组，供不同 sink 按需消费
    """

    metadata = dict(result.metadata)
    if result.kind == ACPOutboundKind.TOOL:
        metadata.setdefault("_tool_hint", True)
    return result.content, metadata, list(result.media)
