"""State-owned outbound schema builders.

These helpers replace the old root-level outbound_content_schema module so that
progress/final payload construction stays within the state owner boundary.
"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPOutboundKind, FlushResult


def build_progress_payload(result: FlushResult) -> tuple[str, JSONMap, list[str]]:
    """Convert a FlushResult into `(content, metadata, media)` for progress sinks."""

    # The progress schema now lives inside state so the state owner also defines how
    # structured progress is expressed to the outside world.
    metadata = dict(result.metadata)
    if result.kind == ACPOutboundKind.TOOL:
        metadata.setdefault("_tool_hint", True)
    return result.content, metadata, list(result.media)
