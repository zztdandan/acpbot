"""ACP progress 事件模型。

该结构用于在 session_update router 与 progress router 之间传递“完整 update + 最小索引”。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nanobot.acp.state.models import ACPUpdateType


@dataclass(slots=True)
class ACPProgressEvent:
    """统一的 ACP progress 事件。

    约束：raw_update/raw_json 以“尽量不裁剪”的方式完整保留，
    extracted 仅放路由所需索引字段。
    """

    session_id: str
    raw_update: Any
    raw_json: Any
    update_type: ACPUpdateType | str
    family: str
    route_key: str
    extracted: dict[str, Any] = field(default_factory=dict)
    ext: dict[str, Any] = field(default_factory=dict)
    received_at_ms: int = 0
