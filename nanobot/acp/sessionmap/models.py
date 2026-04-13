"""会话映射数据模型：SessionMapBindingEntry（持久化）和 SessionRuntimeEntry（运行时）。"""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap
from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities


@dataclass(slots=True)
class SessionMapBindingEntry:
    """持久化绑定条目：nanobot 会话键与 ACP 会话 ID 的映射。"""

    cwd: str
    nanobot_side_session_key: str
    acp_side_session_id: str
    bound_model: str | None
    bound_agent: str | None
    updated_at: str
    revision: int

    def as_payload(self) -> JSONMap:
        """序列化为 JSON payload，None 可选字段不输出。"""
        payload: JSONMap = {
            "cwd": self.cwd,
            "nanobotSideSessionKey": self.nanobot_side_session_key,
            "acpSideSessionId": self.acp_side_session_id,
            "updatedAt": self.updated_at,
            "revision": self.revision,
        }
        # 可选字段：为 None 时不输出（精简 JSON）
        if self.bound_model:
            payload["boundModel"] = self.bound_model
        if self.bound_agent:
            payload["boundAgent"] = self.bound_agent
        return payload


@dataclass(slots=True)
class SessionRuntimeEntry:
    """运行时会话条目（仅内存），额外包含 ready 标志和 capabilities 缓存。"""

    nanobot_side_session_key: str
    acp_side_session_id: str
    ready: bool
    capabilities: _SessionCapabilities = field(default_factory=_SessionCapabilities)
