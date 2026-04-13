"""会话映射模型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap


@dataclass(slots=True)
class SessionMapBindingEntry:
    """持久化绑定真相条目。"""

    cwd: str
    # 绑定所属工作目录。
    nanobot_side_session_key: str
    # 业务侧会话主键。
    acp_side_session_id: str
    # 协议侧会话标识。
    bound_model: str | None
    # 绑定模型名称。
    bound_agent: str | None
    # 绑定代理名称。
    updated_at: str
    # 最近更新时间。
    revision: int
    # 持久化版本号。

    def as_payload(self) -> JSONMap:
        """将条目转换为持久化载荷。"""
        payload: JSONMap = {
            "cwd": self.cwd,
            "nanobotSideSessionKey": self.nanobot_side_session_key,
            "acpSideSessionId": self.acp_side_session_id,
            "updatedAt": self.updated_at,
            "revision": self.revision,
        }
        if self.bound_model:
            payload["boundModel"] = self.bound_model
        if self.bound_agent:
            payload["boundAgent"] = self.bound_agent
        return payload


@dataclass(slots=True)
class SessionRuntimeEntry:
    """当前运行时代际的就绪会话条目。"""

    nanobot_side_session_key: str
    # 业务侧会话主键。
    acp_side_session_id: str
    # 协议侧会话标识。
    ready: bool
    # 是否就绪可用。
    bound_model: str | None = None
    # 当前会话绑定模型。
    bound_agent: str | None = None
    # 当前会话绑定代理。
    capabilities: "_SessionCapabilities" = field(default_factory=lambda: _SessionCapabilities())
    # 运行时能力缓存。


class _SessionCapabilities:
    """单会话模型与代理能力缓存。"""

    def __init__(self) -> None:
        """初始化能力缓存字段。"""
        self.available_models: list[str] = []
        # 可选模型列表。
        self.current_model: str | None = None
        # 当前模型。
        self.available_agents: list[str] = []
        # 可选代理列表。
        self.current_agent: str | None = None
        # 当前代理。
