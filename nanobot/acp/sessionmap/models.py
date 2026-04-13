"""会话映射数据模型定义。

本模块定义了 sessionmap 模块使用的核心数据结构。

主要模型：
    1. SessionMapBindingEntry: 持久化绑定条目（磁盘存储格式）
    2. SessionRuntimeEntry: 运行时会话条目（内存状态）
    3. _SessionCapabilities: 会话能力缓存（模型/代理列表）

设计原则：
    - 使用 dataclass 简化数据对象定义
    - slots=True 优化内存占用
    - 类型注解完整（支持静态类型检查）
    - 序列化方法（as_payload）与内部表示分离
"""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap
from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities


@dataclass(slots=True)
class SessionMapBindingEntry:
    """持久化绑定真相条目：nanobot 会话键与 ACP 会话 ID 的映射。

    字段说明：
        cwd: 绑定所属工作目录的绝对路径（用于工作区隔离）
        nanobot_side_session_key: 业务侧会话主键（如 "user123:chat456"）
        acp_side_session_id: ACP 协议侧会话标识（如 "abc123"）
        bound_model: 绑定的模型名称（如 "gpt-4"，可选）
        bound_agent: 绑定的代理名称（如 "assistant"，可选）
        updated_at: ISO 8601 格式的时间戳（带时区）
        revision: 持久化版本号（用于并发控制，从 1 开始递增）

    使用场景：
        - 持久化存储：序列化为 JSON 保存到磁盘
        - 启动对账：从磁盘加载后反序列化为该对象
        - 绑定查询：通过 nanobot_key 查找 acp_id

    示例：
        entry = SessionMapBindingEntry(
            cwd="/workspace",
            nanobot_side_session_key="user123:chat456",
            acp_side_session_id="abc123",
            bound_model="gpt-4",
            bound_agent="assistant",
            updated_at="2026-04-13T10:00:00+08:00",
            revision=1,
        )
    """

    cwd: str
    nanobot_side_session_key: str
    acp_side_session_id: str
    bound_model: str | None
    bound_agent: str | None
    updated_at: str
    revision: int

    def as_payload(self) -> JSONMap:
        """将条目转换为持久化载荷（JSON 格式）。

        返回：
            JSONMap: 字典格式的序列化数据

        序列化规则：
            1. 字段名使用驼峰命名（cwd 除外）
            2. bound_model/bound_agent 为 None 时不输出（精简 JSON）
            3. 所有必填字段始终输出

        输出格式：
            {
                "cwd": "/workspace",
                "nanobotSideSessionKey": "user123:chat456",
                "acpSideSessionId": "abc123",
                "boundModel": "gpt-4",  # 可选
                "boundAgent": "assistant",  # 可选
                "updatedAt": "2026-04-13T10:00:00+08:00",
                "revision": 1
            }

        使用场景：
            - persist 方法中序列化到磁盘
            - 测试代码中断言序列化结果
        """
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
    """运行时会话条目：内存中的会话状态表示。

    与 SessionMapBindingEntry 的区别：
        - SessionMapBindingEntry: 持久化到磁盘（长期存储）
        - SessionRuntimeEntry: 仅存在于内存（运行时状态）
        - SessionRuntimeEntry 额外包含 ready 标志和 capabilities 缓存

    字段说明：
        nanobot_side_session_key: 业务侧会话主键（与持久化条目一致）
        acp_side_session_id: ACP 协议侧会话标识（与持久化条目一致）
        ready: 会话是否就绪可用（True=可接收请求，False=初始化中）
        capabilities: 会话能力缓存（可用模型/代理列表 + 当前选择）

    使用场景：
        - session_runtime_manager 中维护运行时会话状态
        - dispatch_inbound 中检查会话是否就绪
        - process_direct 中获取会话能力信息

    示例：
        entry = SessionRuntimeEntry(
            nanobot_side_session_key="user123:chat456",
            acp_side_session_id="abc123",
            ready=True,
        )
    """

    nanobot_side_session_key: str
    acp_side_session_id: str
    ready: bool
    capabilities: _SessionCapabilities = field(default_factory=_SessionCapabilities)
