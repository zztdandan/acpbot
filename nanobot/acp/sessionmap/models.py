"""会话映射数据模型：SessionMapBindingEntry（持久化）和 SessionRuntimeEntry（运行时）。

核心职责：
    提供会话映射的持久化与运行时表示，分离 binding truth（磁盘）与 runtime state（内存）。
    持久化层负责跨 runtime 周期保存绑定，运行时层负责当前进程的会话状态管理。

使用场景：
    SessionMapBindingEntry: 用于 session_map.json 的序列化与反序列化，保证工作区隔离与版本控制
    SessionRuntimeEntry: 用于 ACPRuntime 内存中的会话查询、能力缓存与 ready 状态判断
"""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap
from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities


@dataclass(slots=True)
class SessionMapBindingEntry:
    """持久化绑定条目：nanobot 侧会话键 ↔ ACP 侧会话 ID 的映射关系。

    核心职责：
        作为 session_map.json 的最小持久化单元，记录工作区级别的会话绑定关系。
        通过 cwd 字段实现多工作区隔离，通过 revision 实现乐观并发控制。

    使用场景：
        - 启动时从磁盘加载所有绑定，与 ACP 侧会话列表对账
        - 会话建立/模型切换时创建新绑定或更新现有绑定
        - 会话失效时删除绑定（保留历史记录，只标记为删除）

    属性说明：
        cwd: 工作区绝对路径，用于多工作区隔离；不同 cwd 的绑定互不干扰
        nanobot_side_session_key: nanobot 侧会话标识，格式通常为 "user_id:chat_id" 或 "cli:direct"
        acp_side_session_id: ACP 侧会话标识，由 ACP 后端生成的唯一字符串
        bound_model: 当前绑定的模型 ID（如 "gpt-4"），None 表示未绑定
        bound_agent: 旧版本兼容字段；运行时不再读取，写回时不再输出 boundAgent
        updated_at: 最后更新时间（ISO 8601 格式），用于调试与清理策略
        revision: 版本号，用于乐观并发控制；每次更新 revision+，避免并发写入冲突

    生命周期：
        - 创建：ensure_ready_session 成功建立会话后初始化（revision=1）
        - 更新：切换模型时 increment revision（用于检测并发修改）
        - 删除：clear_binding 时从字典中移除，不直接修改本对象
    """

    cwd: str
    nanobot_side_session_key: str
    acp_side_session_id: str
    bound_model: str | None
    bound_agent: str | None
    updated_at: str
    revision: int

    def as_payload(self) -> JSONMap:
        """序列化为 JSON payload（用于写入 session_map.json）。

        处理流程：
            1. 构建基础 payload（必填字段：cwd/key/acp_id/updated_at/revision）
            2. 仅输出非空 bound_model；bound_agent 兼容字段不再写回
            3. 返回符合 schema version 2 的字典（camelCase 命名）

        返回：
            JSONMap: 包含所有必填字段的字典，可选字段仅在非 None 时包含
        """
        payload: JSONMap = {
            "cwd": self.cwd,
            "nanobotSideSessionKey": self.nanobot_side_session_key,
            "acpSideSessionId": self.acp_side_session_id,
            "updatedAt": self.updated_at,
            "revision": self.revision,
        }
        # 可选字段：为 None 时不输出（精简 JSON）。boundAgent 阶段 1 起不再写回，
        # 避免继续持久化已删除的 agent/mode 用户体系。
        if self.bound_model:
            payload["boundModel"] = self.bound_model
        return payload


@dataclass(slots=True)
class SessionRuntimeEntry:
    """运行时会话条目（仅内存）：SessionRuntimeManager 的内存状态表示。

    职责：
        - 作为 ACPRuntime 进程内会话的内存缓存，用于快速查询会话状态与能力信息
        - 不参与持久化，进程重启后从 binding truth 重建

    属性：
        nanobot_side_session_key: nanobot 侧会话标识，用于反向查找 ACP 侧会话 ID
        acp_side_session_id: ACP 侧会话标识，用于调用 ACP API 时指定目标会话
        ready: 会话是否就绪（True 表示可以正常使用，False 表示正在恢复中）
        capabilities: 会话能力缓存对象（_SessionCapabilities），存储当前可用模型列表与当前选择

    生命周期：
        - 创建：ensure_ready_session 成功激活会话后实例化（ready=True）
        - 更新：update_caps_from_payload 时更新 capabilities，ready 状态维持不变
        - 删除：drop_ready_session 时从内存中移除（不影响持久化绑定）
    """

    nanobot_side_session_key: str
    acp_side_session_id: str
    ready: bool
    capabilities: _SessionCapabilities = field(default_factory=_SessionCapabilities)
