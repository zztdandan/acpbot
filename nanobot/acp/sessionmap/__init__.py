"""ACP SessionMap 模块：会话绑定真相与运行时映射。

本模块负责维护 nanobot 侧会话键与 ACP 侧会话 ID 的映射关系，包括：
1. 持久化绑定（SessionMapBindingEntry）：磁盘上的真相存储
2. 运行时映射（SessionRuntimeEntry）：内存中的就绪会话
3. 绑定管理器（SessionMapBindingManager）：持久化 CRUD 与大对账
4. 运行时管理器（SessionRuntimeManager）：运行时状态与能力缓存

架构位置：
    sessionmap 位于 ACPRuntime 与 ACP 协议层之间：
    - 下行：接收 ACP session payload，更新本地映射
    - 上行：为 runtime 提供会话绑定查询、能力缓存
    - 侧行：持久化到磁盘（JSON），支持重启后恢复

核心概念：
    - nanobot_side_session_key: nanobot 侧会话主键（如 "user123:chat456"）
    - acp_side_session_id: ACP 侧会话 ID（由 ACP backend 生成）
    - bound_model/bound_agent: 会话绑定的模型/代理偏好
    - revision: 持久化版本号（用于并发控制）

使用示例：
    # 获取绑定管理器
    binding_manager = runtime.sessionmap_binding_manager
    await binding_manager.load_persistent_truth()  # 启动时加载

    # 查询绑定
    acp_id = binding_manager.resolve_session_id("user123:chat456")

    # 获取运行时管理器
    runtime_manager = runtime.session_runtime_manager
    entry = runtime_manager.get_by_nanobot_side_session_key("user123:chat456")
"""

from nanobot.acp.sessionmap.binding_manager import SessionMapBindingManager
from nanobot.acp.sessionmap.models import (
    SessionMapBindingEntry,
    SessionRuntimeEntry,
    _SessionCapabilities,
)
from nanobot.acp.sessionmap.runtime_manager import SessionRuntimeManager

# 导出公共 API（内部成员以下划线开头，不导出）
__all__ = [
    "SessionMapBindingEntry",  # 持久化绑定条目
    "SessionMapBindingManager",  # 绑定管理器（持久化）
    "SessionRuntimeEntry",  # 运行时会话条目
    "SessionRuntimeManager",  # 运行时管理器（内存）
    "_SessionCapabilities",  # 会话能力缓存（内部使用）
]
