"""ACPDispatcher 运行时状态初始化辅助。"""

from __future__ import annotations

import asyncio
from typing import Any


def _init_dispatcher_state(dispatcher: Any) -> None:
    """初始化 ACPDispatcher 的运行时可变状态。"""
    # 中文注释：把可变运行时状态集中到一个入口，便于后续拆分 core/ports 时保持构造流程稳定。
    dispatcher._running = False
    dispatcher._conn_cm = None
    dispatcher._conn = None
    dispatcher._proc = None
    dispatcher._connect_lock = asyncio.Lock()
    dispatcher._session_map = {}
    dispatcher._session_locks = {}
    dispatcher._process_locks = {}
    dispatcher._session_states = {}
    dispatcher._session_caps = {}
    # 中文注释：session_key 级别记录期望模型/agent，用于重启后 session map 回放。
    dispatcher._session_desired = {}
    # 中文注释：activation ensure 轮次标记仅用于运行时去抖，不参与磁盘持久化。
    dispatcher._session_activation_ensure_epoch = {}
    # 中文注释：记录启动批次已激活成功的 session_key，连接 epoch 确认后再写 ensured 标记。
    dispatcher._session_bootstrap_activated_keys = set()
    from nanobot.acp.session_map_binding_manager import _SessionMapBindingManager

    dispatcher._session_map_binding_manager = _SessionMapBindingManager(dispatcher)
    dispatcher._active_tasks = {}
    dispatcher.last_target = None
    # 中文注释：通过 dispatcher 兼容导出层读取 get_data_dir，保持历史 monkeypatch 注入点不变。
    from nanobot.acp import dispatcher as dispatcher_exports

    dispatcher._session_map_file = dispatcher_exports.get_data_dir() / "acp-session-map.json"
    dispatcher._session_map_bootstrapped = False
    # 中文注释：记录 session_id -> session_key 的映射，用于把 session_update 工具事件回填到会话审计。
    dispatcher._session_id_to_session_key = {}
    # 中文注释：记录 session_id 当前活跃工具名，便于 ToolCallProgress 归档到对应工具文件。
    dispatcher._session_active_tool_name = {}
    # 中文注释：每个 session_key 最近一次 process_direct 的附件落盘结果，供 _dispatch 组装 final。
    dispatcher._session_result_media = {}
    # 中文注释：_dispatch 预存的 inbound media，process_direct 取出后立即消费，避免跨轮串附件。
    dispatcher._session_pending_media = {}
