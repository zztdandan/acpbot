# nanobot/acp 模块说明

本目录承载 ACP 运行时分发实现，目标是把 ACP 能力与 `dispatch` 共享层解耦。

## 结构

- `dispatcher.py`
  - 对外入口 `ACPDispatcher`，聚焦“调度主流程”。
  - 关键功能（函数）：
    - `run()`：消费 inbound 并创建异步分发任务。
    - `_dispatch()`：命令分支、会话选择、process_direct 调度、异常兜底。
    - `process_direct()`：单轮 prompt 调用，串接 session_update 流式状态。
    - `_ensure_connection()` / `_ensure_session()` / `_activate_existing_session()`：连接和会话生命周期。
    - `_handle_session_update()`：把 ACP 增量事件转成进度/工具事件。
- `session_map.py`
  - `_SessionMapSupport` 混入能力。
  - 关键功能（函数）：
    - `_load_session_map_from_disk()` / `_persist_session_map()`：映射读写与原子落盘。
    - `_bootstrap_session_map()` / `_reconcile_session_map_with_acp()`：启动后会话对账。
    - `send_heartbeat_to_active_sessions()`：向活跃会话广播 heartbeat。
- `state.py`
  - `_StreamState` / `_ACPDispatchError` / `_SessionCapabilities`。
  - 关键功能（函数）：
    - `_StreamState.merge_text()`：文本去重合并。
    - `_StreamState.final()`：返回最终文本。
- `client.py`
  - `_NanobotACPClient` 适配器。
  - 关键功能（函数）：
    - `session_update()`：把 ACP session_update 回调转发给 dispatcher。
    - `request_permission()`：权限请求转发到 dispatcher 策略。
- `progress.py`
  - `_ProgressAccumulator`：进度聚合器，避免 progress 粒度过碎。
  - 关键功能（函数）：
    - `on_progress()`：接收文本/tool-hint 并做类型切换聚合。
    - `flush()` / `close()`：显式 flush + 结束前清理。
    - `_idle_flush_worker()`：2 秒死手机制自动刷出缓存。
- `observability.py`
  - `_ACPObservabilityMixin`：结构化日志与审计落盘能力。
  - 关键功能（函数）：
    - `_publish_outbound_with_debug()`：统一 outbound 发布入口（带 debug + audit）。
    - `_audit_inbound()` / `_audit_outbound()`：输入输出分离 JSONL。
    - `_audit_tool_event()`：按工具拆分日志，记录 tool start/progress 全链路。
    - `_close_observability()`：关闭所有审计句柄，避免跨次运行混写。
- `__init__.py`
  - 导出 `ACPDispatcher`。

## 关键约束

1. `ACPDispatcher` 的对外行为保持稳定，调用方不应依赖内部拆分细节。
2. session map 持久化文件固定在配置根目录：`<config-root>/acp-session-map.json`。
3. session map 仅保留当前活跃映射，`/new` 后旧映射必须同步删除。
4. ACP 模式 heartbeat 要面向活跃渠道会话广播，不能只打到单一 heartbeat 会话。
5. outbound 发布必须优先走 `_publish_outbound_with_debug()`，保证审计链路完整。
6. 如调整此目录结构或行为，必须同步更新本文件，避免后续 session 误读。
