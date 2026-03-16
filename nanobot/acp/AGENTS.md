# nanobot/acp 模块说明

本目录承载 ACP 运行时分发实现，目标是把 ACP 能力与 `dispatch` 共享层解耦。

## 结构

- `dispatcher.py`
  - 对外入口 `ACPDispatcher`。
  - 负责总流程：连接管理、消息分发、命令处理、direct 调用。
- `session_map.py`
  - `_SessionMapSupport` 混入能力。
  - 负责 session map 持久化、启动对账、活跃 session heartbeat 广播。
- `state.py`
  - `_StreamState` / `_ACPDispatchError` / `_SessionCapabilities`。
  - 仅放轻量状态对象与错误类型。
- `client.py`
  - `_NanobotACPClient` 适配器。
  - 对接 ACP 回调到 `ACPDispatcher`。
- `__init__.py`
  - 导出 `ACPDispatcher`。

## 关键约束

1. `ACPDispatcher` 的对外行为保持稳定，调用方不应依赖内部拆分细节。
2. session map 持久化文件固定在配置根目录：`<config-root>/acp-session-map.json`。
3. session map 仅保留当前活跃映射，`/new` 后旧映射必须同步删除。
4. ACP 模式 heartbeat 要面向活跃渠道会话广播，不能只打到单一 heartbeat 会话。
5. 如调整此目录结构或行为，必须同步更新本文件，避免后续 session 误读。
