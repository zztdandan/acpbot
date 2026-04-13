# nanobot/acp 架构注入手册（简版）

本文件是 `nanobot/acp/` 目录的自动注入架构说明。
它描述 ACP 模块长期有效的目标架构与边界约束，不绑定某一次阶段性设计文档。

## 1) 架构目标

- ACP 运行时收敛到 `ACPRuntime` 主对象。
- 入站处理收敛到 `inbound` pipeline，不再由 dispatcher 顶层散逻辑拼接。
- session 绑定真相与运行态分层到 `sessionmap`。
- 单轮执行事实收敛到 `state`，执行生命周期收敛到 `ProcessRuntimeManager`。
- 观测事件走 `observability` queue，业务模块只 push event。

## 2) 模块边界（强约束）

- `runtime`
  - owner：连接生命周期、`request_key -> wait entry`、统一 completion。
  - 对外保留 `process_direct()` await 语义。
  - bus 路径通过 `run()/dispatch_inbound()` 进入同一套 wait/completion 闭环。

- `inbound`
  - owner：输入归一化、AOP 预处理、direct response 判定、`ProcessRequest` 构造。
  - 仅通过 `InboundContext` 传递入站过程态。
  - 双入口：`handle_process_direct(...)` 与 `handle_inbound(...)`。

- `process runtime manager`
  - owner：按 `nanobot_side_session_key` 串行队列、active request 生命周期、收尾与唤醒编排。
  - 不承担连接 owner，不承担绑定真相 owner。

- `state`
  - owner：单轮 request 事实中心（pool / handler / router / request-scope 聚合 / final materialize）。
  - `SessionStateManager` 必须一轮一建、一轮一销毁。
  - `on_progress` 是 state 统一出口的兼容镜像，不反向驱动状态机。

- `sessionmap`
  - `binding manager`：绑定真相与持久化（`nanobot_side_session_key <-> acp_side_session_id`）。
  - `runtime manager`：当前 runtime 生命周期内的 ready session 运行态。

- `observability`
  - runtime 创建 queue。
  - 其他模块只 push 结构化事件。
  - 是否用户可见由原 owner 决定，observability 不直接发消息。

## 3) 主流程约束

- `process_direct()` 必须：创建 `request_key` -> 注册 wait entry -> 进入 inbound -> 等 completion -> 返回 final。
- inbound direct response 与真实执行完成都必须调用统一 completion 入口。
- bus inbound 与 direct 调用共享同一套 `request_key -> wait entry -> complete_process_request()` 语义。
- `complete_process_request()` 只负责完成 wait entry，不负责 publish final、不负责执行态收尾。

## 4) Inbound 固定步骤（首轮）

- `normalize`
- `permission_inbound`
- `command_router`
- `media_prepare`
- `build_process_request`

说明：`direct_response` 与 `process_request` 是两种互斥出口；不再引入额外 `handled/stop` 控制位。

## 5) 命名规范（强制）

仅允许以下三类主键命名：

- `request_key`
- `nanobot_side_session_key`
- `acp_side_session_id`

禁止继续新增语义不清的裸 `session_key` / `session_id`（跨侧混用）。

## 6) 禁止回退到旧形态

禁止新增或恢复以下旧式 runtime 顶层散状态：

- `_session_states`
- `_session_state_routers`
- `_session_request_scope_ids`
- `_session_result_media`
- `_session_pending_media`
- `_session_progress_metadata`
- `_session_active_tool_name`
- `_session_targets`

## 7) `/stop` 与 runtime rebuild

- `/stop`：先定位 session，再由 `ProcessRuntimeManager` 判定 active/queued，必要时通过 runtime 下发 stop/cancel。
- runtime rebuild：整体重建 runtime-owned managers；旧 wait/active/queue 不跨代延续；迟到回调按 orphan/late 记录观测。

## 8) 文档维护规则

发生以下变化时必须同步更新本文件：

- runtime / inbound / sessionmap / state / observability owner 边界变化。
- `process_direct` await/completion 语义变化。
- 主键命名规范变化。
- `/stop` 与 runtime rebuild 策略变化。
