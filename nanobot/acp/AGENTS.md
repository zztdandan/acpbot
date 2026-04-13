# nanobot/acp 架构手册（runtime / inbound / sessionmap / state）

本目录承载 ACP 运行时实现。当前架构目标不是保留历史 `dispatcher + mixin + 顶层散状态` 结构，而是收敛为：

- `ACPRuntime` 作为唯一主对象；
- `inbound` 负责 AOP 化入站处理；
- `sessionmap` 负责绑定真相与 session 运行态；
- `state` 负责单轮 `process_direct` 生命周期；
- `observability` 通过 queue 解耦；
- `dispatch` 只保留顶层编排。

## 1) 目标目录边界

- 根层
  - `runtime.py`：ACP runtime 主对象，持有连接、manager 与 `process_direct()` await 入口。
  - `runtime_client.py`：ACP client 适配与回调桥接。
  - `runtime_lifecycle.py`：连接建立、initialize、reset、子进程回收。
  - `runtime_models.py`：runtime 共享模型与类型。
  - `runtime_process_manager.py`：`ProcessRuntimeManager`，负责 request await、排队、激活执行与完成唤醒。
  - `dispatch.py`：顶层 dispatch 入口。
  - `dispatch_process_direct.py`：调起 inbound / process runtime 的顶层编排。
  - `acp_factory.py` / `acp_errors.py`：ACP helper。

- `state/`
  - 单轮 `process_direct` 状态中心。
  - 负责 pool、handler、router、outbound schema。
  - `SessionStateManager` 必须一轮一建、一轮一销毁。

- `sessionmap/`
  - `binding_manager.py`：`nanobot_side_session_key <-> acp_side_session_id` 绑定真相、持久化、bound selection、能力快照。
  - `runtime_manager.py`：session 运行时主状态、连接代次、activation ensure、活跃任务。
  - `storage.py` / `reconcile.py` / `models.py`：磁盘与对账辅助。

- `inbound/`
  - 入站 queue 消费与 AOP pipeline。
  - slash 命令、permission inbound、媒体落盘、metadata 编制、process request 构造。
  - 支持某步直接拦截并返回 final。

- `runtime_process_manager.py`
  - per-`nanobot_side_session_key` 的 process queue owner。
  - 管 request await、活跃请求、state 挂载、完成唤醒。

- `observability/`
  - queue、audit、tooling、结构化事件消费。
  - 业务模块只 push event，不直接耦合落盘实现。

## 2) 核心命名规范（强制）

### 2.1 三类主键只能这样命名

1. `request_key`
   - 一次 `process_direct` 请求的唯一键。
2. `nanobot_side_session_key`
   - nanobot 侧会话主键。
3. `acp_side_session_id`
   - ACP 侧 session id。

禁止继续使用含义模糊的裸 `session_key` / `session_id` 命名来同时表示两侧概念。

### 2.2 索引命名必须显式表达 key/value

允许：

- `queue_by_nanobot_side_session_key`
- `active_by_request_key`
- `active_by_acp_side_session_id`
- `binding_by_nanobot_side_session_key`

禁止：

- `_session_map`
- `_session_targets`
- `_session_states`
- `_session_state_routers`

这类命名无法从名称本身判断主键语义，后续不允许新增。

## 3) ACPRuntime 规则

### 3.1 强类型连接字段

以下字段允许存在于 runtime，但必须使用真实类型，并统一采用强语义命名，禁止继续写 `Any` 或退回 `_conn/_proc` 这种弱语义缩写：

1. `_acp_client_connection_cm`
   - `AsyncContextManager[tuple[ClientSideConnection, asyncio.subprocess.Process]]`
2. `_acp_client_conn`
   - `ClientSideConnection`
3. `_acp_agent_process`
   - `asyncio.subprocess.Process`
4. `_acp_connection_lock`
   - `asyncio.Lock`
5. `acp_callback_client`
   - `_NanobotACPClient`

### 3.2 `process_direct()` 必须保持 await 语义

`ACPRuntime.process_direct()` 对外必须是 `await` 方法。

实现规则：

1. runtime 为每次调用创建 `request_key` 与 runtime 级等待 entry。
2. runtime 将 inbound 数据送入 inbound pipeline。
3. inbound 直接返回或真实 `process_direct_impl()` 结束时，都只能调用统一 completion 入口。
4. `process_direct()` 等待 completion 信号，拿到 final outbound 后返回。

## 4) Inbound 规则

### 4.1 只允许通过 `InboundContext` 传递状态

每个 inbound step 只允许读写 `InboundContext`，不得直接读写 runtime 顶层散状态。

`InboundContext` 至少应包含：

1. `request_key`
2. 原始 inbound message
3. `nanobot_side_session_key`
4. `content` / `media` / `metadata`
5. `progress_metadata`
6. `direct_response`
7. `process_request`
8. `handled` / `stop`
9. `artifacts`

### 4.2 首轮固定步骤

首轮 pipeline 先做固定步骤：

1. `normalize`
2. `permission_inbound`
3. `command_router`
4. `media_prepare`
5. `build_process_request`

### 4.3 直接返回规则

若 inbound 某一步直接编制 final：

1. 该步骤只负责构造 `OutboundMessage`。
2. 统一调用 `complete_process_request(request_key=..., outbound=...)`。
3. 不允许步骤直接修改 runtime 等待 map。

## 5) ProcessRuntimeManager 规则

### 5.1 按 `nanobot_side_session_key` 串行

`ProcessRuntimeManager` 必须以 `nanobot_side_session_key` 作为排队主键。

原因：

1. 外部输入天然按 nanobot 会话聚合。
2. `acp_side_session_id` 可能在重连或重建时变化。

### 5.2 必须提供队列化缓存

若某个 `nanobot_side_session_key` 已有真实 `process_direct_impl()` 在跑：

1. 新请求不得并发进入真实循环。
2. 必须进入该 key 对应队列。
3. 当前请求 final 完成后，按序拉起下一条。

补充边界：

1. `inbound/` 只拥有 inbound queue 与 AOP 预处理。
2. `ProcessRuntimeManager` 只拥有真正的 per-session process queue。
3. 不允许把这两个 queue 再混成一个 owner。

### 5.3 当前活跃请求需要挂载 state

每个活跃请求必须持有：

1. `request_key`
2. `nanobot_side_session_key`
3. `acp_side_session_id`
4. `SessionStateManager`
5. `ProgressRouter`
6. inbound context

## 6) State 规则

### 6.1 单轮 owner

`state/` 只负责单轮 `process_direct`，不得承担跨轮次全局态。

### 6.2 删除旧状态

以下旧状态不再允许新增或恢复：

1. `_session_states`
2. `_session_state_routers`
3. `_session_request_scope_ids`
4. `_session_result_media`
5. `_session_pending_media`
6. `_session_progress_metadata`
7. `_session_active_tool_name`
8. `last_target`

### 6.3 用 pool 生命周期表达事实

1. tool 活跃态由 tool pool 表达，不再用旁路字符串索引。
2. media 结果由 media pool 输出，不再侧写到 runtime map。
3. permission 上下文由 state 自身持有，不再依赖 `_session_targets` 反查。

## 7) SessionMap 规则

### 7.1 `binding_manager.py`

只负责绑定真相与持久化，不负责 process 调度。

### 7.2 `runtime_manager.py`

负责 session 运行时主状态：

1. `connection_epoch`
2. activation ensure
3. session 级运行态 entry
4. 活跃任务

### 7.3 不再保留旧平行字典

不允许继续维护：

1. `_session_locks`
2. `_session_caps`
3. `_active_tasks`

这些能力必须内收为 manager 主状态或 entry 字段，而不是顶层散字典。

## 8) Observability 规则

1. runtime 层建立 queue。
2. 其他模块只 push event。
3. `observability/` 独占 audit/tooling 消费实现。

## 9) 迁移纪律

### 9.1 迁移阶段禁止新增 ACP 单测

1. 删除现有 ACP 单元测试。
2. 迁移阶段不写新的 ACP 单元测试。
3. 不为旧单测补兼容代码。
4. 仅维护计划文件追踪迁移进度。

### 9.2 迁移完成后再重建测试

1. 先补模块单测。
2. 再补 ACP runtime 集成测试。
3. e2e 继续作为外部链路验证。

## 10) 注释要求

以下分支必须保留中文注释，解释“为什么这么做”：

1. ACP 协议兼容分支。
2. 连接重置、自愈重试、partial fallback。
3. request 排队与唤醒。
4. completion 入口的幂等与重复完成保护。

## 11) 文档同步要求

发生以下变化时，必须同步更新本文件：

1. 目录结构变动。
2. runtime / inbound / sessionmap / state 边界变动。
3. 主键命名规范变动。
4. `process_direct()` await 模型变动。
