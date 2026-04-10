# nanobot/acp 模块说明（可读性优先）

本目录承载 ACP 运行时分发实现。目标不是追求“机械拆文件”，而是持续保证：

- 代码阅读路径清晰（入口薄、路由薄、实现下沉）；
- 文件结构可理解（按职责家族组织，而不是按历史堆叠）；
- 对外行为稳定（仅暴露 `ACPDispatcher`，不泄漏内部重构细节）。

## 与 spec 对齐的结构总览

以下结构与 `docs/superpowers/specs/2026-03-26-acp-package-refactor-design.md` 对齐：

- 入口与编排
  - `dispatcher.py`：薄导出层，仅维持历史导入路径兼容。
  - `dispatcher_core.py`：`ACPDispatcher` 主体壳层（生命周期、编排入口、兼容方法）。
  - `dispatcher_dispatch.py`：`_dispatch` 主流程编排与异常兜底。
  - `dispatch_commands.py`：slash 命令路由执行。
  - `dispatcher_state.py`：dispatcher 运行时状态初始化。
  - `dispatcher_ports.py`：家族间最小能力端口定义。

- 运行时与协议 helper
  - `session_runtime.py`：连接建立、会话确保、prompt 执行与自愈重试。
  - `session_runtime_mcp.py`：MCP 配置转 ACP schema。
  - `session_caps.py`：models/agents 能力提取与渲染。
  - `acp_factory.py`：ACP lazy import 与 block 工厂。
  - `acp_errors.py`：ACP 错误识别与兼容判断。

- 五大家族
  - `session_update_router.py` + `session_update_events.py`：事件归一与统一入口分发。
  - `progress_router.py` + `progress_event_types.py`：progress 多缓冲路由与发布策略。
  - `media_codec_*`：inbound/outbound 多媒体编解码。
  - `observability_*`：审计写盘与工具事件结构化。
  - `session_map_*`：会话映射持久化、对账与 heartbeat。

- 其他基础模块
  - `state.py`：轻量状态对象与异常容器。
  - `client.py`：ACP client 回调适配层。
  - `__init__.py`：仅导出 `ACPDispatcher`。

## 拆分规则（强制，读写友好优先）

### 1) 入口/路由薄层规则

1. `dispatcher.py` 只做导出兼容，不承载业务逻辑。
2. `dispatcher_core.py` 只做编排与生命周期入口，不承载家族重逻辑。
3. `*_router.py` 与 dispatch 路由文件只做分发和必要校验，不实现复杂状态机。

### 2) 五家族边界规则

1. `session_update` 家族仅处理 ACP 增量事件到内部状态/进度桥接，不处理命令路由和连接初始化。
2. `progress` 家族仅处理文本/tool/media/other 聚合、节流与刷出策略，不解析连接与命令语义。
3. `media_codec_*` 仅处理媒体内容在 ACP blocks 与本地文件间转换，不承担 session 编排。
4. `observability_*` 仅处理调试/审计日志、工具事件结构化，不承担业务决策。
5. `session_map_*` 仅处理 session map 的 load/persist/reconcile/heartbeat，不承担 prompt 主流程。

### 3) 依赖方向规则

1. 允许：`dispatcher_core/dispatcher_dispatch` -> 各 family/helper。
2. 禁止：family 模块反向依赖 dispatch 主流程模块实现细节。
3. 跨 family 协作必须通过 dispatcher 字段或 `dispatcher_ports.py` 的最小端口，不做隐式耦合。

### 4) 可读性注释规则

出现以下分支时必须保留中文注释，说明“为什么这样做”：

1. 协议兼容分支（snake/camel、不同 ACP schema 形态）。
2. 异常兜底分支（自愈重试、降级路径、不中断主链路）。
3. 状态回退分支（session 失效重建、tool 终态优先刷出等）。

## 非目标与稳定性约束

1. 不引入 channel/session 路由 UI。
2. 不改变 ACP 与 native 双后端边界。
3. 不新增对外公开 API，`nanobot.acp` 仅导出 `ACPDispatcher`。
4. 对外行为必须保持等价：命令语义、session 生命周期、progress/tool_hint/final、审计语义不变。

## 行数阈值说明（辅助信号，不是主目标）

行数阈值只作为“可读性风险信号”，不作为重构目的本身：

1. `>500`：通常表示职责已明显堆叠，应触发拆分审查。
2. `>350`：建议在 PR/设计说明中明确 keep/split 决策与理由。
3. 最终决策以“是否提升可读性与边界清晰度”为准，而非单纯压行数。

当前告警记录（Stage-D）：

- `dispatcher_core.py`（363 行）：已下沉 `_dispatch` 到 `dispatcher_dispatch.py`，现保留编排壳层。
- `session_runtime.py`（367 行）：连接/会话/prompt 自愈状态机耦合较强，当前保持单文件并在后续能力扩展时再分段。

## 维护要求

1. 目录结构或边界规则变更时，必须同步更新本文件。
2. 任何新拆分都应先满足“读者可快速定位职责”的标准，再考虑形式化指标。
