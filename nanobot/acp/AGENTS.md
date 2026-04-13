# nanobot/acp 架构注入手册（runtime 重构版）

本目录只保留 ACP 新架构代码，不再保留旧 dispatcher family/mixin/history 兼容实现。

## 1) 当前目标结构

- `runtime.py`
  - `ACPRuntime` / `ACPDispatcher` 主入口
  - owner：连接生命周期、`request_key -> wait entry`、bus inbound 主循环、统一 completion
- `runtime_lifecycle.py`
  - 连接建立、reset、close
- `runtime_process_manager.py`
  - owner：按 `nanobot_side_session_key` 串行排队、active request 生命周期、`/stop` 下的排队清理
- `dispatch_process_direct.py`
  - 真实 ACP prompt 执行 helper；只服务执行期
- `runtime_models.py`
  - runtime/inbound/process/state 共用 dataclass 与 enum
- `runtime_client.py`
  - ACP callback -> runtime owner bridge

- `inbound/`
  - owner：`ProcessDirectInput` / `InboundMessage` -> `InboundContext`
  - 固定步骤：`normalize -> permission_inbound -> command_router -> media_prepare -> build_process_request`
  - 只负责 direct response 或 `ProcessRequest` 构造

- `sessionmap/`
  - `binding_manager.py`：绑定真相与持久化
  - `runtime_manager.py`：当前 runtime 生命周期里的 ready session
  - `models.py` / `storage.py` / `reconcile.py`：配套模型与持久化/对账 helper

- `state/`
  - owner：单轮 request 的 state manager、progress router、permission waiter、final materialize
  - 一轮一建、一轮一销毁

- `observability/`
  - owner：结构化事件 queue 与消费
  - 业务模块只 push event，不直接耦合 audit/tooling 落地

## 2) 强约束

- 不再新增或恢复旧式根层 family 文件，例如：
  - `session_update_*`
  - `progress_*`
  - `media_codec_*`
  - `observability_*`
  - `session_map_*`
  - `dispatcher_*`（除 `dispatcher.py` 导出兼容层外）
- 不再新增 runtime 顶层散状态字典来表达 request/session 过程态
- 不再为旧 ACP 测试、旧 mixin、旧 helper 维持兼容代码

## 3) 读写规则

- 入口薄：`dispatcher.py` 仅保留外部导入兼容
- owner 清晰：runtime / inbound / process manager / sessionmap / state / observability 各自收口
- 注释优先解释“为什么这样做”，尤其是：
  - ACP schema 兼容分支
  - timeout / reset / rebuild 兜底分支
  - `/stop`、permission、late event 等生命周期边界

## 4) 维护要求

- 若目录结构或 owner 边界变化，必须同步更新本文件
- 若重新引入旧家族文件，默认视为架构回退，除非用户明确要求
