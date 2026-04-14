# ACP tool start/progress 对齐实施方案

## 1. 目标

- `ToolPoolPayload` 与 `ToolCallStart` / `ToolCallProgress` 共有字段结构保持一致。
- `rawInput` / `rawOutput` / `content` 任意深度字符串都做分层裁剪，单字段不超过 10KB，结构不丢。
- flush 的正文只用 `title` 主导，`status` 作为补充；其余结构化信息全部放入 metadata。
- `ToolUpdateHandler.consume()` 在识别到 `status in {completed, failed}` 后，`accept` 完立即返回允许销毁池。
- 池超时触发时，构造出的 flush 结构与正常流完全同形，只是 `status = timed_out`。

## 2. 统一字段契约

`ToolPoolPayload` 使用以下字段（与 ACP 共享字段对齐）：

- `session_update` <=> `sessionUpdate`
- `tool_call_id` <=> `toolCallId`
- `title`
- `kind`
- `status`
- `content`
- `locations`
- `raw_input` <=> `rawInput`
- `raw_output` <=> `rawOutput`
- `field_meta` <=> `_meta`

说明：

- pool 内部使用 snake_case 字段保存，出包 metadata 使用 ACP alias（camelCase + `_meta`）。
- `ToolCallProgress` 保持 patch 语义：仅对出现字段做 snapshot 覆盖。

## 3. handler 裁剪策略（锁内完成）

在 `ToolUpdateHandler.consume()` 里执行：

1. `update.model_dump(by_alias=True, exclude_none=True)` 读取 ACP alias 结构。
2. 对整棵对象做递归字符串裁剪：
   - 上限：`10 * 1024` 字节（UTF-8）
   - 后缀：`...(truncated)`
   - 非字符串类型保持原值
3. 由裁剪后的结构组装 `ToolPoolPayload`，再 `pool.accept(payload)`。

保证：

- 裁剪点在 handler 与 pool 之间唯一且固定。
- 进入 pool 的 `content/rawInput/rawOutput` 已满足 10KB 限制。

## 4. flush 出包契约

### 4.1 content 规则

- `content = title + status`
- 具体格式：
  - `title` + `status` 同时存在：`{title} [{status}]`
  - 只有 `title`：`{title}`
  - 只有 `status`：`tool [{status}]`
  - 都缺失：`tool progress`

### 4.2 metadata 规则

保持与现有链路兼容并新增结构透传：

- 兼容标记：`tool_hint = true`, `_tool_hint = true`
- 兼容字段：`status`, `previous`（渲染文本历史）
- 新增结构：
  - `tool_event`：最后一次事件（ACP alias）
  - `tool_snapshot`：当前 latest snapshot（ACP alias）
  - `tool_events_previous`：历史事件（可选）

## 5. 终态与销毁规则

- 正常流：`completed | failed`
  - handler 先 `accept`
  - 再返回 `HandlerConsumeResult(immediate_finalize=True)`，允许 router 当轮 flush + drop。
- 超时流：
  - deadhand 触发 `pool.mark_timeout()`
  - 使用与普通 progress 相同字段构造 payload
  - `status = timed_out`
  - flush 结构（content + metadata）与正常流一致。

## 6. 测试验收

- `tests/acp/state/test_tool_pool.py`
  - 断言 flush 正文为 `title + status`
  - 断言 `tool_event/tool_snapshot` 存在且结构完整
  - 断言 timeout 流结构与正常流同形，状态为 `timed_out`
- `tests/acp/state/test_tool_handler.py`
  - 断言 `content/rawInput/rawOutput` 深层字符串均 <= 10KB 且带截断标记
  - 断言 `completed` 状态返回 `immediate_finalize=True`
