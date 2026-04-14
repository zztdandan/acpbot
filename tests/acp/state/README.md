# ACP State Real Fixture Notes

- `state_updates.real_fixture.json` 保存的是可被 `acp.schema.SessionNotification.model_validate(...)` 校验的原始 payload，而不是测试内联字符串。
- 测试运行时会先沿用 `tests/acp/sessionmap/` 的真实 opencode backend 初始化链路，恢复 `nanobot-sessionmap-target` 对应的真实 ACP session，然后把 fixture 中的 `__REAL_SESSION_ID__` 替换为本轮真实 `sessionId`。
- fixture 分成三组：`message_only`、`tool_only`、`mixed`。前两组分别覆盖消息块与 tool-call 附件，第三组验证混合输入时 state 聚合结果与拆开单测一致。
- `message_only` 重点验证 `TextContentBlock` 拼接、`ImageContentBlock` / `ResourceContentBlock` / `EmbeddedResourceContentBlock` 去重与路径归一化。
- `tool_only` 重点验证 `ToolCallStart` / `ToolCallProgress` 的提示流，以及 tool 内容里的附件是否进入 state 聚合；这里的附件数据刻意保留 image/resource/diff/terminal 多种结构，确保 payload 足够接近真实 session update。
- 收集策略不是立即断言，而是通过测试侧 queue 等待一个很短的 idle 窗口，并设置总超时，模拟 state 在短时间内接收 burst updates 后再统一消费结果。
