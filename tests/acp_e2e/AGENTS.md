# ACP E2E Test Suite

该目录用于真实对接 OpenCode ACP 的端到端测试。

## 设计目标

- 测试端口固定为 `inbound/outbound`（`MessageBus`），贴近 gateway 运行链路。
- 允许 `inbound` 一次携带多个文件，并在同一轮 prompt 下传多个 `resource_link`。
- FT 样例文件与 canonical plugin 统一由 harness 自动 stage 到 ACP workspace。
- 输出断言允许多条消息，不强制单条 final。

## 启动前准备

1. 确保本机可执行 ACP 命令（默认 `opencode`）。
2. 确保 OpenCode 侧模型与认证已就绪。
3. 建议先跑 smoke：`E2E-000~003`。
4. FT real-chain 用例会自动准备：
   - `<workspace>/fixtures/file_transport/`
   - `<workspace>/.opencode/plugin/acp-send-file.ts`

## 环境变量

- `NANOBOT_ACP_E2E=1`：开启本目录测试（必填）。
- `NANOBOT_ACP_E2E_COMMAND`：ACP 命令，默认 `opencode`。
- `NANOBOT_ACP_E2E_ARGS`：ACP 参数，默认 `acp --print-logs --log-level WARN`。
- `NANOBOT_ACP_E2E_MODEL`：可选，覆盖默认模型（默认 `RCode_OpenAI/gpt-5.4`）。
- `NANOBOT_ACP_E2E_MODE`：可选，覆盖默认 mode（默认 `build`）。
- `NANOBOT_ACP_E2E_PERMISSION_POLICY`：可选，`strict|trusted|yolo`。
- `NANOBOT_ACP_E2E_STARTUP_TIMEOUT`：可选，启动超时秒数。
- `NANOBOT_ACP_E2E_SESSION_KEY`：可选，固定外部 session key；不填则随机。

说明：

- 默认 `acp_e2e_harness` 会固定 `permissions_policy="strict"`，避免环境变量污染 FT-001~003。
- `acp_e2e_trusted_harness` 仅供 FT-004~005 使用，用来验证 `acp_send_file` 真实回路。

## 运行方式

```bash
uv run pytest -q tests/acp_e2e/test_e2e_smoke.py
uv run pytest -q tests/acp_e2e/test_e2e_file_transport.py
uv run pytest -q tests/acp_e2e/test_e2e_ws_channel_blob.py
```

## 当前阶段范围

- 已进入实现：A（基础链路）、B（文件传输）、C（WS blob 通道）。
- A/B/C 已具备真实链路回归：smoke、FT-001~005、WS blob 均可执行。
- 延后实现：D/E/F（权限流、session/update 全覆盖、JSON-RPC 韧性）。
