<div align="center">
  <img src="acpbot.jpg" alt="acpbot" width="500">
  <h1>acpbot</h1>
  <p>
    基于 ACP 协议标准层、保留渠道能力的 AI 运行时。
  </p>
  <p>
    <a href="./README.md">English</a> | <a href="./README.zh-CN.md">简体中文</a>
  </p>
  <p>
    <img src="https://img.shields.io/badge/python-%E2%89%A53.11-blue" alt="Python">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  </p>
</div>

## acpbot 是什么

`acpbot` 是本项目的新定位。

本仓库脱胎于 `nanobot`，保留了 nanobot 精妙的 inbound/outbound 与 channel 架构，同时将底层 agent runtime 替换为 ACP（Agent Client Protocol）协议标准层。

也就是说：本项目已不再是仅用于日常跟随同步的 nanobot 分支，而是面向开发者工作流的 ACP-first 改造版本。

## 为什么改造 nanobot

OpenClaw 系服务很多都自研了底层 agent runtime。对于通用助手场景这很有效，但对于编程人员而言，直接复用 Claude Code、Codex/OpenCode 等现有编程 agent 里已经打磨好的预设与工作流，通常更符合实际开发需求。

因此，本项目暂时替换了 nanobot 原有 runtime 实现，改为 ACP 协议层承载。

## 你可以得到什么

- 保留 nanobot 成熟的渠道能力与 inbound/outbound 管线。
- 直接接入 ACP 兼容的编程 agent 与你原有预设。
- 作为可调试的 AI 底座用于日常工程任务。
- 可随时手动接管会话、介入处理后再交回。
- 会话记录可持续保留，便于回放、审计与控制。

## 兼容性说明

为保证现有用户与工具链平滑迁移，当前代码与命令命名暂不变更（例如命令仍为 `nanobot`）。

项目品牌与运行时方向已转向 `acpbot`，但迁移路径保持渐进与务实。

## 当前开发进展（2026-03）

基于当前项目文档（`docs/design`、`docs/issue`、`docs/research`、`docs/superpowers`）综合状态，ACP 改造已完成核心链路可用，当前处于收口与韧性增强阶段。

### 已完成并验证

- ACP runtime 基线与配置体系文档化并已落地。
- 超大帧导致的会话中断与会话韧性问题已修复。
- Telegram 出站进度/最终消息重复与批处理问题已修复。
- inbound/outbound JSONL 运行日志已支持轮转与进程退出关闭。
- ACP 可观测性加固已完成（inbound 去重、outbound JSON、gateway 启动分片日志）。
- ACP session 持久化、heartbeat 广播、cron 会话模式改造已完成。
- session 列表解析/对账与重启后激活链路已修复。
- ACP 文件传输统一基线已完成（media <-> ACP blocks、WS blob/path 双模式、2MB 限制、100KB 回归通过）。
- ACP E2E smoke `E2E-000~003` 已稳定通过。
- WS blob E2E（`E2E-WS-001~003`）已通过。

### 当前进行中

- FT 真实链路落地（TDD）：入站 `resource_link` 收敛 + 出站 plugin 回路（`acp_send_file`）。
- ACP E2E backlog 的 D/E/F 套件分阶段落地准备。
- ACP 全离线双阶段容器部署子库持续推进中。

### 仍待解决的缺口

- ACP JSON-RPC 覆盖不完整（`fs/read_text_file`、`terminal/*` 等回调能力缺口）。
- 权限流仍以静态策略（`strict` / `trusted` / `yolo`）为主，尚未形成渠道用户交互式授权。
- 授权链路中的 `tool_call` 上下文尚未完整保留，影响“批准什么操作”的展示能力。
- 多种 `session/update` 类型（如 `agent_thought_chunk`、`plan`、`usage_update`）尚未端到端覆盖。

## Roadmap

### 阶段 1：文件传输真实链路收口（当前）

- 完成入站 media 规范化，统一为稳定的 `resource_link` 语义。
- 完成基于 canonical plugin（`acp_send_file`）的真实出站文件回路。
- 跑通并稳定 FT `E2E-FT-001~005`，以结构化审计事件作为关键验收依据。

### 阶段 2：权限与交互模型完善

- 在权限请求链路保留 `session_id` 与 `tool_call` 完整上下文。
- 打通渠道侧“批准/拒绝”交互授权与超时兜底。
- 让静态策略回归兜底能力，而非主交互路径。

### 阶段 3：session/update 与 JSON-RPC 全量覆盖

- 扩展 `session/update` 对 thought/plan/usage/config/info/commands 等更新类型的处理。
- 增强“能力关闭场景下异常 RPC”韧性，避免会话崩溃。
- 按 `tests/acp_e2e` 逐项完成 D/E/F backlog 并收敛验收口径。

### 阶段 4：交付与运行保障

- 完成离线部署产物与可复现运行手册。
- 持续稳定审计与可观测输出，支撑排障与合规。
- 持续保持对 AgentLoop 与 native/acp 双后端边界的兼容。

## 致谢

本项目脱胎于 [nanobot](https://github.com/HKUDS/nanobot)。

感谢 nanobot 为社区提供了优秀灵感与小巧精致的类 OpenClaw 实现。

## 许可证

本项目保持原许可证不变，详见 [LICENSE](./LICENSE)。
