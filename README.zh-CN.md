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
- 直接接入 ACP 兼容的编程 agent 与你原有预设skill tool subagent CLAUDE.md等一切。
- 作为可调试的 AI 底座用于日常工程任务。
- 可随时手动接管会话、介入处理后再交回。
- 会话记录可持续保留在开发 cli 中，便于回放、审计与控制。
- 使用开发 cli 更好的记忆功能、生态体系、适配的工具

## 兼容性说明

为保证现有用户与工具链平滑迁移，当前代码与命令命名暂不变更（例如命令仍为 `nanobot`）。

项目品牌与运行时方向已转向 `acpbot`，但迁移路径保持渐进与务实。

## v0.9.0 版本说明

`v0.9.0` 是一次以 ACP 重构成果为核心的版本发布，而不再只是“内部实现调整”。

### 本版重点

- 将 `nanobot/acp` 从早期的大型 mixin/dispatcher 拼装模式，重构为以显式 runtime owner 为中心的面向对象架构。
- 明确划分 `runtime`、`inbound`、`sessionmap`、`state`、`observability` 等模块边界，让请求生命周期、会话真相、结构化进度、审计记录各归其位。
- 将单次请求执行态独立为完整的 state 领域，拆出消息池、路由器、handler、权限协调器等子域，使 text、media、tool、thought、plan 等更新类型都能按职责扩展。
- 将入站处理收口为固定多阶段 AOP 流水线，使 normalize、permission reply、命令路由、媒体预处理、请求构建形成稳定编排边界。
- 将映射体系与监控体系独立为单独 owner，既提升当前的可调试性，也为后续协议扩展和功能增量预留空间。

### 这意味着什么

- ACP 后端不再依赖一个不断膨胀、跨模块互相调用的大型 helper/mixin 组合体，而是拥有稳定的对象模型与清晰生命周期。
- 后续新增能力时，可以直接扩展所属领域模块，而不是继续回到中心化巨型 dispatcher 中打补丁。
- 未来无论是测试补齐、问题定位，还是继续扩展协议覆盖面，都会建立在更窄、更稳定、更可维护的 owner 边界之上。

## Roadmap

### 阶段 1：架构重组与真实链路收口（已完成）

- 将 `nanobot/acp` 从早期大型 mixin 式拼装重构为显式 runtime owner 驱动的模块化架构。
- 将 `runtime`、`inbound`、`sessionmap`、`state`、`observability` 拆分为稳定 owner，明确请求态与会话态边界。
- 完成面向当前版本发布链路的 media 规范化、`resource_link` 与出站文件回路基线收口。

### 阶段 2：权限上下文与聊天式授权交互（已完成）

- 在权限链路中保留 `session_id` 与 `tool_call` 上下文，使授权决策能够绑定到正确的活跃请求。
- 将文本式授权视为聊天工具的一等交互形态，以 `/permission <number>` 回复与超时兜底作为主交互，而不是预设按钮式 UI。
- 让静态策略模式回归兜底能力，而不是主要交互入口。

### 阶段 3：富更新类型、metadata 与 E2E 收口（`v0.9.0` 已完成）

- 将 thought、plan、usage、commands、config、session info 等更丰富的 `session/update` 家族收口到独立 state handler 与最终 metadata。
- 完成当前运行时主链路所需的 ACP E2E 验证，包括文件传输与面向聊天工具的 progress/permission 流程。

### 阶段 4：交付加固与长尾兼容性

- 继续细化按 tool kind 提供差异化渲染建议，让不同操作在聊天渠道中拥有更清晰的展示文案，而不改变核心授权模型。
- 将意外 `fs/*`、`terminal/*` 回调的 JSON-RPC 长尾兼容与韧性加固归入交付阶段处理，而不再作为当前版本里程碑的阻塞项。
- 完成离线部署产物与可复现运行手册。
- 持续稳定审计与可观测输出，支撑排障与合规。
- 持续保持对 AgentLoop 与 native/acp 双后端边界的兼容。

## 致谢

本项目脱胎于 [nanobot](https://github.com/HKUDS/nanobot)。

感谢 nanobot 为社区提供了优秀灵感与小巧精致的类 OpenClaw 实现。

## 许可证

本项目保持原许可证不变，详见 [LICENSE](./LICENSE)。
