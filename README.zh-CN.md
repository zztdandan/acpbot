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

## 致谢

本项目脱胎于 [nanobot](https://github.com/HKUDS/nanobot)。

感谢 nanobot 为社区提供了优秀灵感与小巧精致的类 OpenClaw 实现。

## 许可证

本项目保持原许可证不变，详见 [LICENSE](./LICENSE)。
