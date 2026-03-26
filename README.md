<div align="center">
  <img src="acpbot.jpg" alt="acpbot" width="500">
  <h1>acpbot</h1>
  <p>
    ACP-based, channel-first AI runtime derived from nanobot.
  </p>
  <p>
    <a href="./README.md">English</a> | <a href="./README.zh-CN.md">简体中文</a>
  </p>
  <p>
    <img src="https://img.shields.io/badge/python-%E2%89%A53.11-blue" alt="Python">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
  </p>
</div>

## What Is acpbot

`acpbot` is this project's new identity.

This repository was born from the `nanobot` project and keeps nanobot's elegant inbound/outbound and channel architecture, while replacing the underlying agent runtime with an ACP (Agent Client Protocol) standard layer.

In short: this is no longer just a downstream nanobot branch for daily syncing; it is an ACP-first evolution for developer workflows.

## Why Refactor nanobot

Many OpenClaw-style services implement their own agent runtime. That works well for general assistants, but for developers, reusing mature presets and workflows from existing coding agents (such as Claude Code, Codex/OpenCode, and similar tools) is often a better fit.

So this project swaps out nanobot's original runtime core and introduces an ACP protocol layer instead.

## What You Get

- Keep nanobot's strong channel capabilities and inbound/outbound pipeline.
- Connect ACP-compatible coding agents and existing personal presets.
- Use it as a debuggable AI foundation for day-to-day engineering work.
- Manually take over sessions at any time, intervene, process, and hand control back.
- Preserve session records for transparent and controllable operations.

## Compatibility Notes

To avoid breaking existing users and tooling, code and command naming remain unchanged for now (for example, command names still use `nanobot`).

Branding and runtime direction have changed to `acpbot`, but the migration path stays practical and incremental.

## Acknowledgements

This project is derived from [nanobot](https://github.com/HKUDS/nanobot).

Special thanks to nanobot for providing the community with excellent inspiration and a compact, refined OpenClaw-style implementation.

## License

This project keeps the original license. See [LICENSE](./LICENSE).
