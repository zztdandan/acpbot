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

## v0.9.0 Release Notes

Version `v0.9.0` is the first release that makes the ACP refactor itself the product story, not just an implementation detail.

### Highlights

- Rebuilt `nanobot/acp` around explicit runtime objects instead of the previous large mixin-style dispatcher assembly.
- Established clear module ownership across `runtime`, `inbound`, `sessionmap`, `state`, and `observability`, so request lifecycle, session truth, structured progress, and auditing no longer compete inside one oversized host object.
- Split per-request state into an independent state domain with dedicated pool, router, handler, and permission coordination layers, making text, media, tool, thought, and plan flows easier to reason about and extend.
- Moved inbound preprocessing into a fixed multi-step pipeline, so normalization, permission reply handling, command routing, media preparation, and request building each have a stable boundary.
- Isolated observability and session mapping into standalone owners, which improves debuggability today and lowers the cost of adding future ACP capabilities tomorrow.

### Why this matters

- The ACP backend is no longer organized as a growing collection of cross-calling helpers; it now has a stable object model and explicit lifecycle boundaries.
- New features can be added by extending the relevant domain module instead of reopening a central mixin stack.
- Testing, debugging, and future protocol coverage work all benefit from narrower owners and more predictable data flow.

## Heartbeat Provider Config

Heartbeat task decisions now use a dedicated provider configuration instead of sharing the main agent provider or ACP-specific shortcuts.

- Scheduling remains under `gateway.heartbeat` (`enabled`, `intervalS`, `keepRecentMessages`).
- Decision-time model selection lives under `heartbeat.providerConfig`.
- `heartbeat.providerConfig.providers` reuses the same provider block structure as the main `providers` config, but it is isolated and does not fall back to the main provider pool.
- Native and ACP both use the same heartbeat provider for the `skip/run` decision; execution still follows their existing runtime-specific paths.

## Roadmap

### Phase 1 - Architecture restructuring and real-chain closure (completed)

- Rebuild `nanobot/acp` from the earlier large mixin-style assembly into explicit runtime-owned modules.
- Separate `runtime`, `inbound`, `sessionmap`, `state`, and `observability` into stable owners with clearer request and session boundaries.
- Complete the ACP real-chain baseline for media normalization, `resource_link`, and outbound file return loops for the release path.

### Phase 2 - Permission context and chat-native approval flow (completed)

- Preserve `session_id` and `tool_call` context through the permission chain so approval decisions are bound to the correct active request.
- Treat text-based approval as the primary chat UX, with `/permission <number>` style replies and timeout fallback instead of button-specific assumptions.
- Keep static policy modes as fallback behavior, not the main interactive path.

### Phase 3 - Rich updates, metadata, and E2E closure (completed for v0.9.0)

- Route richer `session/update` families such as thought, plan, usage, commands, config, and session info into dedicated state handlers and final metadata.
- Complete the release-scope ACP E2E verification for the current runtime path, including file transport and chat-facing progress/permission flows.

### Phase 4 - Delivery hardening and long-tail compatibility

- Continue refining tool-call rendering guidance by tool kind so different operations can receive clearer channel-facing descriptions without redefining the core approval flow.
- Harden long-tail JSON-RPC compatibility for unexpected `fs/*` and `terminal/*` callbacks as a delivery and resilience concern, rather than a blocker for the current release milestone.
- Finalize offline deployment artifacts and reproducible runbooks.
- Keep observability/audit outputs stable for troubleshooting and compliance.
- Maintain compatibility with the existing AgentLoop and native/acp dual-backend boundary.

## Acknowledgements

This project is derived from [nanobot](https://github.com/HKUDS/nanobot).

Special thanks to nanobot for providing the community with excellent inspiration and a compact, refined OpenClaw-style implementation.

## License

This project keeps the original license. See [LICENSE](./LICENSE).
