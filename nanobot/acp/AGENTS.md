# nanobot/acp Architecture Handbook (runtime rearchitecture)

This directory keeps only the new ACP architecture code and no longer preserves
legacy dispatcher family, mixin, or history compatibility implementations.

## 1) Current target structure

- `runtime.py`
  - main entrypoint for `ACPRuntime` / `ACPDispatcher`
  - owner of connection lifecycle, `request_key -> wait entry`, bus inbound loop,
    and unified completion
- `runtime_lifecycle.py`
  - connection bootstrap, reset, and close
- `runtime_process_manager.py`
  - owner of per-`nanobot_side_session_key` serial queues, active request lifecycle,
    and queued cleanup under `/stop`
- `dispatch_process_direct.py`
  - real ACP prompt execution helper used only during execution
- `runtime_models.py`
  - shared dataclasses and enums for runtime / inbound / process / state
- `runtime_client.py`
  - ACP callback -> runtime owner bridge

- `inbound/`
  - owner of `ProcessDirectInput` / `InboundMessage` -> `InboundContext`
  - fixed steps: `normalize -> permission_inbound -> command_router -> media_prepare -> build_process_request`
  - responsible only for direct responses or `ProcessRequest` construction

- `sessionmap/`
  - `binding_manager.py`: binding truth and persistence
  - `runtime_manager.py`: ready sessions within the current runtime lifetime
  - `models.py`: shared binding truth and runtime-ready entry models
  - `internal/`: non-owner persistence, reconciliation, and session capability helpers used by sessionmap-driven ACP flows only

- `state/`
  - owner of one-request state manager, progress router, permission waiter, and final materialization
  - created once per request and destroyed once per request

- `observability/`
  - owner of the structured event queue and its consumer
  - business modules only push events and do not couple themselves to audit/tooling sinks

## 2) Hard constraints

- Do not add or restore legacy root-level family files such as:
  - `session_update_*`
  - `progress_*`
  - `media_codec_*`
  - `observability_*`
  - `session_map_*`
  - `dispatcher_*` (except `dispatcher.py` as the compatibility export layer)
- Do not add new runtime-top-level scattered state dictionaries for request/session process state
- Do not keep compatibility code for old ACP tests, old mixins, or old helpers

## 3) Read/write rules

- Keep the entrypoint thin: `dispatcher.py` only preserves external import compatibility
- Keep owner boundaries explicit: runtime / inbound / process manager / sessionmap / state / observability each owns its own closure
- Comments should explain *why*, especially around:
  - ACP schema compatibility branches
  - timeout / reset / rebuild fallback branches
  - lifecycle edges such as `/stop`, permission, and late events

## 4) Maintenance requirements

- If the directory structure or owner boundaries change, update this file in the same change
- Reintroducing legacy family files is treated as an architecture rollback unless the user explicitly requests it
