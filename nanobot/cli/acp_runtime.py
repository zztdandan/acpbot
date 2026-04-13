"""ACP-aware CLI runtime helpers.

This module isolates the CLI pieces that differ from upstream nanobot:
- dispatch backend override parsing (native/acp)
- ACP config override parsing (inline JSON or JSON file)
- runtime dispatcher construction for gateway/agent commands
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from nanobot.bus.queue import MessageBus
from nanobot.config.schema import Config
from nanobot.cron.service import CronService
from nanobot.providers.base import LLMProvider
from nanobot.session.manager import SessionManager

_CLI_DISPATCHER_OVERRIDE: str | None = None
_CLI_ACP_CONFIG_OVERRIDE: str | None = None


def reset_dispatch_overrides() -> None:
    """Clear process-level CLI dispatch overrides.

    Kept explicit so each CLI invocation starts from a clean state.
    """

    global _CLI_DISPATCHER_OVERRIDE, _CLI_ACP_CONFIG_OVERRIDE
    _CLI_DISPATCHER_OVERRIDE = None
    _CLI_ACP_CONFIG_OVERRIDE = None


def set_dispatch_overrides(*, dispatcher: str | None, acp_config: str | None) -> None:
    """Store dispatch overrides from global CLI flags."""

    global _CLI_DISPATCHER_OVERRIDE, _CLI_ACP_CONFIG_OVERRIDE
    if dispatcher:
        _CLI_DISPATCHER_OVERRIDE = dispatcher.strip().lower()
    if acp_config:
        _CLI_ACP_CONFIG_OVERRIDE = acp_config


def _parse_acp_override(raw: str) -> dict[str, object]:
    """Parse ACP override from either inline JSON or a JSON file path."""

    candidate = Path(raw).expanduser()
    if candidate.exists() and candidate.is_file():
        return json.loads(candidate.read_text(encoding="utf-8"))
    return json.loads(raw)


def apply_dispatch_overrides(
    config: Config,
    *,
    dispatcher: str | None = None,
    acp_config: str | None = None,
) -> Config:
    """Apply dispatcher/acp-config overrides onto loaded config.

    Raises ValueError when overrides are malformed.
    """

    dispatch_choice = (dispatcher or _CLI_DISPATCHER_OVERRIDE or "").strip().lower()
    if dispatch_choice:
        if dispatch_choice not in {"native", "acp"}:
            raise ValueError("--dispatcher must be 'native' or 'acp'")
        config.dispatch.backend = dispatch_choice  # type: ignore[assignment]

    acp_raw = acp_config or _CLI_ACP_CONFIG_OVERRIDE
    if acp_raw:
        overrides = _parse_acp_override(acp_raw)
        config.dispatch.acp = config.dispatch.acp.model_copy(update=overrides)

    return config


def dispatch_requires_provider(config: Config) -> bool:
    """Return whether the selected backend needs an LLM provider instance."""

    return config.dispatch.backend == "native"


def create_dispatch_runtime(
    *,
    bus: MessageBus,
    config: Config,
    provider: LLMProvider | None,
    cron_service: CronService | None,
    session_manager: SessionManager | None,
) -> Any:
    """Build the runtime dispatcher from dispatch backend settings.

    Native backend uses AgentLoop via NativeDispatcher.
    ACP backend delegates to ACPDispatcher.
    """

    from nanobot.dispatch import ACPDispatcher, NativeDispatcher

    if config.dispatch.backend == "acp":
        return ACPDispatcher(
            bus=bus,
            workspace=config.workspace_path,
            acp_config=config.dispatch.acp,
            channels_config=config.channels,
        )

    if provider is None:
        raise RuntimeError("Native backend requires a configured provider")

    return NativeDispatcher(
        bus=bus,
        provider=provider,
        config=config,
        cron_service=cron_service,
        session_manager=session_manager,
    )
