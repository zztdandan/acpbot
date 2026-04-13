"""Session capability helpers kept internal to sessionmap-driven ACP flows."""

from __future__ import annotations

from nanobot.acp.contracts import ACPSessionPayload, JSONMap
from nanobot.acp.sessionmap.models import _SessionCapabilities


def _pick(obj: ACPSessionPayload, *names: str) -> ACPSessionPayload:
    """Return the first matching snake_case/camelCase attribute if present."""

    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _update_caps_from_session_payload(
    caps: _SessionCapabilities,
    payload: ACPSessionPayload,
) -> None:
    """Extract model/agent capability caches from an ACP session payload."""

    models = _pick(payload, "models")
    if models is not None:
        current = _pick(models, "current_model_id", "currentModelId")
        if isinstance(current, str) and current:
            caps.current_model = current
        available = _pick(models, "available_models", "availableModels") or []
        parsed_models: list[str] = []
        for entry in available:
            model_id = _pick(entry, "model_id", "modelId")
            if isinstance(model_id, str) and model_id:
                parsed_models.append(model_id)
        if parsed_models:
            caps.available_models = parsed_models

    modes = _pick(payload, "modes")
    if modes is not None:
        current = _pick(modes, "current_mode_id", "currentModeId")
        if isinstance(current, str) and current:
            caps.current_agent = current
        available = _pick(modes, "available_modes", "availableModes") or []
        parsed_agents: list[str] = []
        for entry in available:
            mode_id = _pick(entry, "id")
            if isinstance(mode_id, str) and mode_id:
                parsed_agents.append(mode_id)
        if parsed_agents:
            caps.available_agents = parsed_agents


def _render_models_command(
    caps: _SessionCapabilities | None,
) -> str:
    """Format the model catalog for the current session."""

    if not caps or not caps.available_models:
        return "No model catalog returned by current ACP backend for this session."
    lines = []
    current = caps.current_model
    for model_id in caps.available_models:
        prefix = "* " if current == model_id else "  "
        lines.append(f"{prefix}{model_id}")
    header = f"Current model: {current}" if current else "Current model: unknown"
    return "\n".join([header, "Available models:", *lines])


def _render_agents_command(
    caps: _SessionCapabilities | None,
) -> str:
    """Format the agent/mode catalog for the current session."""

    if not caps or not caps.available_agents:
        return "No agent/mode catalog returned by current ACP backend for this session."
    lines = []
    current = caps.current_agent
    for agent_id in caps.available_agents:
        prefix = "* " if current == agent_id else "  "
        lines.append(f"{prefix}{agent_id}")
    header = f"Current agent: {current}" if current else "Current agent: unknown"
    return "\n".join([header, "Available agents:", *lines])


def _build_prompt_metadata(caps: _SessionCapabilities | None) -> JSONMap:
    """Build prompt metadata from runtime capability cache."""

    prompt_meta: JSONMap = {}
    if caps is not None and isinstance(caps.current_model, str) and caps.current_model:
        prompt_meta["nanobot_session_model"] = caps.current_model
    if caps is not None and isinstance(caps.current_agent, str) and caps.current_agent:
        prompt_meta["nanobot_session_agent"] = caps.current_agent
    return prompt_meta
