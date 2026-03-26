"""ACP session 能力解析与展示辅助。"""

from __future__ import annotations

from typing import Any

from nanobot.acp.state import _SessionCapabilities


def _pick(obj: Any, *names: str) -> Any:
    """兼容 snake/camel 字段名时，按候选名顺序取值。"""
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _update_caps_from_session_payload(
    session_caps: dict[str, _SessionCapabilities],
    session_id: str,
    payload: Any,
) -> None:
    """从 ACP session payload 中提取模型/agent 能力缓存。"""
    caps = session_caps.setdefault(session_id, _SessionCapabilities())
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
    session_caps: dict[str, _SessionCapabilities],
    session_id: str,
) -> str:
    """格式化当前 session 的模型列表。"""
    caps = session_caps.get(session_id)
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
    session_caps: dict[str, _SessionCapabilities],
    session_id: str,
) -> str:
    """格式化当前 session 的 agent(mode) 列表。"""
    caps = session_caps.get(session_id)
    if not caps or not caps.available_agents:
        return "No agent/mode catalog returned by current ACP backend for this session."
    lines = []
    current = caps.current_agent
    for agent_id in caps.available_agents:
        prefix = "* " if current == agent_id else "  "
        lines.append(f"{prefix}{agent_id}")
    header = f"Current agent: {current}" if current else "Current agent: unknown"
    return "\n".join([header, "Available agents:", *lines])
