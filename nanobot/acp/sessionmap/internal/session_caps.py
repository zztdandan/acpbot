"""会话能力缓存 owner：封装模型/代理目录与当前选择。

本模块统一承载 `_SessionCapabilities` 的数据、行为与 ACP payload 解析逻辑，
避免 runtime_manager 再维护一层纯转发方法。
"""

from __future__ import annotations

from nanobot.acp.contracts import ACPSessionPayload, JSONMap


class _SessionCapabilities:
    """单会话模型与代理能力缓存。

    该对象只负责本地能力缓存与派生视图，不直接访问 ACP；
    真正的 ACP 取值由 runtime_manager 拿到 payload 后，再交给本对象解析/合并。
    """

    def __init__(self) -> None:
        self.available_models: list[str] = []
        self.current_model: str | None = None
        self.available_agents: list[str] = []
        self.current_agent: str | None = None

    def apply_session_payload(self, payload: ACPSessionPayload) -> None:
        """把 ACP session payload 合并进当前能力缓存。"""

        models = _pick(payload, "models")
        if models is not None:
            current = _pick(models, "current_model_id", "currentModelId")
            if isinstance(current, str) and current:
                self.current_model = current

            available = _pick(models, "available_models", "availableModels") or []
            parsed_models: list[str] = []
            for entry in available:
                model_id = _pick(entry, "model_id", "modelId")
                if isinstance(model_id, str) and model_id:
                    parsed_models.append(model_id)
            if parsed_models:
                self.available_models = parsed_models

        modes = _pick(payload, "modes")
        if modes is not None:
            current = _pick(modes, "current_mode_id", "currentModeId")
            if isinstance(current, str) and current:
                self.current_agent = current

            available = _pick(modes, "available_modes", "availableModes") or []
            parsed_agents: list[str] = []
            for entry in available:
                mode_id = _pick(entry, "id")
                if isinstance(mode_id, str) and mode_id:
                    parsed_agents.append(mode_id)
            if parsed_agents:
                self.available_agents = parsed_agents

    def remember_current_model(self, model_id: str) -> None:
        """同步本地缓存中的当前模型，不触发任何 ACP IO。"""

        self.current_model = model_id

    def remember_current_agent(self, agent_id: str) -> None:
        """同步本地缓存中的当前代理，不触发任何 ACP IO。"""

        self.current_agent = agent_id

    def build_prompt_metadata(self) -> JSONMap:
        """导出 prompt metadata 视图。"""

        prompt_meta: JSONMap = {}
        if isinstance(self.current_model, str) and self.current_model:
            prompt_meta["nanobot_session_model"] = self.current_model
        if isinstance(self.current_agent, str) and self.current_agent:
            prompt_meta["nanobot_session_agent"] = self.current_agent
        return prompt_meta

    def render_models_command(self) -> str:
        """导出 `/models` 文本。"""

        if not self.available_models:
            return "No model catalog returned by current ACP backend for this session."

        lines = []
        for model_id in self.available_models:
            prefix = "* " if self.current_model == model_id else "  "
            lines.append(f"{prefix}{model_id}")

        header = (
            f"Current model: {self.current_model}"
            if self.current_model
            else "Current model: unknown"
        )
        return "\n".join([header, "Available models:", *lines])

    def render_agents_command(self) -> str:
        """导出 `/agents` 文本。"""

        if not self.available_agents:
            return "No agent/mode catalog returned by current ACP backend for this session."

        lines = []
        for agent_id in self.available_agents:
            prefix = "* " if self.current_agent == agent_id else "  "
            lines.append(f"{prefix}{agent_id}")

        header = (
            f"Current agent: {self.current_agent}"
            if self.current_agent
            else "Current agent: unknown"
        )
        return "\n".join([header, "Available agents:", *lines])


def build_session_capabilities_from_payload(payload: ACPSessionPayload) -> _SessionCapabilities:
    """从 ACP payload 创建新的能力缓存对象。"""

    caps = _SessionCapabilities()
    caps.apply_session_payload(payload)
    return caps


def _pick(obj: ACPSessionPayload, *names: str) -> ACPSessionPayload:
    """从对象中按候选字段名顺序获取属性值。

    参数：
        obj: 要查找的对象（通常是 Pydantic model 或 dict）
        *names: 候选字段名列表（按优先级排序）

    返回：
        ACPSessionPayload: 第一个存在的属性值，如果都不存在则返回 None

    使用场景：
        - 兼容不同 SDK 版本的字段命名
        - 支持 snake_case 和 camelCase 两种风格

    示例：
        # SDK v1 使用 snake_case
        value = _pick(payload, "current_model_id", "currentModelId")
        # SDK v2 使用 camelCase
        value = _pick(payload, "currentModelId", "current_model_id")

    注意：
        此函数是私有的（_ 前缀），仅在本模块内部使用。
    """

    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None
