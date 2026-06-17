"""会话 model 能力缓存：解析 ACP payload 中真实返回的模型目录与当前选择。

核心职责：
    提供单会话的 model-only 能力缓存，支持从 ACP payload 解析、本地记忆更新、
    保存 raw payload 调试视图，并导出 prompt metadata 与 `/models` 命令文本。

设计约束：
    - 本对象不直接访问 ACP，所有 ACP 数据由 SessionRuntimeManager 传入。
    - 字段命名兼容不同 ACP SDK 版本（snake_case / camelCase）。
    - 不解析、不保存、不导出 mode/agent；Hermes 的 mode 是审批策略，不是 nanobot agent。
    - raw payload 只用于调试、source 判定与测试断言，不进入 prompt。
    - unknown source 允许 runtime 额外注入 default-only fallback，使 `/models` 与 prompt
      在无 catalog backend 上仍可呈现一个稳定 current model，但该 fallback 不意味着
      backend 真正支持 model switch。
"""

from __future__ import annotations

import copy
from enum import StrEnum

from nanobot.acp.contracts import ACPSessionPayload, JSONMap


class ModelSource(StrEnum):
    """会话模型目录来源枚举：约束 capability/source 判定结果。"""

    SESSION_MODELS = "session_models"
    CONFIG_OPTIONS = "config_options"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class _SessionCapabilities:
    """单会话 model 能力缓存（本地内存层）。

    本轮按 STAGE1 规则彻底收口为 model-only：
    - `models: SessionModelState` 代表 Hermes / legacy ACP model 能力。
    - `configOptions[id=model]` 代表 opencode 新选择器能力。
    - 两者都不存在或没有 catalog 时标记为 `unknown`，上层禁止盲目 set。
    """

    def __init__(self) -> None:
        """初始化空的 model 能力缓存。"""
        self.available_models: list[str] = []
        self.current_model: str | None = None
        self.model_source: ModelSource = ModelSource.UNKNOWN
        self.raw_payload: object | None = None
        self.raw_payload_json: JSONMap | None = None
        # 中文注释：保留来源子目录，供 mixed source 下确定目标 model 优先 set 通道。
        self.session_models_catalog: list[str] = []
        self.config_options_catalog: list[str] = []

    def apply_session_payload(self, payload: ACPSessionPayload) -> None:
        """从 ACP session payload 解析并合并 model 能力。

        处理流程：
            1. 总是保存 raw_payload，并尽力生成 JSON/dict 视图。
            2. 解析 `models` 中的 currentModelId / availableModels。
            3. 解析 `configOptions[id=model]` 中的 currentValue / options[].value。
            4. 根据 payload 真实形态标记 source 并合并 catalog。

        兼容性说明：
            set_model 的空返回 `{}` / `None` / `configOptions=[]` 不应破坏已有 catalog，
            所以当本次 payload 没有非空 catalog 时，仅更新 raw 视图，不清空旧能力。
            只有初始空能力遇到 unknown payload 时才保持 unknown + empty catalog。
        """
        self.raw_payload = payload
        self.raw_payload_json = _payload_to_json_map(payload)

        session_catalog, session_current = _parse_session_models_catalog(payload)
        config_catalog, config_current = _parse_config_options_model_catalog(payload)

        if not session_catalog and not config_catalog:
            # 中文注释：空 set response 或不含 model catalog 的兼容响应不能清空旧 catalog；
            # 但全新 caps 仍应保持 unknown，令 /models 明确提示不兼容。
            return

        self.session_models_catalog = session_catalog
        self.config_options_catalog = config_catalog

        if session_catalog and config_catalog:
            self.available_models = _dedupe([*config_catalog, *session_catalog])
            self.current_model = config_current or session_current or self.current_model
            self.model_source = ModelSource.MIXED
        elif config_catalog:
            self.available_models = list(config_catalog)
            self.current_model = config_current or self.current_model
            self.model_source = ModelSource.CONFIG_OPTIONS
        else:
            self.available_models = list(session_catalog)
            self.current_model = session_current or self.current_model
            self.model_source = ModelSource.SESSION_MODELS

        if self.current_model and self.current_model not in self.available_models:
            # 中文注释：部分 backend 会返回当前 model 但 catalog 不完整；只有已经确认存在
            # 非空 catalog 时才把 current 补进去，避免 unknown source 被 current 单值伪造成可用目录。
            self.available_models.append(self.current_model)

    def remember_current_model(self, model_id: str) -> None:
        """本地缓存当前模型选择（不触发任何 ACP IO）。"""
        if isinstance(model_id, str) and model_id:
            self.current_model = model_id
            if self.available_models and model_id not in self.available_models:
                self.available_models.append(model_id)

    def materialize_unknown_default_model(self, model_id: str) -> None:
        """为 unknown source 注入 default-only fallback 视图。

        中文注释：
        - 只在没有 backend catalog 时使用。
        - 保持 `model_source == ModelSource.UNKNOWN`，这样上层仍能识别“不可真实切换”的 backend。
        - 该 fallback 只改善 `/models` 与 prompt/current model 展示，不表示 backend 已支持
          `set_model` wire call。
        """
        if self.model_source != ModelSource.UNKNOWN:
            return
        if not isinstance(model_id, str) or not model_id:
            return
        self.current_model = model_id
        self.available_models = [model_id]

    def build_prompt_metadata(self) -> JSONMap:
        """导出 prompt metadata 视图；仅包含 model，不包含 raw payload 或 agent/mode。"""
        prompt_meta: JSONMap = {}
        if isinstance(self.current_model, str) and self.current_model:
            prompt_meta["nanobot_session_model"] = self.current_model
        return prompt_meta

    def render_models_command(self) -> str:
        """导出 `/models` 命令文本响应。"""
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


def build_session_capabilities_from_payload(payload: ACPSessionPayload) -> _SessionCapabilities:
    """从 ACP payload 创建新的 model 能力缓存对象（工厂函数）。"""
    caps = _SessionCapabilities()
    caps.apply_session_payload(payload)
    return caps


def _parse_session_models_catalog(payload: object) -> tuple[list[str], str | None]:
    """解析 Hermes / SessionModelState 风格的 model catalog。"""
    models = _pick(payload, "models")
    if models is None:
        return [], None

    current = _pick(models, "current_model_id", "currentModelId")
    current_model = current if isinstance(current, str) and current else None
    parsed: list[str] = []
    raw_available = _pick(models, "available_models", "availableModels") or []
    available = raw_available if isinstance(raw_available, list) else []
    for entry in available:
        model_id = _pick(entry, "model_id", "modelId")
        if isinstance(model_id, str) and model_id:
            parsed.append(model_id)
    return _dedupe(parsed), current_model


def _parse_config_options_model_catalog(payload: object) -> tuple[list[str], str | None]:
    """解析 opencode `configOptions[id=model]` 风格的 model selector。"""
    options = _pick(payload, "config_options", "configOptions") or []
    if not isinstance(options, list):
        return [], None

    for option in options:
        option_id = _pick(option, "id")
        if option_id != "model":
            continue
        option_type = _pick(option, "type")
        candidates = _pick(option, "options") or []
        if option_type not in (None, "select") or not isinstance(candidates, list):
            continue
        parsed: list[str] = []
        for candidate in candidates:
            value = _pick(candidate, "value")
            if isinstance(value, str) and value:
                parsed.append(value)
        current = _pick(option, "current_value", "currentValue")
        current_model = current if isinstance(current, str) and current else None
        return _dedupe(parsed), current_model
    return [], None


def _payload_to_json_map(payload: object) -> JSONMap | None:
    """尽力把 payload 转成安全 JSON map；失败不影响主流程。"""
    try:
        if isinstance(payload, dict):
            copied = copy.deepcopy(payload)
            return copied if isinstance(copied, dict) else None
        model_dump = getattr(payload, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump(mode="json", by_alias=True, exclude_none=True)
            return dumped if isinstance(dumped, dict) else None
    except Exception:
        return None
    return None


def _dedupe(items: list[str]) -> list[str]:
    """按出现顺序去重，保留 backend 原始 model id。"""
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _pick(obj: object, *names: str) -> object:
    """按候选字段名顺序获取属性值（兼容 dict / Pydantic / SDK 对象）。"""
    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj[name]
        return None

    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None
