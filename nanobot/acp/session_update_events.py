"""session_update -> ACPProgressEvent 归一化。

该模块位于 ACP 会话进度链路的入口适配层，核心目标是：
1. 将 ACP SDK 发出的多态 update（对象/字典、不同命名风格）统一成内部事件。
2. 在进入 progress router 前完成稳定分类（family）与路由键（route_key）计算。
3. 提取上层最常用的观测字段，降低后续分发、日志、回放逻辑的耦合度。
"""

from __future__ import annotations

import time
from typing import Any

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state import ACPUpdateType


def _pick(obj: Any, *names: str) -> Any:
    """从对象或字典中按优先顺序取值。

    设计意图：
    - 统一兼容对象属性访问与 dict 键访问；
    - 兼容同一语义在不同来源中的命名差异（如 camelCase/snake_case）。

    返回：命中的第一个值；若都不存在则返回 None。
    """
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
        if isinstance(obj, dict) and name in obj:
            return obj[name]
    return None


def _to_jsonable(update: Any) -> Any:
    """将 update 尽力转换为可序列化 JSON 结构。

    处理顺序：
    1. 若为 pydantic-like 对象，优先使用 model_dump 输出 json 友好结构；
    2. 若本身是 dict，直接复用；
    3. 其余情况降级为字符串包装，确保事件不会因序列化失败而丢失。
    """
    model_dump = getattr(update, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump(by_alias=True, exclude_none=True, mode="json")
        except Exception:
            pass
    if isinstance(update, dict):
        return update
    return {"raw": str(update)}


def _normalize_update_type(update: Any, raw_json: Any) -> ACPUpdateType:
    session_update = _pick(raw_json, "sessionUpdate", "session_update")
    raw_label = session_update or type(update).__name__
    mapping = {
        "user_message_chunk": ACPUpdateType.USER_MESSAGE_CHUNK,
        "UserMessageChunk": ACPUpdateType.USER_MESSAGE_CHUNK,
        "agent_message_chunk": ACPUpdateType.AGENT_MESSAGE_CHUNK,
        "AgentMessageChunk": ACPUpdateType.AGENT_MESSAGE_CHUNK,
        "agent_thought_chunk": ACPUpdateType.AGENT_THOUGHT_CHUNK,
        "AgentThoughtChunk": ACPUpdateType.AGENT_THOUGHT_CHUNK,
        "tool_call_start": ACPUpdateType.TOOL_CALL_START,
        "ToolCallStart": ACPUpdateType.TOOL_CALL_START,
        "tool_call_progress": ACPUpdateType.TOOL_CALL_PROGRESS,
        "ToolCallProgress": ACPUpdateType.TOOL_CALL_PROGRESS,
        "tool_call_update": ACPUpdateType.TOOL_CALL_UPDATE,
        "ToolCallUpdate": ACPUpdateType.TOOL_CALL_UPDATE,
        "agent_plan_update": ACPUpdateType.AGENT_PLAN_UPDATE,
        "AgentPlanUpdate": ACPUpdateType.AGENT_PLAN_UPDATE,
        "available_commands_update": ACPUpdateType.AVAILABLE_COMMANDS_UPDATE,
        "AvailableCommandsUpdate": ACPUpdateType.AVAILABLE_COMMANDS_UPDATE,
        "current_mode_update": ACPUpdateType.CURRENT_MODE_UPDATE,
        "CurrentModeUpdate": ACPUpdateType.CURRENT_MODE_UPDATE,
        "config_option_update": ACPUpdateType.CONFIG_OPTION_UPDATE,
        "ConfigOptionUpdate": ACPUpdateType.CONFIG_OPTION_UPDATE,
        "session_info_update": ACPUpdateType.SESSION_INFO_UPDATE,
        "SessionInfoUpdate": ACPUpdateType.SESSION_INFO_UPDATE,
        "usage_update": ACPUpdateType.USAGE_UPDATE,
        "UsageUpdate": ACPUpdateType.USAGE_UPDATE,
    }
    return mapping.get(str(raw_label), ACPUpdateType.UNKNOWN)


def _classify_family(update: Any, update_type: ACPUpdateType) -> str:
    """将 ACP update 归类到内部 family。

    family 用于后续 router 的分流与聚合，尽量贴合业务语义而非底层类型细节：
    - user/thought/text/media：消息呈现相关；
    - tool：工具调用生命周期；
    - usage/plan/state：会话状态与元信息更新；
    - other：兜底分类，保障前向兼容。
    """
    from acp.interfaces import AgentMessageChunk, AgentThoughtChunk, UserMessageChunk
    from acp.schema import EmbeddedResourceContentBlock, ImageContentBlock, ResourceContentBlock

    if isinstance(update, UserMessageChunk):
        return "user"
    if isinstance(update, AgentThoughtChunk):
        return "thought"
    if isinstance(update, AgentMessageChunk):
        content = _pick(update, "content")
        # 媒体/资源内容走独立 family，便于与纯文本消息分路处理。
        if isinstance(
            content, (ImageContentBlock, ResourceContentBlock, EmbeddedResourceContentBlock)
        ):
            return "media"
        text = _pick(content, "text")
        # 有明确 text 字段时归入 text，否则保守归入 other。
        if isinstance(text, str):
            return "text"
        return "other"
    if update_type in {
        ACPUpdateType.TOOL_CALL_START,
        ACPUpdateType.TOOL_CALL_PROGRESS,
        ACPUpdateType.TOOL_CALL_UPDATE,
    }:
        return "tool"
    if update_type == ACPUpdateType.USAGE_UPDATE:
        return "usage"
    if update_type == ACPUpdateType.AGENT_PLAN_UPDATE:
        return "plan"
    if update_type in {
        ACPUpdateType.CURRENT_MODE_UPDATE,
        ACPUpdateType.CONFIG_OPTION_UPDATE,
        ACPUpdateType.AVAILABLE_COMMANDS_UPDATE,
        ACPUpdateType.SESSION_INFO_UPDATE,
    }:
        return "state"
    if update_type == ACPUpdateType.AGENT_THOUGHT_CHUNK:
        return "thought"
    return "other"


def _build_route_key(session_id: str, family: str, raw_json: Any) -> str:
    """根据 family 生成路由键 route_key。

    路由键用于决定同类进度事件的归并粒度：
    - tool：优先按 tool_call_id 聚合，保证同一次工具调用事件串联；
    - media：优先按资源唯一标识（uri/name）聚合，避免多媒体消息串扰；
    - 其余 family：按 session 级聚合，保持会话视角一致性。
    """
    if family == "tool":
        tool_call_id = _pick(raw_json, "toolCallId", "tool_call_id")
        if isinstance(tool_call_id, str) and tool_call_id.strip():
            return tool_call_id.strip()
    if family == "media":
        resource = _pick(raw_json, "content")
        resource_key = _pick(resource, "uri", "name", "resourceUri", "resource_uri")
        if isinstance(resource_key, str) and resource_key.strip():
            return resource_key.strip()
    if family in {"state", "usage", "plan", "thought", "other", "text", "user"}:
        return session_id
    return session_id


def to_progress_event(*, session_id: str, update: Any) -> ACPProgressEvent:
    """将 ACP update 归一化为内部事件 ACPProgressEvent。

    该函数是本模块的主入口，负责组装统一事件对象：
    1. 产出 raw_json（可观测/可记录）；
    2. 计算 update_type、family、route_key（可路由）；
    3. 提取 extracted 关键字段（供上层快速消费）；
    4. 记录 received_at_ms（用于时序分析与回放）。
    """

    raw_json = _to_jsonable(update)
    update_type = _normalize_update_type(update, raw_json)
    family = _classify_family(update, update_type)
    route_key = _build_route_key(session_id, family, raw_json)
    extracted = {
        # 提供给上层常用的轻量索引字段，避免重复解析 raw_json。
        "tool_call_id": _pick(raw_json, "toolCallId", "tool_call_id"),
        "status": _pick(raw_json, "status"),
        "content_type": _pick(_pick(raw_json, "content"), "type"),
    }
    return ACPProgressEvent(
        session_id=session_id,
        raw_update=update,
        raw_json=raw_json,
        update_type=update_type,
        family=family,
        route_key=route_key,
        extracted=extracted,
        received_at_ms=int(time.time() * 1000),
    )
