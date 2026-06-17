"""ACP model 切换兼容适配器。

为什么放在 nanobot 内部而不改 python-sdk：
    python-sdk 当前已暴露 `set_config_option`，但不同 backend 对 legacy
    `session/set_model` 的支持仍处于过渡期；本轮目标是修 nanobot 的用户选择层，
    不改变 SDK 公开 API 或生成 schema，避免把 backend 兼容策略扩散到上游库。

为什么 `set_session_model` 需要 raw fallback：
    Hermes 与 opencode 仍保留 wire method `session/set_model`，但当前
    ClientSideConnection 未必有 typed `set_session_model`。raw bridge 集中封装
    私有 `_conn.send_request` 访问，避免业务层到处直接碰 SDK 私有对象。

为什么空返回也视为成功：
    legacy `session/set_model` 和 Hermes 兼容 `set_config_option` 常返回 `{}`、
    `None` 或 `configOptions=[]`；只要 JSON-RPC 没有 error/exception，就说明 set
    request 已被 backend 接受，不能因为空 payload 误报失败。

为什么 model set 后禁止 refresh：
    多个 ACP backend 的 resume/load 可能重置 session model。正确做法是 set 成功后
    只更新本地 capability 与 binding truth，下一次自然 resume/load/new 后再重解析。

为什么 source unknown 直接失败：
    unknown 表示 backend 没返回 nanobot 可理解的 model catalog；此时盲试 set 会把
    用户输入的任意字符串发给 backend，既不可靠也会污染 session 状态。

raw JSON-RPC 风险边界：
    本文件只在 typed `set_session_model` 不存在时访问 `conn._conn.send_request`，
    且仅发送已由 capability catalog 校验过的 model id；如果 SDK 私有结构变化，
    该尝试会被标记 unsupported 并交给 fallback/错误汇总处理。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal

from nanobot.acp.sessionmap.internal.session_caps import ModelSource, _SessionCapabilities

ModelSwitchMethod = Literal["set_config_option", "set_session_model"]


@dataclass(slots=True)
class ModelSwitchAttempt:
    method: ModelSwitchMethod
    supported: bool
    success: bool
    error: str | None = None
    response_payload: object | None = None


@dataclass(slots=True)
class ModelSwitchCompatResult:
    success: bool
    selected_method: ModelSwitchMethod | None
    attempts: list[ModelSwitchAttempt]
    reason: str
    response_payload: object | None = None


async def switch_model_compat(
    conn: Any,
    *,
    session_id: str,
    model_id: str,
    caps: _SessionCapabilities | None,
) -> ModelSwitchCompatResult:
    """按 capability source 决定 set 调用顺序，并执行最多一次 fallback。"""
    if caps is None or caps.model_source is ModelSource.UNKNOWN or not caps.available_models:
        return ModelSwitchCompatResult(
            success=False,
            selected_method=None,
            attempts=[],
            reason="current ACP backend did not return a model catalog compatible with nanobot",
        )
    if model_id not in caps.available_models:
        return ModelSwitchCompatResult(
            success=False,
            selected_method=None,
            attempts=[],
            reason="invalid model id for current ACP backend catalog",
        )

    attempts: list[ModelSwitchAttempt] = []
    for method in _ordered_methods(caps, model_id):
        attempt = await _attempt_method(conn, method=method, session_id=session_id, model_id=model_id)
        attempts.append(attempt)
        if attempt.success:
            return ModelSwitchCompatResult(
                success=True,
                selected_method=method,
                attempts=attempts,
                reason="ok",
                response_payload=attempt.response_payload,
            )

    return ModelSwitchCompatResult(
        success=False,
        selected_method=None,
        attempts=attempts,
        reason="; ".join(_format_attempt_error(attempt) for attempt in attempts),
    )


def _ordered_methods(caps: _SessionCapabilities, model_id: str) -> list[ModelSwitchMethod]:
    """根据 source 与目标 id 所在 catalog 决定优先方法。"""
    source: ModelSource = caps.model_source
    if source is ModelSource.CONFIG_OPTIONS:
        return ["set_config_option", "set_session_model"]
    if source is ModelSource.SESSION_MODELS:
        return ["set_session_model", "set_config_option"]
    if source is ModelSource.MIXED:
        in_config = model_id in caps.config_options_catalog
        in_session = model_id in caps.session_models_catalog
        if in_config and not in_session:
            return ["set_config_option", "set_session_model"]
        if in_session and not in_config:
            return ["set_session_model", "set_config_option"]
        return ["set_config_option", "set_session_model"]
    return []


async def _attempt_method(
    conn: Any,
    *,
    method: ModelSwitchMethod,
    session_id: str,
    model_id: str,
) -> ModelSwitchAttempt:
    try:
        if method == "set_config_option":
            response = await _call_set_config_option(conn, session_id=session_id, model_id=model_id)
        else:
            response = await _call_set_session_model(conn, session_id=session_id, model_id=model_id)
    except _UnsupportedModelSwitchMethodError as exc:
        return ModelSwitchAttempt(method=method, supported=False, success=False, error=str(exc))
    except Exception as exc:
        return ModelSwitchAttempt(
            method=method,
            supported=True,
            success=False,
            error=f"{type(exc).__name__}: {exc}",
        )
    return ModelSwitchAttempt(
        method=method,
        supported=True,
        success=True,
        response_payload=response,
    )


async def _call_set_config_option(conn: Any, *, session_id: str, model_id: str) -> object:
    typed = getattr(conn, "set_config_option", None)
    if not callable(typed):
        raise _UnsupportedModelSwitchMethodError("set_config_option is not exposed by ACP connection")
    response = typed(config_id="model", session_id=session_id, value=model_id)
    if asyncio.iscoroutine(response):
        return await response
    return response


async def _call_set_session_model(conn: Any, *, session_id: str, model_id: str) -> object:
    typed = getattr(conn, "set_session_model", None)
    if callable(typed):
        response = typed(model_id=model_id, session_id=session_id)
        if asyncio.iscoroutine(response):
            return await response
        return response

    raw_conn = getattr(conn, "_conn", None)
    send_request = getattr(raw_conn, "send_request", None)
    if not callable(send_request):
        raise _UnsupportedModelSwitchMethodError(
            "set_session_model is not exposed and raw _conn.send_request is unavailable"
        )
    # 中文注释：已用真实 opencode ACP wire call 确认 legacy method 为 `session/set_model`，
    # 参数沿 schema camelCase 发送：sessionId + modelId。Hermes 同样保留该 legacy method。
    response = send_request(
        "session/set_model",
        {"sessionId": session_id, "modelId": model_id},
    )
    if asyncio.iscoroutine(response):
        return await response
    return response


def _format_attempt_error(attempt: ModelSwitchAttempt) -> str:
    detail = attempt.error or "unknown error"
    return f"{attempt.method} failed: {detail}"


class _UnsupportedModelSwitchMethodError(Exception):
    """表示当前连接不支持某一 model set 通道。"""
