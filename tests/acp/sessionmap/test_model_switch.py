from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from nanobot.acp.sessionmap.internal.model_switch import switch_model_compat
from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities


@dataclass
class FakeRawConnection:
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    response: object = field(default_factory=dict)

    async def send_request(self, method: str, params: dict[str, str]) -> object:
        self.calls.append((method, params))
        return self.response


class FakeConnection:
    def __init__(
        self,
        *,
        config_response: object = None,
        session_response: object = None,
        config_error: Exception | None = None,
        session_error: Exception | None = None,
        expose_session_typed: bool = True,
        raw_response: object = None,
    ) -> None:
        self.config_response = config_response
        self.session_response = session_response
        self.config_error = config_error
        self.session_error = session_error
        self.config_calls: list[tuple[str, str, str | bool]] = []
        self.session_calls: list[tuple[str, str]] = []
        self._conn = FakeRawConnection(response={} if raw_response is None else raw_response)
        if not expose_session_typed:
            # 中文注释：实例属性遮蔽类方法，模拟 SDK 未暴露 typed set_session_model。
            self.set_session_model = None  # type: ignore[method-assign]

    async def set_config_option(self, config_id: str, session_id: str, value: str | bool) -> object:
        self.config_calls.append((config_id, session_id, value))
        if self.config_error is not None:
            raise self.config_error
        return self.config_response

    async def set_session_model(self, model_id: str, session_id: str) -> object:
        self.session_calls.append((model_id, session_id))
        if self.session_error is not None:
            raise self.session_error
        return self.session_response


def _caps(source_payload: dict[str, Any]) -> _SessionCapabilities:
    caps = _SessionCapabilities()
    caps.apply_session_payload(source_payload)
    return caps


@pytest.mark.asyncio
async def test_config_option_success_with_payload_returns_success() -> None:
    caps = _caps(
        {
            "configOptions": [
                {
                    "id": "model",
                    "currentValue": "opencode/big-pickle",
                    "options": [{"value": "opencode/big-pickle"}],
                }
            ]
        }
    )
    conn = FakeConnection(config_response={"configOptions": [{"id": "model"}]})

    result = await switch_model_compat(conn, session_id="ses", model_id="opencode/big-pickle", caps=caps)

    assert result.success is True
    assert result.selected_method == "set_config_option"
    assert conn.config_calls == [("model", "ses", "opencode/big-pickle")]
    assert conn.session_calls == []


@pytest.mark.asyncio
async def test_config_option_empty_response_is_success() -> None:
    caps = _caps(
        {"configOptions": [{"id": "model", "options": [{"value": "openai/gpt-4.1"}]}]}
    )
    conn = FakeConnection(config_response={})

    result = await switch_model_compat(conn, session_id="ses", model_id="openai/gpt-4.1", caps=caps)

    assert result.success is True
    assert result.response_payload == {}
    assert conn.session_calls == []


@pytest.mark.asyncio
async def test_session_model_empty_response_is_success() -> None:
    caps = _caps({"models": {"availableModels": [{"modelId": "openai/gpt-5.4"}]}})
    conn = FakeConnection(session_response={})

    result = await switch_model_compat(conn, session_id="ses", model_id="openai/gpt-5.4", caps=caps)

    assert result.success is True
    assert result.selected_method == "set_session_model"
    assert conn.session_calls == [("openai/gpt-5.4", "ses")]
    assert conn.config_calls == []


@pytest.mark.asyncio
async def test_config_options_source_falls_back_to_session_model() -> None:
    caps = _caps({"configOptions": [{"id": "model", "options": [{"value": "openai/gpt-4.1"}]}]})
    conn = FakeConnection(config_error=RuntimeError("config failed"), session_response={})

    result = await switch_model_compat(conn, session_id="ses", model_id="openai/gpt-4.1", caps=caps)

    assert result.success is True
    assert result.selected_method == "set_session_model"
    assert [attempt.method for attempt in result.attempts] == [
        "set_config_option",
        "set_session_model",
    ]


@pytest.mark.asyncio
async def test_session_models_source_falls_back_to_config_option() -> None:
    caps = _caps({"models": {"availableModels": [{"modelId": "openai/gpt-5.4"}]}})
    conn = FakeConnection(session_error=RuntimeError("session failed"), config_response={})

    result = await switch_model_compat(conn, session_id="ses", model_id="openai/gpt-5.4", caps=caps)

    assert result.success is True
    assert result.selected_method == "set_config_option"
    assert [attempt.method for attempt in result.attempts] == [
        "set_session_model",
        "set_config_option",
    ]


@pytest.mark.asyncio
async def test_unknown_source_does_not_call_backend() -> None:
    caps = _caps({"sessionId": "ses"})
    conn = FakeConnection(config_response={}, session_response={})

    result = await switch_model_compat(conn, session_id="ses", model_id="anything", caps=caps)

    assert result.success is False
    assert "did not return a model catalog" in result.reason
    assert result.attempts == []
    assert conn.config_calls == []
    assert conn.session_calls == []


@pytest.mark.asyncio
async def test_both_methods_fail_returns_structured_reason() -> None:
    caps = _caps({"configOptions": [{"id": "model", "options": [{"value": "openai/gpt-4.1"}]}]})
    conn = FakeConnection(
        config_error=RuntimeError("config failed"),
        session_error=RuntimeError("session failed"),
    )

    result = await switch_model_compat(conn, session_id="ses", model_id="openai/gpt-4.1", caps=caps)

    assert result.success is False
    assert "set_config_option failed" in result.reason
    assert "set_session_model failed" in result.reason


@pytest.mark.asyncio
async def test_raw_bridge_only_used_when_typed_session_model_is_missing() -> None:
    caps = _caps({"models": {"availableModels": [{"modelId": "openai/gpt-5.4"}]}})
    conn = FakeConnection(expose_session_typed=False, raw_response={})

    result = await switch_model_compat(conn, session_id="ses", model_id="openai/gpt-5.4", caps=caps)

    assert result.success is True
    assert result.selected_method == "set_session_model"
    assert conn.session_calls == []
    assert conn._conn.calls == [
        ("session/set_model", {"sessionId": "ses", "modelId": "openai/gpt-5.4"})
    ]
