from __future__ import annotations

from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities


def test_apply_session_payload_supports_plain_dict_camel_case() -> None:
    """纯 dict + camelCase payload 也应被正确解析。"""

    caps = _SessionCapabilities()
    caps.apply_session_payload(
        {
            "models": {
                "currentModelId": "gpt-4o",
                "availableModels": [{"modelId": "gpt-4o"}, {"modelId": "gpt-4.1"}],
            },
            "modes": {
                "currentModeId": "code",
                "availableModes": [{"id": "code"}, {"id": "default"}],
            },
        }
    )

    assert caps.current_model == "gpt-4o"
    assert caps.available_models == ["gpt-4o", "gpt-4.1"]
    assert caps.current_agent == "code"
    assert caps.available_agents == ["code", "default"]


def test_apply_session_payload_supports_plain_dict_snake_case() -> None:
    """纯 dict + snake_case payload 也应被正确解析。"""

    caps = _SessionCapabilities()
    caps.apply_session_payload(
        {
            "models": {
                "current_model_id": "gpt-4.1-mini",
                "available_models": [{"model_id": "gpt-4.1-mini"}],
            },
            "modes": {
                "current_mode_id": "agent-a",
                "available_modes": [{"id": "agent-a"}],
            },
        }
    )

    assert caps.current_model == "gpt-4.1-mini"
    assert caps.available_models == ["gpt-4.1-mini"]
    assert caps.current_agent == "agent-a"
    assert caps.available_agents == ["agent-a"]
