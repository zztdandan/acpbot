from __future__ import annotations

from nanobot.acp.sessionmap.internal.session_caps import ModelSource, _SessionCapabilities


def test_apply_session_payload_supports_hermes_session_model_state() -> None:
    """Hermes 风格 `models: SessionModelState` 应识别为 session_models 来源。"""

    caps = _SessionCapabilities()
    payload = {
        "models": {
            "currentModelId": "openai/gpt-5.4",
            "availableModels": [
                {"modelId": "openai/gpt-5.4"},
                {"modelId": "anthropic/claude-sonnet-4-5"},
            ],
        }
    }

    caps.apply_session_payload(payload)

    assert caps.available_models == ["openai/gpt-5.4", "anthropic/claude-sonnet-4-5"]
    assert caps.current_model == "openai/gpt-5.4"
    assert caps.model_source is ModelSource.SESSION_MODELS


def test_apply_session_payload_supports_opencode_config_options_model_selector() -> None:
    """opencode 风格 `configOptions[id=model]` 应识别为 config_options 来源。"""

    caps = _SessionCapabilities()
    payload = {
        "configOptions": [
            {
                "id": "model",
                "type": "select",
                "currentValue": "opencode/big-pickle",
                "options": [
                    {"value": "opencode/big-pickle"},
                    {"value": "openai/gpt-4.1"},
                ],
            }
        ]
    }

    caps.apply_session_payload(payload)

    assert caps.available_models == ["opencode/big-pickle", "openai/gpt-4.1"]
    assert caps.current_model == "opencode/big-pickle"
    assert caps.model_source is ModelSource.CONFIG_OPTIONS


def test_apply_session_payload_merges_config_options_and_session_models() -> None:
    """混合 payload 应按 configOptions 优先、再补 session models 去重。"""

    caps = _SessionCapabilities()
    payload = {
        "models": {
            "currentModelId": "openai/gpt-5.4",
            "availableModels": [
                {"modelId": "openai/gpt-5.4"},
                {"modelId": "anthropic/claude-sonnet-4-5"},
            ],
        },
        "configOptions": [
            {
                "id": "model",
                "currentValue": "openai/gpt-4.1",
                "options": [
                    {"value": "openai/gpt-4.1"},
                    {"value": "openai/gpt-5.4"},
                ],
            }
        ],
    }

    caps.apply_session_payload(payload)

    assert caps.available_models == [
        "openai/gpt-4.1",
        "openai/gpt-5.4",
        "anthropic/claude-sonnet-4-5",
    ]
    assert caps.current_model == "openai/gpt-4.1"
    assert caps.model_source is ModelSource.MIXED


def test_apply_session_payload_keeps_raw_payload_json_for_debugging() -> None:
    """能力缓存应保留原始 payload 与 JSON 视图，供调试和断言使用。"""

    caps = _SessionCapabilities()
    payload = {
        "configOptions": [
            {
                "id": "model",
                "currentValue": "openai/gpt-4.1",
                "options": [{"value": "openai/gpt-4.1"}],
            }
        ],
        "extra": {"debug": True},
    }

    caps.apply_session_payload(payload)

    assert caps.raw_payload is payload
    assert caps.raw_payload_json == payload


def test_apply_session_payload_ignores_modes() -> None:
    """本轮 capability 已 model-only，modes 不再生成 agent 状态或 metadata。"""

    caps = _SessionCapabilities()
    payload = {
        "modes": {
            "currentModeId": "builder",
            "availableModes": [{"id": "builder"}, {"id": "reviewer"}],
        }
    }

    caps.apply_session_payload(payload)

    assert caps.available_models == []
    assert caps.current_model is None
    assert caps.model_source is ModelSource.UNKNOWN
    assert caps.build_prompt_metadata() == {}


def test_unknown_payload_stays_unknown_until_runtime_supplies_default() -> None:
    """无 models/configOptions 时先保持 unknown/empty，后续由 runtime 注入 default fallback。"""

    caps = _SessionCapabilities()

    caps.apply_session_payload({"sessionId": "ses_unknown"})

    assert caps.available_models == []
    assert caps.current_model is None
    assert caps.model_source is ModelSource.UNKNOWN
    assert caps.render_models_command() == "No model catalog returned by current ACP backend for this session."
