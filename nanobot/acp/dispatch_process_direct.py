"""Execution-stage helpers for ACP process requests."""

from __future__ import annotations

from typing import Any

from nanobot.acp.acp_errors import _is_invalid_params_request_error
from nanobot.acp.acp_factory import _acp_resource_link_block, _acp_text_block
from nanobot.acp.state import _ACPDispatchError


def build_prompt_blocks(process_request: Any) -> list[Any]:
    """Build ACP prompt blocks only for the real execution path."""

    # 中文注释：prompt blocks builder 明确只属于执行期私有 helper，
    # 不再挂在 runtime/inbound 顶层，避免 runtime 重新膨胀成细节 owner。
    blocks: list[Any] = [_acp_text_block(process_request.content)]
    for artifact in process_request.artifacts.get("media_artifacts", []):
        blocks.append(
            _acp_resource_link_block(
                name=artifact["name"],
                uri=artifact["uri"],
                mime_type=artifact["mime_type"],
                size=artifact["size"],
            )
        )
    return blocks


async def execute_process_request(runtime: Any, active_entry: Any) -> Any:
    """Execute one ACP prompt against a ready ACP session and let callbacks fill state."""

    if runtime._acp_client_conn is None:
        raise RuntimeError("ACP connection is not available")
    process_request = active_entry.process_request
    # 中文注释：当前 ready session 的 model/agent 会一起透传给 ACP prompt，
    # 这样执行链路与 session replay 的选择事实保持一致。
    caps = runtime._session_caps.get(active_entry.acp_side_session_id)
    prompt_meta: dict[str, Any] = {}
    if caps is not None and isinstance(caps.current_model, str) and caps.current_model:
        prompt_meta["nanobot_session_model"] = caps.current_model
    if caps is not None and isinstance(caps.current_agent, str) and caps.current_agent:
        prompt_meta["nanobot_session_agent"] = caps.current_agent

    try:
        await runtime.await_acp_prompt(
            prompt=build_prompt_blocks(process_request),
            session_id=active_entry.acp_side_session_id,
            **prompt_meta,
        )
        return active_entry.state_manager.materialize_final_outbound(partial=False)
    except Exception as exc:
        # 中文注释：invalid params 往往意味着历史 session 真相已经失效，
        # 这里直接清掉 binding truth + runtime-ready entry，让下一轮重新 ensure。
        if _is_invalid_params_request_error(exc):
            runtime.drop_session_binding_and_runtime_entry(
                nanobot_side_session_key=process_request.nanobot_side_session_key,
            )
        # 中文注释：即使执行失败，也优先尝试从 state 聚合里 materialize partial，
        # 保持“partial fallback 仍属于 state owner”这一设计约束。
        partial = active_entry.state_manager.materialize_final_outbound(partial=True)
        if partial.content or partial.media:
            raise _ACPDispatchError(partial_response=partial.content) from exc
        raise
