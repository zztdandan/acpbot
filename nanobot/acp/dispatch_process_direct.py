"""Execution-stage helpers for ACP process requests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.acp_errors import _is_invalid_params_request_error
from nanobot.acp.acp_factory import _acp_resource_link_block, _acp_text_block
from nanobot.acp.contracts import ACPArtifactList, JSONMap
from nanobot.acp.runtime_models import ActiveProcessEntry, ProcessRequest
from nanobot.acp.state import _ACPDispatchError
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


def build_prompt_blocks(process_request: ProcessRequest) -> list[object]:
    """Build ACP prompt blocks only for the real execution path."""

    # Prompt-block building belongs strictly to the execution stage so runtime and
    # inbound do not grow back into low-level detail owners.
    blocks: list[object] = [_acp_text_block(process_request.content)]
    raw_media_artifacts = process_request.artifacts.get("media_artifacts", [])
    media_artifacts: ACPArtifactList = (
        raw_media_artifacts if isinstance(raw_media_artifacts, list) else []
    )
    for artifact in media_artifacts:
        if not isinstance(artifact, dict):
            continue
        size_value = artifact.get("size")
        blocks.append(
            _acp_resource_link_block(
                name=str(artifact["name"]),
                uri=str(artifact["uri"]),
                mime_type=str(artifact["mime_type"]) if artifact.get("mime_type") else None,
                size=size_value if isinstance(size_value, int) else None,
            )
        )
    return blocks


async def execute_process_request(
    runtime: ACPRuntime, active_entry: ActiveProcessEntry
) -> OutboundMessage:
    """Execute one ACP prompt against a ready ACP session and let callbacks fill state."""

    if runtime._acp_client_conn is None:
        raise RuntimeError("ACP connection is not available")
    process_request = active_entry.process_request
    # Forward the ready session's model/agent selection to ACP prompt so execution
    # uses the same selection facts that session replay established.
    prompt_meta: JSONMap = runtime.session_runtime_manager.build_prompt_metadata(
        acp_side_session_id=active_entry.acp_side_session_id
    )

    try:
        await runtime.await_acp_prompt(
            prompt=build_prompt_blocks(process_request),
            session_id=active_entry.acp_side_session_id,
            **prompt_meta,
        )
        return active_entry.state_manager.materialize_final_outbound(partial=False)
    except Exception as exc:
        # `invalid params` usually means the historical session truth is stale, so drop
        # both binding truth and runtime-ready state to force a fresh ensure next time.
        if _is_invalid_params_request_error(exc):
            runtime.drop_session_binding_and_runtime_entry(
                nanobot_side_session_key=process_request.nanobot_side_session_key,
            )
        # Even on execution failure, prefer materializing a partial response from state so
        # partial fallback remains a state-owned responsibility.
        partial = active_entry.state_manager.materialize_final_outbound(partial=True)
        if partial.content or partial.media:
            raise _ACPDispatchError(partial_response=partial.content) from exc
        raise
