from __future__ import annotations

import pytest

from nanobot.acp.state import (
    ACPBucketType,
    ACPOutboundKind,
    ACPUpdateType,
    ConsumeOnlyPool,
    SessionStateManager,
)


def test_update_type_mapping_includes_python_sdk_union_and_internal_tool_call_update() -> None:
    assert ACPUpdateType.USER_MESSAGE_CHUNK.value == "user_message_chunk"
    assert ACPUpdateType.TOOL_CALL_UPDATE.value == "tool_call_update"
    assert ACPBucketType.NONE.value == "none"
    assert ACPOutboundKind.NONE.value == "none"


@pytest.mark.asyncio
async def test_consume_only_pool_is_terminal_after_accept() -> None:
    pool = ConsumeOnlyPool(pool_id="p1", session_id="s1")

    assert pool.is_terminal() is False

    await pool.accept({"update_type": ACPUpdateType.USER_MESSAGE_CHUNK.value})

    assert pool.is_terminal() is True


@pytest.mark.asyncio
async def test_manager_removes_all_indexes_when_pool_is_destroyed() -> None:
    manager = SessionStateManager()
    pool = ConsumeOnlyPool(pool_id="pool-1", session_id="session-1")

    manager.register_pool(pool, bucket_key="route-1")

    assert manager.get_pool("pool-1") is pool
    assert (
        manager.get_pool_by_key(
            bucket_type=ACPBucketType.NONE,
            session_id="session-1",
            bucket_key="route-1",
        )
        is pool
    )

    await manager.destroy_pool("pool-1")

    assert manager.get_pool("pool-1") is None
    assert (
        manager.get_pool_by_key(
            bucket_type=ACPBucketType.NONE,
            session_id="session-1",
            bucket_key="route-1",
        )
        is None
    )


@pytest.mark.asyncio
async def test_manager_clears_request_scope_aggregation_after_finalize() -> None:
    manager = SessionStateManager()

    manager.merge_request_text("request-1", "hello")
    manager.merge_request_text("request-1", " world")
    manager.add_request_media("request-1", "/tmp/a.png")

    finalized = manager.finalize_request_scope("request-1")

    assert finalized.final_text == "hello world"
    assert finalized.media_paths == ["/tmp/a.png"]
    assert manager.get_request_state("request-1") is None
