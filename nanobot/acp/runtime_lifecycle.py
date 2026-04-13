"""ACP runtime connection lifecycle helpers."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from nanobot.acp.contracts import ObservabilityEventName, ObservabilityScopeName
from nanobot.acp.acp_factory import _acp_spawn_agent_process
from nanobot.acp.runtime_client import _NanobotACPClient

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


async def _clear_connection_handles_without_lock(runtime: ACPRuntime) -> None:
    """Clear ACP connection handles while already inside the connection lock."""

    # This helper is only called while already holding the connection lock so failed
    # bootstrap cleanup does not re-enter the same lock and deadlock.
    old_cm = runtime._acp_client_connection_cm
    if old_cm is not None:
        try:
            await old_cm.__aexit__(None, None, None)
        except Exception as exc:
            logger.warning(
                "ACP runtime reset __aexit__ failed error_type={} error={}",
                type(exc).__name__,
                exc,
            )
    runtime._acp_client_connection_cm = None
    runtime._acp_client_conn = None
    runtime._acp_agent_process = None


async def ensure_connection(runtime: ACPRuntime) -> None:
    """Establish ACP client connection and bootstrap runtime-owned managers."""

    if runtime._acp_client_conn is not None:
        return
    async with runtime._acp_connection_lock:
        if runtime._acp_client_conn is not None:
            return
        # The callback client is the single bridge between runtime and the ACP SDK, so
        # every session_update and permission callback re-enters the runtime owner path.
        runtime.acp_callback_client = _NanobotACPClient(runtime)
        env = {**os.environ, **runtime.acp_config.env}
        cwd = (
            Path(runtime.acp_config.cwd).expanduser()
            if runtime.acp_config.cwd
            else runtime.workspace
        )
        timeout = max(1, runtime.acp_config.startup_timeout_seconds)
        runtime._acp_client_connection_cm = _acp_spawn_agent_process()(
            runtime.acp_callback_client,
            runtime.acp_config.command,
            *runtime.acp_config.args,
            env=env,
            cwd=cwd,
        )
        try:
            from acp.schema import ClientCapabilities, Implementation

            # Enter and initialize stay on one protected path so runtime becomes ready
            # only after the entire connection bootstrap succeeds.
            connection_cm = runtime._acp_client_connection_cm
            if connection_cm is None:
                raise RuntimeError("ACP connection bootstrap did not create a context manager")
            runtime._acp_client_conn, runtime._acp_agent_process = await asyncio.wait_for(
                connection_cm.__aenter__(),
                timeout=timeout,
            )
            await asyncio.wait_for(
                runtime._acp_client_conn.initialize(
                    protocol_version=runtime.acp_config.protocol_version,
                    client_capabilities=ClientCapabilities(),
                    client_info=Implementation(name="nanobot", version="0.1.4.post2"),
                ),
                timeout=timeout,
            )
            await runtime.push_observability(
                runtime.new_observability_event(
                    scope=ObservabilityScopeName.RUNTIME,
                    event=ObservabilityEventName.CONNECTION_READY,
                )
            )
            await runtime.session_runtime_manager.bootstrap_ready_sessions()
        except Exception:
            # Bootstrap failure must roll back the whole runtime-owned generation. Leaving
            # half-open handles or half-complete wait entries would poison later requests.
            await _clear_connection_handles_without_lock(runtime)
            await runtime.process_runtime_manager.rebuild(
                error=RuntimeError("ACP connection bootstrap failed")
            )
            runtime.session_runtime_manager.rebuild()
            runtime.sessionmap_binding_manager.mark_unbootstrapped()
            runtime.fail_all_wait_entries(RuntimeError("ACP connection bootstrap failed"))
            raise


async def reset_connection(runtime: ACPRuntime) -> None:
    """Reset ACP runtime-owned connection state and runtime-owned managers."""

    async with runtime._acp_connection_lock:
        # Runtime rebuild invalidates the entire generation, so reset clears the
        # connection, fails all wait entries, and rebuilds process/session runtime state.
        await _clear_connection_handles_without_lock(runtime)
        await runtime.process_runtime_manager.rebuild(error=RuntimeError("ACP runtime rebuilt"))
        runtime.session_runtime_manager.rebuild()
        runtime.sessionmap_binding_manager.mark_unbootstrapped()
        runtime.fail_all_wait_entries(RuntimeError("ACP runtime rebuilt"))
        await runtime.push_observability(
            runtime.new_observability_event(
                scope=ObservabilityScopeName.RUNTIME,
                event=ObservabilityEventName.CONNECTION_RESET,
            )
        )


async def close_runtime(runtime: ACPRuntime) -> None:
    """Close runtime-owned background loops and ACP connection resources."""

    runtime._running = False
    # Close resets first and stops the observability consumer second so runtime,
    # process, and state can still enqueue their final reset events before shutdown.
    await reset_connection(runtime)
    await runtime.observability_manager.stop()
