"""运行时主入口与请求等待主链。"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from nanobot.acp.acp_factory import _acp_spawn_agent_process
from nanobot.acp.contracts import ObservabilityEventName, ObservabilityScopeName
from nanobot.acp.runtime_client import _NanobotACPClient

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


async def _clear_connection_handles_without_lock(runtime: ACPRuntime) -> None:
    """执行该方法定义的处理流程并返回结果。"""

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
    """确保协议连接处于可用状态。"""

    if runtime._acp_client_conn is not None:
        return
    async with runtime._acp_connection_lock:
        if runtime._acp_client_conn is not None:
            return
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
            # 连接建立后先完成 binding 真相加载与大对账；真正的 session 激活延迟到 ensure_ready_session。
            await runtime.sessionmap_binding_manager.load_persistent_truth()
        except Exception:
            await _clear_connection_handles_without_lock(runtime)
            await runtime.process_runtime_manager.rebuild(
                error=RuntimeError("ACP connection bootstrap failed")
            )
            runtime.session_runtime_manager.rebuild()
            runtime.sessionmap_binding_manager.mark_unbootstrapped()
            runtime.fail_all_wait_entries(RuntimeError("ACP connection bootstrap failed"))
            raise


async def reset_connection(runtime: ACPRuntime) -> None:
    """重置协议连接并清理代际状态。"""

    async with runtime._acp_connection_lock:
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
    """启动主循环并持续消费入站消息。"""

    runtime._running = False
    await reset_connection(runtime)
    await runtime.observability_manager.stop()
