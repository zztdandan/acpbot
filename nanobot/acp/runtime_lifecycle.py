"""ACP runtime connection lifecycle helpers."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.acp.acp_factory import _acp_spawn_agent_process
from nanobot.acp.runtime_client import _NanobotACPClient


async def _clear_connection_handles_without_lock(runtime: Any) -> None:
    """Clear ACP connection handles while already inside the connection lock."""

    # 中文注释：这个 helper 只在已经拿到连接锁的上下文里调用，
    # 用来避免 ensure_connection 失败时再次重入同一把锁导致死锁。
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


async def ensure_connection(runtime: Any) -> None:
    """Establish ACP client connection and bootstrap runtime-owned managers."""

    if runtime._acp_client_conn is not None:
        return
    async with runtime._acp_connection_lock:
        if runtime._acp_client_conn is not None:
            return
        # 中文注释：callback client 是 runtime 与 ACP SDK 的唯一桥接点，
        # 之后所有 session_update / permission callback 都回到 runtime owner 分派。
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

            # 中文注释：连接 enter 与 initialize 都放在同一条受保护链路里，
            # 只有完全成功，runtime 才算真正 ready。
            runtime._acp_client_conn, runtime._acp_agent_process = await asyncio.wait_for(
                runtime._acp_client_connection_cm.__aenter__(),
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
                runtime.new_observability_event(scope="runtime", event="connection_ready")
            )
            await runtime.session_runtime_manager.bootstrap_ready_sessions()
        except Exception:
            # 中文注释：初始化失败时必须整代回滚 runtime-owned 状态，
            # 不能留下半连通句柄、半激活 queue、半完成 wait entry 污染后续请求。
            await _clear_connection_handles_without_lock(runtime)
            await runtime.process_runtime_manager.rebuild(
                error=RuntimeError("ACP connection bootstrap failed")
            )
            runtime.session_runtime_manager.rebuild()
            runtime.sessionmap_binding_manager.mark_unbootstrapped()
            runtime.fail_all_wait_entries(RuntimeError("ACP connection bootstrap failed"))
            raise


async def reset_connection(runtime: Any) -> None:
    """Reset ACP runtime-owned connection state and runtime-owned managers."""

    async with runtime._acp_connection_lock:
        # 中文注释：runtime rebuild 的规则是“整代失效”，所以这里不仅清连接，
        # 还同步 fail 掉所有 wait entry，并重建 process/session runtime 两侧运行态。
        await _clear_connection_handles_without_lock(runtime)
        await runtime.process_runtime_manager.rebuild(error=RuntimeError("ACP runtime rebuilt"))
        runtime.session_runtime_manager.rebuild()
        runtime.sessionmap_binding_manager.mark_unbootstrapped()
        runtime.fail_all_wait_entries(RuntimeError("ACP runtime rebuilt"))
        await runtime.push_observability(
            runtime.new_observability_event(scope="runtime", event="connection_reset")
        )


async def close_runtime(runtime: Any) -> None:
    """Close runtime-owned background loops and ACP connection resources."""

    runtime._running = False
    # 中文注释：close 先触发 reset，再停 observability consumer，保证关机前
    # runtime/process/state 仍有机会把最后一批 reset 事件推入 queue。
    await reset_connection(runtime)
    await runtime.observability_manager.stop()
