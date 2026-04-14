"""ACP 运行时生命周期管理：连接建立、重置、关闭。

职责：
    - ensure_connection: 建立 ACP 连接（含 SDK spawn + initialize + binding 对账）
    - reset_connection: 重置连接并清理全部代际状态（process/sessionmap/wait）
    - close_runtime: 关闭运行时（stop + reset + 停止观测）
    - _clear_connection_handles_without_lock: 内部清理连接句柄（无锁版本，调用方已持锁）

设计约束：
    连接操作受 _acp_connection_lock 保护，防止并发重建。
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

from loguru import logger

from nanobot.acp.acp_factory import _acp_spawn_agent_process
from nanobot.acp.contracts import ObservabilityEventName, ObservabilityScopeName
from nanobot.acp.runtime_client import _NanobotACPClient

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


async def _clear_connection_handles_without_lock(runtime: ACPRuntime) -> None:
    """清理连接句柄并关闭 context manager（无锁版本，调用方已持有 _acp_connection_lock）。

    处理流程：
        1. 取出旧的 connection_cm，调用 __aexit__ 释放子进程
        2. 将 _acp_client_conn / _acp_agent_process / _acp_client_connection_cm 全部置 None
        3. __aexit__ 失败时仅 warning 不抛出（避免影哏重置链路）
    """

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
    """确保 ACP 协议连接可用（已连接则直接返回，未连接则建立新连接）。

    处理流程：
        1. 快速路径：_acp_client_conn 已存在则直接返回
        2. 加锁后二次检查（double-check locking）
        3. 创建 _NanobotACPClient 并 spawn ACP Agent 子进程
        4. 等待连接 __aenter__ + initialize 完成（带 startup_timeout）
        5. 推送 CONNECTION_READY 观测事件
        6. 触发 sessionmap binding 对账（load_persistent_truth）

    失败处理：
        - 连接异帰时清理全部代际状态（process rebuild + session rebuild + binding unbootstrap + fail all waits）
        - 异常向上抛出，由调用方决定 далее重试或降级

    异常：
        RuntimeError: 连接建立失败
        asyncio.TimeoutError: 连接或 initialize 超时
    """

    if runtime._acp_client_conn is not None:
        return
    async with runtime._acp_connection_lock:
        if runtime._acp_client_conn is not None:
            return
        runtime.acp_callback_client = _NanobotACPClient(runtime)
        env = {**os.environ, **runtime.acp_config.env}
        cwd = runtime.resolve_acp_workspace_path()
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
    """重置 ACP 连接并清理全部代际状态（连接、进程、会话、等待链）。

    处理流程：
        1. 加锁后调用 _clear_connection_handles_without_lock 清理连接句柄
        2. 重建 process_runtime_manager（失败所有活跃/排队请求）
        3. 重建 session_runtime_manager（清空内存条目）
        4. 标记 binding 未启动（mark_unbootstrapped，下次 ensure 时重新对账）
        5. 失败所有 wait_entry
        6. 推送 CONNECTION_RESET 观测事件
    """

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
    """关闭运行时：标记停止、重置连接、停止观测管理器。

    处理流程：
        1. 设置 _running = False（通知主循环退出）
        2. 调用 reset_connection 清理全部连接与代际状态
        3. 调用 observability_manager.stop() 停止观测队列
    """

    runtime._running = False
    await reset_connection(runtime)
    await runtime.observability_manager.stop()
