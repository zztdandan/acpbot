"""ACP session runtime 连接/会话/直连 prompt 辅助。"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol, cast

from loguru import logger

from nanobot.acp.acp_errors import _is_invalid_params_request_error
from nanobot.acp.acp_factory import _acp_spawn_agent_process
from nanobot.acp.client import _NanobotACPClient
from nanobot.acp.session_caps import _update_caps_from_session_payload
from nanobot.acp.state import _ACPDispatchError, _SessionCapabilities, _StreamState


class _SessionRuntimePorts(Protocol):
    """session runtime helper 访问 dispatcher 所需的最小端口。"""

    workspace: Path
    acp_config: Any
    _conn_cm: Any
    _conn: Any
    _proc: Any
    _connect_lock: asyncio.Lock
    _session_map: dict[str, str]
    _session_locks: dict[str, asyncio.Lock]
    _session_caps: dict[str, _SessionCapabilities]
    _session_states: dict[str, _StreamState]
    _session_id_to_session_key: dict[str, str]
    _session_active_tool_name: dict[str, str]
    _session_result_media: dict[str, list[str]]
    _session_pending_media: dict[str, list[str]]
    _session_map_bootstrapped: bool

    async def _bootstrap_session_map(self) -> None: ...

    def _resolved_acp_cwd(self) -> str: ...

    def _persist_session_map(self) -> None: ...

    def _convert_mcp_servers(self) -> list[Any]: ...

    def _build_inbound_prompt_blocks(
        self,
        content: str,
        media: list[str],
        *,
        session_key: str,
        channel: str,
    ) -> list[Any]: ...


async def _ensure_connection(runtime: _SessionRuntimePorts) -> None:
    """确保 ACP 连接可用；首次调用时完成进程拉起与 initialize。"""
    if runtime._conn is not None:
        return

    async with runtime._connect_lock:
        if runtime._conn is not None:
            return

        from acp.schema import ClientCapabilities, Implementation

        # client 负责把 ACP 回调转发到 dispatcher。
        client = _NanobotACPClient(runtime)
        env = {**os.environ, **runtime.acp_config.env}
        cwd = (
            Path(runtime.acp_config.cwd).expanduser()
            if runtime.acp_config.cwd
            else runtime.workspace
        )
        timeout = max(1, runtime.acp_config.startup_timeout_seconds)

        # 启动 ACP 子进程并进入连接上下文。
        runtime._conn_cm = _acp_spawn_agent_process()(
            client,
            runtime.acp_config.command,
            *runtime.acp_config.args,
            env=env,
            cwd=cwd,
        )
        try:
            # 连接建立与 initialize 都受 startup timeout 保护。
            runtime._conn, runtime._proc = await asyncio.wait_for(
                runtime._conn_cm.__aenter__(), timeout=timeout
            )
            await asyncio.wait_for(
                runtime._conn.initialize(
                    protocol_version=runtime.acp_config.protocol_version,
                    client_capabilities=ClientCapabilities(),
                    client_info=Implementation(name="nanobot", version="0.1.4.post2"),
                ),
                timeout=timeout,
            )
            # initialize 成功后再加载并对账 session map。
            await runtime._bootstrap_session_map()
        except Exception:
            # 失败时完整回收连接上下文，避免半初始化残留。
            if runtime._conn_cm is not None:
                await runtime._conn_cm.__aexit__(None, None, None)
            runtime._conn_cm = None
            runtime._conn = None
            runtime._proc = None
            runtime._session_map_bootstrapped = False
            raise


async def _activate_existing_session(
    runtime: _SessionRuntimePorts,
    *,
    session_key: str,
    session_id: str,
) -> bool:
    """尝试把已映射 session 激活到 ACP 当前进程内存态。"""
    await _ensure_connection(runtime)
    if runtime._conn is None:
        raise RuntimeError("ACP connection is not available")

    cwd = runtime._resolved_acp_cwd()
    mcp_servers = runtime._convert_mcp_servers()
    resume_session = cast(
        Callable[..., Awaitable[Any]] | None,
        getattr(runtime._conn, "resume_session", None),
    )
    load_session = cast(
        Callable[..., Awaitable[Any]] | None,
        getattr(runtime._conn, "load_session", None),
    )

    # 中文注释：兼容不支持 resume/load 的测试桩或旧后端，保持既有复用行为。
    if resume_session is None and load_session is None:
        logger.debug(
            "ACP existing session activation skipped session_key={} session_id={} reason=no_resume_or_load_method",
            session_key,
            session_id,
        )
        return True

    resume_exc: Exception | None = None
    if resume_session is not None:
        try:
            response = await resume_session(
                cwd=cwd,
                session_id=session_id,
                mcp_servers=mcp_servers,
            )
            _update_caps_from_session_payload(runtime._session_caps, session_id, response)
            return True
        except Exception as exc:
            resume_exc = exc
            logger.debug(
                "ACP resume_session failed session_key={} session_id={} error_type={} error={}",
                session_key,
                session_id,
                type(exc).__name__,
                exc,
            )

    if load_session is not None:
        try:
            response = await load_session(
                cwd=cwd,
                session_id=session_id,
                mcp_servers=mcp_servers,
            )
            _update_caps_from_session_payload(runtime._session_caps, session_id, response)
            return True
        except Exception as load_exc:
            logger.warning(
                "ACP existing session activation failed session_key={} session_id={} resume_error_type={} resume_error={} load_error_type={} load_error={}",
                session_key,
                session_id,
                type(resume_exc).__name__ if resume_exc is not None else "n/a",
                resume_exc if resume_exc is not None else "n/a",
                type(load_exc).__name__,
                load_exc,
            )
            return False

    logger.warning(
        "ACP existing session activation failed session_key={} session_id={} reason=resume_not_available resume_error_type={} resume_error={}",
        session_key,
        session_id,
        type(resume_exc).__name__ if resume_exc is not None else "n/a",
        resume_exc if resume_exc is not None else "n/a",
    )
    return False


async def _ensure_session(
    runtime: _SessionRuntimePorts,
    session_key: str,
    preferred_model: str | None = None,
    preferred_agent: str | None = None,
) -> str:
    """确保 session_key 对应 ACP session 存在并可复用。"""
    session_id = runtime._session_map.get(session_key)
    if session_id:
        if await _activate_existing_session(
            runtime, session_key=session_key, session_id=session_id
        ):
            return session_id
        runtime._session_map.pop(session_key, None)
        runtime._session_caps.pop(session_id, None)
        runtime._persist_session_map()

    # 每个 session_key 单独加锁，避免并发创建重复会话。
    lock = runtime._session_locks.setdefault(session_key, asyncio.Lock())
    async with lock:
        session_id = runtime._session_map.get(session_key)
        if session_id:
            if await _activate_existing_session(
                runtime, session_key=session_key, session_id=session_id
            ):
                return session_id
            runtime._session_map.pop(session_key, None)
            runtime._session_caps.pop(session_id, None)
            runtime._persist_session_map()
        await _ensure_connection(runtime)
        if runtime._conn is None:
            raise RuntimeError("ACP connection is not available")
        cwd = Path(runtime._resolved_acp_cwd())
        response = await runtime._conn.new_session(
            cwd=str(cwd),
            mcp_servers=runtime._convert_mcp_servers(),
        )
        session_id = response.session_id
        selected_model = preferred_model or runtime.acp_config.default_model
        selected_agent = preferred_agent or runtime.acp_config.default_mode
        logger.info(
            "New ACP session created: {}, applying selection: model={} (preferred={} default={}), mode={} (preferred={} default={})",
            session_id,
            selected_model,
            preferred_model,
            runtime.acp_config.default_model,
            selected_agent,
            preferred_agent,
            runtime.acp_config.default_mode,
        )

        if selected_model:
            try:
                # 中文注释：会话模型遵循“首帧偏好优先，配置 default 回落”。
                await runtime._conn.set_session_model(
                    model_id=selected_model, session_id=session_id
                )
                caps = runtime._session_caps.setdefault(session_id, _SessionCapabilities())
                caps.current_model = selected_model
                logger.info("Successfully set model to: {}", selected_model)
            except Exception as e:
                logger.warning(
                    "Failed to set selected model {} for session {}: {}",
                    selected_model,
                    session_id,
                    e,
                )

        if selected_agent:
            try:
                # 中文注释：会话 agent(mode) 与模型策略一致，优先使用首帧，再回落 default。
                await runtime._conn.set_session_mode(mode_id=selected_agent, session_id=session_id)
                caps = runtime._session_caps.setdefault(session_id, _SessionCapabilities())
                caps.current_agent = selected_agent
                logger.info("Successfully set mode to: {}", selected_agent)
            except Exception as e:
                logger.warning(
                    "Failed to set selected mode {} for session {}: {}",
                    selected_agent,
                    session_id,
                    e,
                )

        # 新建映射立即持久化，避免进程异常导致映射丢失。
        runtime._session_map[session_key] = session_id
        runtime._persist_session_map()
        _update_caps_from_session_payload(runtime._session_caps, session_id, response)
        return session_id


async def _process_direct_impl(
    runtime: _SessionRuntimePorts,
    content: str,
    session_key: str = "cli:direct",
    channel: str = "cli",
    chat_id: str = "direct",
    preferred_model: str | None = None,
    preferred_agent: str | None = None,
    on_progress: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    """直接发送一轮 prompt 到指定 session，并返回聚合后的文本。"""
    del chat_id
    await _ensure_connection(runtime)
    if runtime._conn is None:
        raise RuntimeError("ACP connection is not available")
    session_id = await _ensure_session(
        runtime,
        session_key,
        preferred_model=preferred_model,
        preferred_agent=preferred_agent,
    )
    # 注册会话级流式状态，供 session_update 回调写入。
    state = _StreamState(on_progress=on_progress)
    runtime._session_states[session_id] = state
    runtime._session_id_to_session_key[session_id] = session_key
    tracked_session_ids = {session_id}
    try:
        # 日志中对原文做转义和截断，避免污染终端。
        content_esc = content.encode("unicode_escape", "ignore").decode("ascii")
        if len(content_esc) > 320:
            content_esc = f"{content_esc[:320]}..."
        logger.debug(
            "ACP prompt input session_key={} session_id={} chars={} content_esc='{}'",
            session_key,
            session_id,
            len(content),
            content_esc,
        )
        media_paths = runtime._session_pending_media.pop(session_key, [])

        for attempt in range(2):
            try:
                await runtime._conn.prompt(
                    prompt=runtime._build_inbound_prompt_blocks(
                        content,
                        media_paths,
                        session_key=session_key,
                        channel=channel,
                    ),
                    session_id=session_id,
                )
                break
            except Exception as exc:
                # 中文注释：部分 ACP 后端在单轮完成后会主动断开连接，
                # 下一轮 prompt 直接复用旧连接会报 "Connection closed"。
                # 这里做一次“重建连接 + 重建会话”的自愈重试。
                if attempt == 0 and _is_connection_closed_error(exc):
                    runtime._conn = None
                    runtime._proc = None
                    runtime._conn_cm = None
                    runtime._session_map_bootstrapped = False
                    runtime._session_states.pop(session_id, None)
                    session_id = await _ensure_session(
                        runtime,
                        session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    tracked_session_ids.add(session_id)
                    runtime._session_states[session_id] = state
                    runtime._session_id_to_session_key[session_id] = session_key
                    logger.warning(
                        "ACP prompt retry after connection closed session_key={} new_session_id={}",
                        session_key,
                        session_id,
                    )
                    continue

                # 中文注释：历史 session 映射在某些后端重启场景会失效，
                # 这里对 invalid params 做一次“删映射+重建会话”的自愈重试。
                if attempt == 0 and _is_invalid_params_request_error(exc):
                    stale_session_id = runtime._session_map.get(session_key)
                    if stale_session_id == session_id:
                        runtime._session_map.pop(session_key, None)
                        runtime._session_caps.pop(session_id, None)
                        runtime._persist_session_map()
                    runtime._session_states.pop(session_id, None)
                    session_id = await _ensure_session(
                        runtime,
                        session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    tracked_session_ids.add(session_id)
                    runtime._session_states[session_id] = state
                    runtime._session_id_to_session_key[session_id] = session_key
                    logger.warning(
                        "ACP prompt retry with new session_key={} old_session_id={} new_session_id={}",
                        session_key,
                        stale_session_id,
                        session_id,
                    )
                    continue

                # 如果 ACP 中断但已有部分文本，交给上层决定是否回退输出。
                partial_esc = state.final().encode("unicode_escape", "ignore").decode("ascii")
                if len(partial_esc) > 320:
                    partial_esc = f"{partial_esc[:320]}..."
                logger.warning(
                    "ACP prompt failed session_key={} session_id={} partial_chars={} partial_esc='{}'",
                    session_key,
                    session_id,
                    len(state.final()),
                    partial_esc,
                )
                raise _ACPDispatchError(partial_response=state.final()) from exc
        return state.final()
    finally:
        runtime._session_pending_media.pop(session_key, None)
        runtime._session_result_media[session_key] = state.final_media()
        # 请求结束后清理 session 状态，避免跨请求串流。
        for tracked_session_id in tracked_session_ids:
            runtime._session_states.pop(tracked_session_id, None)
            runtime._session_active_tool_name.pop(tracked_session_id, None)


def _is_connection_closed_error(exc: Exception) -> bool:
    """Return True when the exception indicates ACP transport closed.

    中文注释：优先按异常类型名判断（ConnectionError），并保留消息兜底，
    兼容不同依赖版本对错误类型的差异封装。
    """

    if type(exc).__name__ == "ConnectionError":
        return True
    return "connection closed" in str(exc).lower()
