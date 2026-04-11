"""ACP session runtime 连接/会话/直连 prompt 辅助。"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, cast

from loguru import logger

from nanobot.acp.acp_errors import _is_invalid_params_request_error
from nanobot.acp.acp_factory import _acp_spawn_agent_process
from nanobot.acp.client import _NanobotACPClient
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.router import ProgressRouter
from nanobot.acp.session_caps import _update_caps_from_session_payload
from nanobot.acp.state import _ACPDispatchError, _SessionCapabilities
from nanobot.bus.events import OutboundMessage
from nanobot.config.schema import ACPBackendConfig

if TYPE_CHECKING:
    from nanobot.acp.session_map_binding_manager import _SessionMapBindingManager


class _SessionRuntimePorts(Protocol):
    """session runtime helper 访问 dispatcher 所需的最小端口。"""

    workspace: Path
    acp_config: ACPBackendConfig
    _conn_cm: Any
    _conn: Any
    _proc: Any
    _connect_lock: asyncio.Lock
    _session_map_binding_manager: _SessionMapBindingManager
    _session_locks: dict[str, asyncio.Lock]
    _session_caps: dict[str, _SessionCapabilities]
    _session_map: dict[str, str]
    _session_states: dict[str, SessionStateManager]
    _session_state_routers: dict[str, ProgressRouter]
    _session_request_scope_ids: dict[str, str]
    _session_id_to_session_key: dict[str, str]
    _session_active_tool_name: dict[str, str]
    _session_result_media: dict[str, list[str]]
    _session_pending_media: dict[str, list[str]]
    _session_progress_metadata: dict[str, dict[str, Any]]
    _session_targets: dict[str, tuple[str, str, str]]
    _session_map_bootstrapped: bool
    _connection_epoch: int
    _session_activation_ensure_epoch: dict[str, int]
    _session_bootstrap_activated_keys: set[str]
    _session_state_manager: SessionStateManager

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

    @classmethod
    def _sanitize_outbound_metadata(cls, metadata: dict[str, Any] | None) -> dict[str, Any]: ...

    async def _publish_outbound_with_debug(
        self,
        *,
        msg: OutboundMessage,
        reason: str,
        session_key: str,
    ) -> None: ...


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
            # 中文注释：仅在连接完全可用后递增 epoch，失败回滚场景不前进，避免误伤 ensure 标记。
            runtime._connection_epoch += 1
            for session_key in runtime._session_bootstrap_activated_keys:
                _mark_session_activation_ensured(runtime, session_key=session_key)
            runtime._session_bootstrap_activated_keys.clear()
        except Exception:
            # 失败时完整回收连接上下文，避免半初始化残留。
            if runtime._conn_cm is not None:
                try:
                    await runtime._conn_cm.__aexit__(None, None, None)
                except Exception as rollback_exc:
                    logger.warning(
                        "ACP ensure_connection rollback __aexit__ failed error_type={} error={}",
                        type(rollback_exc).__name__,
                        rollback_exc,
                    )
            # 中文注释：无论 rollback __aexit__ 是否异常，都必须回到干净状态，
            # 避免半初始化句柄残留影响后续重连与会话引导。
            runtime._conn_cm = None
            runtime._conn = None
            runtime._proc = None
            runtime._session_map_bootstrapped = False
            runtime._session_map_binding_manager.mark_unbootstrapped()
            runtime._session_bootstrap_activated_keys.clear()
            raise


def _is_session_activation_ensured_in_current_epoch(
    runtime: _SessionRuntimePorts,
    *,
    session_key: str,
) -> bool:
    """判断 session_key 在当前连接 epoch 内是否已经 ensure/activate 过。"""

    return runtime._session_activation_ensure_epoch.get(session_key) == runtime._connection_epoch


def _mark_session_activation_ensured(
    runtime: _SessionRuntimePorts,
    *,
    session_key: str,
) -> None:
    """把 session_key 标记为当前 epoch 已 ensure，避免同连接周期重复激活。"""

    runtime._session_activation_ensure_epoch[session_key] = runtime._connection_epoch


async def _reset_connection_runtime_state(runtime: _SessionRuntimePorts) -> None:
    """受保护地清理连接句柄，避免并发重连时出现资源泄漏或状态撕裂。"""

    async with runtime._connect_lock:
        old_conn_cm = runtime._conn_cm
        # 中文注释：连接断开自愈可能与并发 ensure_connection 交错，
        # 这里先在同一把锁下回收旧 context manager，再统一清空句柄，避免悬挂子进程和竞态覆盖。
        if old_conn_cm is not None:
            try:
                await old_conn_cm.__aexit__(None, None, None)
            except Exception as exc:
                logger.warning(
                    "ACP connection reset __aexit__ failed error_type={} error={}",
                    type(exc).__name__,
                    exc,
                )
        runtime._conn = None
        runtime._proc = None
        runtime._conn_cm = None
        runtime._session_map_bootstrapped = False
        runtime._session_map_binding_manager.mark_unbootstrapped()
        runtime._session_bootstrap_activated_keys.clear()


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
    return await runtime._session_map_binding_manager.activate_session(session_key, session_id)


async def _ensure_session(
    runtime: _SessionRuntimePorts,
    session_key: str,
    preferred_model: str | None = None,
    preferred_agent: str | None = None,
) -> str:
    """确保 session_key 对应 ACP session 存在并可复用。"""
    # 每个 session_key 单独加锁，避免并发创建重复会话。
    lock = runtime._session_locks.setdefault(session_key, asyncio.Lock())
    async with lock:
        await _ensure_connection(runtime)
        if runtime._conn is None:
            raise RuntimeError("ACP connection is not available")

        session_id = runtime._session_map_binding_manager.resolve_session_id(session_key)
        if session_id:
            # 中文注释：把“检查 ensured + activate + 写 marker”放在同一临界区，
            # 确保并发 ensure_session 同 key 时同一 epoch 只会激活一次。
            if _is_session_activation_ensured_in_current_epoch(runtime, session_key=session_key):
                return session_id
            if await _activate_existing_session(
                runtime, session_key=session_key, session_id=session_id
            ):
                _mark_session_activation_ensured(runtime, session_key=session_key)
                return session_id
            # 中文注释：激活/desired 回放失败时保留映射并跳过 ensured 标记，
            # 让下一轮请求继续尝试激活回放，而不是立刻替换成新会话。
            runtime._session_activation_ensure_epoch.pop(session_key, None)
            return session_id

        cwd = Path(runtime._resolved_acp_cwd())
        response = await runtime._conn.new_session(
            cwd=str(cwd),
            mcp_servers=runtime._convert_mcp_servers(),
        )
        session_id = response.session_id
        bound_model, bound_agent = runtime._session_map_binding_manager.get_bound_selection(
            session_key
        )
        selected_model = preferred_model or bound_model or runtime.acp_config.default_model
        selected_agent = preferred_agent or bound_agent or runtime.acp_config.default_mode
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
        runtime._session_map_binding_manager.bind_session(session_key, session_id)
        # 中文注释：新建 session 天然处于当前连接内存态，直接记为 ensured，避免本 epoch 内重复 activate。
        _mark_session_activation_ensured(runtime, session_key=session_key)
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
    on_progress: Callable[..., Awaitable[None]] | None = None,
) -> OutboundMessage:
    """直接发送一轮 prompt 到指定 session，并返回标准 OutboundMessage。acpdispatcher 核心方法"""
    await _ensure_connection(runtime)
    if runtime._conn is None:
        raise RuntimeError("ACP connection is not available")
    session_id = await _ensure_session(
        runtime,
        session_key,
        preferred_model=preferred_model,
        preferred_agent=preferred_agent,
    )
    progress_meta = runtime._sanitize_outbound_metadata(
        runtime._session_progress_metadata.pop(session_key, None)
    )
    progress_meta["_progress"] = True

    async def _publish_progress(content: str, metadata: dict[str, Any], reason: str) -> None:
        outbound = OutboundMessage(
            channel=channel,
            chat_id=chat_id,
            content=content,
            metadata={**progress_meta, **metadata},
        )
        await runtime._publish_outbound_with_debug(
            msg=outbound,
            reason=reason,
            session_key=session_key,
        )
        if on_progress is not None and content:
            # 中文注释：on_progress 仅作为降级文本 sink 镜像，不参与 ACP 结构化事件处理本身；
            # 对 tool family 保留 tool_hint=True，兼容 native/CLI 既有进度消费约定。
            try:
                await on_progress(content, tool_hint=metadata.get("_acp_kind") == "tool")
            except TypeError:
                # 中文注释：保留对旧式单参数 progress sink 的兼容，避免 direct 调用方被关键字参数打断。
                await on_progress(content)

    state_manager = getattr(runtime, "_session_state_manager", None)
    if state_manager is None:
        state_manager = SessionStateManager()
        runtime._session_state_manager = state_manager

    progress_router = ProgressRouter(
        acp_config=runtime.acp_config,
        manager=state_manager,
        publish=_publish_progress,
    )
    runtime._session_states[session_id] = state_manager
    runtime._session_state_routers[session_id] = progress_router
    runtime._session_request_scope_ids[session_id] = session_key
    runtime._session_id_to_session_key[session_id] = session_key
    runtime._session_targets[session_id] = (channel, chat_id, session_key)
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
                caps = runtime._session_caps.get(session_id)
                prompt_meta: dict[str, Any] = {}
                # 中文注释：把当前会话模型/agent 作为 _meta 透传给 ACP，便于链路审计与协议扩展。
                if caps is not None and isinstance(caps.current_model, str) and caps.current_model:
                    prompt_meta["nanobot_session_model"] = caps.current_model
                if caps is not None and isinstance(caps.current_agent, str) and caps.current_agent:
                    prompt_meta["nanobot_session_agent"] = caps.current_agent
                await runtime._conn.prompt(
                    prompt=runtime._build_inbound_prompt_blocks(
                        content,
                        media_paths,
                        session_key=session_key,
                        channel=channel,
                    ),
                    session_id=session_id,
                    **prompt_meta,
                )
                break
            except Exception as exc:
                # 中文注释：部分 ACP 后端在单轮完成后会主动断开连接，
                # 下一轮 prompt 直接复用旧连接会报 "Connection closed"。
                # 这里做一次“重建连接 + 重建会话”的自愈重试。
                if attempt == 0 and _is_connection_closed_error(exc):
                    await _reset_connection_runtime_state(runtime)
                    runtime._session_states.pop(session_id, None)
                    session_id = await _ensure_session(
                        runtime,
                        session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    tracked_session_ids.add(session_id)
                    runtime._session_states[session_id] = state_manager
                    runtime._session_state_routers[session_id] = progress_router
                    runtime._session_request_scope_ids[session_id] = session_key
                    runtime._session_id_to_session_key[session_id] = session_key
                    runtime._session_targets[session_id] = (channel, chat_id, session_key)
                    logger.warning(
                        "ACP prompt retry after connection closed session_key={} new_session_id={}",
                        session_key,
                        session_id,
                    )
                    continue

                # 中文注释：历史 session 映射在某些后端重启场景会失效，
                # 这里对 invalid params 做一次“删映射+重建会话”的自愈重试。
                if attempt == 0 and _is_invalid_params_request_error(exc):
                    stale_session_id = runtime._session_map_binding_manager.resolve_session_id(
                        session_key
                    )
                    if stale_session_id == session_id:
                        runtime._session_map_binding_manager.clear_binding(session_key)
                        runtime._session_caps.pop(session_id, None)
                        runtime._session_activation_ensure_epoch.pop(session_key, None)
                    runtime._session_states.pop(session_id, None)
                    session_id = await _ensure_session(
                        runtime,
                        session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    tracked_session_ids.add(session_id)
                    runtime._session_states[session_id] = state_manager
                    runtime._session_state_routers[session_id] = progress_router
                    runtime._session_request_scope_ids[session_id] = session_key
                    runtime._session_id_to_session_key[session_id] = session_key
                    runtime._session_targets[session_id] = (channel, chat_id, session_key)
                    logger.warning(
                        "ACP prompt retry with new session_key={} old_session_id={} new_session_id={}",
                        session_key,
                        stale_session_id,
                        session_id,
                    )
                    continue

                # 如果 ACP 中断但已有部分文本，交给上层决定是否回退输出。
                request_state = state_manager.get_request_state(session_key)
                partial_text = request_state.final_text if request_state is not None else ""
                partial_esc = partial_text.encode("unicode_escape", "ignore").decode("ascii")
                if len(partial_esc) > 320:
                    partial_esc = f"{partial_esc[:320]}..."
                logger.warning(
                    "ACP prompt failed session_key={} session_id={} partial_chars={} partial_esc='{}'",
                    session_key,
                    session_id,
                    len(partial_text),
                    partial_esc,
                )
                raise _ACPDispatchError(partial_response=partial_text) from exc
        request_state = state_manager.get_request_state(session_key)
        response_text = request_state.final_text.strip() if request_state is not None else ""
        response_media = list(request_state.media_paths) if request_state is not None else []
        return OutboundMessage(
            channel=channel,
            chat_id=chat_id,
            content=response_text,
            media=response_media,
            metadata={},
        )
    finally:
        for tracked_session_id in tracked_session_ids:
            progress_router = runtime._session_state_routers.get(tracked_session_id)
            if progress_router is not None:
                await progress_router.close(tracked_session_id)
        runtime._session_pending_media.pop(session_key, None)
        runtime._session_progress_metadata.pop(session_key, None)
        finalized_request_state = state_manager.finalize_request_scope(session_key)
        runtime._session_result_media[session_key] = finalized_request_state.media_paths
        # 请求结束后清理 session 状态，避免跨请求串流。
        for tracked_session_id in tracked_session_ids:
            runtime._session_states.pop(tracked_session_id, None)
            runtime._session_state_routers.pop(tracked_session_id, None)
            runtime._session_request_scope_ids.pop(tracked_session_id, None)
            runtime._session_id_to_session_key.pop(tracked_session_id, None)
            runtime._session_active_tool_name.pop(tracked_session_id, None)
            runtime._session_targets.pop(tracked_session_id, None)


def _is_connection_closed_error(exc: Exception) -> bool:
    """Return True when the exception indicates ACP transport closed.

    中文注释：优先按异常类型名判断（ConnectionError），并保留消息兜底，
    兼容不同依赖版本对错误类型的差异封装。
    """

    if type(exc).__name__ == "ConnectionError":
        return True
    return "connection closed" in str(exc).lower()
