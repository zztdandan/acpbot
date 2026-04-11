"""ACP 分发主入口。

该文件负责：
1) ACP 连接生命周期管理；
2) inbound 消息到 ACP prompt 的调度；
3) ACP 增量事件到 nanobot 进度/最终消息的桥接。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar

from nanobot.acp.acp_factory import (
    _acp_embedded_blob_resource,
    _acp_embedded_text_resource,
    _acp_image_block,
    _acp_resource_block,
    _acp_resource_link_block,
    _acp_text_block,
)
from nanobot.acp.dispatch_commands import register_acp_builtin_commands
from nanobot.acp.dispatcher_dispatch import _dispatch_inbound
from nanobot.acp.media_codec import _ACPFileTransportMixin
from nanobot.acp.observability import _ACPObservabilityMixin
from nanobot.acp.permission_bridge import PermissionBridge
from nanobot.acp.session_caps import (
    _pick as session_caps_pick,
    _render_agents_command,
    _render_models_command,
    _update_caps_from_session_payload,
)
from nanobot.acp.session_update_router import _route_session_update
from nanobot.acp.session_map import _SessionMapSupport
from nanobot.acp.session_map_binding_manager import _SessionMapBindingManager
from nanobot.acp.session_runtime import (
    _activate_existing_session,
    _ensure_connection,
    _ensure_session,
    _process_direct_impl,
)
from nanobot.acp.session_runtime_mcp import _convert_mcp_servers
from nanobot.acp.dispatcher_state import _init_dispatcher_state
from nanobot.acp.state import _SessionCapabilities, _StreamState
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.command.router import CommandRouter
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig, MCPServerConfig


class ACPDispatcher(_ACPFileTransportMixin, _SessionMapSupport, _ACPObservabilityMixin):
    """ACP backend 的运行时分发器。"""

    _running: bool
    _conn_cm: Any
    _conn: Any
    _proc: Any
    _connect_lock: asyncio.Lock
    _session_map: dict[str, str]
    _session_locks: dict[str, asyncio.Lock]
    _process_locks: dict[str, asyncio.Lock]
    _session_states: dict[str, _StreamState]
    _session_caps: dict[str, _SessionCapabilities]
    _session_desired: dict[str, dict[str, str | None]]
    _active_tasks: dict[str, list[asyncio.Task[Any]]]
    last_target: tuple[str, str] | None
    _session_map_file: Path
    _session_map_bootstrapped: bool
    _session_id_to_session_key: dict[str, str]
    _session_active_tool_name: dict[str, str]
    _session_result_media: dict[str, list[str]]
    _session_pending_media: dict[str, list[str]]
    _session_progress_metadata: dict[str, dict[str, Any]]
    _session_targets: dict[str, tuple[str, str, str]]
    _connection_epoch: int
    _session_activation_ensure_epoch: dict[str, int]
    _session_bootstrap_activated_keys: set[str]
    _session_map_binding_manager: _SessionMapBindingManager
    _permission_bridge: PermissionBridge
    commands: CommandRouter

    # ACP 模式下可用的 slash 命令帮助文本。
    _HELP_TEXT: ClassVar[str] = (
        "🐈 nanobot commands:\n"
        "/new — Start a new conversation\n"
        "/stop — Stop the current task\n"
        "/help — Show available commands\n"
        "/models — List available/current models\n"
        "/set_model <model_id> — Switch model\n"
        "/agents — List available/current agents\n"
        "/set_agent <agent_id> — Switch agent"
    )
    _INBOUND_CONTROL_METADATA_KEYS = frozenset({"_acp_session_model", "_acp_session_agent"})

    def __init__(
        self,
        *,
        bus: MessageBus,
        workspace: Path,
        acp_config: ACPBackendConfig,
        mcp_servers: dict[str, MCPServerConfig] | None = None,
        channels_config: ChannelsConfig | None = None,
    ):
        # bus: 统一消息总线；workspace: ACP 默认 cwd。
        # acp_config: ACP 进程命令/参数/权限策略等。
        self.bus = bus
        self.workspace = workspace
        self.channels_config = channels_config
        self.acp_config = acp_config
        self.mcp_servers = mcp_servers or {}

        # 运行时状态：连接、会话映射、并发锁、活跃任务等。
        self._running: bool
        self._conn_cm: Any
        self._conn: Any
        self._proc: Any
        self._connect_lock: asyncio.Lock
        self._session_map: dict[str, str]
        self._session_locks: dict[str, asyncio.Lock]
        self._process_locks: dict[str, asyncio.Lock]
        self._session_states: dict[str, _StreamState]
        self._session_caps: dict[str, _SessionCapabilities]
        self._session_desired: dict[str, dict[str, str | None]]
        self._active_tasks: dict[str, list[asyncio.Task[Any]]]
        self.last_target: tuple[str, str] | None
        self._session_map_file: Path
        self._session_map_bootstrapped: bool
        self._session_id_to_session_key: dict[str, str]
        self._session_active_tool_name: dict[str, str]
        self._session_result_media: dict[str, list[str]]
        self._session_pending_media: dict[str, list[str]]
        self._session_progress_metadata: dict[str, dict[str, Any]]
        self._session_targets: dict[str, tuple[str, str, str]]
        self._connection_epoch: int
        self._session_activation_ensure_epoch: dict[str, int]
        self._session_bootstrap_activated_keys: set[str]
        self._session_map_binding_manager: _SessionMapBindingManager
        self.commands: CommandRouter
        _init_dispatcher_state(self)
        # 中文注释：ACP 命令采用与 native 一致的 CommandRouter 编制，减少双后端命令语义漂移。
        self.commands = CommandRouter()
        register_acp_builtin_commands(self, self.commands)
        # 中文注释：connection epoch 用于标记“当前稳定连接周期”，重连后递增，驱动会话重激活一次。
        self._connection_epoch = 0
        # 中文注释：记录 session_key 最近一次完成 ensure/activate 的 epoch，实现同一连接周期内去重。
        self._session_activation_ensure_epoch = {}
        # 中文注释：审计文件状态由可观测性 mixin 统一维护，避免主调度器继续膨胀。
        self._init_observability_state()
        self._permission_bridge = PermissionBridge(self)

    @staticmethod
    def _pick(obj: Any, *names: str) -> Any:
        """兼容 snake/camel 字段名时，按候选名顺序取值。"""
        return session_caps_pick(obj, *names)

    @staticmethod
    def _acp_text_block(content: str) -> Any:
        """延迟导入 ACP text_block，避免模块导入期强依赖 ACP。"""
        return _acp_text_block(content)

    @staticmethod
    def _acp_image_block(data: str, mime_type: str, *, uri: str | None = None) -> Any:
        """延迟导入 ACP image_block。"""
        return _acp_image_block(data=data, mime_type=mime_type, uri=uri)

    @staticmethod
    def _acp_resource_link_block(
        name: str,
        uri: str,
        *,
        mime_type: str | None,
        size: int | None,
    ) -> Any:
        """延迟导入 ACP resource_link_block。"""
        return _acp_resource_link_block(name=name, uri=uri, mime_type=mime_type, size=size)

    @staticmethod
    def _acp_embedded_text_resource(uri: str, text: str, *, mime_type: str | None) -> Any:
        """延迟导入 ACP embedded_text_resource。"""
        return _acp_embedded_text_resource(uri=uri, text=text, mime_type=mime_type)

    @staticmethod
    def _acp_embedded_blob_resource(uri: str, blob: str, *, mime_type: str | None) -> Any:
        """延迟导入 ACP embedded_blob_resource。"""
        return _acp_embedded_blob_resource(uri=uri, blob=blob, mime_type=mime_type)

    @staticmethod
    def _acp_resource_block(resource: Any) -> Any:
        """延迟导入 ACP resource_block。"""
        return _acp_resource_block(resource=resource)

    @staticmethod
    def _parse_command(content: str) -> tuple[str, str]:
        """解析 slash 命令和参数。"""
        raw = content.strip()
        if not raw:
            return "", ""
        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""
        return cmd, arg

    @classmethod
    def _sanitize_outbound_metadata(cls, metadata: dict[str, Any] | None) -> dict[str, Any]:
        """移除仅用于 inbound 控制的 metadata，避免回传给客户端造成状态误导。"""
        if not metadata:
            return {}
        return {
            key: value
            for key, value in metadata.items()
            if key not in cls._INBOUND_CONTROL_METADATA_KEYS
        }

    def _update_caps_from_session_payload(self, session_id: str, payload: Any) -> None:
        """从 ACP session payload 中提取模型/agent 能力缓存。"""
        _update_caps_from_session_payload(self._session_caps, session_id, payload)

    async def _list_models_command(self, session_id: str) -> str:
        """格式化当前 session 的模型列表。"""
        return _render_models_command(self._session_caps, session_id)

    async def _list_agents_command(self, session_id: str) -> str:
        """格式化当前 session 的 agent(mode) 列表。"""
        return _render_agents_command(self._session_caps, session_id)

    async def _emit_progress(
        self,
        callback: Callable[..., Awaitable[None]] | None,
        content: str,
        *,
        tool_hint: bool = False,
        tool_event: dict[str, Any] | None = None,
    ) -> None:
        """兼容壳：旧 _emit_progress 链路已弃用，仅保留空实现以便迁移期观测。"""
        del callback, content, tool_hint, tool_event

    async def _permission_response(self, options: list[Any]) -> Any:
        """兼容壳：权限决策已迁移到 permission bridge。"""
        del options
        from acp.schema import RequestPermissionResponse

        return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})

    async def _request_permission_bridge(
        self, *, options: list[Any], session_id: str, tool_call: Any
    ) -> Any:
        """通过 permission bridge 执行“请求上送 -> inbound 回填 -> ACP 回应”闭环。"""
        return await self._permission_bridge.request_permission(
            options=options,
            session_id=session_id,
            tool_call=tool_call,
        )

    async def _handle_session_update(self, session_id: str, update: Any) -> None:
        """处理 ACP session 增量更新并合并成可用输出。"""
        # 只有 process_direct 注册过的 session 才需要聚合输出。
        state = self._session_states.get(session_id)
        if state is None:
            return
        # 中文注释：_handle_session_update 对外签名保持不变，仅把分支细节下沉到 router/family handlers。
        await _route_session_update(self, session_id, state, update)

    async def _ensure_connection(self) -> None:
        """确保 ACP 连接可用；首次调用时完成进程拉起与 initialize。"""
        await _ensure_connection(self)

    async def _ensure_session(
        self,
        session_key: str,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
    ) -> str:
        """确保 session_key 对应 ACP session 存在并可复用。"""
        return await _ensure_session(
            self,
            session_key,
            preferred_model=preferred_model,
            preferred_agent=preferred_agent,
        )

    async def _activate_existing_session(self, *, session_key: str, session_id: str) -> bool:
        """尝试把已映射 session 激活到 ACP 当前进程内存态。"""
        return await _activate_existing_session(
            self,
            session_key=session_key,
            session_id=session_id,
        )

    def _convert_mcp_servers(self) -> list[Any]:
        """把 nanobot MCP 配置转换为 ACP schema。"""
        return _convert_mcp_servers(self.mcp_servers)

    async def process_direct(
        self,
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress: Callable[..., Awaitable[None]] | None = None,
    ) -> OutboundMessage:
        """直接发送一轮 prompt 到指定 session，并返回标准 OutboundMessage。"""
        return await _process_direct_impl(
            self,
            content,
            session_key=session_key,
            channel=channel,
            chat_id=chat_id,
            preferred_model=preferred_model,
            preferred_agent=preferred_agent,
            on_progress=on_progress,
        )

    async def _handle_stop(self, msg: InboundMessage) -> None:
        """停止当前 session_key 下仍在运行的任务。"""
        tasks = self._active_tasks.pop(msg.session_key, [])
        cancelled = sum(1 for task in tasks if not task.done() and task.cancel())
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        content = f"⏹ Stopped {cancelled} task(s)." if cancelled else "No active task to stop."
        await self._publish_outbound_with_debug(
            msg=OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=content),
            reason="command_stop",
            session_key=msg.session_key,
        )

    async def _dispatch(self, msg: InboundMessage) -> None:
        """处理单条 inbound 消息并发布 outbound。"""
        await _dispatch_inbound(self, msg)

    async def run(self) -> None:
        """主循环：消费 inbound，并把每条消息分发为异步任务。"""
        self._running = True
        await self._ensure_connection()
        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.consume_inbound(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if msg.content.strip().lower() == "/stop":
                # /stop 优先由 dispatcher 控制层处理。
                await self._handle_stop(msg)
                continue

            task = asyncio.create_task(self._dispatch(msg))
            # 维护 session 维度活跃任务列表，便于 /stop 定位取消。
            self._active_tasks.setdefault(msg.session_key, []).append(task)
            task.add_done_callback(
                lambda done, key=msg.session_key: (
                    self._active_tasks.get(key, []) and self._active_tasks[key].remove(done)
                    if done in self._active_tasks.get(key, [])
                    else None
                )
            )

    def stop(self) -> None:
        """请求停止主循环。"""
        self._running = False

    async def close(self) -> None:
        """关闭 ACP 连接上下文并重置运行时句柄。"""
        self.stop()
        if self._conn_cm is not None:
            await self._conn_cm.__aexit__(None, None, None)
        self._conn_cm = None
        self._conn = None
        self._proc = None
        self._session_map_bootstrapped = False
        self._session_map_binding_manager.mark_unbootstrapped()
        await self._permission_bridge.close()
        # 中文注释：可观测性资源统一由 mixin 关闭，避免主流程混入文件句柄细节。
        self._close_observability()
