"""ACP 分发主入口。

该文件负责：
1) ACP 连接生命周期管理；
2) inbound 消息到 ACP prompt 的调度；
3) ACP 增量事件到 nanobot 进度/最终消息的桥接。
"""

from __future__ import annotations

import asyncio
import os
from importlib import import_module
from pathlib import Path
from typing import Any, Awaitable, Callable

from loguru import logger

from nanobot.acp.client import _NanobotACPClient
from nanobot.acp.session_map import _SessionMapSupport
from nanobot.acp.state import _ACPDispatchError, _SessionCapabilities, _StreamState
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.paths import get_data_dir
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig, MCPServerConfig


class ACPDispatcher(_SessionMapSupport):
    """ACP backend 的运行时分发器。"""

    # ACP 模式下可用的 slash 命令帮助文本。
    _HELP_TEXT = (
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
        self._running = False
        self._conn_cm = None
        self._conn = None
        self._proc = None
        self._connect_lock = asyncio.Lock()
        self._session_map: dict[str, str] = {}
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._process_locks: dict[str, asyncio.Lock] = {}
        self._session_states: dict[str, _StreamState] = {}
        self._session_caps: dict[str, _SessionCapabilities] = {}
        self._active_tasks: dict[str, list[asyncio.Task[Any]]] = {}
        self.last_target: tuple[str, str] | None = None
        self._session_map_file = get_data_dir() / "acp-session-map.json"
        self._session_map_bootstrapped = False

    @staticmethod
    def _pick(obj: Any, *names: str) -> Any:
        """兼容 snake/camel 字段名时，按候选名顺序取值。"""
        for name in names:
            if hasattr(obj, name):
                return getattr(obj, name)
        return None

    @staticmethod
    def _acp_spawn_agent_process() -> Any:
        """延迟导入 ACP 包的进程启动函数。"""
        return import_module("acp").spawn_agent_process

    @staticmethod
    def _acp_text_block(content: str) -> Any:
        """延迟导入 ACP text_block，避免模块导入期强依赖 ACP。"""
        return import_module("acp").text_block(content)

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
        caps = self._session_caps.setdefault(session_id, _SessionCapabilities())
        models = self._pick(payload, "models")
        if models is not None:
            current = self._pick(models, "current_model_id", "currentModelId")
            if isinstance(current, str) and current:
                caps.current_model = current
            available = self._pick(models, "available_models", "availableModels") or []
            parsed_models: list[str] = []
            for entry in available:
                model_id = self._pick(entry, "model_id", "modelId")
                if isinstance(model_id, str) and model_id:
                    parsed_models.append(model_id)
            if parsed_models:
                caps.available_models = parsed_models

        modes = self._pick(payload, "modes")
        if modes is not None:
            current = self._pick(modes, "current_mode_id", "currentModeId")
            if isinstance(current, str) and current:
                caps.current_agent = current
            available = self._pick(modes, "available_modes", "availableModes") or []
            parsed_agents: list[str] = []
            for entry in available:
                mode_id = self._pick(entry, "id")
                if isinstance(mode_id, str) and mode_id:
                    parsed_agents.append(mode_id)
            if parsed_agents:
                caps.available_agents = parsed_agents

    async def _list_models_command(self, session_id: str) -> str:
        """格式化当前 session 的模型列表。"""
        caps = self._session_caps.get(session_id)
        if not caps or not caps.available_models:
            return "No model catalog returned by current ACP backend for this session."
        lines = []
        current = caps.current_model
        for model_id in caps.available_models:
            prefix = "* " if current == model_id else "  "
            lines.append(f"{prefix}{model_id}")
        header = f"Current model: {current}" if current else "Current model: unknown"
        return "\n".join([header, "Available models:", *lines])

    async def _list_agents_command(self, session_id: str) -> str:
        """格式化当前 session 的 agent(mode) 列表。"""
        caps = self._session_caps.get(session_id)
        if not caps or not caps.available_agents:
            return "No agent/mode catalog returned by current ACP backend for this session."
        lines = []
        current = caps.current_agent
        for agent_id in caps.available_agents:
            prefix = "* " if current == agent_id else "  "
            lines.append(f"{prefix}{agent_id}")
        header = f"Current agent: {current}" if current else "Current agent: unknown"
        return "\n".join([header, "Available agents:", *lines])

    async def _emit_progress(
        self,
        callback: Callable[..., Awaitable[None]] | None,
        content: str,
        *,
        tool_hint: bool = False,
    ) -> None:
        """安全触发进度回调，兼容不同回调签名。"""
        if not callback or not content:
            return
        try:
            await callback(content, tool_hint=tool_hint)
        except TypeError:
            await callback(content)

    async def _permission_response(self, options: list[Any]) -> Any:
        """按配置策略构造 ACP 权限响应。"""
        from acp.schema import RequestPermissionResponse

        # strict: 全拒绝；trusted/yolo: 优先选 allow_always/allow_once。
        policy = self.acp_config.permissions_policy
        if policy == "strict":
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})

        preferred = "allow_always" if policy == "trusted" else "allow_once"
        option_id = None
        for option in options:
            kind = getattr(option.kind, "value", option.kind)
            if kind == preferred:
                option_id = option.option_id
                break
        if option_id is None and options:
            option_id = options[0].option_id
        if option_id is None:
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})
        return RequestPermissionResponse.model_validate(
            {"outcome": {"outcome": "selected", "optionId": option_id}}
        )

    async def _handle_session_update(self, session_id: str, update: Any) -> None:
        """处理 ACP session 增量更新并合并成可用输出。"""
        from acp.schema import (
            AgentMessageChunk,
            CurrentModeUpdate,
            TextContentBlock,
            ToolCallProgress,
            ToolCallStart,
        )

        # 只有 process_direct 注册过的 session 才需要聚合输出。
        state = self._session_states.get(session_id)
        if state is None:
            return

        if isinstance(update, AgentMessageChunk) and isinstance(update.content, TextContentBlock):
            # 文本分片进入去重合并，并按需流式回调给上层。
            state.merge_text(update.content.text)
            await self._emit_progress(state.on_progress, update.content.text)
            return

        if isinstance(update, ToolCallStart):
            # tool start/progress 走 tool_hint 通道，便于前端区分展示。
            title = update.title or "tool"
            await self._emit_progress(state.on_progress, title, tool_hint=True)
            return

        if isinstance(update, ToolCallProgress):
            status = getattr(update.status, "value", update.status) if update.status else None
            if status:
                await self._emit_progress(state.on_progress, status, tool_hint=True)

        if isinstance(update, CurrentModeUpdate):
            caps = self._session_caps.get(session_id)
            if caps is not None:
                caps.current_agent = update.current_mode_id

    async def _ensure_connection(self) -> None:
        """确保 ACP 连接可用；首次调用时完成进程拉起与 initialize。"""
        if self._conn is not None:
            return

        async with self._connect_lock:
            if self._conn is not None:
                return

            from acp.schema import ClientCapabilities, Implementation

            # client 负责把 ACP 回调转发到 dispatcher。
            client = _NanobotACPClient(self)
            env = {**os.environ, **self.acp_config.env}
            cwd = Path(self.acp_config.cwd).expanduser() if self.acp_config.cwd else self.workspace
            timeout = max(1, self.acp_config.startup_timeout_seconds)

            # 启动 ACP 子进程并进入连接上下文。
            self._conn_cm = self._acp_spawn_agent_process()(
                client,
                self.acp_config.command,
                *self.acp_config.args,
                env=env,
                cwd=cwd,
            )
            try:
                # 连接建立与 initialize 都受 startup timeout 保护。
                self._conn, self._proc = await asyncio.wait_for(
                    self._conn_cm.__aenter__(), timeout=timeout
                )
                await asyncio.wait_for(
                    self._conn.initialize(
                        protocol_version=self.acp_config.protocol_version,
                        client_capabilities=ClientCapabilities(),
                        client_info=Implementation(name="nanobot", version="0.1.4.post2"),
                    ),
                    timeout=timeout,
                )
                # initialize 成功后再加载并对账 session map。
                await self._bootstrap_session_map()
            except Exception:
                # 失败时完整回收连接上下文，避免半初始化残留。
                if self._conn_cm is not None:
                    await self._conn_cm.__aexit__(None, None, None)
                self._conn_cm = None
                self._conn = None
                self._proc = None
                self._session_map_bootstrapped = False
                raise

    async def _ensure_session(
        self,
        session_key: str,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
    ) -> str:
        """确保 session_key 对应 ACP session 存在并可复用。"""
        session_id = self._session_map.get(session_key)
        if session_id:
            return session_id

        # 每个 session_key 单独加锁，避免并发创建重复会话。
        lock = self._session_locks.setdefault(session_key, asyncio.Lock())
        async with lock:
            session_id = self._session_map.get(session_key)
            if session_id:
                return session_id
            await self._ensure_connection()
            if self._conn is None:
                raise RuntimeError("ACP connection is not available")
            cwd = Path(self._resolved_acp_cwd())
            response = await self._conn.new_session(
                cwd=str(cwd),
                mcp_servers=self._convert_mcp_servers(),
            )
            session_id = response.session_id
            selected_model = preferred_model or self.acp_config.default_model
            selected_agent = preferred_agent or self.acp_config.default_mode
            logger.info(
                "New ACP session created: {}, applying selection: model={} (preferred={} default={}), mode={} (preferred={} default={})",
                session_id,
                selected_model,
                preferred_model,
                self.acp_config.default_model,
                selected_agent,
                preferred_agent,
                self.acp_config.default_mode,
            )

            if selected_model:
                try:
                    # 中文注释：会话模型遵循“首帧偏好优先，配置 default 回落”。
                    await self._conn.set_session_model(
                        model_id=selected_model, session_id=session_id
                    )
                    caps = self._session_caps.setdefault(session_id, _SessionCapabilities())
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
                    await self._conn.set_session_mode(mode_id=selected_agent, session_id=session_id)
                    caps = self._session_caps.setdefault(session_id, _SessionCapabilities())
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
            self._session_map[session_key] = session_id
            self._persist_session_map()
            self._update_caps_from_session_payload(session_id, response)
            return session_id

    def _convert_mcp_servers(self) -> list[Any]:
        """把 nanobot MCP 配置转换为 ACP schema。"""
        from acp.schema import EnvVariable, HttpHeader, HttpMcpServer, McpServerStdio, SseMcpServer

        converted = []
        for name, cfg in self.mcp_servers.items():
            if cfg.command:
                # stdio MCP
                converted.append(
                    McpServerStdio(
                        name=name,
                        command=cfg.command,
                        args=cfg.args,
                        env=[EnvVariable(name=k, value=v) for k, v in cfg.env.items()],
                        field_meta={"toolTimeout": cfg.tool_timeout},
                    )
                )
                continue
            if cfg.url:
                # http/sse MCP
                headers = [HttpHeader(name=k, value=v) for k, v in cfg.headers.items()]
                if cfg.url.startswith("http://") or cfg.url.startswith("https://"):
                    converted.append(
                        HttpMcpServer(
                            type="http",
                            name=name,
                            url=cfg.url,
                            headers=headers,
                            field_meta={"toolTimeout": cfg.tool_timeout},
                        )
                    )
                else:
                    converted.append(
                        SseMcpServer(
                            type="sse",
                            name=name,
                            url=cfg.url,
                            headers=headers,
                            field_meta={"toolTimeout": cfg.tool_timeout},
                        )
                    )
        return converted

    async def process_direct(
        self,
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress: Callable[[str], Awaitable[None]] | None = None,
    ) -> str:
        """直接发送一轮 prompt 到指定 session，并返回聚合后的文本。"""
        del channel, chat_id
        await self._ensure_connection()
        if self._conn is None:
            raise RuntimeError("ACP connection is not available")
        session_id = await self._ensure_session(
            session_key,
            preferred_model=preferred_model,
            preferred_agent=preferred_agent,
        )
        # 注册会话级流式状态，供 session_update 回调写入。
        state = _StreamState(on_progress=on_progress)
        self._session_states[session_id] = state
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

            try:
                await self._conn.prompt(
                    prompt=[self._acp_text_block(content)],
                    session_id=session_id,
                )
            except Exception as exc:
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
            # 请求结束后清理 session 状态，避免跨请求串流。
            self._session_states.pop(session_id, None)

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
        await self.bus.publish_outbound(
            OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=content)
        )

    async def _dispatch(self, msg: InboundMessage) -> None:
        """处理单条 inbound 消息并发布 outbound。"""
        try:
            key = msg.session_key
            if msg.channel == "system":
                # system 通道复用来源会话，避免上下文断层。
                origin = msg.chat_id if ":" in msg.chat_id else f"cli:{msg.chat_id}"
                key = origin

            command, arg = self._parse_command(msg.content)
            if command == "/help":
                await self.bus.publish_outbound(
                    OutboundMessage(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        content=self._HELP_TEXT,
                    )
                )
                return
            if command == "/new":
                # /new 仅清理当前会话映射，并同步落盘。
                old_session_id = self._session_map.pop(key, None)
                if old_session_id:
                    self._session_caps.pop(old_session_id, None)
                    self._persist_session_map()
                await self.bus.publish_outbound(
                    OutboundMessage(
                        channel=msg.channel, chat_id=msg.chat_id, content="New session started."
                    )
                )
                return
            if command == "/models":
                session_id = await self._ensure_session(key)
                content = await self._list_models_command(session_id)
                await self.bus.publish_outbound(
                    OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=content)
                )
                return
            if command == "/agents":
                session_id = await self._ensure_session(key)
                content = await self._list_agents_command(session_id)
                await self.bus.publish_outbound(
                    OutboundMessage(channel=msg.channel, chat_id=msg.chat_id, content=content)
                )
                return
            if command == "/set_model":
                if not arg:
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            channel=msg.channel,
                            chat_id=msg.chat_id,
                            content="Usage: /set_model <model_id>",
                        )
                    )
                    return
                await self._ensure_connection()
                if self._conn is None:
                    raise RuntimeError("ACP connection is not available")
                session_id = await self._ensure_session(key)
                await self._conn.set_session_model(model_id=arg, session_id=session_id)
                caps = self._session_caps.setdefault(session_id, _SessionCapabilities())
                caps.current_model = arg
                await self.bus.publish_outbound(
                    OutboundMessage(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        content=f"Model switched to: {arg}",
                    )
                )
                return
            if command == "/set_agent":
                if not arg:
                    await self.bus.publish_outbound(
                        OutboundMessage(
                            channel=msg.channel,
                            chat_id=msg.chat_id,
                            content="Usage: /set_agent <agent_id>",
                        )
                    )
                    return
                await self._ensure_connection()
                if self._conn is None:
                    raise RuntimeError("ACP connection is not available")
                session_id = await self._ensure_session(key)
                await self._conn.set_session_mode(mode_id=arg, session_id=session_id)
                caps = self._session_caps.setdefault(session_id, _SessionCapabilities())
                caps.current_agent = arg
                await self.bus.publish_outbound(
                    OutboundMessage(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        content=f"Agent switched to: {arg}",
                    )
                )
                return

            if msg.channel not in {"cli", "system"} and msg.chat_id:
                # 记录最近一次真实渠道目标，供其它功能选路时参考。
                self.last_target = (msg.channel, msg.chat_id)

            content_esc = msg.content.encode("unicode_escape", "ignore").decode("ascii")
            if len(content_esc) > 320:
                content_esc = f"{content_esc[:320]}..."
            logger.debug(
                "ACP dispatch inbound channel={} sender={} chat={} session_key={} chars={} metadata_keys={} content_esc='{}'",
                msg.channel,
                msg.sender_id,
                msg.chat_id,
                key,
                len(msg.content),
                sorted((msg.metadata or {}).keys()),
                content_esc,
            )

            # 每个会话串行执行，避免同一会话并发 prompt 互相覆盖状态。
            lock = self._process_locks.setdefault(key, asyncio.Lock())
            async with lock:
                metadata = msg.metadata or {}
                # 中文注释：WS 首帧透传偏好时，仅在“首次创建 ACP session”阶段参与默认选择。
                preferred_model_raw = metadata.get("_acp_session_model")
                preferred_agent_raw = metadata.get("_acp_session_agent")
                preferred_model = (
                    str(preferred_model_raw).strip()
                    if isinstance(preferred_model_raw, str) and preferred_model_raw.strip()
                    else None
                )
                preferred_agent = (
                    str(preferred_agent_raw).strip()
                    if isinstance(preferred_agent_raw, str) and preferred_agent_raw.strip()
                    else None
                )
                try:
                    response = await self.process_direct(
                        msg.content,
                        session_key=key,
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                except _ACPDispatchError as exc:
                    # ACP 异常时优先尝试输出 partial，减少用户感知中断。
                    partial = exc.partial_response.strip()
                    if partial:
                        partial_esc = partial.encode("unicode_escape", "ignore").decode("ascii")
                        if len(partial_esc) > 320:
                            partial_esc = f"{partial_esc[:320]}..."
                        logger.warning(
                            "ACP dispatch fallback using partial response channel={} chat={} session_key={} partial_chars={} partial_esc='{}'",
                            msg.channel,
                            msg.chat_id,
                            key,
                            len(partial),
                            partial_esc,
                        )
                        await self.bus.publish_outbound(
                            OutboundMessage(
                                channel=msg.channel,
                                chat_id=msg.chat_id,
                                content=partial,
                                metadata=self._sanitize_outbound_metadata(msg.metadata),
                            )
                        )
                        return
                    raise
            await self.bus.publish_outbound(
                OutboundMessage(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    content=response,
                    metadata=self._sanitize_outbound_metadata(msg.metadata),
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # 兜底保护：防止异常导致消息链路静默。
            logger.exception("ACP dispatcher failed for {}", msg.session_key)
            await self.bus.publish_outbound(
                OutboundMessage(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    content="Sorry, I encountered an error.",
                )
            )

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
