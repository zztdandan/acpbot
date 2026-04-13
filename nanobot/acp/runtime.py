"""ACP runtime main entrypoint."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncContextManager, Awaitable, Callable
from uuid import uuid4

from loguru import logger

from nanobot.acp.inbound import InboundManager
from nanobot.acp.observability import ObservabilityEvent, ObservabilityManager
from nanobot.acp.runtime_lifecycle import close_runtime, ensure_connection, reset_connection
from nanobot.acp.runtime_models import (
    ProcessDirectInput,
    RequestStatus,
    RuntimeWaitEntry,
    StopResult,
)
from nanobot.acp.runtime_process_manager import ProcessRuntimeManager
from nanobot.acp.session_caps import _render_agents_command, _render_models_command
from nanobot.acp.sessionmap import SessionMapBindingManager, SessionRuntimeManager
from nanobot.acp.state import _SessionCapabilities
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.paths import get_data_dir
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig

if TYPE_CHECKING:
    from acp.client import ClientSideConnection

    from nanobot.acp.runtime_client import _NanobotACPClient
else:
    ClientSideConnection = Any
    _NanobotACPClient = Any


class ACPRuntime:
    """The ACP backend runtime owner defined by the ACP redesign docs."""

    def __init__(
        self,
        *,
        bus: MessageBus,
        workspace: Path,
        acp_config: ACPBackendConfig,
        channels_config: ChannelsConfig | None = None,
    ) -> None:
        self.bus = bus
        self.workspace = workspace
        self.acp_config = acp_config
        self.channels_config = channels_config
        self._running = False
        # 中文注释：保存 ACP SDK 返回的异步上下文管理器，runtime close/reset 时要靠它
        # 统一退出底层连接、transport 和 agent subprocess，避免资源泄漏或半关闭句柄残留。
        self._acp_client_connection_cm: (
            AsyncContextManager[tuple[ClientSideConnection, asyncio.subprocess.Process]] | None
        ) = None
        # 中文注释：这是 nanobot 主动向 ACP 发起 initialize/new_session/prompt 等
        # client->agent 请求的唯一 RPC 连接句柄。
        self._acp_client_conn: ClientSideConnection | None = None
        # 中文注释：显式保留 agent subprocess 句柄，是为了在 runtime reset/close/诊断时
        # 能判断协议对端本地进程是否仍存活，并把“连接”和“对端进程”作为同一生命周期资源处理。
        self._acp_agent_process: asyncio.subprocess.Process | None = None
        # 中文注释：连接建立、重置、清理都必须串行，避免并发 ensure/reset 把 runtime
        # 打成半连接状态，所以 runtime 直接 owner 这把生命周期锁。
        self._acp_connection_lock = asyncio.Lock()
        # 中文注释：ACP 回调桥只负责把 agent->client 回调重新导回 runtime owner，
        # 避免 session_update/request_permission 直接散落到其他模块。
        # _NanobotACPClient 是 ClientSideConnection 链接的必要注入条件，必须由 runtime侧实现这些方法，否则ClientSideConnection 无法建立
        self.acp_callback_client: _NanobotACPClient | None = None
        self._wait_by_request_key: dict[str, RuntimeWaitEntry] = {}
        self._session_caps: dict[str, _SessionCapabilities] = {}
        self._session_map_file = get_data_dir() / "acp" / "session_map.json"

        # 中文注释：runtime 只 owner 总控对象；真正的请求排队、单轮状态、绑定真相
        # 都分别下沉到 process manager / state / sessionmap，避免再长出顶层散字典。
        self.observability_manager = ObservabilityManager()
        self.sessionmap_binding_manager = SessionMapBindingManager(self)
        self.session_runtime_manager = SessionRuntimeManager(
            runtime=self,
            binding_manager=self.sessionmap_binding_manager,
        )
        self.process_runtime_manager = ProcessRuntimeManager(runtime=self)
        self.inbound_manager = InboundManager(runtime=self)

    def new_request_key(self) -> str:
        return f"acp:{uuid4().hex}"

    async def await_acp_prompt(self, **kwargs: Any) -> None:
        """Await one ACP prompt call under a bounded timeout."""

        if self._acp_client_conn is None:
            raise RuntimeError("ACP connection is not available")
        # 中文注释：真实 prompt 执行必须有超时保护；否则 direct/bus 两条 wait 链路
        # 都可能永远卡在统一 completion 之前，破坏 runtime 的 await 语义闭环。
        timeout = max(30, self.acp_config.startup_timeout_seconds)
        await asyncio.wait_for(self._acp_client_conn.prompt(**kwargs), timeout=timeout)

    def register_wait_entry(self, request_key: str) -> RuntimeWaitEntry:
        # 中文注释：runtime 只记录 request_key -> future 的等待关系，
        # 不记录执行态细节；执行态 owner 在 ProcessRuntimeManager。
        wait_entry = RuntimeWaitEntry(
            request_key=request_key,
            done_future=asyncio.get_running_loop().create_future(),
        )
        self._wait_by_request_key[request_key] = wait_entry
        return wait_entry

    def remove_wait_entry(self, request_key: str) -> None:
        self._wait_by_request_key.pop(request_key, None)

    def fail_all_wait_entries(self, error: Exception) -> None:
        """Fail all unresolved wait entries during runtime rebuild/reset."""

        for wait_entry in self._wait_by_request_key.values():
            if not wait_entry.done_future.done():
                wait_entry.done_future.set_exception(error)

    async def complete_process_request(
        self,
        request_key: str,
        *,
        outbound: OutboundMessage | None = None,
        error: Exception | None = None,
    ) -> None:
        # 中文注释：所有 direct response / 正常执行完成 / 错误完成都必须收敛到这里，
        # 这样 runtime 才能保持唯一 completion 入口，不让 inbound/state 直接碰 wait map。
        wait_entry = self._wait_by_request_key.get(request_key)
        if wait_entry is None or wait_entry.done_future.done():
            return
        if error is not None:
            wait_entry.done_future.set_exception(error)
            return
        if outbound is None:
            wait_entry.done_future.set_exception(RuntimeError("request completed without outbound"))
            return
        wait_entry.done_future.set_result(outbound)

    async def ensure_connection(self) -> None:
        await ensure_connection(self)

    async def reset_connection(self) -> None:
        await reset_connection(self)

    async def run(self) -> None:
        self._running = True
        await self.ensure_connection()
        await self.observability_manager.start()
        while self._running:
            # 中文注释：bus inbound 不再绕道旧 dispatcher helper，
            # 统一由 runtime 生成 request_key 后进入同一条 wait/completion 主链。
            message = await self.bus.consume_inbound()
            asyncio.create_task(self.dispatch_inbound(message))

    async def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        await close_runtime(self)

    async def process_direct(
        self,
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        media: list[str] | None = None,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress: Callable[[str], Awaitable[None]] | None = None,
    ) -> OutboundMessage:
        await self.observability_manager.start()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key)
        try:
            # 中文注释：direct 路径先把散参数收敛成 runtime 级输入对象，
            # 再交给 inbound 转成统一 InboundContext，避免再次回退到大 handle(...散参数) 形态。
            input = ProcessDirectInput(
                content=content,
                nanobot_side_session_key=session_key,
                channel=channel,
                chat_id=chat_id,
                media=list(media or []),
                metadata={
                    **({"_acp_session_model": preferred_model} if preferred_model else {}),
                    **({"_acp_session_agent": preferred_agent} if preferred_agent else {}),
                },
                on_progress=on_progress,
            )
            await self.inbound_manager.handle_process_direct(input, request_key=request_key)
            # 中文注释：runtime 只等待最终结果；真正排队、激活、state close 都不在这里做。
            return await wait_entry.done_future
        finally:
            self.remove_wait_entry(request_key)

    async def dispatch_inbound(self, message: InboundMessage) -> None:
        await self.observability_manager.start()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key)
        try:
            # 中文注释：bus 路径与 direct 共用同一套 request_key / wait entry / completion，
            # 只是 direct 最终 return，bus 最终 publish。
            await self.inbound_manager.handle_inbound(message, request_key=request_key)
            outbound = await wait_entry.done_future
            await self.publish_final_outbound(message=message, outbound=outbound)
        except Exception as exc:
            logger.exception("ACP dispatch_inbound failed")
            await self.push_observability(
                self.new_observability_event(
                    scope="runtime",
                    event="dispatch_inbound_error",
                    request_key=request_key,
                    nanobot_side_session_key=self.resolve_nanobot_side_session_key(message),
                    payload={"error": str(exc)},
                )
            )
        finally:
            self.remove_wait_entry(request_key)

    async def publish_final_outbound(
        self, *, message: InboundMessage, outbound: OutboundMessage
    ) -> None:
        # 中文注释：final publish 是 bus 路径自己的消费者动作，
        # 不下沉到 complete_process_request()，避免“完成请求”和“对外投递”再次混在一起。
        final_metadata = dict(message.metadata or {})
        final_metadata.update(outbound.metadata or {})
        await self.bus.publish_outbound(
            OutboundMessage(
                channel=message.channel,
                chat_id=message.chat_id,
                content=outbound.content,
                media=list(outbound.media),
                metadata=final_metadata,
            )
        )

    async def handle_session_update(self, *, acp_side_session_id: str, update: Any) -> None:
        # 中文注释：runtime 只做 owner 分派：先根据 acp_side_session_id 找 active request，
        # 命中后把 update 交给该 request 的 state；找不到就记 orphan observability。
        active_entry = self.process_runtime_manager.get_active_by_acp_side_session_id(
            acp_side_session_id
        )
        if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
            await self.push_observability(
                self.new_observability_event(
                    scope="runtime",
                    event="orphan_session_update",
                    acp_side_session_id=acp_side_session_id,
                )
            )
            return
        await active_entry.state_manager.consume_session_update(
            update,
            progress_router=active_entry.progress_router,
        )

    async def handle_permission_request(
        self, *, acp_side_session_id: str, options: list[Any], tool_call: Any
    ) -> Any:
        # 中文注释：permission callback 也不在 runtime 里做业务判断；
        # runtime 只负责把请求投递到当前 active request 的 state owner。
        active_entry = self.process_runtime_manager.get_active_by_acp_side_session_id(
            acp_side_session_id
        )
        if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
            await self.push_observability(
                self.new_observability_event(
                    scope="runtime",
                    event="orphan_permission_request",
                    acp_side_session_id=acp_side_session_id,
                )
            )
            return await self.permission_response(options)
        return await active_entry.state_manager.handle_permission_request(
            options=options, tool_call=tool_call
        )

    async def permission_response(self, options: list[Any]) -> Any:
        from acp.schema import RequestPermissionResponse

        policy = self.acp_config.permissions_policy
        if policy == "strict":
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})
        preferred = "allow_always" if policy == "trusted" else "allow_once"
        option_id = None
        for option in options:
            kind = getattr(getattr(option, "kind", None), "value", getattr(option, "kind", None))
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

    async def stop_session(self, *, nanobot_side_session_key: str) -> StopResult:
        await self.ensure_sessionmap_truth_loaded()
        # 中文注释：/stop 的身份真相来自 sessionmap，是否 active/queued 的执行态真相
        # 则只认 ProcessRuntimeManager，符合设计里的双 owner 分层。
        acp_side_session_id = self.sessionmap_binding_manager.resolve_session_id(
            nanobot_side_session_key
        )
        (
            active_cancel_requested,
            dropped_queued_count,
        ) = await self.process_runtime_manager.stop_session(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )
        return StopResult(
            nanobot_side_session_key=nanobot_side_session_key,
            active_cancel_requested=active_cancel_requested,
            dropped_queued_count=dropped_queued_count,
            acp_side_session_id=acp_side_session_id,
        )

    def drop_session_binding_and_runtime_entry(
        self, *, nanobot_side_session_key: str
    ) -> str | None:
        """Drop binding truth and runtime-ready entry together to force a fresh session."""

        self.session_runtime_manager.drop_ready_session(
            nanobot_side_session_key=nanobot_side_session_key,
        )
        return self.sessionmap_binding_manager.clear_binding(nanobot_side_session_key)

    async def ensure_sessionmap_truth_loaded(self) -> None:
        """Load binding truth even when commands run before ACP bootstrap."""

        await self.sessionmap_binding_manager.load_persistent_truth()

    def resolve_nanobot_side_session_key(self, message: InboundMessage) -> str:
        if message.channel == "system":
            return message.chat_id if ":" in message.chat_id else f"cli:{message.chat_id}"
        return message.session_key

    async def push_observability(self, event: ObservabilityEvent) -> None:
        await self.observability_manager.push(event)

    def new_observability_event(
        self,
        *,
        scope: str,
        event: str,
        request_key: str | None = None,
        nanobot_side_session_key: str | None = None,
        acp_side_session_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ObservabilityEvent:
        return ObservabilityEvent(
            scope=scope,
            event=event,
            request_key=request_key,
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            payload=dict(payload or {}),
        )

    @staticmethod
    def new_outbound_message(*, channel: str, chat_id: str, content: str) -> OutboundMessage:
        return OutboundMessage(channel=channel, chat_id=chat_id, content=content)

    def new_session_capabilities(self) -> _SessionCapabilities:
        return _SessionCapabilities()

    async def list_models_command(self, acp_side_session_id: str) -> str:
        return _render_models_command(self._session_caps, acp_side_session_id)

    async def list_agents_command(self, acp_side_session_id: str) -> str:
        return _render_agents_command(self._session_caps, acp_side_session_id)


class ACPDispatcher(ACPRuntime):
    """Compatibility class name preserved for external callers and CLI wiring."""
