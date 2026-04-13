"""ACP runtime main entrypoint."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, AsyncContextManager, Awaitable, Callable, cast
from uuid import uuid4

from loguru import logger

from nanobot.acp.contracts import (
    ACPCallbackUpdate,
    ACPChannelName,
    ACPDirectIdentity,
    ACPPermissionKind,
    ACPPermissionOption,
    ACPPermissionPolicyName,
    ACPToolCall,
    JSONMap,
    ObservabilityEventName,
    ObservabilityScopeName,
    build_permission_cancelled_payload,
    build_permission_selected_payload,
)
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
from nanobot.acp.sessionmap import SessionMapBindingManager, SessionRuntimeManager
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig

if TYPE_CHECKING:
    from acp.client import ClientSideConnection

    from nanobot.acp.runtime_client import _NanobotACPClient
else:
    ClientSideConnection = object
    _NanobotACPClient = object


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
        # Keep the SDK-owned async context manager so runtime close/reset can exit the
        # transport, RPC connection, and spawned ACP process as one lifecycle unit.
        self._acp_client_connection_cm: (
            AsyncContextManager[tuple[ClientSideConnection, asyncio.subprocess.Process]] | None
        ) = None
        # This is nanobot's single client->agent RPC handle for initialize,
        # new_session, prompt, and later session-level control methods.
        self._acp_client_conn: ClientSideConnection | None = None
        # Keep the subprocess handle explicitly so reset/close/diagnostics can reason
        # about the remote agent process and the RPC connection together.
        self._acp_agent_process: asyncio.subprocess.Process | None = None
        # Connection ensure/reset/cleanup must stay serialized or the runtime can end
        # up in a half-connected generation, so runtime owns the lifecycle lock.
        self._acp_connection_lock = asyncio.Lock()
        # The callback bridge exists only to route agent->client callbacks back to the
        # runtime owner instead of letting session_update/request_permission fan out.
        self.acp_callback_client: _NanobotACPClient | None = None
        self._wait_by_request_key: dict[str, RuntimeWaitEntry] = {}

        # Runtime owns only the top-level coordinators. Request queueing, request state,
        # and binding truth stay in their dedicated owners to avoid new top-level maps.
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

    async def await_acp_prompt(self, **kwargs: object) -> None:
        """Await one ACP prompt call under a bounded timeout."""

        if self._acp_client_conn is None:
            raise RuntimeError("ACP connection is not available")
        # Real prompt execution must stay bounded or both direct and bus wait chains can
        # hang forever before unified completion fires.
        timeout = max(30, self.acp_config.startup_timeout_seconds)
        prompt_callable = cast(Callable[..., Awaitable[None]], self._acp_client_conn.prompt)
        await asyncio.wait_for(prompt_callable(**kwargs), timeout=timeout)

    def register_wait_entry(self, request_key: str) -> RuntimeWaitEntry:
        # Runtime only tracks the request_key -> future await relation. Execution-state
        # details belong to ProcessRuntimeManager.
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
        # All direct responses, normal completions, and failure completions converge here
        # so runtime keeps one completion entrypoint and inbound/state never touch wait maps.
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
            # Bus inbound no longer routes through legacy dispatcher helpers. Runtime
            # generates the request_key and sends both entry paths through one wait chain.
            message = await self.bus.consume_inbound()
            asyncio.create_task(self.dispatch_inbound(message))

    async def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        await close_runtime(self)

    async def process_direct(
        self,
        content: str,
        session_key: str = ACPDirectIdentity.SESSION_KEY.value,
        channel: str = ACPChannelName.CLI.value,
        chat_id: str = ACPDirectIdentity.CHAT_ID.value,
        media: list[str] | None = None,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress: Callable[[str], Awaitable[None]] | None = None,
    ) -> OutboundMessage:
        await self.observability_manager.start()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key)
        try:
            # Direct input first collapses scattered parameters into one runtime-level
            # object so inbound can normalize both entry paths through InboundContext.
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
            # Runtime waits only for the final result. Queueing, activation, and state
            # shutdown remain below this layer.
            return await wait_entry.done_future
        finally:
            self.remove_wait_entry(request_key)

    async def dispatch_inbound(self, message: InboundMessage) -> None:
        await self.observability_manager.start()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key)
        try:
            # Bus and direct share the same request_key, wait-entry, and completion path.
            # The only difference is direct returns while bus publishes.
            await self.inbound_manager.handle_inbound(message, request_key=request_key)
            outbound = await wait_entry.done_future
            await self.publish_final_outbound(message=message, outbound=outbound)
        except Exception as exc:
            logger.exception("ACP dispatch_inbound failed")
            await self.push_observability(
                self.new_observability_event(
                    scope=ObservabilityScopeName.RUNTIME,
                    event=ObservabilityEventName.DISPATCH_INBOUND_ERROR,
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
        # Final publish belongs to the bus path's consumer step. It stays outside
        # complete_process_request() so completion and outward delivery remain separate.
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

    async def handle_session_update(
        self, *, acp_side_session_id: str, update: ACPCallbackUpdate
    ) -> None:
        # Runtime only dispatches by owner boundary: resolve the active request from
        # acp_side_session_id, hand the update to that request state, or record it as orphaned.
        active_entry = self.process_runtime_manager.get_active_by_acp_side_session_id(
            acp_side_session_id
        )
        if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
            await self.push_observability(
                self.new_observability_event(
                    scope=ObservabilityScopeName.RUNTIME,
                    event=ObservabilityEventName.ORPHAN_SESSION_UPDATE,
                    acp_side_session_id=acp_side_session_id,
                )
            )
            return
        await active_entry.state_manager.consume_session_update(
            update,
            progress_router=active_entry.progress_router,
        )

    async def handle_permission_request(
        self,
        *,
        acp_side_session_id: str,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall,
    ) -> object:
        # Permission callbacks also avoid business decisions at runtime level. Runtime
        # only forwards the request to the active request-owned state machine.
        active_entry = self.process_runtime_manager.get_active_by_acp_side_session_id(
            acp_side_session_id
        )
        if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
            await self.push_observability(
                self.new_observability_event(
                    scope=ObservabilityScopeName.RUNTIME,
                    event=ObservabilityEventName.ORPHAN_PERMISSION_REQUEST,
                    acp_side_session_id=acp_side_session_id,
                )
            )
            return await self.permission_response(options)
        return await active_entry.state_manager.handle_permission_request(
            options=options, tool_call=tool_call
        )

    async def permission_response(self, options: list[ACPPermissionOption]) -> object:
        from acp.schema import RequestPermissionResponse

        policy = self.acp_config.permissions_policy
        if policy == ACPPermissionPolicyName.STRICT.value:
            return RequestPermissionResponse.model_validate(build_permission_cancelled_payload())
        preferred = (
            ACPPermissionKind.ALLOW_ALWAYS.value
            if policy == ACPPermissionPolicyName.TRUSTED.value
            else ACPPermissionKind.ALLOW_ONCE.value
        )
        option_id = None
        for option in options:
            kind = getattr(getattr(option, "kind", None), "value", getattr(option, "kind", None))
            if kind == preferred:
                option_id = option.option_id
                break
        if option_id is None and options:
            option_id = options[0].option_id
        if option_id is None:
            return RequestPermissionResponse.model_validate(build_permission_cancelled_payload())
        return RequestPermissionResponse.model_validate(
            build_permission_selected_payload(option_id)
        )

    async def stop_session(self, *, nanobot_side_session_key: str) -> StopResult:
        await self.ensure_sessionmap_truth_loaded()
        # `/stop` resolves identity truth from sessionmap, while active/queued execution
        # truth still belongs exclusively to ProcessRuntimeManager.
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
        if message.channel == ACPChannelName.SYSTEM.value:
            return message.chat_id if ":" in message.chat_id else f"cli:{message.chat_id}"
        return message.session_key

    async def push_observability(self, event: ObservabilityEvent) -> None:
        await self.observability_manager.push(event)

    def new_observability_event(
        self,
        *,
        scope: ObservabilityScopeName,
        event: ObservabilityEventName,
        request_key: str | None = None,
        nanobot_side_session_key: str | None = None,
        acp_side_session_id: str | None = None,
        payload: JSONMap | None = None,
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

    async def list_models_command(self, acp_side_session_id: str) -> str:
        return self.session_runtime_manager.render_models_command(
            acp_side_session_id=acp_side_session_id
        )

    async def list_agents_command(self, acp_side_session_id: str) -> str:
        return self.session_runtime_manager.render_agents_command(
            acp_side_session_id=acp_side_session_id
        )


class ACPDispatcher(ACPRuntime):
    """Compatibility class name preserved for external callers and CLI wiring."""
