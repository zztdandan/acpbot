"""Process runtime manager for ACP request queueing and active request lifecycle."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING

from nanobot.acp.contracts import ObservabilityEventName, ObservabilityScopeName
from nanobot.acp.dispatch_process_direct import execute_process_request
from nanobot.acp.runtime_models import (
    ActiveProcessEntry,
    ProcessRequest,
    RequestStatus,
    SessionQueueState,
)
from nanobot.acp.state import ProgressRouter, SessionStateManager, _ACPDispatchError
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


class ProcessRuntimeManager:
    """Owns per-session serial queues and active request indexes."""

    def __init__(self, *, runtime: ACPRuntime) -> None:
        self._runtime = runtime
        self.queue_by_nanobot_side_session_key: dict[str, SessionQueueState] = {}
        self.active_by_request_key: dict[str, ActiveProcessEntry] = {}
        self.active_by_acp_side_session_id: dict[str, ActiveProcessEntry] = {}

    async def enqueue(self, process_request: ProcessRequest) -> None:
        # ProcessRuntimeManager owns the real serial queue. Inbound only assembles
        # ProcessRequest objects and does not own the active/queue state machine.
        queue_state = self.queue_by_nanobot_side_session_key.setdefault(
            process_request.nanobot_side_session_key,
            SessionQueueState(nanobot_side_session_key=process_request.nanobot_side_session_key),
        )
        queue_state.queued_requests.append(process_request)
        await self._runtime.push_observability(
            self._runtime.new_observability_event(
                scope=ObservabilityScopeName.PROCESS,
                event=ObservabilityEventName.REQUEST_QUEUED,
                request_key=process_request.request_key,
                nanobot_side_session_key=process_request.nanobot_side_session_key,
            )
        )
        if queue_state.draining_task is None or queue_state.draining_task.done():
            queue_state.draining_task = asyncio.create_task(self._drain_session_queue(queue_state))

    def get_active_by_acp_side_session_id(
        self, acp_side_session_id: str
    ) -> ActiveProcessEntry | None:
        return self.active_by_acp_side_session_id.get(acp_side_session_id)

    def find_request_waiting_permission(
        self, *, nanobot_side_session_key: str
    ) -> ActiveProcessEntry | None:
        queue_state = self.queue_by_nanobot_side_session_key.get(nanobot_side_session_key)
        if queue_state is None:
            return None
        for active_entry in self.active_by_request_key.values():
            if active_entry.nanobot_side_session_key != nanobot_side_session_key:
                continue
            if active_entry.state_manager.has_pending_permission():
                return active_entry
        return None

    async def stop_session(
        self, *, nanobot_side_session_key: str, acp_side_session_id: str | None
    ) -> tuple[bool, int]:
        """Drop queued requests and try to stop the active ACP session if present."""

        # `/stop` semantics are "drop queued immediately, try to cancel active". Clear
        # queued requests first and complete them so direct/bus callers never wait forever.
        active_cancel_requested = False
        dropped_queued_count = 0
        queue_state = self.queue_by_nanobot_side_session_key.get(nanobot_side_session_key)
        if queue_state is not None:
            dropped_queued_count = len(queue_state.queued_requests)
            while queue_state.queued_requests:
                dropped_request = queue_state.queued_requests.popleft()
                await self._runtime.complete_process_request(
                    dropped_request.request_key,
                    error=RuntimeError("ACP request dropped by /stop"),
                )
            queue_state.queued_requests = deque()
        active_entry = None
        if acp_side_session_id is not None:
            active_entry = self.active_by_acp_side_session_id.get(acp_side_session_id)
        if active_entry is not None and self._runtime._acp_client_conn is not None:
            cancel_session = getattr(self._runtime._acp_client_conn, "cancel", None)
            if callable(cancel_session):
                try:
                    result = cancel_session(session_id=acp_side_session_id)
                    if asyncio.iscoroutine(result):
                        await result
                    active_cancel_requested = True
                except Exception:
                    active_cancel_requested = False
        return active_cancel_requested, dropped_queued_count

    async def rebuild(self, *, error: Exception) -> None:
        # Runtime rebuild invalidates every active and queued request owned here. All
        # waiters must terminate through unified completion instead of silently vanishing.
        for active_entry in list(self.active_by_request_key.values()):
            try:
                await active_entry.close()
            except Exception:
                pass
            await self._runtime.complete_process_request(active_entry.request_key, error=error)
        for queue_state in self.queue_by_nanobot_side_session_key.values():
            while queue_state.queued_requests:
                queued_request = queue_state.queued_requests.popleft()
                await self._runtime.complete_process_request(
                    queued_request.request_key, error=error
                )
            if queue_state.draining_task is not None:
                queue_state.draining_task.cancel()
        self.queue_by_nanobot_side_session_key.clear()
        self.active_by_request_key.clear()
        self.active_by_acp_side_session_id.clear()

    async def _drain_session_queue(self, queue_state: SessionQueueState) -> None:
        while queue_state.queued_requests:
            # A single nanobot_side_session_key always runs serially. This loop is the
            # only place allowed to advance the per-session process queue.
            process_request = queue_state.queued_requests.popleft()
            await self._activate_and_run(process_request)
        queue_state.draining_task = None

    async def _activate_and_run(self, process_request: ProcessRequest) -> None:
        active_entry = None
        outbound: OutboundMessage | None = None
        error: Exception | None = None
        try:
            # Ensure the ready session before real execution, then create the request-
            # scoped state/router pair and attach them to the active entry.
            preferred_model = process_request.metadata.get("_acp_session_model")
            preferred_agent = process_request.metadata.get("_acp_session_agent")
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=process_request.nanobot_side_session_key,
                preferred_model=preferred_model if isinstance(preferred_model, str) else None,
                preferred_agent=preferred_agent if isinstance(preferred_agent, str) else None,
            )
            state_manager = SessionStateManager(
                runtime=self._runtime,
                request_key=process_request.request_key,
                nanobot_side_session_key=process_request.nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
                channel=process_request.channel,
                chat_id=process_request.chat_id,
                on_progress=process_request.on_progress,
            )
            progress_router = ProgressRouter(state_manager=state_manager)
            state_manager.bind_progress_router(progress_router)
            active_entry = ActiveProcessEntry(
                request_key=process_request.request_key,
                nanobot_side_session_key=process_request.nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
                process_request=process_request,
                state_manager=state_manager,
                progress_router=progress_router,
            )
            self.active_by_request_key[active_entry.request_key] = active_entry
            self.active_by_acp_side_session_id[active_entry.acp_side_session_id] = active_entry
            active_entry.status = RequestStatus.ACTIVE
            await self._runtime.push_observability(
                self._runtime.new_observability_event(
                    scope=ObservabilityScopeName.PROCESS,
                    event=ObservabilityEventName.REQUEST_ACTIVE,
                    request_key=active_entry.request_key,
                    nanobot_side_session_key=active_entry.nanobot_side_session_key,
                    acp_side_session_id=active_entry.acp_side_session_id,
                )
            )
            outbound = await execute_process_request(self._runtime, active_entry)
            active_entry.status = RequestStatus.COMPLETED
        except _ACPDispatchError as exc:
            if active_entry is not None:
                active_entry.status = RequestStatus.FAILED
            if exc.partial_response:
                if active_entry is not None:
                    outbound = active_entry.state_manager.materialize_final_outbound(partial=True)
            else:
                error = exc
        except Exception as exc:
            if active_entry is not None:
                active_entry.status = RequestStatus.FAILED
            error = exc
        finally:
            if active_entry is not None:
                # Close the request-owned resources before runtime completion so
                # completion itself only carries the minimal request_key + outbound/error.
                active_entry.status = RequestStatus.FINISHING
                try:
                    await active_entry.close()
                except Exception as close_exc:
                    if error is None:
                        error = close_exc
                self.active_by_request_key.pop(active_entry.request_key, None)
                self.active_by_acp_side_session_id.pop(active_entry.acp_side_session_id, None)
                await self._runtime.push_observability(
                    self._runtime.new_observability_event(
                        scope=ObservabilityScopeName.PROCESS,
                        event=ObservabilityEventName.REQUEST_FINISHED,
                        request_key=active_entry.request_key,
                        nanobot_side_session_key=active_entry.nanobot_side_session_key,
                        acp_side_session_id=active_entry.acp_side_session_id,
                        payload={"status": active_entry.status.value},
                    )
                )
            await self._runtime.complete_process_request(
                process_request.request_key,
                outbound=outbound,
                error=error,
            )
