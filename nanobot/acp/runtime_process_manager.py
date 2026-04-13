"""Process runtime manager for ACP request queueing and active request lifecycle."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

from nanobot.acp.dispatch_process_direct import execute_process_request
from nanobot.acp.runtime_models import ActiveProcessEntry, RequestStatus, SessionQueueState
from nanobot.acp.state import ProgressRouter, SessionStateManager, _ACPDispatchError


class ProcessRuntimeManager:
    """Owns per-session serial queues and active request indexes."""

    def __init__(self, *, runtime: Any) -> None:
        self._runtime = runtime
        self.queue_by_nanobot_side_session_key: dict[str, SessionQueueState] = {}
        self.active_by_request_key: dict[str, ActiveProcessEntry] = {}
        self.active_by_acp_side_session_id: dict[str, ActiveProcessEntry] = {}

    async def enqueue(self, process_request: Any) -> None:
        # 中文注释：真正的串行队列 owner 是 ProcessRuntimeManager，
        # inbound 只负责构造 ProcessRequest，不负责 active/queue 状态机。
        queue_state = self.queue_by_nanobot_side_session_key.setdefault(
            process_request.nanobot_side_session_key,
            SessionQueueState(nanobot_side_session_key=process_request.nanobot_side_session_key),
        )
        queue_state.queued_requests.append(process_request)
        await self._runtime.push_observability(
            self._runtime.new_observability_event(
                scope="process",
                event="request_queued",
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

        # 中文注释：/stop 的设计要求是“queued 立即丢弃，active 尝试取消”，
        # 这里先清理 queued request 并补 completion，避免 direct/bus 调用方永久等待。
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
        # 中文注释：runtime rebuild 时，manager owner 的 active/queue 必须整批失效，
        # 所有等待中的请求都要通过统一 completion 以 error 结束，不能静默丢失。
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
            # 中文注释：同一个 nanobot_side_session_key 永远串行；
            # 这里是设计里 per-session process queue 的唯一推进点。
            process_request = queue_state.queued_requests.popleft()
            await self._activate_and_run(process_request)
        queue_state.draining_task = None

    async def _activate_and_run(self, process_request: Any) -> None:
        active_entry = None
        outbound = None
        error: Exception | None = None
        try:
            # 中文注释：进入真实执行前，先 ensure ready session，
            # 再创建 request-scope state/router，并挂到 active entry 上。
            acp_side_session_id = await self._runtime.session_runtime_manager.ensure_ready_session(
                nanobot_side_session_key=process_request.nanobot_side_session_key,
                preferred_model=process_request.metadata.get("_acp_session_model"),
                preferred_agent=process_request.metadata.get("_acp_session_agent"),
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
                    scope="process",
                    event="request_active",
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
                # 中文注释：entry close 先于 runtime completion，
                # 这样 state/router/pending permission 等 request-scope 资源先完成收尾，
                # completion 阶段只保留 request_key + outbound/error 这种最小参数。
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
                        scope="process",
                        event="request_finished",
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
