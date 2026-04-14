"""ACP 请求处理级运行时编排：会话队列入队、单请求激活与执行、停止与重建。

核心类：
    - ProcessRuntimeManager — 管理每会话串行队列，激活单请求并驱动 process_direct 执行主链。

设计约束：
    - 同一 nanobot_side_session_key 下的请求严格串行（由 draining_task 保证）；
    - 活跃请求同时维护 request_key 与 acp_side_session_id 双索引；
    - rebuild 时主动清理所有活跃/排队请求并向上游报告错误。
"""

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
    """ACP 请求处理编排器：管理会话串行队列、单请求激活/执行/清理全生命周期。

    职责：
        - 请求入队并按会话键串行调度（enqueue → _drain_session_queue → _activate_and_run）；
        - 维护活跃请求双索引（request_key / acp_side_session_id），用于停止和权限查询；
        - 错误场景下的批量重建（rebuild）与 /stop 请求清理。

    生命周期：
        - 随 ACPRuntime 创建，与 runtime 同生命周期；
        - rebuild 会清空全部内部状态并重新开始。
    """

    def __init__(self, *, runtime: ACPRuntime) -> None:
        """绑定上游 runtime 并初始化会话队列与活跃请求的三个索引表。"""
        self._runtime = runtime
        self.queue_by_nanobot_side_session_key: dict[str, SessionQueueState] = {}
        self.active_by_request_key: dict[str, ActiveProcessEntry] = {}
        self.active_by_acp_side_session_id: dict[str, ActiveProcessEntry] = {}

    async def enqueue(self, process_request: ProcessRequest) -> None:
        """将请求追加到所属会话队列；若当前无消费协程则启动 draining_task。

        处理流程：
            - 按 nanobot_side_session_key 获取或创建 SessionQueueState；
            - 追加请求到 queued_requests 并上报 REQUEST_QUEUED；
            - 若 draining_task 为空或已完成，创建新的 _drain_session_queue 协程。
        """
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
        """按 ACP 协议侧会话 ID 查询当前活跃请求，未找到返回 None。"""
        return self.active_by_acp_side_session_id.get(acp_side_session_id)

    def find_request_waiting_permission(
        self, *, nanobot_side_session_key: str
    ) -> ActiveProcessEntry | None:
        """在指定会话下查找第一个处于 permission_pending 状态的活跃请求。

        处理流程：
            - 按会话键定位队列状态，不存在则返回 None；
            - 遍历 active_by_request_key，匹配会话键且有 pending permission 的条目。
        """
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
        """停止指定会话的全部排队请求，并尝试取消当前活跃请求。

        处理流程：
            - 从队列中逐个弹出到 drop 的请求，调用 complete_process_request 报告丢弃；
            - 若有 acp_side_session_id，调用 ACP SDK cancel 尝试取消正在执行的请求；
            - 返回 (是否已发送取消指令, 被丢弃的排队请求数)。
        """

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
        """错误恢复重建：关闭全部活跃请求、清空所有队列并重置内部索引。

        处理流程：
            - 遍历 active_by_request_key，逐个 close 并向上游报告错误；
            - 遍历所有 SessionQueueState，弹出排队请求并报告错误，取消 draining_task；
            - 清空三个索引表（queue / active_by_request / active_by_acp_session）。
        """
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
        """串行消费单个会话队列中的所有请求，消费完毕后将 draining_task 置 None。

        处理流程：
            - 循环弹出 queued_requests，逐个调用 _activate_and_run；
            - 队列清空后将 draining_task 置 None，允许 enqueue 重新创建。
        """
        while queue_state.queued_requests:
            process_request = queue_state.queued_requests.popleft()
            await self._activate_and_run(process_request)
        queue_state.draining_task = None

    async def _activate_and_run(self, process_request: ProcessRequest) -> None:
        """单请求全生命周期：激活会话 → 构建状态 → 执行 process_direct → 清理回收。

        处理流程：
            - 从 metadata 提取 preferred_model/agent，调用 ensure_ready_session 激活会话；
            - 创建 SessionStateManager + ProgressRouter，构建 ActiveProcessEntry 并注册双索引；
            - 状态置 ACTIVE，上报 REQUEST_ACTIVE 可观测事件；
            - 调用 execute_process_request 驱动 process_direct 执行链路。

        异常处理：
            - _ACPDispatchError：检查 partial_response 尝试返回部分结果，否则作为错误上报；
            - 通用异常：标记 FAILED 并上报。

        清理（finally）：
            - 状态置 FINISHING，close ActiveProcessEntry 释放资源；
            - 从双索引中移除，上报 REQUEST_FINISHED；
            - 调用 complete_process_request 向上游报告最终结果或错误。
        """
        active_entry = None
        outbound: OutboundMessage | None = None
        error: Exception | None = None
        try:
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
