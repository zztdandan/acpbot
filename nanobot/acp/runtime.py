"""ACP 运行时核心引擎：连接管理、请求编排、等待链、会话映射、权限决策、可观测性。"""

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
    RequestSource,
    RequestStatus,
    RuntimeWaitEntry,
    StopResult,
)
from nanobot.acp.runtime_process_manager import ProcessRuntimeManager
from nanobot.acp.sessionmap import SessionMapBindingManager, SessionRuntimeManager
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig

# TYPE_CHECKING 块：避免运行时导入 ACP SDK 类型（减少启动开销）
if TYPE_CHECKING:
    from acp.client import ClientSideConnection

    from nanobot.acp.runtime_client import _NanobotACPClient
else:
    # 运行时用 object 占位，实际类型通过 cast 在使用点标注
    ClientSideConnection = object
    _NanobotACPClient = object


class ACPRuntime:
    """ACP 运行时主引擎：编排连接生命周期、请求等待链、会话映射与可观测性。

    职责：
        - 连接管理：ensure/reset/close，由 _acp_connection_lock 保护并发重建；
        - 请求等待链：register_wait_entry ↔ complete_process_request，Future 驱动异步结果；
        - 会话映射：sessionmap 双向绑定（nanobot_key ↔ acp_session_id），持久化 truths；
        - 可观测性：结构化事件上报（ObservabilityManager）。

    管理器组件：
        - sessionmap_binding_manager — nanobot_key ↔ acp_session_id 绑定与持久化；
        - session_runtime_manager — 会话级状态（模型/代理列表、capability）；
        - process_runtime_manager — 请求队列、活跃请求双索引、/stop 与 rebuild；
        - inbound_manager — 入站消息路由（process_direct / dispatch_inbound）。
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        workspace: Path,
        acp_config: ACPBackendConfig,
        channels_config: ChannelsConfig | None = None,
    ) -> None:
        """初始化运行时全部状态：连接句柄、等待链索引、管理器组件。"""
        # ===== 基础配置 =====
        self.bus = bus
        self.workspace = workspace
        # 说明：ACP backend 的标准工作目录优先取 acp_config.cwd；workspace 仅作兼容兜底。
        self.acp_config = acp_config
        self.channels_config = channels_config

        # ===== 连接状态（由 runtime_lifecycle.py 管理）=====
        self._running = False  # 运行时运行标志（run/stop 控制）
        self._acp_client_connection_cm: (
            AsyncContextManager[tuple[ClientSideConnection, asyncio.subprocess.Process]] | None
        ) = None  # ACP 连接上下文管理器
        self._acp_client_conn: ClientSideConnection | None = None  # ACP 客户端连接
        self._acp_agent_process: asyncio.subprocess.Process | None = None  # ACP Agent 子进程
        self._acp_connection_lock = asyncio.Lock()  # 连接操作互斥锁

        # ===== 回调客户端（接收 ACP callback）=====
        self.acp_callback_client: _NanobotACPClient | None = None

        # ===== 请求等待链（核心异步模式）=====
        self._wait_by_request_key: dict[str, RuntimeWaitEntry] = {}
        # 设计说明：每个请求生成唯一 request_key，注册一个 Future 等待结果。
        # 当 ACP 处理完成后，通过 complete_process_request 设置 Future 结果，
        # 调用方通过 await wait_entry.done_future 获取最终响应。

        # ===== 管理器组件（职责分离）=====
        self.observability_manager = ObservabilityManager()
        # 可观测性：上报错误、状态变更、生命周期事件
        self._observability_start_lock = asyncio.Lock()

        self.sessionmap_binding_manager = SessionMapBindingManager(self)
        # 会话绑定：维护 nanobot_side_session_key <-> acp_side_session_id 映射

        self.session_runtime_manager = SessionRuntimeManager(
            runtime=self,
            binding_manager=self.sessionmap_binding_manager,
        )
        # 会话运行时：渲染模型/代理列表、管理会话级状态

        self.process_runtime_manager = ProcessRuntimeManager(runtime=self)
        # 进程运行时：跟踪活跃请求、管理请求队列、处理 /stop

        self.inbound_manager = InboundManager(runtime=self)
        # 入站处理：处理 process_direct 和 dispatch_inbound 请求

    def resolve_acp_workspace_path(self) -> Path:
        """ACP 工作目录统一解析入口（acp_config.cwd 优先，workspace 兜底）。

        设计约束：
            - acp_config.cwd 是 ACP workspace 的单一配置入口；
            - 其他子模块不得各自解析 cwd，统一通过本方法获取。
        """

        if self.acp_config.cwd:
            return Path(self.acp_config.cwd).expanduser().resolve()
        return self.workspace.resolve()

    def new_request_key(self) -> str:
        """生成全局唯一请求键（格式 acp:{uuid4_hex}），用于等待链索引与可观测性追踪。"""
        return f"acp:{uuid4().hex}"

    async def ensure_observability_started(self) -> None:
        """幂等启动可观测消费循环，避免 run/process_direct 并发启动竞争。"""

        async with self._observability_start_lock:
            await self.observability_manager.start()

    async def await_acp_prompt(self, **kwargs: object) -> None:
        """ACP 连接健康检查：调用 prompt 并等待响应，用于 ensure_connection 探测 Agent 就绪。

        处理流程：
            - 检查 _acp_client_conn 可用性（否则 RuntimeError）；
            - 以 max(30, startup_timeout_seconds) 为上限 asyncio.wait_for 包装 prompt 调用。
        """
        if self._acp_client_conn is None:
            raise RuntimeError("ACP connection is not available")
        timeout = max(30, self.acp_config.startup_timeout_seconds)
        prompt_callable = cast(Callable[..., Awaitable[None]], self._acp_client_conn.prompt)
        await asyncio.wait_for(prompt_callable(**kwargs), timeout=timeout)

    def register_wait_entry(
        self,
        request_key: str,
        *,
        source: RequestSource = "direct",
    ) -> RuntimeWaitEntry:
        """为请求创建 Future 并注册到等待链，返回 wait_entry 供调用方 await。

        使用模式：
            wait_entry = runtime.register_wait_key(key)
            try:
                result = await wait_entry.done_future
            finally:
                runtime.remove_wait_key(key)
        """
        wait_entry = RuntimeWaitEntry(
            request_key=request_key,
            done_future=asyncio.get_running_loop().create_future(),
            source=source,
        )
        self._wait_by_request_key[request_key] = wait_entry
        return wait_entry

    def attach_wait_entry_task(self, request_key: str, task: asyncio.Task[None]) -> None:
        """将 bus dispatch task 绑定到 wait_entry，便于 close/reset 统一取消回收。"""

        wait_entry = self._wait_by_request_key.get(request_key)
        if wait_entry is None:
            return
        wait_entry.dispatch_task = task

    def remove_wait_entry(self, request_key: str) -> None:
        """将请求从等待链移除（pop 含 Key 不存在时的静默处理），通常在 finally 中调用。"""
        self._wait_by_request_key.pop(request_key, None)

    def fail_all_wait_entries(self, error: Exception) -> None:
        """批量失败所有未完成的 wait_entry（运行时关闭或连接断开时调用）。

        处理流程：
            - 遍历 _wait_by_request_key，对未 done 的 Future 设置异常；
            - 不移除条目，调用方需后绕 cleanup。
        """
        for wait_entry in self._wait_by_request_key.values():
            if not wait_entry.done_future.done():
                wait_entry.done_future.set_exception(error)

    async def cancel_all_wait_entry_tasks(self) -> None:
        """取消并等待所有挂在 wait_entry 上的 dispatch task 收敛。"""

        tasks: list[asyncio.Task[None]] = []
        for wait_entry in self._wait_by_request_key.values():
            task = wait_entry.dispatch_task
            if task is None or task.done():
                continue
            task.cancel()
            tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def clear_all_wait_entries(self) -> None:
        """清空 wait_entry 索引；用于 close/reset 的最终收口。"""

        self._wait_by_request_key.clear()

    async def complete_process_request(
        self,
        request_key: str,
        *,
        outbound: OutboundMessage | None = None,
        error: Exception | None = None,
    ) -> None:
        """等待链终点：设置对应 request_key 的 Future 结果（error 优先于 outbound），幂等。"""
        wait_entry = self._wait_by_request_key.get(request_key)
        # 防御性检查：如果条目不存在或已完成，直接返回（幂等）
        if wait_entry is None or wait_entry.done_future.done():
            return
        # 优先处理错误情况
        if error is not None:
            wait_entry.done_future.set_exception(error)
            return
        # 防御性检查：确保有 outbound 消息
        if outbound is None:
            wait_entry.done_future.set_exception(RuntimeError("request completed without outbound"))
            return
        # 设置成功结果
        wait_entry.done_future.set_result(outbound)

    async def ensure_connection(self) -> None:
        """确保 ACP 连接可用（已连接直接返回，否则建立新连接），委托 runtime_lifecycle 实现。"""
        await ensure_connection(self)

    async def reset_connection(self) -> None:
        """强制关闭现有连接并重建，用于异常恢复或配置变更后重连。"""
        await reset_connection(self)

    async def run(self) -> None:
        """启动运行时主循环：ensure_connection → observability.start → 持续消费总线并 create_task 分发。"""
        self._running = True
        await self.ensure_connection()
        await self.ensure_observability_started()
        while self._running:
            message = await self.bus.consume_inbound()
            # 先注册 wait_entry，再创建 dispatch task，避免 task 抢跑导致的注册竞态。
            request_key = self.new_request_key()
            wait_entry = self.register_wait_entry(request_key, source="bus")
            task = asyncio.create_task(
                self.dispatch_inbound(
                    message,
                    request_key=request_key,
                    wait_entry=wait_entry,
                )
            )
            self.attach_wait_entry_task(request_key, task)

    async def stop(self) -> None:
        """置 _running=False 请求主循环退出（异步，不等待完成）。"""
        self._running = False

    async def close(self) -> None:
        """关闭运行时并释放全部资源（连接、进程、wait_entry、可观测性），委托 runtime_lifecycle 实现。"""
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
        """CLI/直接调用主入口：构造请求、注册等待链、委托 inbound_manager 并 await 最终结果。

        处理流程：
            - 启动可观测性 → new_request_key → register_wait_entry；
            - 将 preferred_model/agent 注入 metadata，构造 ProcessDirectInput；
            - 委托 inbound_manager.handle_process_direct 发至 ACP；
            - await wait_entry.done_future 获取最终 OutboundMessage；
            - finally 中 remove_wait_entry。"""
        await self.ensure_observability_started()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key, source="direct")
        try:
            # 构造 ProcessDirectInput 对象（封装所有请求参数）
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
            # 委托给 inbound_manager 处理（实际发送到 ACP）
            await self.inbound_manager.handle_process_direct(input, request_key=request_key)
            # 异步等待最终结果（阻塞直到 ACP 处理完成）
            return await wait_entry.done_future
        finally:
            # 清理等待条目（无论成功或失败都执行）
            self.remove_wait_entry(request_key)

    async def dispatch_inbound(
        self,
        message: InboundMessage,
        *,
        request_key: str,
        wait_entry: RuntimeWaitEntry,
    ) -> None:
        """总线入站消息处理器（run 主循环调用）：等待链 + inbound_manager + publish_final_outbound。

        处理流程：
            - 启动可观测性 → register_wait_entry → inbound_manager.handle_inbound；
            - await done_future 后 publish_final_outbound 路由回原始频道；
            - 异常全捕获 + logger.exception + 上报 DISPATCH_INBOUND_ERROR（不中断主循环）。
        """
        try:
            # 委托给 inbound_manager 处理（实际发送到 ACP）
            await self.inbound_manager.handle_inbound(message, request_key=request_key)
            # 等待 ACP 处理完成
            outbound = await wait_entry.done_future
            # 发布最终结果到总线（路由回原始频道）
            await self.publish_final_outbound(message=message, outbound=outbound)
        except asyncio.CancelledError:
            # close/reset 主动取消时不视为业务错误，直接退出。
            return
        except Exception as exc:
            # 记录异常堆栈（便于调试）
            logger.exception("ACP dispatch_inbound failed")
            # 上报结构化观测事件
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
            # 清理等待条目（无论成功或失败都执行）
            self.remove_wait_entry(request_key)

    async def publish_final_outbound(
        self, *, message: InboundMessage, outbound: OutboundMessage
    ) -> None:
        """将 ACP 结果路由回原始频道：合并 metadata + publish_outbound。

        元数据合并策略：
            - 以 message.metadata 为基础，outbound.metadata 覆盖追加。
        """
        # 元数据合并：保留原始上下文，追加 ACP 响应元数据
        final_metadata = dict(message.metadata or {})
        final_metadata.update(outbound.metadata or {})
        # 发布到总线（由总线负责路由到具体频道）
        await self.bus.publish_outbound(
            OutboundMessage(
                channel=message.channel,  # 路由回原始频道
                chat_id=message.chat_id,  # 路由回原始聊天
                content=outbound.content,  # ACP 生成的响应内容
                media=list(outbound.media),  # ACP 返回的媒体文件
                metadata=final_metadata,  # 合并后的元数据
            )
        )

    async def global_publish_progress_outbound(
        self,
        *,
        outbound: OutboundMessage,
        request_key: str | None = None,
        drop_if_inactive: bool = False,
    ) -> None:
        """发布 progress outbound 到总线；可按需在 runtime 边界丢弃已失活请求的旁路消息。"""

        if drop_if_inactive and request_key is not None:
            active_entry = self.process_runtime_manager.active_by_request_key.get(request_key)
            if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
                # reuqestkey已经不在执行范围
                return
        # 只做 outbound发布，on_progress任务已owner到 state 完成
        await self.bus.publish_outbound(outbound)

    async def handle_session_update(
        self, *, acp_side_session_id: str, update: ACPCallbackUpdate
    ) -> None:
        """ACP 会话更新回调入口（progress/tool_call/response），委托 state_manager 消费。

        处理流程：
            - 按 acp_side_session_id 查找活跃请求，不存在或非 ACTIVE 则上报 ORPHAN_SESSION_UPDATE；
            - 正常路径委托给 active_entry.state_manager.consume_session_update。
        """
        # 查找活跃请求条目
        active_entry = self.process_runtime_manager.get_active_by_acp_side_session_id(
            acp_side_session_id
        )
        # 检查是否找到且状态为 ACTIVE
        if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
            # 孤立更新：上报观测事件（不抛出异常）
            await self.push_observability(
                self.new_observability_event(
                    scope=ObservabilityScopeName.RUNTIME,
                    event=ObservabilityEventName.ORPHAN_SESSION_UPDATE,
                    acp_side_session_id=acp_side_session_id,
                )
            )
            return
        # 正常更新：委托给 state_manager 处理
        await active_entry.state_manager.consume_session_update(update)

    async def handle_permission_request(
        self,
        *,
        acp_side_session_id: str,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall,
    ) -> object:
        """ACP 工具调用授权回调：按策略自动决策 ALLOW/DENY。

        处理流程：
            - 按 acp_side_session_id 查找活跃请求，不存在则上报 ORPHAN 并走默认策略；
            - 正常路径委托 progress_router.handle_permission_request。

        权限策略（acp_config.permissions_policy）：
            - STRICT: 全部拒绝 cancelled；
            - TRUSTED: 全部批准 ALLOW_ALWAYS；
            - DEFAULT: 单次批准 ALLOW_ONCE。
        """
        # 查找活跃请求条目
        active_entry = self.process_runtime_manager.get_active_by_acp_side_session_id(
            acp_side_session_id
        )
        if active_entry is None or active_entry.status != RequestStatus.ACTIVE:
            # 孤立权限请求：上报观测事件，使用默认策略响应
            await self.push_observability(
                self.new_observability_event(
                    scope=ObservabilityScopeName.RUNTIME,
                    event=ObservabilityEventName.ORPHAN_PERMISSION_REQUEST,
                    acp_side_session_id=acp_side_session_id,
                )
            )
            # 使用默认策略响应（不委托给 state_manager）
            return await self.permission_response(options)
        # 正常权限请求：委托给 progress router 处理权限子域等待链
        return await active_entry.progress_router.handle_permission_request(
            options=options, tool_call=tool_call
        )

    async def permission_response(self, options: list[ACPPermissionOption]) -> object:
        """根据权限策略自动生成权限响应（STRICT→cancelled / TRUSTED→ALLOW_ALWAYS / DEFAULT→ALLOW_ONCE）。"""
        from acp.schema import RequestPermissionResponse

        policy = self.acp_config.permissions_policy
        # STRICT 模式：总是拒绝
        if policy == ACPPermissionPolicyName.STRICT.value:
            return RequestPermissionResponse.model_validate(build_permission_cancelled_payload())

        # TRUSTED/DEFAULT 模式：确定首选 kind
        preferred = (
            ACPPermissionKind.ALLOW_ALWAYS.value
            if policy == ACPPermissionPolicyName.TRUSTED.value
            else ACPPermissionKind.ALLOW_ONCE.value
        )

        # 遍历 options，查找匹配的 kind
        option_id = None
        for option in options:
            # 兼容不同属性访问方式（kind 可能是枚举或字符串）
            kind = getattr(getattr(option, "kind", None), "value", getattr(option, "kind", None))
            if kind == preferred:
                option_id = option.option_id
                break

        # 回退策略：如果找不到首选 kind，使用第一个选项
        if option_id is None and options:
            option_id = options[0].option_id

        # 如果 options 为空，返回 cancelled
        if option_id is None:
            return RequestPermissionResponse.model_validate(build_permission_cancelled_payload())

        # 返回选中的选项
        return RequestPermissionResponse.model_validate(
            build_permission_selected_payload(option_id)
        )

    async def stop_session(self, *, nanobot_side_session_key: str) -> StopResult:
        """下发 /stop 到指定会话：加载 sessionmap 真相 → 解析 ACP 侧 session_id → 委托 process_runtime_manager 停止活跃与排队请求。"""
        # 步骤 1: 加载 sessionmap 持久化真相
        await self.ensure_sessionmap_truth_loaded()
        # 步骤 2: 解析 ACP 侧会话 ID
        acp_side_session_id = self.sessionmap_binding_manager.resolve_session_id(
            nanobot_side_session_key
        )
        # 步骤 3: 委托给 process_runtime_manager 处理停止逻辑
        (
            active_cancel_requested,
            dropped_queued_count,
        ) = await self.process_runtime_manager.stop_session(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )
        # 步骤 4: 构造并返回 StopResult
        return StopResult(
            nanobot_side_session_key=nanobot_side_session_key,
            active_cancel_requested=active_cancel_requested,
            dropped_queued_count=dropped_queued_count,
            acp_side_session_id=acp_side_session_id,
        )

    def drop_session_binding_and_runtime_entry(
        self, *, nanobot_side_session_key: str
    ) -> str | None:
        """完全清理会话本地状态：drop runtime entry + clear binding，返回被删除的 ACP 侧 session_id。

        注意：不停止活跃请求（需先 stop_session），不通知 ACP 侧。
        """
        # 清理会话运行时状态
        self.session_runtime_manager.drop_ready_session(
            nanobot_side_session_key=nanobot_side_session_key,
        )
        # 清理会话绑定映射，返回被删除的 ACP 侧会话 ID
        return self.sessionmap_binding_manager.clear_binding(nanobot_side_session_key)

    async def ensure_sessionmap_truth_loaded(self) -> None:
        """确保 sessionmap 持久化真相已加载（幂等），委托 binding_manager.load_persistent_truth。"""
        await self.sessionmap_binding_manager.load_persistent_truth()

    def resolve_nanobot_side_session_key(self, message: InboundMessage) -> str:
        """从 InboundMessage 解析 nanobot 侧会话键：system 频道按 chat_id 含冒号判断，其他频道路由用 session_key。"""
        if message.channel == ACPChannelName.SYSTEM.value:
            # SYSTEM 频道：chat_id 可能是会话键（如果包含 ":"）
            return message.chat_id if ":" in message.chat_id else f"cli:{message.chat_id}"
        # 其他频道：直接使用 session_key
        return message.session_key

    async def push_observability(self, event: ObservabilityEvent) -> None:
        """上报结构化观测事件（委托 observability_manager.push）。"""
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
        """构造 ObservabilityEvent 辅助方法（填充 scope/event/request_key/session_key/payload）。"""
        return ObservabilityEvent(
            scope=scope,
            event=event,
            request_key=request_key,
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            payload=dict(payload or {}),  # 确保 payload 是字典（避免 None）
        )

    @staticmethod
    def new_outbound_message(*, channel: str, chat_id: str, content: str) -> OutboundMessage:
        """构造基础 OutboundMessage 辅助方法（media/metadata 为空）。"""
        return OutboundMessage(channel=channel, chat_id=chat_id, content=content)

    async def list_models_command(self, acp_side_session_id: str) -> str:
        """渲染 /models 命令结果：委托 session capability 输出模型列表。"""
        caps = self.session_runtime_manager.get_session_capabilities(acp_side_session_id)
        if caps is None:
            return "No model catalog returned by current ACP backend for this session."
        return caps.render_models_command()

    async def list_agents_command(self, acp_side_session_id: str) -> str:
        """渲染 /agents 命令结果：委托 session capability 输出代理列表。"""
        caps = self.session_runtime_manager.get_session_capabilities(acp_side_session_id)
        if caps is None:
            return "No agent/mode catalog returned by current ACP backend for this session."
        return caps.render_agents_command()


class ACPDispatcher(ACPRuntime):
    """ACPRuntime 的语义子类，用于 create_dispatch_runtime 中区分 ACP 后端类型（当前空实现，预留扩展点）。"""
