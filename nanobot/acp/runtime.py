"""ACP 运行时核心引擎。

本模块是 ACP（Agent Communication Protocol）运行时的大脑，负责：
1. 连接管理：与 ACP Agent 进程建立/维护/重置连接
2. 请求编排：处理直接请求（process_direct）和总线入站请求（dispatch_inbound）
3. 等待链：为每个请求注册等待条目，异步等待最终响应
4. 会话映射：维护 nanobot 侧会话键与 ACP 侧会话 ID 的绑定关系
5. 权限决策：根据策略（STRICT/TRUSTED/DEFAULT）自动响应权限请求
6. 可观测性：上报结构化观测事件（错误/状态变更/生命周期事件）

架构位置：
    ACPRuntime 位于 nanobot 总线与 ACP 协议层之间：
    - 下行：接收 InboundMessage，委托给 ACP Agent 处理
    - 上行：接收 ACP callback，发布 OutboundMessage 到总线
    - 侧行：管理 sessionmap binding、process runtime state、observability

关键设计模式：
    - Request-Response 异步等待：每个请求生成唯一 request_key，注册 Future 等待结果
    - 连接单例：整个运行时共享一个 ACP 连接（通过 ensure_connection 保证）
    - 管理器分层：将职责拆分为多个 Manager（sessionmap/process/inbound/observability）
"""

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

# TYPE_CHECKING 块：避免运行时导入 ACP SDK 类型（减少启动开销）
if TYPE_CHECKING:
    from acp.client import ClientSideConnection

    from nanobot.acp.runtime_client import _NanobotACPClient
else:
    # 运行时用 object 占位，实际类型通过 cast 在使用点标注
    ClientSideConnection = object
    _NanobotACPClient = object


class ACPRuntime:
    """ACP 运行时主引擎：编排连接、请求、会话与可观测性。

    核心职责：
        1. 连接生命周期：管理 ACP Client 连接的建立、重置、关闭
        2. 请求等待链：为每个请求注册 Future，异步等待最终响应
        3. 入站路由：将 InboundMessage 委托给 inbound_manager 处理
        4. 会话绑定：维护 nanobot 侧会话键与 ACP 侧会话 ID 的映射
        5. 权限决策：根据配置策略自动响应 ACP 权限请求
        6. 观测事件：上报结构化事件到 observability_manager

    线程安全：
        - _acp_connection_lock: 保证并发请求不会同时重建连接
        - _wait_by_request_key: dict 操作在单线程 asyncio 中安全（无锁）

    使用示例：
        runtime = ACPRuntime(bus=bus, workspace=Path("."), acp_config=acp_config)
        await runtime.run()  # 启动入站消费循环
        result = await runtime.process_direct("Hello")  # 发送直接请求
    """

    def __init__(
        self,
        *,
        bus: MessageBus,
        workspace: Path,
        acp_config: ACPBackendConfig,
        channels_config: ChannelsConfig | None = None,
    ) -> None:
        """初始化 ACPRuntime 实例。

        参数：
            bus: 消息总线实例（用于 inbound/outbound 消息路由）
            workspace: 工作区路径（ACP Agent 进程的工作目录）
            acp_config: ACP 后端配置（心跳、权限策略、启动超时等）
            channels_config: 频道配置（可选，用于 channel 路由）

        初始化状态分组：
            【连接状态】
                - _acp_client_connection_cm: ACP 连接上下文管理器（生命周期管理）
                - _acp_client_conn: ACP 客户端连接对象（实际通信句柄）
                - _acp_agent_process: ACP Agent 子进程句柄（stdio 传输层）
                - _acp_connection_lock: 连接操作互斥锁（防止并发重建）

            【回调客户端】
                - acp_callback_client: 接收 ACP callback 的本地服务器（可选）

            【请求等待链】
                - _wait_by_request_key: dict[request_key -> RuntimeWaitEntry]
                  每个请求注册一个 Future，用于异步等待最终响应

            【管理器组件】（职责分离）
                - observability_manager: 观测事件上报
                - sessionmap_binding_manager: 会话绑定映射（nanobot_key <-> acp_id）
                - session_runtime_manager: 会话运行时状态（模型/代理列表等）
                - process_runtime_manager: 进程级运行时状态（活跃请求队列）
                - inbound_manager: 入站消息处理（process_direct/process_inbound）
        """
        # ===== 基础配置 =====
        self.bus = bus
        self.workspace = workspace
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

    def new_request_key(self) -> str:
        """生成全局唯一的请求标识符。

        格式：acp:{uuid4_hex}
        示例：acp:f47ac10b58cc4372a5670e02b2c3d479

        用途：
            - 作为 _wait_by_request_key 字典的键
            - 在观测事件中标识请求来源
            - 在日志中追踪请求生命周期

        设计说明：
            使用 uuid4 保证全局唯一性（即使跨进程/跨会话）。
            前缀 "acp:" 用于区分其他来源的请求键（如 native 后端可能用不同前缀）。
        """
        return f"acp:{uuid4().hex}"

    async def await_acp_prompt(self, **kwargs: object) -> None:
        """等待 ACP 协议 prompt 执行完成（用于连接健康检查）。

        使用场景：
            - ensure_connection 中验证连接可用性
            - 启动时探测 ACP Agent 是否就绪

        参数：
            **kwargs: 传递给 ACP client.prompt 的参数（通常为空）

        超时策略：
            超时时间 = max(30 秒，acp_config.startup_timeout_seconds)
            确保至少等待 30 秒，同时尊重配置中的更长超时设置。

        异常：
            RuntimeError: 当 _acp_client_conn 为 None 时（连接未建立）
            asyncio.TimeoutError: 当 prompt 调用超过超时时间时

        关键流程：
            1. 检查连接是否可用
            2. 计算超时时间（至少 30 秒）
            3. 使用 asyncio.wait_for 包装 prompt 调用
            4. 等待 ACP Agent 响应
        """
        if self._acp_client_conn is None:
            raise RuntimeError("ACP connection is not available")
        timeout = max(30, self.acp_config.startup_timeout_seconds)
        prompt_callable = cast(Callable[..., Awaitable[None]], self._acp_client_conn.prompt)
        await asyncio.wait_for(prompt_callable(**kwargs), timeout=timeout)

    def register_wait_entry(self, request_key: str) -> RuntimeWaitEntry:
        """为当前请求注册等待条目。

        核心机制：
            每个异步请求都需要一个 Future 来等待最终结果。
            此函数创建 RuntimeWaitEntry 并存入 _wait_by_request_key 字典。

        参数：
            request_key: 请求唯一标识符（由 new_request_key 生成）

        返回：
            RuntimeWaitEntry 对象，包含：
                - request_key: 请求键
                - done_future: asyncio.Future，用于等待结果

        线程安全：
            在 asyncio 单线程模型中安全，无需加锁。
            但如果涉及多线程，需要在调用此函数时持有锁。

        使用模式：
            request_key = runtime.new_request_key()
            wait_entry = runtime.register_wait_entry(request_key)
            try:
                # ... 发送请求到 ACP ...
                result = await wait_entry.done_future  # 等待结果
            finally:
                runtime.remove_wait_entry(request_key)  # 清理
        """
        wait_entry = RuntimeWaitEntry(
            request_key=request_key,
            done_future=asyncio.get_running_loop().create_future(),
        )
        self._wait_by_request_key[request_key] = wait_entry
        return wait_entry

    def remove_wait_entry(self, request_key: str) -> None:
        """移除请求等待条目（清理资源）。

        调用时机：
            - 请求完成（无论成功或失败）后必须调用
            - 通常在 finally 块中调用，确保资源释放

        参数：
            request_key: 要移除的请求键

        注意：
            使用 pop(key, None) 避免 KeyError（如果 key 已不存在）。
        """
        self._wait_by_request_key.pop(request_key, None)

    def fail_all_wait_entries(self, error: Exception) -> None:
        """使所有未完成的等待条目以指定异常结束。

        使用场景：
            - 运行时关闭时（关闭所有 pending 请求）
            - 连接意外断开时（通知所有等待方）
            - 发生全局错误时（批量失败处理）

        参数：
            error: 要设置到所有 Future 的异常对象

        关键检查：
            if not wait_entry.done_future.done()
            避免对已完成的 Future 重复设置结果/异常（会引发 InvalidStateError）。

        注意：
            此函数不会从 _wait_by_request_key 中移除条目。
            调用方需要在处理后显式调用 remove_wait_entry。
        """
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
        """完成请求并通知等待方。

        这是请求等待链的终点：当 ACP 处理完成后，调用此函数设置 Future 结果。

        参数：
            request_key: 请求唯一标识符
            outbound: 最终出站消息（成功时提供）
            error: 异常对象（失败时提供）

        处理逻辑（优先级从高到低）：
            1. 如果 wait_entry 不存在或已完成：直接返回（避免重复完成）
            2. 如果 error 不为 None：设置异常（error 优先于 outbound）
            3. 如果 outbound 为 None：设置 RuntimeError（防御性检查）
            4. 否则：设置成功结果（outbound）

        异常处理策略：
            - error 优先：即使提供了 outbound，如果 error 不为 None 也以 error 为准
            - 防御性检查：如果 outbound 为 None 且 error 为 None，抛出 RuntimeError
              （这表示逻辑错误：请求完成但没有结果）

        使用示例：
            # 成功完成
            await runtime.complete_process_request(request_key, outbound=result)

            # 失败完成
            await runtime.complete_process_request(request_key, error=SomeException("..."))

            # 重复完成会被忽略（幂等）
            await runtime.complete_process_request(request_key, outbound=result)  # 第一次
            await runtime.complete_process_request(request_key, error=err)  # 被忽略
        """
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
        """确保 ACP 连接处于可用状态（如果未连接则建立连接）。

        委托实现：
            实际逻辑在 runtime_lifecycle.py 的 ensure_connection 函数中。
            此函数仅作为实例方法包装器，方便外部调用。

        使用场景：
            - 运行时启动时（run 方法中）
            - 发送请求前（process_direct/dispatch_inbound 中）
            - 连接健康检查失败后（重连逻辑）

        线程安全：
            使用 _acp_connection_lock 保证同一时间只有一个协程在建立连接。
            避免并发重建连接导致资源泄漏。

        幂等性：
            如果连接已建立且可用，此函数直接返回（不重复建立）。
        """
        await ensure_connection(self)

    async def reset_connection(self) -> None:
        """重置 ACP 连接并清理代际状态。

        与 ensure_connection 的区别：
            - ensure_connection: 保持现有连接（如果可用）
            - reset_connection: 强制关闭现有连接，重新建立新连接

        使用场景：
            - 连接状态异常时（超时/协议错误）
            - 配置变更后需要重新握手
            - 调试时手动重置连接

        清理内容：
            - 关闭现有 _acp_client_conn
            - 终止 _acp_agent_process
            - 清除 _acp_client_connection_cm
            - 重置代际状态（generation counter）

        注意：
            调用此函数后，所有 pending 的 wait_entry 会被失败处理。
            调用方需要处理由此产生的异常。
        """
        await reset_connection(self)

    async def run(self) -> None:
        """启动 ACP 运行时主循环。

        主循环职责：
            持续从消息总线消费 InboundMessage，并派发给 dispatch_inbound 处理。

        启动流程：
            1. 设置 _running = True（运行标志）
            2. 确保 ACP 连接可用（ensure_connection）
            3. 启动可观测性管理器（observability_manager.start）
            4. 进入无限循环：
                a. 从总线消费入站消息（bus.consume_inbound）
                b. 创建异步任务处理消息（dispatch_inbound）
                c. 继续下一次循环

        关键设计：
            - 使用 asyncio.create_task 并发处理消息（不阻塞主循环）
            - 如果 dispatch_inbound 抛出异常，不会中断主循环（异常在 task 内部捕获）
            - 通过 stop 方法设置 _running = False 退出循环

        退出条件：
            - 调用 stop 方法
            - 发生未捕获异常（会中断循环）
            - 进程终止

        使用示例：
            runtime = ACPRuntime(...)
            await runtime.run()  # 启动主循环（阻塞直到 stop 被调用）
        """
        self._running = True
        await self.ensure_connection()
        await self.observability_manager.start()
        while self._running:
            message = await self.bus.consume_inbound()
            # 并发处理：不阻塞主循环，每个消息独立处理
            asyncio.create_task(self.dispatch_inbound(message))

    async def stop(self) -> None:
        """请求停止主循环。

        处理逻辑：
            设置 _running = False，主循环在下一次检查时退出。

        注意：
            - 此函数不会等待主循环完全停止（异步停止）
            - 不会主动取消正在处理的 dispatch_inbound 任务
            - 调用方如果需要等待完全停止，需要在 stop 后添加额外同步逻辑

        使用示例：
            await runtime.stop()  # 请求停止
            # 主循环会在下一次检查 _running 时退出
        """
        self._running = False

    async def close(self) -> None:
        """关闭 ACP 运行时并释放所有资源。

        委托实现：
            实际逻辑在 runtime_lifecycle.py 的 close_runtime 函数中。

        清理内容：
            1. 停止主循环（调用 stop）
            2. 关闭 ACP 连接（_acp_client_conn）
            3. 终止 ACP Agent 进程（_acp_agent_process）
            4. 失败处理所有 pending 的 wait_entry
            5. 停止可观测性管理器
            6. 清理其他资源（回调客户端等）

        使用示例：
            try:
                await runtime.run()
            finally:
                await runtime.close()  # 确保资源释放
        """
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
        """处理直接入口请求（同步调用模式）。

        这是 CLI/直接调用的主要入口：调用方发送一个请求并等待最终响应。

        参数：
            content: 用户输入内容（文本消息）
            session_key: nanobot 侧会话键（默认："cli:direct"）
                - 用于标识会话，保持多轮对话上下文
                - 如果多次调用使用相同 session_key，ACP 会视为同一会话
            channel: 频道类型（默认："cli"）
                - 可选值："cli" | "telegram" | "discord" | "system" 等
                - 影响 ACP 侧的路由和权限决策
            chat_id: 聊天标识符（默认："direct"）
                - 在 system 频道中，chat_id 会被用作 session_key（见 resolve_nanobot_side_session_key）
            media: 媒体文件列表（可选）
                - 文件路径列表，会被上传到 ACP 侧
                - 示例：["/path/to/image.png", "/path/to/file.pdf"]
            preferred_model: 首选模型（可选）
                - 如果提供，会被设置到 metadata["_acp_session_model"]
                - ACP 侧会尝试使用该模型（如果可用）
            preferred_agent: 首选代理（可选）
                - 如果提供，会被设置到 metadata["_acp_session_agent"]
                - ACP 侧会尝试使用该代理（如果可用）
            on_progress: 进度回调函数（可选）
                - 签名：async def callback(progress_message: str) -> None
                - 当 ACP 发送 progress 更新时调用

        返回：
            OutboundMessage: 最终响应消息（包含 content/media/metadata）

        关键流程：
            1. 启动可观测性管理器（确保观测链路可用）
            2. 生成请求唯一键（new_request_key）
            3. 注册等待条目（register_wait_entry）
            4. 构造 ProcessDirectInput 对象（封装请求参数）
            5. 委托给 inbound_manager.handle_process_direct 处理
            6. 异步等待 wait_entry.done_future（阻塞直到完成）
            7. 清理等待条目（finally 块中）

        异常处理：
            - 如果 ACP 处理失败，done_future 会被设置异常（由 complete_process_request 设置）
            - 调用方需要捕获异常并处理

        使用示例：
            # CLI 直接调用
            result = await runtime.process_direct("Hello, world!")
            print(result.content)

            # 带进度回调
            async def on_progress(msg: str):
                print(f"Progress: {msg}")
            result = await runtime.process_direct(
                "Long running task...",
                on_progress=on_progress
            )

            # 指定会话和模型
            result = await runtime.process_direct(
                "Continue our conversation",
                session_key="user123:chat456",
                preferred_model="gpt-4"
            )
        """
        await self.observability_manager.start()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key)
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

    async def dispatch_inbound(self, message: InboundMessage) -> None:
        """处理总线入站消息并路由到 ACP（异步模式）。

        这是 run 主循环中调用的核心处理方法：每个入站消息独立处理，不阻塞主循环。

        参数：
            message: InboundMessage 对象（从总线消费得到）
                - channel: 消息来源频道（telegram/discord/cli 等）
                - chat_id: 聊天标识符
                - content: 消息内容
                - metadata: 附加元数据

        关键流程：
            1. 启动可观测性管理器（确保观测链路可用）
            2. 生成请求唯一键（new_request_key）
            3. 注册等待条目（register_wait_entry）
            4. 委托给 inbound_manager.handle_inbound 处理
            5. 异步等待 wait_entry.done_future（ACP 处理完成）
            6. 发布最终出站消息（publish_final_outbound）
            7. 如果发生异常：记录日志并上报观测事件
            8. 清理等待条目（finally 块中）

        异常处理策略：
            - 捕获所有 Exception（防止单个请求失败中断主循环）
            - 记录完整堆栈（logger.exception）
            - 上报结构化观测事件（包含 request_key/session_key/error）
            - 不重新抛出异常（避免中断主循环）

        与 process_direct 的区别：
            - process_direct: 同步调用模式，直接返回 OutboundMessage
            - dispatch_inbound: 异步处理模式，发布到总线后返回（无返回值）

        使用场景：
            - run 主循环中处理总线消息
            - 外部系统通过总线发送消息到 ACP

        注意：
            此函数被 asyncio.create_task 包装，因此异常不会传播到主循环。
            所有错误必须在此函数内部处理（通过 try-except）。
        """
        await self.observability_manager.start()
        request_key = self.new_request_key()
        wait_entry = self.register_wait_entry(request_key)
        try:
            # 委托给 inbound_manager 处理（实际发送到 ACP）
            await self.inbound_manager.handle_inbound(message, request_key=request_key)
            # 等待 ACP 处理完成
            outbound = await wait_entry.done_future
            # 发布最终结果到总线（路由回原始频道）
            await self.publish_final_outbound(message=message, outbound=outbound)
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
        """将 ACP 处理结果发布回原始消息频道。

        核心职责：
            将 ACP 返回的 OutboundMessage 路由回原始 InboundMessage 的频道和聊天。

        参数：
            message: 原始入站消息（提供 channel/chat_id/metadata）
            outbound: ACP 处理结果（提供 content/media/metadata）

        元数据合并策略：
            - 以 message.metadata 为基础（保留原始上下文）
            - 用 outbound.metadata 覆盖/追加（优先级更高）
            - 合并后的 metadata 包含双方信息

        路由规则：
            - channel: 使用 message.channel（路由回原始频道）
            - chat_id: 使用 message.chat_id（路由回原始聊天）
            - content: 使用 outbound.content（ACP 生成的响应）
            - media: 使用 outbound.media（ACP 返回的媒体文件）

        使用场景：
            dispatch_inbound 处理完成后调用，将结果发布回用户。

        示例：
            # Telegram 消息路由
            message = InboundMessage(channel="telegram", chat_id="user123", ...)
            outbound = OutboundMessage(content="Hello!", ...)
            await runtime.publish_final_outbound(message=message, outbound=outbound)
            # 结果会发布到 Telegram channel，chat_id=user123
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

    async def handle_session_update(
        self, *, acp_side_session_id: str, update: ACPCallbackUpdate
    ) -> None:
        """处理 ACP 会话更新回调。

        使用场景：
            ACP 主动推送会话状态更新（如 progress/tool_call/response）时调用。

        参数：
            acp_side_session_id: ACP 侧会话 ID（用于查找活跃请求）
            update: ACPCallbackUpdate 对象（包含更新类型和数据）

        关键流程：
            1. 查找活跃请求条目（process_runtime_manager.get_active_by_acp_side_session_id）
            2. 如果找不到或状态不是 ACTIVE：上报孤立更新事件（orphan update）
            3. 如果找到：委托给 state_manager.consume_session_update 处理

        孤立更新（Orphan Update）处理：
            - 定义：ACP 推送的更新找不到对应的活跃请求
            - 原因：可能是请求已完成/超时/被取消，但 ACP 仍在推送
            - 处理：记录观测事件，不抛出异常（避免中断其他请求）

        正常更新处理：
            - 委托给 state_manager.consume_session_update
            - 由 progress_router 路由到对应的 progress 回调（如果有）

        注意：
            此函数通常由 acp_callback_client 接收回调后调用。
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
        """处理 ACP 权限请求（工具调用授权）。

        使用场景：
            ACP 在执行需要授权的工具前，会暂停并请求用户授权。
            此函数决定自动批准还是拒绝。

        参数：
            acp_side_session_id: ACP 侧会话 ID（用于查找活跃请求）
            options: 权限选项列表（ALLOW_ONCE/ALLOW_ALWAYS/DENY 等）
            tool_call: ACPToolCall 对象（包含工具名称和参数）

        返回：
            RequestPermissionResponse 对象（包含选中的 option_id）

        关键流程：
            1. 查找活跃请求条目
            2. 如果找不到或状态不是 ACTIVE：上报孤立请求事件，使用默认策略响应
            3. 如果找到：委托给 state_manager.handle_permission_request 处理

        权限策略（由 acp_config.permissions_policy 决定）：
            - STRICT: 总是拒绝（返回 cancelled）
            - TRUSTED: 总是批准 ALLOW_ALWAYS
            - DEFAULT: 批准 ALLOW_ONCE（默认策略）

        注意：
            此函数是同步决策（不等待用户输入）。
            如果需要交互式授权，需要在 state_manager 中实现。
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
        # 正常权限请求：委托给 state_manager 处理
        return await active_entry.state_manager.handle_permission_request(
            options=options, tool_call=tool_call
        )

    async def permission_response(self, options: list[ACPPermissionOption]) -> object:
        """根据权限策略自动生成权限响应。

        使用场景：
            - 孤立权限请求（找不到活跃请求）
            - 默认策略响应（无状态管理器时）

        参数：
            options: ACP 提供的权限选项列表
                - 每个选项包含 option_id 和 kind（ALLOW_ONCE/ALLOW_ALWAYS/DENY）

        权限策略（acp_config.permissions_policy）：
            1. STRICT（严格模式）：
                - 总是拒绝所有权限请求
                - 返回 cancelled 响应
                - 适用场景：高安全要求环境

            2. TRUSTED（信任模式）：
                - 总是批准 ALLOW_ALWAYS（永久授权）
                - 如果 options 中没有 ALLOW_ALWAYS，回退到第一个选项
                - 适用场景：可信环境/开发调试

            3. DEFAULT（默认模式）：
                - 批准 ALLOW_ONCE（单次授权）
                - 如果 options 中没有 ALLOW_ONCE，回退到第一个选项
                - 适用场景：生产环境（平衡安全与便利）

        返回：
            RequestPermissionResponse 对象（包含选中的 option_id）

        关键流程：
            1. 读取权限策略配置
            2. STRICT: 直接返回 cancelled
            3. TRUSTED/DEFAULT: 确定首选 kind（ALLOW_ALWAYS/ALLOW_ONCE）
            4. 遍历 options，查找匹配的 kind
            5. 如果找不到：回退到第一个选项（如果有）
            6. 如果 options 为空：返回 cancelled

        设计说明：
            - 使用 build_permission_cancelled_payload/build_permission_selected_payload
              构造标准响应载荷
            - 使用 RequestPermissionResponse.model_validate 确保响应格式正确
            - 回退策略保证总有响应（不会让 ACP 无限等待）
        """
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
        """停止指定会话的所有请求（活跃 + 排队）。

        使用场景：
            - 用户发送 /stop 命令
            - 会话超时自动清理
            - 管理员强制终止会话

        参数：
            nanobot_side_session_key: nanobot 侧会话键（如 "user123:chat456"）

        返回：
            StopResult 对象，包含：
                - nanobot_side_session_key: 会话键
                - active_cancel_requested: 是否请求取消活跃请求（bool）
                - dropped_queued_count: 丢弃的排队请求数量（int）
                - acp_side_session_id: ACP 侧会话 ID

        关键流程：
            1. 加载 sessionmap 真相（确保持久化映射已加载）
            2. 解析 ACP 侧会话 ID（通过 sessionmap_binding_manager）
            3. 委托给 process_runtime_manager.stop_session 处理
            4. 构造并返回 StopResult

        停止策略：
            - 活跃请求：发送取消信号（如果支持取消）
            - 排队请求：直接从队列中移除（不执行）

        注意：
            此函数不会删除 sessionmap 绑定。
            如果需要完全清理，需要调用 drop_session_binding_and_runtime_entry。
        """
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
        """删除会话绑定和运行时条目（完全清理）。

        使用场景：
            - 会话结束后的资源清理
            - 会话过期自动回收
            - 管理员手动删除会话

        参数：
            nanobot_side_session_key: nanobot 侧会话键

        返回：
            str | None: 被删除的 ACP 侧会话 ID（如果存在），否则 None

        清理内容：
            1. session_runtime_manager: 删除会话运行时状态（模型/代理列表等）
            2. sessionmap_binding_manager: 删除会话绑定映射（nanobot_key <-> acp_id）

        注意：
            - 此函数不会停止活跃请求（需要先调用 stop_session）
            - 此函数不会通知 ACP 侧（仅清理本地状态）
            - 调用方需要确保会话已停止再调用此函数

        与 stop_session 的区别：
            - stop_session: 停止请求（活跃 + 排队），保留绑定
            - drop_session_binding_and_runtime_entry: 删除绑定和状态，不停止请求
        """
        # 清理会话运行时状态
        self.session_runtime_manager.drop_ready_session(
            nanobot_side_session_key=nanobot_side_session_key,
        )
        # 清理会话绑定映射，返回被删除的 ACP 侧会话 ID
        return self.sessionmap_binding_manager.clear_binding(nanobot_side_session_key)

    async def ensure_sessionmap_truth_loaded(self) -> None:
        """确保 sessionmap 持久化真相已加载到内存。

        使用场景：
            - stop_session 前（确保映射是最新的）
            - 运行时启动后（预加载映射）
            - 会话相关操作前（确保状态一致）

        委托实现：
            实际逻辑在 sessionmap_binding_manager.load_persistent_truth()。

        注意：
            此函数是幂等的：如果已加载，不会重复加载。
        """
        await self.sessionmap_binding_manager.load_persistent_truth()

    def resolve_nanobot_side_session_key(self, message: InboundMessage) -> str:
        """解析 InboundMessage 的 nanobot 侧会话键。

        解析规则：
            1. SYSTEM 频道特殊处理：
                - 如果 chat_id 包含 ":"，直接使用 chat_id 作为会话键
                - 否则，添加 "cli:" 前缀（格式：cli:{chat_id}）
            2. 其他频道：
                - 直接使用 message.session_key

        使用场景：
            - dispatch_inbound 中上报观测事件（标识会话）
            - stop_session 中解析目标会话
            - 日志记录（追踪会话来源）

        示例：
            # SYSTEM 频道
            message.channel = "system", message.chat_id = "user123:chat456"
            -> 返回 "user123:chat456"

            message.channel = "system", message.chat_id = "direct"
            -> 返回 "cli:direct"

            # Telegram 频道
            message.channel = "telegram", message.session_key = "user123:chat456"
            -> 返回 "user123:chat456"
        """
        if message.channel == ACPChannelName.SYSTEM.value:
            # SYSTEM 频道：chat_id 可能是会话键（如果包含 ":"）
            return message.chat_id if ":" in message.chat_id else f"cli:{message.chat_id}"
        # 其他频道：直接使用 session_key
        return message.session_key

    async def push_observability(self, event: ObservabilityEvent) -> None:
        """上报结构化观测事件。

        使用场景：
            - 错误上报（DISPATCH_INBOUND_ERROR）
            - 孤立事件（ORPHAN_SESSION_UPDATE/ORPHAN_PERMISSION_REQUEST）
            - 生命周期事件（连接建立/重置/关闭）

        参数：
            event: ObservabilityEvent 对象（包含 scope/event/payload 等）

        委托实现：
            实际逻辑在 observability_manager.push(event)。

        注意：
            此函数是异步的，不会阻塞调用方。
            观测事件会被批量处理（如果配置了批量上报）。
        """
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
        """构造结构化观测事件对象。

        参数：
            scope: 观测作用域（RUNTIME/SESSION/PROCESS 等）
            event: 事件名称（DISPATCH_INBOUND_ERROR/ORPHAN_SESSION_UPDATE 等）
            request_key: 请求唯一键（可选，用于关联具体请求）
            nanobot_side_session_key: nanobot 侧会话键（可选）
            acp_side_session_id: ACP 侧会话 ID（可选）
            payload: 事件载荷字典（可选，包含额外上下文信息）

        返回：
            ObservabilityEvent 对象（可直接传递给 push_observability）

        使用示例：
            event = runtime.new_observability_event(
                scope=ObservabilityScopeName.RUNTIME,
                event=ObservabilityEventName.DISPATCH_INBOUND_ERROR,
                request_key=request_key,
                nanobot_side_session_key="user123:chat456",
                payload={"error": "Connection timeout"}
            )
            await runtime.push_observability(event)
        """
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
        """构造基础出站消息对象（辅助函数）。

        参数：
            channel: 频道类型（"telegram" | "discord" | "cli" | "system"）
            chat_id: 聊天标识符
            content: 消息内容

        返回：
            OutboundMessage 对象（media 和 metadata 为空）

        使用场景：
            - 测试代码中快速构造消息
            - 简单响应（不需要 media/metadata）

        注意：
            这是静态方法，不需要 runtime 实例即可调用。
        """
        return OutboundMessage(channel=channel, chat_id=chat_id, content=content)

    async def list_models_command(self, acp_side_session_id: str) -> str:
        """渲染当前会话可用模型列表（用于 /models 命令）。

        参数：
            acp_side_session_id: ACP 侧会话 ID

        返回：
            格式化文本字符串（包含模型列表）

        委托实现：
            实际逻辑由 session capability 对象负责渲染。

        使用场景：
            用户发送 /models 命令时，返回可用模型列表。
        """
        caps = self.session_runtime_manager.get_session_capabilities(acp_side_session_id)
        if caps is None:
            return "No model catalog returned by current ACP backend for this session."
        return caps.render_models_command()

    async def list_agents_command(self, acp_side_session_id: str) -> str:
        """渲染当前会话可用代理列表（用于 /agents 命令）。

        参数：
            acp_side_session_id: ACP 侧会话 ID

        返回：
            格式化文本字符串（包含代理列表）

        委托实现：
            实际逻辑由 session capability 对象负责渲染。

        使用场景：
            用户发送 /agents 命令时，返回可用代理列表。
        """
        caps = self.session_runtime_manager.get_session_capabilities(acp_side_session_id)
        if caps is None:
            return "No agent/mode catalog returned by current ACP backend for this session."
        return caps.render_agents_command()


class ACPDispatcher(ACPRuntime):
    """ACP 调度器：ACPRuntime 的子类，用于 CLI 运行时创建。

    职责说明：
        ACPDispatcher 与 ACPRuntime 共享相同的实现（当前为空子类）。
        保留此子类是为了：
        1. 语义清晰：在 create_dispatch_runtime 中区分"运行时"和"调度器"
        2. 未来扩展：可能添加 ACP 特有的调度逻辑
        3. 类型标识：通过类型检查区分 native/acp 后端

    与 ACPRuntime 的关系：
        - 继承所有 ACPRuntime 方法（连接管理/请求处理/会话管理等）
        - 当前没有额外方法或属性（空子类）
        - 在 create_dispatch_runtime 中实例化

    使用示例：
        dispatcher = ACPDispatcher(
            bus=bus,
            workspace=config.workspace_path,
            acp_config=config.dispatch.acp,
            channels_config=config.channels,
        )
        await dispatcher.run()  # 启动主循环
    """
