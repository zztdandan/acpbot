"""入站管理器：协调请求进入 ACP 执行前的固定编排步骤。"""

from __future__ import annotations

from nanobot.acp.inbound.command_router import CommandRouter
from nanobot.acp.inbound.pipeline import (
    InboundStep,
    build_command_router_step,
    build_media_prepare_step,
    build_normalize_step,
    build_permission_inbound_step,
    build_process_request_step,
)

from nanobot.acp.runtime_models import InboundContext, ProcessDirectInput


class InboundManager:
    """入站管理器：负责 direct 输入与总线消息的固定编排链路。

    职责：
        - 为 `process_direct` 与 bus inbound 构造各自的固定步骤链
        - 统一驱动 normalize、permission、command、media、process_request 五段式编排

    生命周期：
        - 创建：`ACPRuntime` 初始化时创建，并与当前 runtime 绑定
        - 销毁：随 runtime 一起销毁，不单独持久化请求状态
    """

    def __init__(self, *, runtime) -> None:
        """建立入站管理器并绑定 runtime 与命令路由器。"""
        self._runtime = runtime
        self._command_router = CommandRouter(runtime=runtime)

    async def handle_process_direct(
        self,
        input: ProcessDirectInput,
        *,
        request_key: str,
    ) -> None:
        """处理 direct 输入请求；把外部直接调用转换为统一入站上下文。

        处理流程：
            - 用 direct 输入构造 `InboundContext`
            - 选择 direct 专用步骤链，不经过权限回复拦截分支
            - 交给 `_run_pipeline` 执行，最终生成直返或入队请求
        """
        ctx = InboundContext(
            request_key=request_key,
            nanobot_side_session_key=input.nanobot_side_session_key,
            channel=input.channel,
            chat_id=input.chat_id,
            sender_id=input.sender_id,
            content=input.content,
            media=list(input.media),
            metadata=dict(input.metadata),
            on_progress=input.on_progress,
        )
        await self._run_pipeline(ctx, steps=self.process_direct_steps())

    async def handle_inbound(self, message, *, request_key: str) -> None:
        """处理总线入站消息；把 bus message 转成统一入站上下文后执行流水线。

        处理流程：
            - 构造总线专用进度回调，把进度片段镜像回 outbound bus
            - 基于原始消息构造 `InboundContext`
            - 交给 bus inbound 步骤链执行，按结果直返或入队
        """

        async def _bus_progress(content: str, **metadata) -> None:
            """把请求级进度镜像成总线 outbound 消息；供 bus 场景下逐步回显。"""
            outbound = self._runtime.new_outbound_message(
                channel=message.channel,
                chat_id=message.chat_id,
                content=content,
            )
            outbound.metadata = {"_progress": True, **metadata}
            await self._runtime.bus.publish_outbound(outbound)

        ctx = InboundContext(
            request_key=request_key,
            nanobot_side_session_key=self._runtime.resolve_nanobot_side_session_key(message),
            raw_message=message,
            channel=message.channel,
            chat_id=message.chat_id,
            sender_id=message.sender_id,
            content=message.content,
            media=list(message.media),
            metadata=dict(message.metadata or {}),
            on_progress=_bus_progress,
        )
        await self._run_pipeline(ctx, steps=self.bus_inbound_steps())

    def process_direct_steps(self) -> list[InboundStep]:
        """返回 direct 输入使用的固定步骤链；允许 direct 入口显式回复 permission。"""
        return [
            build_normalize_step(self._runtime),
            build_permission_inbound_step(self._runtime),
            build_command_router_step(self._command_router),
            build_media_prepare_step(self._runtime),
            build_process_request_step(self._runtime),
        ]

    def bus_inbound_steps(self) -> list[InboundStep]:
        """返回总线入站使用的固定步骤链；优先插入权限回复拦截分支。"""
        return [
            build_normalize_step(self._runtime),
            build_permission_inbound_step(self._runtime),
            build_command_router_step(self._command_router),
            build_media_prepare_step(self._runtime),
            build_process_request_step(self._runtime),
        ]

    async def _run_pipeline(self, ctx: InboundContext, *, steps: list[InboundStep]) -> None:
        """执行一条入站步骤链；在直返响应与 process_request 入队之间做统一收口。

        处理流程：
            - 依次执行传入的步骤函数，让各步骤按 owner 边界写入 `InboundContext`
            - 若某一步生成 `direct_response`，立即完成当前请求并结束流水线
            - 全部步骤结束后要求 `process_request` 已就绪，否则视为编排缺失
        """
        for step in steps:
            await step(ctx)
            if ctx.direct_response is not None:
                await self._runtime.complete_process_request(
                    ctx.request_key,
                    outbound=ctx.direct_response,
                )
                return
        if ctx.process_request is None:
            raise RuntimeError("inbound finished without process request")
        await self._runtime.process_runtime_manager.enqueue(ctx.process_request)
