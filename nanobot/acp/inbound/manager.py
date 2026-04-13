"""入站归一化与步骤编排层。"""

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
    """负责对应领域状态与流程编排。"""

    def __init__(self, *, runtime) -> None:
        """初始化当前对象并建立必要状态。"""
        self._runtime = runtime
        self._command_router = CommandRouter(runtime=runtime)

    async def handle_process_direct(
        self,
        input: ProcessDirectInput,
        *,
        request_key: str,
    ) -> None:
        """处理直接入口请求并等待最终结果。"""
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
        """处理总线入口并执行入站流水线。"""
        async def _bus_progress(content: str, **metadata) -> None:
            """执行该方法定义的处理流程并返回结果。"""
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
        """处理直接入口请求并等待最终结果。"""
        return [
            build_normalize_step(self._runtime),
            build_command_router_step(self._command_router),
            build_media_prepare_step(self._runtime),
            build_process_request_step(self._runtime),
        ]

    def bus_inbound_steps(self) -> list[InboundStep]:
        """构建总线入口步骤列表。"""
        return [
            build_normalize_step(self._runtime),
            build_permission_inbound_step(self._runtime),
            build_command_router_step(self._command_router),
            build_media_prepare_step(self._runtime),
            build_process_request_step(self._runtime),
        ]

    async def _run_pipeline(self, ctx: InboundContext, *, steps: list[InboundStep]) -> None:
        """启动主循环并持续消费入站消息。"""
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
