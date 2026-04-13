"""ACP inbound manager with explicit direct and bus entrypoints."""

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
    """Transforms direct/bus inputs into InboundContext and fixed pipeline steps."""

    def __init__(self, *, runtime) -> None:
        self._runtime = runtime
        self._command_router = CommandRouter(runtime=runtime)

    async def handle_process_direct(
        self,
        input: ProcessDirectInput,
        *,
        request_key: str,
    ) -> None:
        # 中文注释：direct 入口先把 runtime 输入收敛成统一 ctx，
        # 之后与 bus 路径共享同一种 pipeline 协议，而不是保留两套散装前处理。
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
        async def _bus_progress(content: str, **metadata) -> None:
            # 中文注释：bus 路径没有 direct 调用方可回调，
            # 因此 state 的 progress sink 在这里桥接成 bus outbound progress message。
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
        return [
            build_normalize_step(self._runtime),
            build_command_router_step(self._command_router),
            build_media_prepare_step(self._runtime),
            build_process_request_step(self._runtime),
        ]

    def bus_inbound_steps(self) -> list[InboundStep]:
        return [
            build_normalize_step(self._runtime),
            build_permission_inbound_step(self._runtime),
            build_command_router_step(self._command_router),
            build_media_prepare_step(self._runtime),
            build_process_request_step(self._runtime),
        ]

    async def _run_pipeline(self, ctx: InboundContext, *, steps: list[InboundStep]) -> None:
        for step in steps:
            await step(ctx)
            if ctx.direct_response is not None:
                # 中文注释：inbound 可以决定 direct return，但不能直接碰 runtime wait map；
                # 仍必须回到统一 completion 入口结束请求。
                await self._runtime.complete_process_request(
                    ctx.request_key,
                    outbound=ctx.direct_response,
                )
                return
        if ctx.process_request is None:
            raise RuntimeError("inbound finished without process request")
        # 中文注释：没有 direct response 时，inbound 的职责到此结束；
        # 后续 active/queue 由 ProcessRuntimeManager 接管。
        await self._runtime.process_runtime_manager.enqueue(ctx.process_request)
