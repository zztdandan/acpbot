"""ACP dispatcher 单条消息主流程实现。"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Protocol, cast

from loguru import logger

from nanobot.acp.dispatch_commands import pop_command_reason
from nanobot.acp.state import _ACPDispatchError
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.command.router import CommandContext, CommandRouter


class _DispatcherFlowPorts(Protocol):
    """_dispatch 流程所需的最小 dispatcher 端口集合。"""

    channels_config: Any
    last_target: tuple[str, str] | None
    _process_locks: dict[str, asyncio.Lock]
    _session_pending_media: dict[str, list[str]]
    _session_result_media: dict[str, list[str]]
    _session_progress_metadata: dict[str, dict[str, Any]]
    _permission_bridge: Any
    acp_config: Any
    commands: CommandRouter

    @staticmethod
    def _parse_command(content: str) -> tuple[str, str]: ...

    @classmethod
    def _sanitize_outbound_metadata(cls, metadata: dict[str, Any] | None) -> dict[str, Any]: ...

    async def process_direct(
        self,
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress: Any = None,
    ) -> OutboundMessage: ...

    async def _audit_inbound(self, *, msg: InboundMessage, session_key: str) -> None: ...

    async def _publish_outbound_with_debug(
        self,
        *,
        msg: OutboundMessage,
        reason: str,
        session_key: str,
    ) -> None: ...

    def _format_final_content(self, content: str) -> str: ...


def _extract_session_preferences(
    metadata: dict[str, Any],
) -> tuple[str | None, str | None]:
    """解析 WS 首帧透传的模型/agent 偏好。"""
    # 中文注释：偏好仅接受非空字符串，避免把非字符串 metadata 误当成选择值。
    preferred_model_raw = metadata.get("_acp_session_model")
    preferred_agent_raw = metadata.get("_acp_session_agent")
    preferred_model = (
        str(preferred_model_raw).strip()
        if isinstance(preferred_model_raw, str) and preferred_model_raw.strip()
        else None
    )
    preferred_agent = (
        str(preferred_agent_raw).strip()
        if isinstance(preferred_agent_raw, str) and preferred_agent_raw.strip()
        else None
    )
    return preferred_model, preferred_agent


async def _dispatch_inbound(dispatcher: _DispatcherFlowPorts, msg: InboundMessage) -> None:
    """处理单条 inbound 消息并发布 outbound。"""
    try:
        key = msg.session_key
        if msg.channel == "system":
            # system 通道复用来源会话，避免上下文断层。
            origin = msg.chat_id if ":" in msg.chat_id else f"cli:{msg.chat_id}"
            key = origin

        await dispatcher._audit_inbound(msg=msg, session_key=key)

        # 中文注释：permission 回执必须在 session 锁外预消费，避免会话主流程等待 permission 时被同锁阻塞。
        if await dispatcher._permission_bridge.try_consume_permission_reply(msg):
            return

        # 中文注释：同一 session_key 下，slash 与常规 prompt 共用一把锁串行，
        # 防止 /new、/set_* 与普通对话并发时发生状态覆盖或脏写。
        lock = dispatcher._process_locks.setdefault(key, asyncio.Lock())
        async with lock:
            raw = msg.content.strip()
            ctx = CommandContext(
                msg=msg, session=None, key=key, raw=raw, loop=cast(Any, dispatcher)
            )
            if result := await dispatcher.commands.dispatch(ctx):
                reason = pop_command_reason(result.metadata)
                await dispatcher._publish_outbound_with_debug(
                    msg=result,
                    reason=reason,
                    session_key=key,
                )
                return
            if raw.startswith("/"):
                command, _ = dispatcher._parse_command(raw)
                # 中文注释：slash 未命中时显式回包，不再把未知命令透传给 process_direct。
                await dispatcher._publish_outbound_with_debug(
                    msg=OutboundMessage(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        content=(
                            f"Unknown command: {command}\nUse /help to see available commands."
                        ),
                    ),
                    reason="command_unknown",
                    session_key=key,
                )
                return

            if msg.channel not in {"cli", "system"} and msg.chat_id:
                # 记录最近一次真实渠道目标，供其它功能选路时参考。
                dispatcher.last_target = (msg.channel, msg.chat_id)

            content_esc = msg.content.encode("unicode_escape", "ignore").decode("ascii")
            if len(content_esc) > 320:
                content_esc = f"{content_esc[:320]}..."
            logger.debug(
                "ACP dispatch inbound channel={} sender={} chat={} session_key={} chars={} metadata_keys={} content_esc='{}'",
                msg.channel,
                msg.sender_id,
                msg.chat_id,
                key,
                len(msg.content),
                sorted((msg.metadata or {}).keys()),
                content_esc,
            )

            metadata = msg.metadata or {}
            preferred_model, preferred_agent = _extract_session_preferences(metadata)
            send_final = (
                True
                if dispatcher.channels_config is None
                else dispatcher.channels_config.send_final
            )
            response: OutboundMessage | None = None
            partial: str | None = None
            dispatch_error: _ACPDispatchError | None = None
            response_media: list[str] = []

            try:
                # 中文注释：当前轮附件与 progress metadata 先挂到 session_key，
                # process_direct 内部会创建唯一的 ProgressRouter 并消费这些上下文。
                dispatcher._session_pending_media[key] = list(msg.media or [])
                dispatcher._session_progress_metadata[key] = dispatcher._sanitize_outbound_metadata(
                    metadata
                )
                response = await dispatcher.process_direct(
                    msg.content,
                    session_key=key,
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    preferred_model=preferred_model,
                    preferred_agent=preferred_agent,
                )
            except _ACPDispatchError as exc:
                dispatch_error = exc
                partial = exc.partial_response.strip()
            finally:
                dispatcher._session_pending_media.pop(key, None)
                dispatcher._session_progress_metadata.pop(key, None)
                response_media = dispatcher._session_result_media.pop(key, [])

            if dispatch_error is not None:
                # ACP 异常时优先尝试输出 partial，减少用户感知中断。
                if partial:
                    if send_final:
                        partial_esc = partial.encode("unicode_escape", "ignore").decode("ascii")
                        if len(partial_esc) > 320:
                            partial_esc = f"{partial_esc[:320]}..."
                        logger.warning(
                            "ACP dispatch fallback using partial response channel={} chat={} session_key={} partial_chars={} partial_esc='{}'",
                            msg.channel,
                            msg.chat_id,
                            key,
                            len(partial),
                            partial_esc,
                        )
                        await dispatcher._publish_outbound_with_debug(
                            msg=OutboundMessage(
                                channel=msg.channel,
                                chat_id=msg.chat_id,
                                content=dispatcher._format_final_content(partial),
                                media=response_media,
                                metadata=dispatcher._sanitize_outbound_metadata(msg.metadata),
                            ),
                            reason="final",
                            session_key=key,
                        )
                    return
                raise dispatch_error

            if response is not None and send_final:
                if isinstance(response, str):
                    # Backward compatibility for tests/mocks still returning plain text.
                    response = OutboundMessage(
                        channel=msg.channel, chat_id=msg.chat_id, content=response
                    )
                final_metadata = dispatcher._sanitize_outbound_metadata(msg.metadata)
                final_metadata.update(response.metadata or {})
                await dispatcher._publish_outbound_with_debug(
                    msg=OutboundMessage(
                        channel=msg.channel,
                        chat_id=msg.chat_id,
                        content=dispatcher._format_final_content(response.content),
                        media=response.media or response_media,
                        metadata=final_metadata,
                    ),
                    reason="final",
                    session_key=key,
                )
    except asyncio.CancelledError:
        raise
    except Exception:
        # 兜底保护：防止异常导致消息链路静默。
        logger.exception("ACP dispatcher failed for {}", msg.session_key)
        await dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content="Sorry, I encountered an error.",
            ),
            reason="dispatch_exception",
            session_key=msg.session_key,
        )
