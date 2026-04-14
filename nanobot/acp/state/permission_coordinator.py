"""权限子域协调器：承接 waiter、reply 解析与可观测性收口。"""

from __future__ import annotations

import asyncio

from nanobot.acp.contracts import (
    ACPPermissionKind,
    ACPPermissionOption,
    ACPToolCall,
    ObservabilityEventName,
    ObservabilityScopeName,
)
from nanobot.acp.observability import ObservabilityEvent
from nanobot.acp.state.permission_events import PendingPermissionRequest


class PermissionCoordinator:
    """单请求权限协调器：把权限 waiter 与观测收口为 state 子域组件。

    职责：
        - 持有 request 级权限 waiter 状态，统一承接 reply 与超时状态
        - 负责渲染权限提示文案，并为 router 提供轻量查询接口
        - 对 reply-not-found / timeout 等权限异常路径发出结构化观测事件
    """

    def __init__(
        self,
        *,
        runtime,
        request_key: str,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> None:
        """初始化 request 级权限协调器；生命周期与单次请求严格一致。"""

        self._runtime = runtime
        self._request_key = request_key
        self._nanobot_side_session_key = nanobot_side_session_key
        self._acp_side_session_id = acp_side_session_id
        self._pending_future: asyncio.Future[str] | None = None
        self._pending_request: PendingPermissionRequest | None = None

    def start_request(
        self,
        *,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall | None,
    ) -> PendingPermissionRequest:
        """建立新的权限等待态，并返回供 permission handler 使用的请求事件。"""

        loop = asyncio.get_running_loop()
        self._pending_future = loop.create_future()
        self._pending_request = PendingPermissionRequest(
            options=list(options),
            tool_call=tool_call,
            prompt_text=self.render_prompt(options),
        )
        return self._pending_request

    async def wait_for_reply(self) -> str:
        """等待权限回复结果；真正计时由 permission 池死手承担。"""

        if self._pending_future is None:
            raise RuntimeError("permission waiter not initialized")
        return await self._pending_future

    def clear(self) -> None:
        """清理当前权限等待态；适用于成功、超时和 request 收尾。"""

        self._pending_future = None
        self._pending_request = None

    def has_pending_permission(self) -> bool:
        """返回当前 request 是否仍有活跃权限等待。"""

        return self._pending_future is not None and not self._pending_future.done()

    def looks_like_permission_reply(self, reply_text: str) -> bool:
        """判断一条文本是否像当前权限回复；用于 inbound 的轻量筛选。"""

        request = self._pending_request
        if request is None:
            return False
        normalized = reply_text.strip()
        if normalized.startswith("/permission "):
            normalized = normalized[len("/permission ") :]
        elif normalized.startswith("permission "):
            normalized = normalized[len("permission ") :]
        else:
            return False
        return self.select_option(request.options, normalized) is not None

    def resolve_permission_reply(self, reply_text: str) -> str:
        """解析并消费权限回复；供 `PermissionHandler` 在 consume 主链中调用。"""

        future = self._pending_future
        request = self._pending_request
        if future is None or future.done() or request is None:
            return "not_found"
        normalized_reply = reply_text.strip()
        if normalized_reply.startswith("/permission "):
            normalized_reply = normalized_reply[len("/permission ") :]
        elif normalized_reply.startswith("permission "):
            normalized_reply = normalized_reply[len("permission ") :]
        option_id = self.select_option(request.options, normalized_reply)
        if option_id is None:
            return "invalid"
        future.set_result(option_id)
        self._pending_request = None
        return "accepted"

    def trigger_timeout(self) -> None:
        """触发权限超时；供死手与 request 收尾路径复用。"""

        if self._pending_future is not None and not self._pending_future.done():
            self._pending_future.set_exception(asyncio.TimeoutError("permission reply timed out"))
        self._pending_request = None

    async def emit_permission_timeout(self) -> None:
        """上报权限等待超时事件；适用于 router 等待 reply 失败后的统一收口。"""

        await self._runtime.push_observability(
            ObservabilityEvent(
                scope=ObservabilityScopeName.STATE,
                event=ObservabilityEventName.PERMISSION_TIMEOUT,
                request_key=self._request_key,
                nanobot_side_session_key=self._nanobot_side_session_key,
                acp_side_session_id=self._acp_side_session_id,
            )
        )

    async def emit_permission_reply_not_found(self) -> None:
        """上报权限回复未命中事件；适用于 inbound 误路由到空 waiter 的场景。"""

        await self._runtime.push_observability(
            ObservabilityEvent(
                scope=ObservabilityScopeName.STATE,
                event=ObservabilityEventName.PERMISSION_REPLY_NOT_FOUND,
                request_key=self._request_key,
                nanobot_side_session_key=self._nanobot_side_session_key,
                acp_side_session_id=self._acp_side_session_id,
            )
        )

    @staticmethod
    def render_prompt(options: list[ACPPermissionOption]) -> str:
        """把权限选项渲染成用户可回复的提示文案。"""

        lines = ["ACP requires permission. Reply with /permission <number>: <options>"]
        for index, option in enumerate(options, start=1):
            kind = getattr(
                getattr(option, "kind", None), "value", getattr(option, "kind", "option")
            )
            label = getattr(option, "label", None) or getattr(option, "title", None) or kind
            lines.append(f"{index}. {label}")
        return "\n".join(lines)

    @staticmethod
    def select_option(options: list[ACPPermissionOption], reply_text: str) -> str | None:
        """把用户回复映射到 ACP option_id；适用于编号、kind 与 allow/deny 简写三类输入。"""

        reply = reply_text.strip().lower()
        if not reply:
            return None
        mapping = {str(index + 1): option.option_id for index, option in enumerate(options)}
        if reply in mapping:
            return mapping[reply]
        for option in options:
            kind = str(
                getattr(getattr(option, "kind", None), "value", getattr(option, "kind", "")) or ""
            )
            if reply in {kind.lower(), option.option_id.lower()}:
                return option.option_id
        if reply in {"allow", "yes", "y"}:
            for option in options:
                kind = str(
                    getattr(getattr(option, "kind", None), "value", getattr(option, "kind", ""))
                    or ""
                )
                if kind in {
                    ACPPermissionKind.ALLOW_ONCE.value,
                    ACPPermissionKind.ALLOW_ALWAYS.value,
                }:
                    return option.option_id
        if reply in {"cancel", "deny", "no", "n"}:
            for option in options:
                kind = str(
                    getattr(getattr(option, "kind", None), "value", getattr(option, "kind", ""))
                    or ""
                )
                if kind == ACPPermissionKind.CANCELLED.value:
                    return option.option_id
        return None


__all__ = ["PermissionCoordinator"]
