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
        request_id = self._extract_tool_call_id(tool_call)
        self._pending_future = loop.create_future()
        self._pending_request = PendingPermissionRequest(
            options=list(options),
            tool_call=tool_call,
            request_id=request_id,
            prompt_text=self.render_prompt(options, request_id=request_id),
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

    def current_request_id(self) -> str | None:
        """返回当前 pending permission 的 request_id；无等待态时返回 None。"""

        request = self._pending_request
        return request.request_id if request is not None else None

    def looks_like_permission_reply(self, reply_text: str) -> bool:
        """判断一条文本是否像当前权限回复；用于 inbound 的轻量筛选。"""

        request = self._pending_request
        if request is None:
            return False
        parsed = self._parse_permission_reply(reply_text)
        if parsed is None:
            return False
        request_id, selected = parsed
        if (
            request_id is not None
            and request.request_id is not None
            and request_id != request.request_id
        ):
            return False
        return self.select_option(request.options, selected) is not None

    def resolve_permission_reply(self, reply_text: str) -> str:
        """解析并消费权限回复；供 `PermissionHandler` 在 consume 主链中调用。"""

        future = self._pending_future
        request = self._pending_request
        if future is None or future.done() or request is None:
            return "not_found"
        parsed = self._parse_permission_reply(reply_text)
        if parsed is None:
            return "invalid"
        request_id, selected = parsed
        if (
            request_id is not None
            and request.request_id is not None
            and request_id != request.request_id
        ):
            return "invalid"
        option_id = self.select_option(request.options, selected)
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
    def render_prompt(options: list[ACPPermissionOption], request_id: str | None = None) -> str:
        """把权限选项渲染成用户可回复的提示文案。"""

        if request_id:
            lines = [
                "ACP is asking for permission. "
                "Please reply with /permission <request_id>:<number|option_choice>.",
                f"request_id={request_id}",
            ]
        else:
            lines = ["ACP is asking for permission. Reply with /permission <number|option_choice>."]
        for index, option in enumerate(options, start=1):
            kind = getattr(
                getattr(option, "kind", None), "value", getattr(option, "kind", "option")
            )
            label = getattr(option, "label", None) or getattr(option, "title", None) or kind
            lines.append(f"{index}. {label}")
        return "\n".join(lines)

    @staticmethod
    def _extract_tool_call_id(tool_call: ACPToolCall | None) -> str | None:
        """从 ACP tool_call 里提取稳定 request_id（优先 toolCallId/tool_call_id）。"""

        if tool_call is None:
            return None
        if isinstance(tool_call, dict):
            for key in ("toolCallId", "tool_call_id", "request_id", "id"):
                value = tool_call.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return None
        for attr in ("toolCallId", "tool_call_id", "request_id", "id"):
            value = getattr(tool_call, attr, None)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _parse_permission_reply(reply_text: str) -> tuple[str | None, str] | None:
        """解析 `/permission` 文本，支持 `<number>` 与 `<request_id>:<choice>` 两种格式。"""

        normalized = reply_text.strip()
        lowered = normalized.lower()
        if lowered.startswith("/permission "):
            body = normalized[len("/permission ") :].strip()
        elif lowered.startswith("permission "):
            body = normalized[len("permission ") :].strip()
        else:
            return None
        if not body:
            return None
        if ":" in body:
            request_id, selected = body.split(":", 1)
            request_id = request_id.strip()
            selected = selected.strip()
            if not request_id or not selected:
                return None
            return request_id, selected
        return None, body

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
