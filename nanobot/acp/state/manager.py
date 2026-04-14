"""SessionStateManager：单请求 state owner，统一持有池索引、权限等待与 final 聚合。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Protocol

from loguru import logger

from nanobot.acp.contracts import (
    ACPCallbackUpdate,
    ACPPermissionKind,
    ACPPermissionOption,
    ACPResourceBlock,
    ACPToolCall,
    JSONMap,
    ObservabilityEventName,
    ObservabilityScopeName,
    build_permission_selected_payload,
)
from nanobot.acp.observability import ObservabilityEvent
from nanobot.acp.runtime_models import ProgressCallback
from nanobot.acp.state.handlers.base import sanitize_json_value
from nanobot.acp.state.models import ACPBucketType, ACPPool, PoolKey, RequestScopeState
from nanobot.acp.state.permission_events import PendingPermissionRequest
from nanobot.acp.state.pools import PermissionPool
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.state.router import ProgressRouter


class _RuntimeObservabilityOwner(Protocol):
    """运行时观测上报协议：state 只发事实，不直接操作 observability 内部队列。"""

    async def push_observability(self, event: ObservabilityEvent) -> None:
        """上报一条结构化观测事件。"""
        ...


class SessionStateManager:
    """单请求状态管理器：统一 owner 池索引、权限等待、request-scope 聚合与 final materialize。

    职责：
        - 以 `PoolKey -> pool` 形式持有当前请求全部池实例，替代“每类一个成员变量”的旧模式
        - 提供权限请求/回复入口，保证 pending future 生命周期收口在 state 内
        - 维护 final_text / media_paths / final_metadata，作为最终结果物化事实源

    生命周期：
        - 创建：请求真正进入 ACP 执行时，由 ProcessRuntimeManager 新建
        - 销毁：请求完成或失败后，经 close 链路尾刷并销毁全部池索引
    """

    def __init__(
        self,
        *,
        runtime: _RuntimeObservabilityOwner,
        request_key: str,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        channel: str,
        chat_id: str,
        on_progress: ProgressCallback | None,
    ) -> None:
        """建立单请求 state；所有依赖都按请求粒度注入，避免 runtime 顶层共享字典回潮。"""

        self._runtime = runtime
        self.request_key = request_key
        self.nanobot_side_session_key = nanobot_side_session_key
        self.acp_side_session_id = acp_side_session_id
        self.channel = channel
        self.chat_id = chat_id
        self.on_progress = on_progress
        self.request_scope = RequestScopeState()
        self._pools: dict[PoolKey, ACPPool] = {}
        self._closed = False
        self._progress_router: ProgressRouter | None = None
        self._pending_permission_future: asyncio.Future[str] | None = None
        self._pending_permission_request: PendingPermissionRequest | None = None

    def is_closed(self) -> bool:
        """返回当前 state 是否已关闭；late update/permission 会据此拒绝写入。"""

        return self._closed

    def bind_progress_router(self, progress_router: ProgressRouter) -> None:
        """绑定请求级 ProgressRouter；供权限分支与普通更新共享同一出口。"""

        self._progress_router = progress_router

    def get_or_create_pool(
        self,
        pool_key: PoolKey,
        *,
        factory: Callable[[], ACPPool],
    ) -> ACPPool:
        """按池主键复用或创建池实例；tool 等多实例类型依赖此入口隔离并发。"""

        pool = self._pools.get(pool_key)
        if pool is None:
            pool = factory()
            self._pools[pool_key] = pool
        return pool

    def destroy_pool(self, pool_key: PoolKey) -> None:
        """从索引中销毁一个池；适用于 other 一次性池与 request close 收尾。"""

        self._pools.pop(pool_key, None)

    def iter_pools(self) -> Iterable[tuple[PoolKey, ACPPool]]:
        """遍历当前所有活跃池；router close/flush_all 通过该入口统一处理。"""

        return tuple(self._pools.items())

    def update_text_snapshot(self, text: str) -> None:
        """刷新 final_text 与 partial_text；供文本 handler 在每次增量后同步。"""

        normalized = text.strip()
        self.request_scope.final_text = normalized
        self.request_scope.partial_text = normalized

    def append_media_path(self, media_path: str, *, source: str = "message") -> None:
        """把媒体路径并入请求聚合结果；消息媒体排前，工具附件排后，避免最终顺序漂移。"""

        if not media_path:
            return
        target = (
            self.request_scope.tool_media_paths
            if source == "tool"
            else self.request_scope.message_media_paths
        )
        if media_path not in target:
            target.append(media_path)
        self.request_scope.media_paths = [
            *self.request_scope.message_media_paths,
            *self.request_scope.tool_media_paths,
        ]

    def update_named_metadata(self, key: str, payload: object) -> None:
        """把一条命名事实写入 final_metadata；仅显式采纳的字段允许进入最终结果。"""

        self.request_scope.final_metadata[key] = sanitize_json_value(payload)

    def append_other_update(self, *, label: str, payload: object) -> None:
        """记录未识别更新痕迹；便于后续审计其他池是否仍有漏网类型。"""

        existing = self.request_scope.final_metadata.get("other_updates")
        items = list(existing) if isinstance(existing, list) else []
        items.append({"label": label, "payload": sanitize_json_value(payload)})
        self.request_scope.final_metadata["other_updates"] = items

    async def emit_progress(self, *, content: str, metadata: JSONMap) -> None:
        """向请求级 on_progress 镜像进度；兼容旧 `tool_hint` 参数约定。"""

        if not content or self.on_progress is None:
            return
        callback_kwargs: dict[str, object] = {}
        if "tool_hint" in metadata:
            callback_kwargs["tool_hint"] = metadata["tool_hint"]
        if "tool_event" in metadata:
            callback_kwargs["tool_event"] = metadata["tool_event"]
        try:
            await self.on_progress(content, **callback_kwargs)
        except TypeError:
            try:
                if "tool_hint" in callback_kwargs:
                    await self.on_progress(content, tool_hint=callback_kwargs["tool_hint"])
                else:
                    await self.on_progress(content)
            except Exception:
                logger.exception("ACP state on_progress fallback failed")
        except Exception:
            logger.exception("ACP state on_progress emit failed")

    async def consume_session_update(
        self,
        update: ACPCallbackUpdate,
        *,
        progress_router: ProgressRouter,
    ) -> None:
        """接收一条 session_update 并委托 ProgressRouter 分派；manager 自己不再写大段 if/else。

        处理流程：
            - 若 state 已关闭，则上报 late update 并直接拒绝写入
            - 绑定当前请求的 ProgressRouter，确保普通更新与权限分支共用同一出口
            - 把 update 交给 router 做 handler 解析、池建立/复用、flush 与销毁
        """

        if self._closed:
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.LATE_SESSION_UPDATE,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                )
            )
            return
        self._progress_router = progress_router
        await progress_router.handle_update(update)

    @staticmethod
    def extract_media_path(block: ACPResourceBlock) -> str | None:
        """从 ACP 内容块中提取本地媒体路径；兼容 uri/path/resource.uri 三种来源。"""

        for attr in ("uri", "path"):
            value = getattr(block, attr, None)
            if isinstance(value, str) and value:
                if value.startswith("file://"):
                    return value[7:]
                return value
        resource = getattr(block, "resource", None)
        if resource is not None:
            uri = getattr(resource, "uri", None)
            if isinstance(uri, str) and uri:
                return uri[7:] if uri.startswith("file://") else uri
        return None

    async def handle_permission_request(
        self,
        *,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall | None = None,
    ) -> object:
        """处理一条权限请求；等待 inbound reply 或超时后回传 ACP SDK 需要的响应对象。

        处理流程：
            - 关闭态直接拒绝，避免 late permission 污染已结束 request
            - 创建 pending future，并把权限提示写入权限池镜像给 on_progress
            - 等待用户回复；超时时只上报 observability，不改写 request 生命周期
            - 清理 pending 状态并把用户选择转成 ACP SDK 的 RequestPermissionResponse
        """

        if self._closed:
            raise RuntimeError("permission request received after state closed")

        from acp.schema import RequestPermissionResponse

        loop = asyncio.get_running_loop()
        self._pending_permission_future = loop.create_future()
        prompt = self._render_permission_prompt(options)
        self._pending_permission_request = PendingPermissionRequest(
            options=list(options),
            tool_call=tool_call,
            prompt_text=prompt,
        )
        permission_pool = self.get_or_create_pool(
            PoolKey(bucket_type=ACPBucketType.PERMISSION, bucket_key="permission"),
            factory=lambda: PermissionPool(bucket_key="permission"),
        )
        permission_pool.accept(prompt)
        if self._progress_router is not None:
            await self._progress_router.emit(permission_pool.flush())

        try:
            selected_option_id = await asyncio.wait_for(
                self._pending_permission_future, timeout=300
            )
        except asyncio.TimeoutError as exc:
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.PERMISSION_TIMEOUT,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                )
            )
            raise RuntimeError("permission reply timed out") from exc
        finally:
            self._pending_permission_future = None
            self._pending_permission_request = None

        return RequestPermissionResponse.model_validate(
            build_permission_selected_payload(selected_option_id)
        )

    def has_pending_permission(self) -> bool:
        """判断当前请求是否仍在等待权限回复；供 inbound 侧快速筛选目标 request。"""

        return (
            self._pending_permission_future is not None
            and not self._pending_permission_future.done()
        )

    def looks_like_permission_reply(self, reply_text: str) -> bool:
        """判断一条入站文本是否命中当前 pending permission；只做识别，不写状态。"""

        request = self._pending_permission_request
        if request is None:
            return False
        normalized = reply_text.strip()
        if normalized.startswith("/permission "):
            normalized = normalized[len("/permission ") :]
        elif normalized.startswith("permission "):
            normalized = normalized[len("permission ") :]
        else:
            return False
        return self._select_permission_option(request.options, normalized) is not None

    async def handle_permission_reply(self, *, reply_text: str) -> str:
        """消费权限回复并唤醒等待中的权限 future；未命中时只回提示文本与 observability。"""

        future = self._pending_permission_future
        request = self._pending_permission_request
        if future is None or future.done() or request is None:
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.PERMISSION_REPLY_NOT_FOUND,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                )
            )
            return "No pending permission request for this session."

        normalized_reply = reply_text.strip()
        if normalized_reply.startswith("/permission "):
            normalized_reply = normalized_reply[len("/permission ") :]
        elif normalized_reply.startswith("permission "):
            normalized_reply = normalized_reply[len("permission ") :]
        option_id = self._select_permission_option(request.options, normalized_reply)
        if option_id is None:
            return "Permission reply not understood. Reply with /permission <number>."

        future.set_result(option_id)
        return "Permission reply received."

    @staticmethod
    def _select_permission_option(
        options: list[ACPPermissionOption], reply_text: str
    ) -> str | None:
        """把用户回复映射到 ACP option_id；兼容编号、kind、allow/deny 等简写。"""

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

    @staticmethod
    def _render_permission_prompt(options: list[ACPPermissionOption]) -> str:
        """把权限选项渲染成可回复的提示文本；供 on_progress 与直接回显复用。"""
        lines = ["ACP requires permission. Reply with /permission <number>:"]
        for index, option in enumerate(options, start=1):
            kind = getattr(
                getattr(option, "kind", None), "value", getattr(option, "kind", "option")
            )
            label = getattr(option, "label", None) or getattr(option, "title", None) or kind
            lines.append(f"{index}. {label}")
        return "\n".join(lines)

    def materialize_final_outbound(self, *, partial: bool = False) -> OutboundMessage:
        """按 request-scope 聚合事实物化最终结果；partial 模式用于异常链路回退。"""

        content = self.request_scope.partial_text if partial else self.request_scope.final_text
        return OutboundMessage(
            channel=self.channel,
            chat_id=self.chat_id,
            content=content or "",
            media=list(self.request_scope.media_paths),
            metadata=dict(self.request_scope.final_metadata),
        )

    async def close(self) -> None:
        """关闭当前请求 state；尾刷全部池、取消权限等待并阻止后续写入。"""

        if self._closed:
            return
        self._closed = True
        if self._progress_router is not None:
            await self._progress_router.close()
        if (
            self._pending_permission_future is not None
            and not self._pending_permission_future.done()
        ):
            self._pending_permission_future.cancel()
