"""permission handler：让权限请求与回复也走统一 handler/pool/router 收口。"""

from __future__ import annotations

from typing import cast

from nanobot.acp.contracts import (
    ACP_META_KIND,
    ACP_META_KIND_PERMISSION,
    ACP_META_PERMISSION_REQUEST_ID,
    ACP_META_PROGRESS,
    ACP_META_RENDER_AS,
    ACP_META_RENDER_AS_PERMISSION_REPLY,
    ACP_META_RENDER_AS_PERMISSION_REQUEST,
    JSONMap,
)
from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, ACPUpdateType, FlushResult
from nanobot.acp.state.permission_coordinator import PermissionCoordinator
from nanobot.acp.state.permission_events import PermissionReplyEvent, PermissionRequestEvent
from nanobot.acp.state.pools import PermissionPool
from nanobot.acp.state.pools.base import ACPPoolBase


class PermissionHandler(StateUpdateHandler):
    """权限处理器：统一处理 permission request/reply 的池写入、刷新与销毁。"""

    name = "permission"
    update_type = ACPUpdateType.PERMISSION_REQUEST
    bucket_type = ACPBucketType.PERMISSION

    def match(self, update: object) -> bool:
        """匹配权限请求事件与权限回复事件。"""

        return isinstance(update, PermissionRequestEvent | PermissionReplyEvent)

    def create_pool(self, *, bucket_key: str) -> PermissionPool:
        """创建权限池；一个请求内当前只保留一个活跃权限池。"""

        return PermissionPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """权限池固定使用单一主键；新请求覆盖旧提示，回复负责销毁。"""

        del update
        return "permission"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """根据事件类型刷新权限提示或消费权限回复，并在结束时销毁权限池。"""

        typed_coordinator = cast(PermissionCoordinator, state_manager.permission_coordinator)
        typed_pool = cast(PermissionPool, pool)
        if isinstance(update, PermissionRequestEvent):
            # 必须先写池，确保 permission 池启动 deadhand 计时；随后再即时上发提示。
            typed_pool.accept(update.pending_request.prompt_text)
            request_id = update.pending_request.request_id or ""
            metadata = cast(
                JSONMap,
                {
                    ACP_META_KIND: ACP_META_KIND_PERMISSION,
                    ACP_META_PROGRESS: True,
                    ACP_META_PERMISSION_REQUEST_ID: request_id,
                    ACP_META_RENDER_AS: ACP_META_RENDER_AS_PERMISSION_REQUEST,
                },
            )
            typed_pool.update_metadata(metadata=metadata)
            outbound = self.build_progress_outbound(
                state_manager=state_manager,
                flush_result=FlushResult(
                    kind=ACPOutboundKind.PERMISSION,
                    content=update.pending_request.prompt_text,
                    metadata=metadata,
                ),
            )
            if outbound is not None:
                state_manager.schedule_progress_outbound(outbound=outbound)
            return HandlerConsumeResult()
        if isinstance(update, PermissionReplyEvent):
            typed_update = cast(PermissionReplyEvent, update)
            request_id = typed_coordinator.current_request_id() or ""
            resolved = typed_coordinator.resolve_permission_reply(typed_update.reply_text)
            if resolved == "not_found":
                return HandlerConsumeResult()
            if resolved == "invalid":
                return HandlerConsumeResult()
            normalized_reply = typed_update.reply_text.strip()
            if normalized_reply:
                # 把原始 reply 文本完整写入池，便于 flush metadata/文本保留更丰富上下文。
                typed_pool.accept(f"Permission reply: {normalized_reply}")
            else:
                typed_pool.accept("Permission reply: <empty>")
            typed_pool.update_metadata(
                metadata=cast(
                    JSONMap,
                    {
                        ACP_META_KIND: ACP_META_KIND_PERMISSION,
                        ACP_META_PROGRESS: True,
                        ACP_META_PERMISSION_REQUEST_ID: request_id,
                        ACP_META_RENDER_AS: ACP_META_RENDER_AS_PERMISSION_REPLY,
                    },
                )
            )
            typed_pool.mark_terminal()
            return HandlerConsumeResult(immediate_finalize=True)
        return HandlerConsumeResult()

    def do_deadhand_operate_and_build_progress_outbound(self, *, state_manager, pool):
        """permission 死手到点后自动拒绝并同步上发一条超时通知。"""

        typed_coordinator = cast(PermissionCoordinator, state_manager.permission_coordinator)
        typed_pool = cast(PermissionPool, pool)
        request_id = typed_coordinator.current_request_id() or ""
        typed_coordinator.trigger_timeout()
        typed_pool.accept("Permission request timed out and was automatically rejected.")
        typed_pool.update_metadata(
            metadata=cast(
                JSONMap,
                {
                    ACP_META_KIND: ACP_META_KIND_PERMISSION,
                    ACP_META_PROGRESS: True,
                    ACP_META_PERMISSION_REQUEST_ID: request_id,
                    ACP_META_RENDER_AS: ACP_META_RENDER_AS_PERMISSION_REPLY,
                },
            )
        )
        cast(ACPPoolBase, typed_pool).mark_terminal()
        flush_result = typed_pool.flush()
        if flush_result is None:
            flush_result = FlushResult(
                kind=ACPOutboundKind.PERMISSION,
                content="Permission request timed out and was automatically rejected.",
                metadata={
                    ACP_META_KIND: ACP_META_KIND_PERMISSION,
                    ACP_META_PROGRESS: True,
                    ACP_META_PERMISSION_REQUEST_ID: request_id,
                    ACP_META_RENDER_AS: ACP_META_RENDER_AS_PERMISSION_REPLY,
                },
            )
        return self.build_progress_outbound(
            state_manager=state_manager,
            flush_result=flush_result,
        )
