"""other handler：收口未识别 update，并确保它们落入 other 池后立即销毁。"""

from __future__ import annotations

from nanobot.acp.state.handlers.base import (
    HandlerConsumeResult,
    StateUpdateHandler,
    sanitize_json_value,
)
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import OtherPool


class OtherUpdateHandler(StateUpdateHandler):
    """未知更新处理器：兜底吸收所有未被命中的 session_update，并走 other 池销毁链路。"""

    name = "other"
    update_type = ACPUpdateType.OTHER
    bucket_type = ACPBucketType.OTHER

    def match(self, update: object) -> bool:
        """作为 registry 的最后一个兜底 handler，永远返回 True。"""

        del update
        return True

    def create_pool(self, *, bucket_key: str) -> OtherPool:
        """创建 other 池；该池在首次 accept 后立即进入终态。"""

        return OtherPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """优先使用 sessionUpdate 字面量；无字面量时退回类名。"""

        session_update = getattr(update, "session_update", None) or getattr(
            update, "sessionUpdate", None
        )
        label = str(session_update or type(update).__name__ or "unknown").strip()
        return f"other:{label}"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """记录未知更新痕迹，并让 router 立即销毁对应 other 池。"""

        payload = sanitize_json_value(update)
        pool.accept(payload)
        state_manager.append_other_update(label=self.build_bucket_key(update), payload=payload)
        return HandlerConsumeResult(destroy_pool_keys=[self.build_pool_key(update)])
