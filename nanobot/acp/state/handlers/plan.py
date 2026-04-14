"""plan handler：收口 plan 更新，维护计划摘要与结构化 final_metadata。"""

from __future__ import annotations

from typing import cast

from acp.schema import AgentPlanUpdate

from nanobot.acp.state.handlers.base import (
    HandlerConsumeResult,
    StateUpdateHandler,
    sanitize_json_value,
)
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import PlanPool


class PlanUpdateHandler(StateUpdateHandler):
    """计划更新处理器：处理 ACP `plan` 更新并生成可读摘要。"""

    name = "plan"
    update_type = ACPUpdateType.PLAN
    bucket_type = ACPBucketType.PLAN

    def match(self, update: object) -> bool:
        """只匹配 AgentPlanUpdate。"""

        return isinstance(update, AgentPlanUpdate)

    def create_pool(self, *, bucket_key: str) -> PlanPool:
        """创建计划池；同一请求内仅保留一个当前计划快照。"""

        return PlanPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """使用固定 plan 键；每次更新覆盖整份计划快照。"""

        del update
        return "plan"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """写入计划摘要，并同步完整计划到 final_metadata。"""

        typed_update = cast(AgentPlanUpdate, update)
        summary = self._build_plan_summary(typed_update)
        pool.accept(summary)
        state_manager.update_named_metadata("plan", sanitize_json_value(typed_update))
        return HandlerConsumeResult(flush_results=[pool.flush()])

    @staticmethod
    def _build_plan_summary(update: AgentPlanUpdate) -> str:
        """把完整 plan 转成紧凑的可读摘要，便于 on_progress 镜像。"""

        lines = ["Plan updated:"]
        for index, entry in enumerate(list(getattr(update, "entries", []) or []), start=1):
            status = getattr(getattr(entry, "status", None), "value", getattr(entry, "status", ""))
            priority = getattr(
                getattr(entry, "priority", None),
                "value",
                getattr(entry, "priority", ""),
            )
            content = getattr(entry, "content", "")
            lines.append(f"{index}. [{status}/{priority}] {content}")
        return "\n".join(lines)
