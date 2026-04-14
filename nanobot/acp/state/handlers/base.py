"""handler 抽象与公共工具：统一 update 匹配、池创建和 payload 归一化约束。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

from nanobot.acp.contracts import JSONMap, JSONValue
from nanobot.acp.state.models import ACPBucketType, ACPPool, ACPUpdateType, FlushResult, PoolKey

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager


@dataclass(slots=True)
class HandlerConsumeResult:
    """handler 消费结果：告诉 router 需要镜像哪些 flush，以及是否要销毁某些池。"""

    flush_results: list[FlushResult | None] = field(default_factory=list)
    # 本次消费产生的待镜像结果；None 会由 router 过滤。
    destroy_pool_keys: list[PoolKey] = field(default_factory=list)
    # 需要立即销毁的池主键；适用于 other / consume-only 一次性池。


class StateUpdateHandler(ABC):
    """state 更新处理器基类：负责把一种 update 映射到对应池与 request-scope 聚合。"""

    name: str
    update_type: ACPUpdateType
    bucket_type: ACPBucketType

    @abstractmethod
    def match(self, update: object) -> bool:
        """判断当前 handler 是否负责该 update；registry 按注册顺序依次尝试。"""

    @abstractmethod
    def create_pool(self, *, bucket_key: str) -> ACPPool:
        """创建该 handler 对应的池实例；只在索引缺失时由 router 调用。"""

    @abstractmethod
    def build_bucket_key(self, update: object) -> str:
        """为当前 update 计算池索引键；不同 handler 可以定义不同粒度。"""

    @abstractmethod
    def consume(
        self,
        *,
        state_manager: SessionStateManager,
        update: object,
        pool: ACPPool,
    ) -> HandlerConsumeResult:
        """消费 update 并写入池/聚合态；router 根据返回值执行镜像与销毁。"""

    def build_pool_key(self, update: object) -> PoolKey:
        """把 bucket_type 与 bucket_key 组合为统一索引主键；供 manager 持有。"""

        return PoolKey(bucket_type=self.bucket_type, bucket_key=self.build_bucket_key(update))


class _ModelDumpCapable(Protocol):
    """支持 model_dump 的对象协议：用于在 state 内安全读取 Pydantic 模型。"""

    def model_dump(self, *, by_alias: bool, exclude_none: bool) -> dict[str, object]: ...


def sanitize_json_value(value: object) -> JSONValue:
    """把任意 update 载荷规整为 JSONValue；供 final_metadata 与调试痕迹复用。"""

    if value is None or isinstance(value, str | int | float | bool):
        return value
    if hasattr(value, "model_dump"):
        dumped = cast(_ModelDumpCapable, value).model_dump(by_alias=True, exclude_none=True)
        return sanitize_json_value(dumped)
    if isinstance(value, dict):
        return {str(key): sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [sanitize_json_value(item) for item in value]
    return str(value)


def sanitize_json_map(value: object) -> JSONMap:
    """把任意对象规整为 JSONMap；非映射对象会包成 `{"value": ...}` 结构。"""

    normalized = sanitize_json_value(value)
    if isinstance(normalized, dict):
        return normalized
    return {"value": normalized}
