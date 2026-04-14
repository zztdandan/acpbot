"""处理器抽象与公共工具：统一更新匹配、池创建与载荷归一化约束。"""

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
    """处理器消费结果：描述一次消费后需要镜像和销毁的后续动作。

    职责：
        - 收集本次消费产生的刷新结果，交给路由器统一镜像
        - 声明哪些池需要在消费后立即销毁，避免处理器直接操作索引
    """

    flush_results: list[FlushResult | None] = field(
        default_factory=list
    )  # 本次消费产生的待镜像结果；空值会由路由器过滤。
    destroy_pool_keys: list[PoolKey] = field(
        default_factory=list
    )  # 需要立即销毁的池主键；适用于一次性池。


class StateUpdateHandler(ABC):
    """状态更新处理器基类：约束单类更新如何映射到池与请求级聚合。

    职责：
        - 定义更新匹配规则与池创建规则
        - 把具体更新的消费逻辑封装为独立处理器，避免管理器膨胀
    """

    name: str
    update_type: ACPUpdateType
    bucket_type: ACPBucketType

    @abstractmethod
    def match(self, update: object) -> bool:
        """判断当前处理器是否负责该更新；注册表会按顺序依次尝试。"""

    @abstractmethod
    def create_pool(self, *, bucket_key: str) -> ACPPool:
        """创建该处理器对应的池实例；只在索引缺失时由路由器调用。"""

    @abstractmethod
    def build_bucket_key(self, update: object) -> str:
        """为当前更新计算池索引键；不同处理器可以定义不同隔离粒度。"""

    @abstractmethod
    def consume(
        self,
        *,
        state_manager: SessionStateManager,
        update: object,
        pool: ACPPool,
    ) -> HandlerConsumeResult:
        """消费更新并写入池或聚合态；路由器会根据返回值执行镜像与销毁。"""

    def build_pool_key(self, update: object) -> PoolKey:
        """把 `bucket_type` 与 `bucket_key` 组合为统一索引主键；供管理器持有。"""

        return PoolKey(bucket_type=self.bucket_type, bucket_key=self.build_bucket_key(update))


class _ModelDumpCapable(Protocol):
    """支持 `model_dump` 的对象协议：用于在状态域内安全读取 Pydantic 模型。"""

    def model_dump(self, *, by_alias: bool, exclude_none: bool) -> dict[str, object]: ...


def sanitize_json_value(value: object) -> JSONValue:
    """把任意载荷规整为 `JSONValue`；供最终元数据与调试痕迹复用。

    处理流程：
        - 原始标量直接返回，保持值语义不变
        - 支持 `model_dump` 的对象先转字典，再递归规整嵌套字段
        - 容器类型递归处理，其他对象最终退化为字符串表示
    """

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
    """把任意对象规整为 `JSONMap`；非映射对象会包成 `{"value": ...}` 结构。"""

    normalized = sanitize_json_value(value)
    if isinstance(normalized, dict):
        return normalized
    return {"value": normalized}
