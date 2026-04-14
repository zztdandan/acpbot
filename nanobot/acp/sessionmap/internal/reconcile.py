"""ACP 会话 ID 提取与分页拉取：用于 sessionmap 启动时的大对账。

核心职责：
    提供两个核心工具函数：
    - extract_acp_side_session_ids: 从嵌套的 ACP payload 递归提取所有 session_id
    - fetch_acp_side_session_ids: 分页拉取 ACP 侧所有会话 ID（含参数兼容性处理）

使用场景：
    - SessionMapBindingManager.load_persistent_truth 调用 fetch_acp_side_session_ids
      获取 ACP 侧真实会话列表，与磁盘持久化绑定对账（删除失效绑定）
    - fetch_acp_side_session_ids 内部调用 extract_acp_side_session_ids 解析响应 payload

设计约束：
    - extract_acp_side_session_ids 支持多种 payload 格式（dict、list、Pydantic 模型）
    - fetch_acp_side_session_ids 兼容不同 ACP SDK 版本的 list_sessions 参数签名
    - 支持分页拉取、cursor 循环检测、异常回退策略
"""

from __future__ import annotations

from typing import Awaitable, Callable, cast

from nanobot.acp.contracts import ACPSessionPayload


def extract_acp_side_session_ids(payload: ACPSessionPayload) -> set[str]:
    """从嵌套的 ACP payload 递归提取所有 session_id（兼容多种数据结构）。

    核心职责：
        深度遍历任意嵌套的 payload 结构（dict、list、Pydantic 模型、普通对象），
        提取所有符合条件的 session_id 字段值，返回去重后的集合。

    使用场景：
        - fetch_acp_side_session_ids 解析 ACP list_sessions 响应时提取所有会话 ID
        - SessionMapBindingManager 对账时比对磁盘绑定与 ACP 侧真实会话列表

    处理流程：
        1. Pydantic 模型检测：
           - 检测到 model_dump 方法时尝试转换为 dict
           - 转换失败时回退到普通对象处理（向后兼容旧 SDK）
        2. dict 类型处理：
           - 检查 session_id/sessionId/id 键（兼容不同字段命名风格）
           - 提取符合条件的值（非空字符串）并加入集合
           - 递归遍历所有 value（支持嵌套 dict/list）
        3. 集合类型处理（list/tuple/set）：
           - 遍历每个元素并递归调用 _walk
        4. 普通对象处理：
           - 检查 session_id/sessionId/id 属性
           - 递归遍历常见集合属性（sessions/data/items）

    参数：
        payload: 任意嵌套结构的 ACP payload
                 来源：ACP client.list_sessions() 或 client.get_session() 的响应
                 类型：dict、list、Pydantic 模型或普通对象（通过 _walk 递归兼容）

    返回：
        set[str]: 所有提取到的 session_id 集合（去重）
                 - 空集合表示 payload 中无 session_id
                 - 返回顺序不保证（使用 set 去重）

    使用示例：
        示例 1: dict 结构
            payload = {"sessions": [{"session_id": "abc123"}, {"sessionId": "def456"}]}
            ids = extract_acp_side_session_ids(payload)
            # ids = {"abc123", "def456"}

        示例 2: list 结构
            payload = [{"id": "abc123"}, {"id": "def456"}]
            ids = extract_acp_side_session_ids(payload)
            # ids = {"abc123", "def456"}

    支持的字段名（按优先级）：
        - session_id (snake_case，常见于新 SDK)
        - sessionId (camelCase，常见于旧 SDK 或 JSON 规范)
        - id (通用字段，可能与其他类型冲突，优先级最低)

    注意事项：
        - 只提取字符串类型且非空的值（过滤 None、int、空字符串等）
        - 支持任意深度嵌套（如 sessions[0].data.items[0].session_id）
        - 递归遍历时遇到 None 值静默跳过（不影响其他分支）
        - Pydantic 模型转换失败时不会抛出异常（防御性处理）
    """
    ids: set[str] = set()

    def _walk(value: ACPSessionPayload) -> None:
        """递归遍历嵌套结构并提取 session_id（内部辅助函数）。"""
        if value is None:
            return

        # 策略 1: Pydantic 模型 -> 转换为 dict
        if hasattr(value, "model_dump"):
            try:
                _walk(value.model_dump(by_alias=True, exclude_none=True))
                return
            except Exception:
                # 如果转换失败，回退到普通对象处理
                pass

        # 策略 2: dict 类型 -> 检查标准键名并递归遍历 values
        if isinstance(value, dict):
            for key in ("session_id", "sessionId", "id"):
                session_id = value.get(key)
                if isinstance(session_id, str) and session_id:
                    ids.add(session_id)
            # 递归遍历所有 value（可能嵌套 dict/list）
            for nested in value.values():
                _walk(nested)
            return

        # 策略 3: 集合类型 -> 遍历每个元素
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _walk(item)
            return

        # 策略 4: 普通对象 -> 检查标准属性名
        for key in ("session_id", "sessionId", "id"):
            session_id = getattr(value, key, None)
            if isinstance(session_id, str) and session_id:
                ids.add(session_id)

        # 递归遍历常见集合属性
        for attr in ("sessions", "data", "items"):
            nested = getattr(value, attr, None)
            if nested is not None:
                _walk(nested)

    _walk(payload)
    return ids


async def fetch_acp_side_session_ids(conn: object, *, cwd: str) -> tuple[set[str], bool]:
    """分页拉取 ACP 侧所有会话 ID（兼容不同 SDK 参数签名）。

    处理流程：
        1. 检查 conn 是否有 list_sessions 方法，无则返回空集合
        2. 分页循环调用 list_sessions：
           - 初始调用使用 cwd 参数
           - 后续调用使用 cursor 参数（从上一次响应的 next_cursor 获取）
           - 终止条件：next_cursor 为空/重复、或抛出异常
        3. 参数兼容性处理（attempts 策略）：
           - 按优先级尝试 {cwd, cursor} -> {cwd} -> {cursor} -> {}
           - 某个尝试抛出 TypeError 时记录异常并尝试下一个
           - 所有尝试都失败时抛出最后一个 TypeError
        4. 回退策略：
           - 第一次调用（带 cwd）失败或返回空结果时，回退到无 cwd 调用
           - 回退成功仍然返回成功标志

    参数：
        conn: ACP 客户端连接对象（必须有 list_sessions 方法）
        cwd: 工作区路径（传递给 list_sessions 的参数之一）

    返回：
        tuple[set[str], bool]:
            - set[str]: 所有找到的 session_id 集合（去重）
            - bool: 是否至少有一次调用成功（用于判断连接可用性）
                    True 表示 ACP 连接可用，False 表示连接不可用或不支持 list_sessions

    兼容性说明：
        不同版本的 ACP SDK 可能接受不同的参数组合：
        - 旧版本：list_sessions() 无参数
        - 新版本：list_sessions(cwd="...", cursor="...")
        - 某些版本：只接受 cwd 或只接受 cursor

    使用场景：
        SessionMapBindingManager.load_persistent_truth 调用本函数获取 ACP 侧真实会话列表，
        与磁盘持久化绑定对账（删除失效绑定）。
    """
    # 检查 conn 是否有 list_sessions 方法
    list_sessions = cast(
        Callable[..., Awaitable[ACPSessionPayload]] | None,
        getattr(conn, "list_sessions", None),
    )
    if list_sessions is None:
        # ACP 客户端不支持 list_sessions，返回空集合
        return set(), False

    async def _collect(cwd_arg: str | None) -> set[str]:
        """分页收集所有会话 ID。"""
        collected: set[str] = set()
        cursor: str | None = None  # 分页游标
        seen_cursors: set[str] = set()  # 已见过的游标（检测循环）

        while True:
            # 构建参数尝试列表（按优先级排序）
            attempts: list[dict[str, str]] = []
            if cwd_arg is not None and cursor is not None:
                attempts.append({"cwd": cwd_arg, "cursor": cursor})
            if cwd_arg is not None:
                attempts.append({"cwd": cwd_arg})
            if cursor is not None:
                attempts.append({"cursor": cursor})
            attempts.append({})  # 兜底：无参数

            # 尝试调用 list_sessions
            response: ACPSessionPayload | None = None
            last_exc: Exception | None = None
            for kwargs in attempts:
                try:
                    response = await list_sessions(**kwargs)
                    break  # 成功则跳出尝试循环
                except TypeError as exc:
                    # 记录 TypeError 并尝试下一个参数组合
                    last_exc = exc

            # 如果所有尝试都失败
            if response is None:
                if last_exc is not None:
                    # 抛出最后一个 TypeError（帮助调试）
                    raise last_exc
                # 理论上不会发生（至少有一个尝试会成功），防御性调用
                response = await list_sessions()

            # 从响应中提取 session_id
            collected.update(extract_acp_side_session_ids(response))

            # 解析 next_cursor（支持不同命名风格）
            next_cursor = getattr(response, "next_cursor", None)
            if next_cursor is None and isinstance(response, dict):
                next_cursor = response.get("nextCursor") or response.get("next_cursor")

            # 检查终止条件
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                break  # 无更多页面或检测到循环

            # 记录游标并继续下一页
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return collected

    # 第一次尝试：带 cwd 参数
    merged: set[str] = set()
    any_success = False
    try:
        merged.update(await _collect(cwd))
        any_success = True
    except Exception:
        # 第一次失败，不抛出异常，尝试回退策略
        pass

    # 回退策略：如果第一次返回空结果，尝试无 cwd 调用
    if not merged:
        try:
            merged.update(await _collect(None))
            any_success = True
        except Exception:
            # 回退也失败，返回空集合和失败标志
            pass

    return merged, any_success
