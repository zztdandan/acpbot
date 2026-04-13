"""ACP 会话 ID 提取与分页拉取：用于 sessionmap 启动时的大对账。"""

from __future__ import annotations

from typing import Awaitable, Callable, cast

from nanobot.acp.contracts import ACPSessionPayload


def extract_acp_side_session_ids(payload: ACPSessionPayload) -> set[str]:
    """从嵌套的 ACP 载荷中递归提取所有 session_id。

    处理流程：
        1. Pydantic 模型 -> model_dump 转为 dict
        2. dict -> 检查 session_id/sessionId/id 键，递归遍历 values
        3. list/tuple/set -> 遍历每个元素
        4. 普通对象 -> 检查 session_id/sessionId/id 属性，递归遍历 sessions/data/items
    """
    ids: set[str] = set()

    def _walk(value: ACPSessionPayload) -> None:
        """递归遍历嵌套结构并提取 session_id。"""
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
    """分页拉取 ACP 侧所有会话 ID 并返回。

    核心职责：
        调用 ACP client.list_sessions() 分页获取所有会话，提取 session_id 集合。

    参数：
        conn: ACP 客户端连接对象（必须有 list_sessions 方法）
        cwd: 工作区路径（传递给 list_sessions 的参数之一）

    返回：
        tuple[set[str], bool]:
            - set[str]: 所有找到的 session_id 集合
            - bool: 是否至少有一次调用成功（用于判断连接可用性）

    分页拉取策略：
        1. 初始调用：使用 cwd 参数（如果提供）
        2. 后续调用：使用 cursor 参数（从上一次响应的 next_cursor 获取）
        3. 终止条件：
           - next_cursor 为 None/空字符串
           - next_cursor 重复（检测到循环）
           - list_sessions 抛出异常

    参数兼容性处理：
        不同版本的 ACP SDK 可能接受不同的参数组合：
        - 旧版本：list_sessions() 无参数
        - 新版本：list_sessions(cwd="...", cursor="...")
        - 某些版本：只接受 cwd 或只接受 cursor

        此函数使用 attempts 策略，按优先级尝试：
        1. {cwd, cursor}: 同时提供 cwd 和 cursor（最完整）
        2. {cwd}: 只提供 cwd
        3. {cursor}: 只提供 cursor
        4. {}: 无参数（兜底）

        如果某个尝试抛出 TypeError，记录异常并尝试下一个。
        如果所有尝试都失败，抛出最后一个 TypeError。

    回退策略：
        如果第一次调用（带 cwd）失败或返回空结果：
        - 回退到无 cwd 调用（_collect(None)）
        - 如果回退成功，仍然返回成功标志

    使用场景：
        - sessionmap 启动时的大对账（reconcile）
        - 持久化真相加载（load_persistent_truth）
        - 会话绑定验证（确保本地映射与 ACP 侧一致）

    示例：
        session_ids, success = await fetch_acp_side_session_ids(conn, cwd="/path/to/workspace")
        if success:
            print(f"成功获取 {len(session_ids)} 个会话 ID")
        else:
            print("获取失败（可能是连接问题或 ACP 不支持 list_sessions）")
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
