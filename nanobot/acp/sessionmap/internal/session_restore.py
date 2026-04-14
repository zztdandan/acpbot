"""ACP 会话恢复工具：resume_session 优先，load_session 兜底。

核心职责：
    提供统一的会话恢复入口，固定采用 resume_session -> load_session 的回退策略。

使用场景：
    - ensure_ready_session 检测到已有会话时调用本函数恢复
    - 对账流程中需要重新激活现有会话时调用本函数

设计约束：
    - 不同 ACP backend 可能只支持 resume_session 或 load_session
    - 当两者都存在时，优先使用 resume_session（更轻量）
"""

from __future__ import annotations

from typing import Awaitable, Callable, cast

from nanobot.acp.contracts import ACPSessionPayload


async def restore_existing_session(
    conn: object,
    *,
    cwd: str,
    session_id: str,
) -> ACPSessionPayload | None:
    """恢复现有 ACP 会话：resume_session 优先，load_session 兜底。

    处理流程：
        1. 尝试 resume_session：
           - 检测 conn 是否有 resume_session 方法
           - 有则调用并返回结果（resume 是更轻量的恢复路径）
        2. resume_session 缺失或抛出异常时回退到 load_session：
           - 检测 conn 是否有 load_session 方法
           - 有则调用并返回结果
        3. 两者都缺失时返回 None（表示无法恢复）
        4. 两者都失败时抛出最后一个异常（便于调用方统一记录）

    参数：
        conn: ACP 客户端连接对象（必须有 resume_session 或 load_session 方法）
        cwd: 工作区路径（传递给恢复方法的参数之一）
        session_id: 要恢复的 ACP 侧会话 ID

    返回：
        ACPSessionPayload | None: 恢复后的 session payload
                               - None 表示 conn 不支持任何恢复方法
                               - 成功时返回完整的 session payload

    兼容性说明：
        - 某些 ACP backend 只暴露 resume_session
        - 某些 ACP backend 只暴露 load_session
        - 当两者都存在时，nanobot/acp 固定优先 resume_session
    """

    resume_session = cast(
        Callable[..., Awaitable[ACPSessionPayload]] | None,
        getattr(conn, "resume_session", None),
    )
    load_session = cast(
        Callable[..., Awaitable[ACPSessionPayload]] | None,
        getattr(conn, "load_session", None),
    )
    if resume_session is None and load_session is None:
        return None

    ordered_calls: list[Callable[..., Awaitable[ACPSessionPayload]] | None] = [
        resume_session,
        load_session,
    ]

    last_exc: Exception | None = None
    for method in ordered_calls:
        if method is None:
            continue
        try:
            return await method(cwd=cwd, session_id=session_id)
        except Exception as exc:
            last_exc = exc

    if last_exc is not None:
        raise last_exc
    return None
