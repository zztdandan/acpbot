"""ACP 错误判定工具：集中维护协议异常的兼容识别逻辑。"""

from __future__ import annotations


def _is_invalid_params_request_error(exc: Exception) -> bool:
    """判断异常是否属于 invalid params；用于触发会话绑定失效回收。"""

    code = getattr(exc, "code", None)
    if code == -32602:
        return True
    return str(exc).strip().lower() == "invalid params"
