"""该模块承接重构后的职责边界。"""

from __future__ import annotations


def _is_invalid_params_request_error(exc: Exception) -> bool:
    """执行该方法定义的处理流程并返回结果。"""

    code = getattr(exc, "code", None)
    if code == -32602:
        return True
    return str(exc).strip().lower() == "invalid params"
