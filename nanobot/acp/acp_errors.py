"""ACP 错误分类辅助。"""

from __future__ import annotations


def _is_invalid_params_request_error(exc: Exception) -> bool:
    """识别 ACP JSON-RPC invalid params（兼容不同 SDK 异常类型实现）。"""
    # 中文注释：python-sdk 的 RequestError 常见 code=-32602。
    # 这里不强依赖具体异常类，避免与 SDK 细节强绑定。
    code = getattr(exc, "code", None)
    if code == -32602:
        return True
    return str(exc).strip().lower() == "invalid params"
