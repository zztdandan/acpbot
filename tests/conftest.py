from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _print_test_runtime_dir(request: pytest.FixtureRequest) -> None:
    """在每条用例开始时打印本轮测试目录，便于人工核对运行态文件。"""

    if "tmp_path" in request.fixturenames:
        tmp_path = request.getfixturevalue("tmp_path")
        print(f"[测试目录] {request.node.nodeid} -> {tmp_path}", flush=True)
        return

    # 对未声明 tmp_path 的测试，回退打印当前工作目录，保证每轮都有可追踪路径。
    print(f"[测试目录] {request.node.nodeid} -> (no tmp_path fixture)", flush=True)
