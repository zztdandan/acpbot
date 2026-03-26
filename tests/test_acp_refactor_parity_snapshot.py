from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_parity_report_has_no_diffs() -> None:
    report_path = Path(".tmp/acp-parity/parity_report.json")
    # 中文注释：测试在干净工作区也要可复现，缺少快照时自动调用脚本生成。
    if not report_path.exists():
        subprocess.run(
            [
                sys.executable,
                "nanobot/scripts/acp_parity_snapshot.py",
                "--output",
                str(report_path),
            ],
            check=True,
        )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["passed"] is True
