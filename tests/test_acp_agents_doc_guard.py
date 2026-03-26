from __future__ import annotations

from pathlib import Path


def test_acp_agents_doc_mentions_family_split_rules() -> None:
    # 这个守卫测试保证 ACP 文档中持续保留“按 family 拆分”的硬性约束。
    agents_doc = Path(__file__).resolve().parents[1] / "nanobot" / "acp" / "AGENTS.md"
    content = agents_doc.read_text(encoding="utf-8")

    # 覆盖 Task 7 约定的五条规则，避免后续重构时文档约束被意外删除。
    required_fragments = [
        "session_update_*",
        "progress_*",
        "media_codec_*",
        "observability_*",
        "session_map_*",
        "`*_router.py` 与 dispatch 路由文件只做分发",
        "必须保留中文注释",
        "行数阈值只作为“可读性风险信号”",
    ]

    for fragment in required_fragments:
        assert fragment in content, f"AGENTS.md missing rule fragment: {fragment}"
