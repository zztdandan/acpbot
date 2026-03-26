from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_OUTPUT = Path(".tmp/acp-parity/parity_report.json")
STREAM_ORDER = ("inbound", "outbound", "tools")

_TS_KEYS = {"recordedAt", "timestamp", "ts", "created_at", "updated_at"}
_SESSION_KEYS = {"session_id", "sessionId"}
_PATH_KEYS = {"path", "file_path", "filePath", "abs_path", "workspace"}
_PID_KEYS = {"pid", "process_id", "processId"}
_RUN_ID_KEYS = {"runId", "run_id", "traceId", "trace_id"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate ACP parity snapshot report")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to parity_report.json",
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=None,
        help="Optional input root containing inbound/outbound/tools jsonl files",
    )
    return parser.parse_args()


def _resolve_input_root(output_path: Path, input_root: Path | None) -> Path:
    if input_root is not None:
        return input_root
    return output_path.parent


def _normalize_scalar(key: str, value: Any) -> Any:
    # 中文注释：时间戳在本地运行中不可稳定复现，统一替换为固定占位符。
    if key in _TS_KEYS:
        return "<TS>"
    # 中文注释：会话标识属于运行时信息，跨进程和跨机器都可能变化，必须脱敏。
    if key in _SESSION_KEYS:
        return "<SESSION_ID>"
    # 中文注释：路径会受到工作目录影响，绝对路径统一折叠为 WORKSPACE 占位前缀。
    if key in _PATH_KEYS and isinstance(value, str):
        if value.startswith("/"):
            return "<WORKSPACE>/..."
        return value
    # 中文注释：PID / runId 是典型非确定性字段，不参与行为等价对比。
    if key in _PID_KEYS:
        return "<PID>"
    if key in _RUN_ID_KEYS:
        return "<RUN_ID>"
    return value


def _normalize_node(node: Any) -> Any:
    if isinstance(node, dict):
        normalized: dict[str, Any] = {}
        for key in sorted(node.keys()):
            value = node[key]
            normalized[key] = _normalize_node(_normalize_scalar(key, value))
        return normalized
    if isinstance(node, list):
        return [_normalize_node(item) for item in node]
    return node


def _read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    if not path.exists():
        return events, errors

    for index, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"{path}:{index}: {exc.msg}")
            continue
        if not isinstance(payload, dict):
            errors.append(f"{path}:{index}: json line must be object")
            continue
        events.append(payload)
    return events, errors


def _collect_stream_files(input_root: Path) -> dict[str, list[Path]]:
    mapping = {
        "inbound": [input_root / "inbound.jsonl"],
        "outbound": [input_root / "outbound.jsonl"],
        "tools": sorted((input_root / "tools").glob("*.jsonl"))
        if (input_root / "tools").exists()
        else [],
    }
    return mapping


def _build_report(input_root: Path) -> dict[str, Any]:
    stream_files = _collect_stream_files(input_root)
    stream_summary: dict[str, Any] = {
        name: {"files": [], "event_count": 0} for name in STREAM_ORDER
    }
    normalized_events: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    seq = 1

    for stream_name in STREAM_ORDER:
        files = stream_files.get(stream_name, [])
        for path in files:
            events, errors = _read_jsonl(path)
            parse_errors.extend(errors)
            stream_summary[stream_name]["files"].append(str(path))
            for event in events:
                normalized = _normalize_node(event)
                # 中文注释：seq 是快照对齐主键；缺失时补齐，占位逻辑保持全局单调递增。
                normalized_seq = normalized.get("seq")
                if not isinstance(normalized_seq, int):
                    normalized["seq"] = seq
                seq += 1

                normalized_events.append(
                    {
                        "stream": stream_name,
                        "file": str(path),
                        "event": normalized,
                    }
                )
                stream_summary[stream_name]["event_count"] += 1

    passed = len(parse_errors) == 0
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "passed": passed,
        "diff_count": len(parse_errors),
        "diff_examples": parse_errors[:20],
        "normalization": {
            "seq_strategy": "use_existing_or_assign_monotonic_placeholder",
            "placeholder_fields": {
                "timestamps": sorted(_TS_KEYS),
                "session": sorted(_SESSION_KEYS),
                "path": sorted(_PATH_KEYS),
                "pid": sorted(_PID_KEYS),
                "run_id": sorted(_RUN_ID_KEYS),
            },
        },
        "summary": {
            "input_root": str(input_root),
            "total_events": len(normalized_events),
            "streams": stream_summary,
        },
        "events": normalized_events,
    }
    return report


def main() -> int:
    args = _parse_args()
    output_path: Path = args.output
    input_root = _resolve_input_root(output_path, args.input_root)
    report = _build_report(input_root)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    if report["passed"]:
        print(f"PASS: parity report generated at {output_path}")
        return 0

    print(f"FAIL: found {report['diff_count']} parse issue(s); see {output_path}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
