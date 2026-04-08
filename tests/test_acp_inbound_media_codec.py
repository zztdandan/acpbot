from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from loguru import logger

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig


def _make_dispatcher(tmp_path: Path, *, inbound_media_dir: str | None = None) -> ACPDispatcher:
    data_root = tmp_path / ".nanobot" / "config"
    data_root.mkdir(parents=True, exist_ok=True)
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    config = ACPBackendConfig()
    if inbound_media_dir is not None:
        config.inbound_media_dir = inbound_media_dir
    dispatcher = ACPDispatcher(bus=MessageBus(), workspace=workspace, acp_config=config)
    # 中文注释：该单测只验证 media 规范化契约，不依赖外部 acp SDK 包。
    dispatcher._acp_text_block = lambda content: SimpleNamespace(type="text", text=content)  # type: ignore[method-assign]  # noqa: SLF001
    dispatcher._acp_resource_link_block = (  # type: ignore[method-assign]  # noqa: SLF001
        lambda name, uri, *, mime_type=None, size=None: SimpleNamespace(
            type="resource_link",
            name=name,
            uri=uri,
            mime_type=mime_type,
            size=size,
        )
    )
    return dispatcher


def _block_types(blocks: list[object]) -> list[str]:
    return [str(getattr(block, "type", "")) for block in blocks]


def test_build_inbound_prompt_blocks_uses_resource_link_for_workspace_file(tmp_path: Path) -> None:
    dispatcher = _make_dispatcher(tmp_path)
    doctor = dispatcher.workspace / "doctor.txt"
    doctor.write_text("doctor inbound", encoding="utf-8")

    blocks = dispatcher._build_inbound_prompt_blocks(  # noqa: SLF001
        "Read the attachment",
        [str(doctor)],
        session_key="session-acp-ft",
        channel="telegram",
    )

    assert _block_types(blocks) == ["text", "resource_link"]
    assert getattr(blocks[1], "name", None) == "doctor.txt"
    assert getattr(blocks[1], "uri", None) == doctor.resolve().as_uri()


def test_build_inbound_prompt_blocks_accepts_file_uri_inside_workspace(tmp_path: Path) -> None:
    dispatcher = _make_dispatcher(tmp_path)
    doctor = dispatcher.workspace / "doctor.txt"
    doctor.write_text("doctor inbound", encoding="utf-8")

    blocks = dispatcher._build_inbound_prompt_blocks(  # noqa: SLF001
        "Read the attachment",
        [doctor.resolve().as_uri()],
        session_key="session-acp-ft",
        channel="telegram",
    )

    assert _block_types(blocks) == ["text", "resource_link"]
    assert getattr(blocks[1], "name", None) == "doctor.txt"
    assert getattr(blocks[1], "uri", None) == doctor.resolve().as_uri()


def test_build_inbound_prompt_blocks_drops_missing_or_external_media_and_logs_error(
    tmp_path: Path,
) -> None:
    dispatcher = _make_dispatcher(tmp_path)
    external = tmp_path / "external.txt"
    external.write_text("outside workspace", encoding="utf-8")
    missing = dispatcher.workspace / "missing.txt"

    records: list[str] = []
    sink_id = logger.add(lambda message: records.append(str(message)), level="ERROR")
    try:
        blocks = dispatcher._build_inbound_prompt_blocks(  # noqa: SLF001
            "Read the attachment",
            [str(external), str(missing)],
            session_key="session-acp-ft",
            channel="telegram",
        )
    finally:
        logger.remove(sink_id)

    joined = "\n".join(records)
    assert _block_types(blocks) == ["text"]
    assert "rejected_media_type=str" in joined
    assert "reason=outside_workspace" in joined
    assert "reason=missing_file" in joined
    assert "session_key=session-acp-ft" in joined
    assert "channel=telegram" in joined
    assert "summary=path=" in joined


def test_build_inbound_prompt_blocks_never_emits_resource_or_image_for_ft_path(
    tmp_path: Path,
) -> None:
    dispatcher = _make_dispatcher(tmp_path)
    text_file = dispatcher.workspace / "doctor.txt"
    image_file = dispatcher.workspace / "sample.png"
    text_file.write_text("doctor inbound", encoding="utf-8")
    image_file.write_bytes(b"\x89PNG\r\n\x1a\n")

    blocks = dispatcher._build_inbound_prompt_blocks(  # noqa: SLF001
        "Read the attachment",
        [str(text_file), str(image_file)],
        session_key="session-acp-ft",
        channel="telegram",
    )

    assert _block_types(blocks) == ["text", "resource_link", "resource_link"]
    assert "resource" not in _block_types(blocks)
    assert "image" not in _block_types(blocks)


def test_inbound_media_dir_resolution_stays_under_workspace(tmp_path: Path) -> None:
    dispatcher = _make_dispatcher(tmp_path, inbound_media_dir="../escape")

    resolved = dispatcher._resolve_inbound_media_dir()  # noqa: SLF001

    assert resolved.is_relative_to(dispatcher.workspace.resolve())
    assert resolved == (
        dispatcher.workspace.resolve() / "Download" / "channel-inbound" / "acp-dispatch"
    )
