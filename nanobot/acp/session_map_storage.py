"""ACP session_map 持久化与 heartbeat 广播能力。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.acp.acp_factory import _acp_text_block
from nanobot.acp.state import _SessionCapabilities


class _SessionMapStorageMixin:
    """封装 session_map 本地存储和活跃会话 heartbeat 能力。"""

    _conn: Any = None
    _session_map: dict[str, str] = {}
    _session_desired: dict[str, dict[str, str]] = {}
    _session_activation_ensure_epoch: dict[str, int] = {}
    _session_map_file: Path = Path()
    _session_map_bootstrapped: bool = False
    _session_caps: dict[str, _SessionCapabilities] = {}
    acp_config: Any = None
    workspace: Path = Path()

    async def _ensure_connection(self) -> None:
        """由具体 Dispatcher 实现连接建立；mixin 只声明能力契约。"""
        raise NotImplementedError

    @staticmethod
    def _acp_text_block(content: str) -> Any:
        """按需导入 ACP 包，避免静态导入导致环境缺包时报错。"""
        return _acp_text_block(content)

    def _resolved_acp_cwd(self) -> str:
        """解析 ACP 实际运行 cwd（优先 acp_config.cwd，其次 workspace）。"""
        cwd = Path(self.acp_config.cwd).expanduser() if self.acp_config.cwd else self.workspace
        return str(cwd.resolve())

    @staticmethod
    def _session_map_entry(
        *,
        cwd: str,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        desired_model: str | None = None,
        desired_agent: str | None = None,
    ) -> dict[str, str]:
        """统一持久化 entry 的字段结构。"""
        entry = {
            "cwd": cwd,
            "nanobotSideSessionKey": nanobot_side_session_key,
            "acpSideSessionId": acp_side_session_id,
        }
        # 中文注释：desired* 是可选扩展字段；缺省不写入，确保旧格式读取/对比稳定。
        if isinstance(desired_model, str) and desired_model:
            entry["desiredModel"] = desired_model
        if isinstance(desired_agent, str) and desired_agent:
            entry["desiredAgent"] = desired_agent
        return entry

    def _resolve_desired_for_session(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> tuple[str | None, str | None]:
        """优先使用 session_key 的 desired，缺失时回落到 session capabilities。"""
        desired = self._session_desired.get(nanobot_side_session_key)
        if isinstance(desired, dict):
            desired_model = desired.get("model")
            desired_agent = desired.get("agent")
            model = desired_model if isinstance(desired_model, str) and desired_model else None
            agent = desired_agent if isinstance(desired_agent, str) and desired_agent else None
            if model or agent:
                return model, agent

        caps = self._session_caps.get(acp_side_session_id)
        if caps is None:
            return None, None
        desired_model = caps.current_model if isinstance(caps.current_model, str) else None
        desired_agent = caps.current_agent if isinstance(caps.current_agent, str) else None
        return (
            desired_model if desired_model else None,
            desired_agent if desired_agent else None,
        )

    def _load_session_map_from_disk(self) -> dict[str, str]:
        """只加载当前 cwd 的映射，避免不同实例互相污染。"""
        if not self._session_map_file.exists():
            logger.debug(
                "ACP session map load skipped: file not found path={} cwd={}",
                self._session_map_file,
                self._resolved_acp_cwd(),
            )
            return {}
        try:
            payload = json.loads(self._session_map_file.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("ACP session map load failed: {}", self._session_map_file)
            return {}
        if not isinstance(payload, dict):
            # 中文注释：兼容历史脏数据/人工误写（如顶层数组）；
            # 此时按空映射处理，避免启动阶段因结构异常直接崩溃。
            self._session_desired = {}
            logger.warning(
                "ACP session map load skipped: malformed top-level payload path={} payload_type={}",
                self._session_map_file,
                type(payload).__name__,
            )
            return {}

        current_cwd = self._resolved_acp_cwd()
        loaded: dict[str, str] = {}
        loaded_desired: dict[str, dict[str, str]] = {}
        mappings_payload = payload.get("mappings", [])
        if not isinstance(mappings_payload, list):
            mappings_payload = []
        for raw in mappings_payload:
            if not isinstance(raw, dict):
                continue
            if raw.get("cwd") != current_cwd:
                continue
            nanobot_side_session_key = raw.get("nanobotSideSessionKey")
            acp_side_session_id = raw.get("acpSideSessionId")
            if isinstance(nanobot_side_session_key, str) and isinstance(acp_side_session_id, str):
                if nanobot_side_session_key and acp_side_session_id:
                    loaded[nanobot_side_session_key] = acp_side_session_id
                    desired_model = raw.get("desiredModel")
                    desired_agent = raw.get("desiredAgent")
                    normalized_desired: dict[str, str] = {}
                    if isinstance(desired_model, str) and desired_model:
                        normalized_desired["model"] = desired_model
                    if isinstance(desired_agent, str) and desired_agent:
                        normalized_desired["agent"] = desired_agent
                    if normalized_desired:
                        loaded_desired[nanobot_side_session_key] = normalized_desired
                        # 中文注释：从磁盘恢复 desired，保证进程重启后首轮 prompt 仍可携带期望模型/agent。
                        caps = self._session_caps.setdefault(
                            acp_side_session_id, _SessionCapabilities()
                        )
                        if "model" in normalized_desired:
                            caps.current_model = normalized_desired["model"]
                        if "agent" in normalized_desired:
                            caps.current_agent = normalized_desired["agent"]
        self._session_desired = loaded_desired
        logger.debug(
            "ACP session map loaded path={} cwd={} loaded={} desired_loaded={} total_entries={}",
            self._session_map_file,
            current_cwd,
            len(loaded),
            len(loaded_desired),
            len(mappings_payload),
        )
        return loaded

    def _persist_session_map(self) -> None:
        """以原子替换方式回写当前 cwd 的映射。"""
        current_cwd = self._resolved_acp_cwd()
        preserved: list[dict[str, str]] = []
        if self._session_map_file.exists():
            try:
                payload = json.loads(self._session_map_file.read_text(encoding="utf-8"))
                for raw in payload.get("mappings", []):
                    if not isinstance(raw, dict):
                        continue
                    if raw.get("cwd") == current_cwd:
                        continue
                    if (
                        isinstance(raw.get("cwd"), str)
                        and isinstance(raw.get("nanobotSideSessionKey"), str)
                        and isinstance(raw.get("acpSideSessionId"), str)
                    ):
                        preserved.append(raw)
            except Exception:
                logger.exception(
                    "ACP session map read-before-write failed: {}", self._session_map_file
                )

        current_cwd_entries: list[dict[str, str]] = []
        for nanobot_side_session_key, acp_side_session_id in sorted(self._session_map.items()):
            desired_model, desired_agent = self._resolve_desired_for_session(
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
            )
            current_cwd_entries.append(
                self._session_map_entry(
                    cwd=current_cwd,
                    nanobot_side_session_key=nanobot_side_session_key,
                    acp_side_session_id=acp_side_session_id,
                    desired_model=desired_model,
                    desired_agent=desired_agent,
                )
            )
        mappings = preserved + current_cwd_entries
        # 中文注释：持久化采用 version 包裹，便于后续结构升级。
        data = {"version": 1, "mappings": mappings}
        self._session_map_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._session_map_file.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self._session_map_file)
        logger.debug(
            "ACP session map persisted path={} cwd={} active={} preserved_other_cwd={} total_written={}",
            self._session_map_file,
            current_cwd,
            len(self._session_map),
            len(preserved),
            len(mappings),
        )

    @staticmethod
    def _is_active_channel_session_key(nanobot_side_session_key: str) -> bool:
        """判断是否为渠道会话 key（过滤 cli/system/cron/heartbeat）。"""
        if ":" not in nanobot_side_session_key:
            return False
        prefix = nanobot_side_session_key.split(":", 1)[0].strip().lower()
        return prefix not in {"cli", "system", "cron", "heartbeat"}

    async def send_heartbeat_to_active_sessions(self, heartbeat_instruction: str) -> int:
        """向当前活跃渠道映射对应的 ACP 会话逐个发送 heartbeat 指令。"""
        await self._ensure_connection()
        if self._conn is None:
            raise RuntimeError("ACP connection is not available")
        active_pairs = [
            (nanobot_side_session_key, acp_side_session_id)
            for nanobot_side_session_key, acp_side_session_id in self._session_map.items()
            if self._is_active_channel_session_key(nanobot_side_session_key)
        ]
        delivered = 0
        for _, acp_side_session_id in active_pairs:
            try:
                await self._conn.prompt(
                    prompt=[self._acp_text_block(heartbeat_instruction)],
                    session_id=acp_side_session_id,
                )
                delivered += 1
            except Exception:
                logger.exception("ACP heartbeat prompt failed for session {}", acp_side_session_id)
        return delivered
