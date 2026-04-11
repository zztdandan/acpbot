"""Configuration schema using Pydantic."""

from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel
from pydantic_settings import BaseSettings

from nanobot.cron.types import CronSchedule


class Base(BaseModel):
    """Base model that accepts both camelCase and snake_case keys."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class ChannelsConfig(Base):
    """Configuration for chat channels.

    Built-in and plugin channel configs are stored as extra fields (dicts).
    Each channel parses its own config in __init__.
    Per-channel "streaming": true enables streaming output (requires send_delta impl).
    """

    model_config = ConfigDict(extra="allow")

    send_progress: bool = True  # stream agent's text progress to the channel
    send_tool_hints: bool = False  # stream tool-call hints (e.g. read_file("…"))
    # 中文注释：ACP/native 共用开关，控制是否发送 final 消息，便于避免上下游重复展示。
    send_final: bool = True
    # 中文注释：tool hint 发布策略。immediate=逐条发布；merge_by_tool_call_id=按调用聚合。
    tool_hint_publish_mode: Literal["immediate", "merge_by_tool_call_id"] = "immediate"
    # 中文注释：merge_by_tool_call_id 模式下单个 tool call 的空闲刷出阈值（秒）。
    tool_hint_merge_idle_seconds: float = 5.0
    # 中文注释：tool hint 内容编码形式。
    tool_hint_payload_mode: Literal["array", "status_with_compact"] = "array"
    # 中文注释：status_with_compact 模式下判定“终态”的状态集合。
    tool_hint_terminal_statuses: list[str] = Field(default_factory=lambda: ["completed", "failed"])
    send_max_retries: int = Field(
        default=3, ge=0, le=10
    )  # Max delivery attempts (initial send included)
    transcription_provider: str = "groq"  # Voice transcription backend: "groq" or "openai"


class DreamConfig(Base):
    """Dream memory consolidation configuration."""

    _HOUR_MS = 3_600_000

    interval_h: int = Field(default=2, ge=1)  # Every 2 hours by default
    cron: str | None = Field(default=None, exclude=True)  # Legacy compatibility override
    model_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("modelOverride", "model", "model_override"),
    )  # Optional Dream-specific model override
    max_batch_size: int = Field(default=20, ge=1)  # Max history entries per run
    max_iterations: int = Field(default=10, ge=1)  # Max tool calls per Phase 2

    def build_schedule(self, timezone: str) -> CronSchedule:
        """Build the runtime schedule, preferring the legacy cron override if present."""
        if self.cron:
            return CronSchedule(kind="cron", expr=self.cron, tz=timezone)
        return CronSchedule(kind="every", every_ms=self.interval_h * self._HOUR_MS)

    def describe_schedule(self) -> str:
        """Return a human-readable summary for logs and startup output."""
        if self.cron:
            return f"cron {self.cron} (legacy)"
        hours = self.interval_h
        return f"every {hours}h"


class AgentDefaults(Base):
    """Default agent configuration."""

    # 兼容策略：workspace 未显式配置时，回落到 config-root。
    workspace: str = ""
    model: str = "anthropic/claude-opus-4-5"
    provider: str = (
        "auto"  # Provider name (e.g. "anthropic", "openrouter") or "auto" for auto-detection
    )
    max_tokens: int = 8192
    context_window_tokens: int = 65_536
    context_block_limit: int | None = None
    temperature: float = 0.1
    max_tool_iterations: int = 200
    max_tool_result_chars: int = 16_000
    provider_retry_mode: Literal["standard", "persistent"] = "standard"
    reasoning_effort: str | None = (
        None  # low / medium / high / adaptive - enables LLM thinking mode
    )
    timezone: str = "UTC"  # IANA timezone, e.g. "Asia/Shanghai", "America/New_York"
    dream: DreamConfig = Field(default_factory=DreamConfig)


class AgentsConfig(Base):
    """Agent configuration."""

    defaults: AgentDefaults = Field(default_factory=AgentDefaults)


class ProviderConfig(Base):
    """LLM provider configuration."""

    api_key: str = ""
    api_base: str | None = None
    extra_headers: dict[str, str] | None = None  # Custom headers (e.g. APP-Code for AiHubMix)


class ProvidersConfig(Base):
    """Configuration for LLM providers."""

    custom: ProviderConfig = Field(default_factory=ProviderConfig)  # Any OpenAI-compatible endpoint
    azure_openai: ProviderConfig = Field(
        default_factory=ProviderConfig
    )  # Azure OpenAI (model = deployment name)
    anthropic: ProviderConfig = Field(default_factory=ProviderConfig)
    openai: ProviderConfig = Field(default_factory=ProviderConfig)
    openrouter: ProviderConfig = Field(default_factory=ProviderConfig)
    deepseek: ProviderConfig = Field(default_factory=ProviderConfig)
    groq: ProviderConfig = Field(default_factory=ProviderConfig)
    zhipu: ProviderConfig = Field(default_factory=ProviderConfig)
    dashscope: ProviderConfig = Field(default_factory=ProviderConfig)
    vllm: ProviderConfig = Field(default_factory=ProviderConfig)
    ollama: ProviderConfig = Field(default_factory=ProviderConfig)  # Ollama local models
    ovms: ProviderConfig = Field(default_factory=ProviderConfig)  # OpenVINO Model Server (OVMS)
    gemini: ProviderConfig = Field(default_factory=ProviderConfig)
    moonshot: ProviderConfig = Field(default_factory=ProviderConfig)
    minimax: ProviderConfig = Field(default_factory=ProviderConfig)
    mistral: ProviderConfig = Field(default_factory=ProviderConfig)
    stepfun: ProviderConfig = Field(default_factory=ProviderConfig)  # Step Fun (阶跃星辰)
    xiaomi_mimo: ProviderConfig = Field(default_factory=ProviderConfig)  # Xiaomi MIMO (小米)
    aihubmix: ProviderConfig = Field(default_factory=ProviderConfig)  # AiHubMix API gateway
    siliconflow: ProviderConfig = Field(default_factory=ProviderConfig)  # SiliconFlow (硅基流动)
    volcengine: ProviderConfig = Field(default_factory=ProviderConfig)  # VolcEngine (火山引擎)
    volcengine_coding_plan: ProviderConfig = Field(
        default_factory=ProviderConfig
    )  # VolcEngine Coding Plan
    byteplus: ProviderConfig = Field(
        default_factory=ProviderConfig
    )  # BytePlus (VolcEngine international)
    byteplus_coding_plan: ProviderConfig = Field(
        default_factory=ProviderConfig
    )  # BytePlus Coding Plan
    openai_codex: ProviderConfig = Field(
        default_factory=ProviderConfig, exclude=True
    )  # OpenAI Codex (OAuth)
    github_copilot: ProviderConfig = Field(
        default_factory=ProviderConfig, exclude=True
    )  # Github Copilot (OAuth)
    qianfan: ProviderConfig = Field(default_factory=ProviderConfig)  # Qianfan (百度千帆)


def _match_provider_from_pool(
    *,
    forced: str,
    model: str,
    providers: "ProvidersConfig",
) -> tuple["ProviderConfig | None", str | None]:
    """Match a provider inside a specific provider pool.

    中文注释：heartbeat 需要复用 native 的 provider 选择语义，但必须限制在自己的专用
    provider 池中，避免隐式回退主 `providers.*` 配置。
    """
    from nanobot.providers.registry import PROVIDERS, find_by_name

    if forced != "auto":
        spec = find_by_name(forced)
        if spec:
            p = getattr(providers, spec.name, None)
            return (p, spec.name) if p else (None, None)
        return None, None

    model_lower = model.lower()
    model_normalized = model_lower.replace("-", "_")
    model_prefix = model_lower.split("/", 1)[0] if "/" in model_lower else ""
    normalized_prefix = model_prefix.replace("-", "_")

    def _kw_matches(kw: str) -> bool:
        kw = kw.lower()
        return kw in model_lower or kw.replace("-", "_") in model_normalized

    # Explicit provider prefix wins — prevents `github-copilot/...codex` matching openai_codex.
    for spec in PROVIDERS:
        p = getattr(providers, spec.name, None)
        if p and model_prefix and normalized_prefix == spec.name:
            if spec.is_oauth or spec.is_local or p.api_key:
                return p, spec.name

    # Match by keyword (order follows PROVIDERS registry)
    for spec in PROVIDERS:
        p = getattr(providers, spec.name, None)
        if p and any(_kw_matches(kw) for kw in spec.keywords):
            if spec.is_oauth or spec.is_local or p.api_key:
                return p, spec.name

    # Fallback: configured local providers can route models without provider-specific keywords.
    local_fallback: tuple[ProviderConfig, str] | None = None
    for spec in PROVIDERS:
        if not spec.is_local:
            continue
        p = getattr(providers, spec.name, None)
        if not (p and p.api_base):
            continue
        if spec.detect_by_base_keyword and spec.detect_by_base_keyword in p.api_base:
            return p, spec.name
        if local_fallback is None:
            local_fallback = (p, spec.name)
    if local_fallback:
        return local_fallback

    # Fallback: gateways first, then others (follows registry order)
    for spec in PROVIDERS:
        if spec.is_oauth:
            continue
        p = getattr(providers, spec.name, None)
        if p and p.api_key:
            return p, spec.name
    return None, None


def _get_api_base_from_pool(
    *,
    forced: str,
    model: str,
    providers: "ProvidersConfig",
) -> str | None:
    """Get API base for the matched provider inside a specific provider pool."""
    from nanobot.providers.registry import find_by_name

    p, name = _match_provider_from_pool(forced=forced, model=model, providers=providers)
    if p and p.api_base:
        return p.api_base
    if name:
        spec = find_by_name(name)
        if spec and (spec.is_gateway or spec.is_local) and spec.default_api_base:
            return spec.default_api_base
    return None


class HeartbeatProviderConfig(Base):
    """Dedicated provider selection for heartbeat decisions."""

    model: str = "anthropic/claude-opus-4-5"
    provider: str = (
        "auto"  # Provider name (e.g. "anthropic", "openrouter") or "auto" for auto-detection
    )
    max_tokens: int = 8192
    temperature: float = 0.1
    provider_retry_mode: Literal["standard", "persistent"] = "standard"
    reasoning_effort: str | None = None
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)

    def _match_provider(
        self, model: str | None = None
    ) -> tuple["ProviderConfig | None", str | None]:
        return _match_provider_from_pool(
            forced=self.provider,
            model=model or self.model,
            providers=self.providers,
        )

    def get_provider(self, model: str | None = None) -> ProviderConfig | None:
        p, _ = self._match_provider(model)
        return p

    def get_provider_name(self, model: str | None = None) -> str | None:
        _, name = self._match_provider(model)
        return name

    def get_api_base(self, model: str | None = None) -> str | None:
        return _get_api_base_from_pool(
            forced=self.provider,
            model=model or self.model,
            providers=self.providers,
        )


class HeartbeatConfig(Base):
    """Heartbeat service configuration."""

    enabled: bool = True
    interval_s: int = 30 * 60  # 30 minutes
    keep_recent_messages: int = 8


class ApiConfig(Base):
    """OpenAI-compatible API server configuration."""

    host: str = "127.0.0.1"  # Safer default: local-only bind.
    port: int = 8900
    timeout: float = 120.0  # Per-request timeout in seconds.


class GatewayConfig(Base):
    """Gateway/server configuration."""

    host: str = "0.0.0.0"
    port: int = 18790
    heartbeat: HeartbeatConfig = Field(default_factory=HeartbeatConfig)


class HeartbeatFeatureConfig(Base):
    """Dedicated heartbeat decision configuration."""

    provider_config: HeartbeatProviderConfig = Field(default_factory=HeartbeatProviderConfig)


class WebSearchConfig(Base):
    """Web search tool configuration."""

    provider: str = "duckduckgo"  # brave, tavily, duckduckgo, searxng, jina
    api_key: str = ""
    base_url: str = ""  # SearXNG base URL
    max_results: int = 5
    timeout: int = 30  # Wall-clock timeout (seconds) for search operations


class WebToolsConfig(Base):
    """Web tools configuration."""

    enable: bool = True
    proxy: str | None = (
        None  # HTTP/SOCKS5 proxy URL, e.g. "http://127.0.0.1:7890" or "socks5://127.0.0.1:1080"
    )
    search: WebSearchConfig = Field(default_factory=WebSearchConfig)


class ExecToolConfig(Base):
    """Shell exec tool configuration."""

    enable: bool = True
    timeout: int = 60
    path_append: str = ""
    sandbox: str = ""  # sandbox backend: "" (none) or "bwrap"


class MCPServerConfig(Base):
    """MCP server connection configuration (stdio or HTTP)."""

    type: Literal["stdio", "sse", "streamableHttp"] | None = None  # auto-detected if omitted
    command: str = ""  # Stdio: command to run (e.g. "npx")
    args: list[str] = Field(default_factory=list)  # Stdio: command arguments
    env: dict[str, str] = Field(default_factory=dict)  # Stdio: extra env vars
    url: str = ""  # HTTP/SSE: endpoint URL
    headers: dict[str, str] = Field(default_factory=dict)  # HTTP/SSE: custom headers
    tool_timeout: int = 30  # seconds before a tool call is cancelled
    enabled_tools: list[str] = Field(
        default_factory=lambda: ["*"]
    )  # Only register these tools; accepts raw MCP names or wrapped mcp_<server>_<tool> names; ["*"] = all tools; [] = no tools


class ToolsConfig(Base):
    """Tools configuration."""

    web: WebToolsConfig = Field(default_factory=WebToolsConfig)
    exec: ExecToolConfig = Field(default_factory=ExecToolConfig)
    restrict_to_workspace: bool = False  # restrict all tool access to workspace directory
    mcp_servers: dict[str, MCPServerConfig] = Field(default_factory=dict)
    ssrf_whitelist: list[str] = Field(
        default_factory=list
    )  # CIDR ranges to exempt from SSRF blocking (e.g. ["100.64.0.0/10"] for Tailscale)


class PathsConfig(Base):
    """Instance-level filesystem paths."""

    # 兼容策略：root 为空时，自动使用 config 文件所在目录作为 config-root。
    root: str = ""


class ACPBackendConfig(Base):
    """ACP backend process settings."""

    command: str = "opencode"
    args: list[str] = Field(default_factory=lambda: ["acp", "--print-logs", "--log-level", "WARN"])
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    protocol_version: int = 1
    permissions_policy: Literal["strict", "trusted", "yolo"] = "strict"
    # 中文注释：入站附件若需要在 dispatch 层落盘，统一写到 workspace 下该相对目录。
    startup_timeout_seconds: int = 20
    inbound_media_dir: str = "Download/channel-inbound/acp-dispatch"
    default_model: str = "xaio/Kimi-K2.5"
    default_mode: str = "OpenCode-Builder"
    permission_timeout_seconds: int = 90
    progress_text_idle_seconds: float = 1.0
    progress_text_max_chars: int = 2048
    progress_tool_idle_seconds: float = 300.0
    progress_tool_terminal_delay_seconds: float = 1.5
    progress_other_idle_seconds: float = 0.2
    progress_media_idle_seconds: float = 0.2


class DispatchConfig(Base):
    """Runtime dispatch backend settings."""

    backend: Literal["native", "acp"] = "native"
    acp: ACPBackendConfig = Field(default_factory=ACPBackendConfig)


class Config(BaseSettings):
    """Root configuration for nanobot."""

    paths: PathsConfig = Field(default_factory=PathsConfig)
    agents: AgentsConfig = Field(default_factory=AgentsConfig)
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    heartbeat: HeartbeatFeatureConfig = Field(default_factory=HeartbeatFeatureConfig)
    tools: ToolsConfig = Field(default_factory=ToolsConfig)
    dispatch: DispatchConfig = Field(default_factory=DispatchConfig)

    @property
    def root_path(self) -> Path:
        """Resolve config-root path.

        递归缺省链路：
        - 显式 paths.root
        - 否则使用 config 文件所在目录（config-root）
        """
        root = (self.paths.root or "").strip()
        if root:
            return Path(root).expanduser()

        # 避免循环依赖：按需导入 loader。
        from nanobot.config.loader import get_config_path

        return get_config_path().expanduser().resolve().parent

    @property
    def workspace_path(self) -> Path:
        """Resolve workspace path with fallback to config-root.

        递归缺省链路：
        - 显式 workspace
        - 否则回落到 config-root
        """
        workspace = (self.agents.defaults.workspace or "").strip()
        if workspace:
            return Path(workspace).expanduser()
        return self.root_path

    def _match_provider(
        self, model: str | None = None
    ) -> tuple["ProviderConfig | None", str | None]:
        """Match provider config and its registry name. Returns (config, spec_name)."""
        return _match_provider_from_pool(
            forced=self.agents.defaults.provider,
            model=model or self.agents.defaults.model,
            providers=self.providers,
        )

    def get_provider(self, model: str | None = None) -> ProviderConfig | None:
        """Get matched provider config (api_key, api_base, extra_headers). Falls back to first available."""
        p, _ = self._match_provider(model)
        return p

    def get_provider_name(self, model: str | None = None) -> str | None:
        """Get the registry name of the matched provider (e.g. "deepseek", "openrouter")."""
        _, name = self._match_provider(model)
        return name

    def get_api_key(self, model: str | None = None) -> str | None:
        """Get API key for the given model. Falls back to first available key."""
        p = self.get_provider(model)
        return p.api_key if p else None

    def get_api_base(self, model: str | None = None) -> str | None:
        """Get API base URL for the given model. Applies default URLs for gateway/local providers."""
        return _get_api_base_from_pool(
            forced=self.agents.defaults.provider,
            model=model or self.agents.defaults.model,
            providers=self.providers,
        )

    model_config = ConfigDict(env_prefix="NANOBOT_", env_nested_delimiter="__")
