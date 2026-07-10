"""Configuration for the LLM mapping-suggestion feature.

Resolves the provider selection, endpoint, credentials, and the
local-vs-remote classification from FLYOVER_LLM_* environment variables.
Misconfiguration never crashes the application: every invalid combination
disables the feature with a single explanatory warning (fail closed).
"""

import logging
import os
from dataclasses import dataclass, field
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

KNOWN_PROVIDERS = ("ollama", "openai", "anthropic")

_DEFAULT_MODELS = {
    "ollama": "llama3.2:3b",
    "anthropic": "claude-opus-4-8",
}

REMOTE_DATA_WARNING = (
    "FLYOVER_LLM_PROVIDER=%s sends CSV column names, distinct categorical "
    "values, and semantic-map variable/term keys to an external service; "
    "set FLYOVER_LLM_ALLOW_REMOTE=true to enable it. LLM suggestions are "
    "disabled."
)


def _env_flag(name: str, default: bool) -> bool:
    """Read a boolean env var; unset falls back to the given default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


def _is_loopback(base_url: str) -> bool:
    """Return True when a URL's host is loopback (localhost/127.x/::1)."""
    host = (urlparse(base_url).hostname or "").lower()
    return host == "localhost" or host == "::1" or host.startswith("127.")


def _classify_remote(provider: str, base_url: str) -> bool:
    """Decide whether a provider sends data outside the deployment.

    Static per-provider defaults with a loopback carve-out; the explicit
    FLYOVER_LLM_REMOTE flag overrides in either direction. Hostname
    heuristics beyond loopback are deliberately avoided: compose service
    names only resolve at runtime, and egress proxies on private IPs would
    otherwise be misclassified as local. The result fails closed — an
    in-network OpenAI-compatible server counts as remote until the operator
    declares FLYOVER_LLM_REMOTE=false.
    """
    override = os.getenv("FLYOVER_LLM_REMOTE")
    if override is not None:
        return override.strip().lower() in ("true", "1", "yes")
    if provider == "anthropic":
        return True
    if provider == "ollama":
        return False
    return not _is_loopback(base_url)


@dataclass
class LLMConfig:
    """Resolved configuration for the LLM suggestion feature.

    Attributes:
        enabled: Whether the feature is active (after all gating).
        provider: Backend identifier — "ollama", "openai", or "anthropic".
        base_url: Endpoint for ollama/openai providers.
        api_key: Credential for openai/anthropic providers.
        model: Model identifier for the selected provider.
        fallback_models: Pull fallbacks (only meaningful for ollama).
        chunk_size: Columns per matching request in the variables phase.
        request_timeout: Read timeout in seconds per LLM request.
        allow_remote: Operator acknowledgement that data may leave the
            deployment (FLYOVER_LLM_ALLOW_REMOTE).
        remote: Computed classification of the selected backend.
    """

    enabled: bool = False
    provider: str = "ollama"
    base_url: str = "http://localhost:11434"
    api_key: str | None = None
    model: str = "llama3.2:3b"
    fallback_models: list[str] = field(default_factory=lambda: ["llama3.2:1b"])
    chunk_size: int = 8
    request_timeout: float = 180.0
    allow_remote: bool = False
    remote: bool = False

    @classmethod
    def from_env(cls) -> "LLMConfig":
        """Build a config from FLYOVER_LLM_* / FLYOVER_OLLAMA_HOST env vars."""
        provider = os.getenv("FLYOVER_LLM_PROVIDER", "ollama").strip().lower()
        base_url_env = os.getenv("FLYOVER_LLM_BASE_URL") or os.getenv(
            "FLYOVER_OLLAMA_HOST"
        )
        base_url = base_url_env or (
            "http://localhost:11434" if provider == "ollama" else ""
        )
        model = os.getenv("FLYOVER_LLM_MODEL") or _DEFAULT_MODELS.get(provider, "")

        any_llm_env = any(
            os.getenv(name)
            for name in (
                "FLYOVER_LLM_BASE_URL",
                "FLYOVER_OLLAMA_HOST",
                "FLYOVER_LLM_PROVIDER",
            )
        )
        enabled = _env_flag("FLYOVER_LLM_ENABLED", default=bool(any_llm_env))

        fallbacks_default = "llama3.2:1b" if provider == "ollama" else ""
        fallbacks = [
            m.strip()
            for m in os.getenv(
                "FLYOVER_LLM_FALLBACK_MODELS", fallbacks_default
            ).split(",")
            if m.strip()
        ]

        config = cls(
            enabled=enabled,
            provider=provider,
            base_url=base_url,
            api_key=os.getenv("FLYOVER_LLM_API_KEY") or None,
            model=model,
            fallback_models=fallbacks,
            chunk_size=int(os.getenv("FLYOVER_LLM_CHUNK_SIZE", "8")),
            request_timeout=float(os.getenv("FLYOVER_LLM_TIMEOUT_S", "180")),
            allow_remote=_env_flag("FLYOVER_LLM_ALLOW_REMOTE", default=False),
        )

        if provider not in KNOWN_PROVIDERS:
            logger.warning(
                "Unknown FLYOVER_LLM_PROVIDER=%r (expected one of %s); "
                "LLM suggestions are disabled.",
                provider,
                ", ".join(KNOWN_PROVIDERS),
            )
            config.enabled = False
            return config

        if provider == "openai" and config.enabled and not (model and base_url):
            logger.warning(
                "FLYOVER_LLM_PROVIDER=openai requires explicit FLYOVER_LLM_MODEL "
                "and FLYOVER_LLM_BASE_URL; LLM suggestions are disabled."
            )
            config.enabled = False
            return config

        config.remote = _classify_remote(provider, base_url)

        if config.enabled and config.remote and not config.allow_remote:
            logger.warning(REMOTE_DATA_WARNING, provider)
            config.enabled = False

        return config
