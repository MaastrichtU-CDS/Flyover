"""Provider factory for the LLM mapping-suggestion feature."""

import logging

from services.llm.anthropic_provider import AnthropicProvider
from services.llm.base import LLMProvider
from services.llm.config import LLMConfig
from services.llm.ollama_provider import OllamaProvider
from services.llm.openai_provider import OpenAICompatProvider

logger = logging.getLogger(__name__)


def create_provider(config: LLMConfig) -> LLMProvider | None:
    """Build the configured provider, or None when the feature is disabled.

    Args:
        config: Resolved LLM configuration (see LLMConfig.from_env).

    Returns:
        A ready-to-use provider instance, or None for a disabled or unknown
        provider — the suggestion service short-circuits on config.enabled
        before ever touching the client.
    """
    if not config.enabled:
        return None
    if config.provider == "ollama":
        return OllamaProvider(
            config.base_url,
            config.model,
            fallback_models=config.fallback_models,
            read_timeout=config.request_timeout,
        )
    if config.provider == "openai":
        return OpenAICompatProvider(
            config.base_url,
            config.model,
            api_key=config.api_key,
            read_timeout=config.request_timeout,
        )
    if config.provider == "anthropic":
        return AnthropicProvider(
            config.model,
            api_key=config.api_key,
            timeout=config.request_timeout,
        )
    logger.warning("No provider implementation for %r.", config.provider)
    return None
