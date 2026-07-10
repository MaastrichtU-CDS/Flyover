"""LLM-based mapping suggestion services.

This package wraps a local Ollama server to suggest semantic mappings for
the describe workflow: CSV columns to semantic variables, and local
categorical values to semantic value-mapping terms.
"""

from services.llm.base import LLMProvider, LLMProviderError
from services.llm.config import LLMConfig
from services.llm.ollama_provider import OllamaError, OllamaProvider
from services.llm.suggestion_service import LLMSuggestionService

__all__ = [
    "LLMProvider",
    "LLMProviderError",
    "OllamaProvider",
    "OllamaError",
    "LLMConfig",
    "LLMSuggestionService",
]
