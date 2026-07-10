"""LLM-based mapping suggestion services.

This package wraps a local Ollama server to suggest semantic mappings for
the describe workflow: CSV columns to semantic variables, and local
categorical values to semantic value-mapping terms.
"""

from services.llm.ollama_client import OllamaClient, OllamaError

__all__ = [
    "OllamaClient",
    "OllamaError",
]
