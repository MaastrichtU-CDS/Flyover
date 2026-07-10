"""Ollama provider for LLM mapping suggestions.

Ported from the standalone llm-mapper prototype (mapping_recommendation.py
and pull_ollama_model.py). Speaks Ollama's native API to keep the two
capabilities the OpenAI-compatible surface lacks: model management
(pull with fallbacks) and grammar-constrained structured output via the
native ``format`` field.
"""

import json
import logging

import requests

from services.llm.base import LLMProvider, LLMProviderError
from services.llm.matching import (
    MATCH_OUTPUT_SCHEMA,
    SYSTEM_PROMPT,
    MatchingError,
    build_user_prompt,
    parse_and_sanitise,
)

logger = logging.getLogger(__name__)


class OllamaError(LLMProviderError):
    """Raised when the Ollama server is unreachable or returns bad data."""


def estimate_num_ctx(list_a: list[str], list_b: list[str]) -> int:
    """Estimate the context window needed for a match request.

    Accounts for the system prompt, both lists in the user prompt, and the
    schema-constrained JSON output (roughly 55 tokens per list_a item).

    Args:
        list_a: Items to match.
        list_b: Candidate targets.

    Returns:
        The next power of two above the estimate, with a floor of 4096.
    """
    estimated = 350 + 6 * len(list_b) + 8 * len(list_a) + 55 * len(list_a) + 700
    num_ctx = 4096
    while num_ctx < estimated:
        num_ctx *= 2
    return num_ctx


class OllamaProvider(LLMProvider):
    """Provider for a local Ollama server with model auto-pull.

    Attributes:
        host: Base URL of the Ollama server, e.g. "http://ollama:11434".
        model: Preferred model; ensure_ready() may resolve a fallback.
        fallback_models: Models to try when the preferred one fails to pull.
    """

    name = "ollama"

    def __init__(
        self,
        base_url: str,
        model: str,
        fallback_models: list[str] | None = None,
        connect_timeout: float = 2.0,
        read_timeout: float = 180.0,
        temperature: float = 0.1,
    ):
        self.host = base_url.rstrip("/")
        self.model = model
        self.fallback_models = list(fallback_models or [])
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.temperature = temperature
        self._resolved_model: str | None = None

    @property
    def _timeout(self) -> tuple[float, float]:
        return (self.connect_timeout, self.read_timeout)

    def is_reachable(self) -> bool:
        """Return True if the Ollama server responds at its base URL."""
        try:
            requests.get(self.host, timeout=self.connect_timeout)
            return True
        except requests.RequestException:
            return False

    def ensure_ready(self) -> str:
        """Ensure a usable model is present, pulling it if necessary.

        Returns:
            The name of the model that is available (the preferred model or
            the first working fallback), remembered for subsequent
            match_equivalents() calls.

        Raises:
            OllamaError: If the server is unreachable or no model could be
                made available.
        """
        self._resolved_model = self.ensure_model(self.model, self.fallback_models)
        return self._resolved_model

    def has_model(self, model_name: str) -> bool:
        """Return True if a model (any tag) is present on the server.

        Args:
            model_name: Model name, optionally with tag (e.g. "llama3.2:3b").
        """
        response = requests.get(
            f"{self.host}/api/tags", timeout=(self.connect_timeout, 15)
        )
        response.raise_for_status()
        models = response.json().get("models", [])
        target = model_name.split(":")[0]
        return any(m.get("name", "").split(":")[0] == target for m in models)

    def pull_model(self, model_name: str) -> None:
        """Pull a model via the Ollama REST API, streaming progress to logs.

        Args:
            model_name: Model to pull.

        Raises:
            OllamaError: If the pull reports an error or never succeeds.
        """
        with requests.post(
            f"{self.host}/api/pull",
            json={"name": model_name, "stream": True},
            stream=True,
            timeout=self._timeout,
        ) as response:
            response.raise_for_status()
            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue

                status = (message.get("status") or "").strip()
                if status:
                    logger.info("Ollama pull %s: %s", model_name, status)

                if "error" in message:
                    raise OllamaError(
                        f"Ollama pull error for '{model_name}': {message['error']}"
                    )

                if status.lower() == "success":
                    return

        if not self.has_model(model_name):
            raise OllamaError(
                f"Pull did not complete for '{model_name}' (no success status)."
            )

    def ensure_model(
        self, model_name: str, fallback_models: list[str] | None = None
    ) -> str:
        """Ensure a usable model is present, trying fallbacks in order.

        Args:
            model_name: Preferred model.
            fallback_models: Models to try if the preferred one fails.

        Returns:
            The name of the model that is available.

        Raises:
            OllamaError: If the server is unreachable or no model could be
                made available.
        """
        if not self.is_reachable():
            raise OllamaError(
                f"Can't reach Ollama at {self.host}. Is the ollama service running?"
            )

        for candidate in [model_name, *(fallback_models or [])]:
            try:
                if self.has_model(candidate):
                    return candidate
                self.pull_model(candidate)
                if self.has_model(candidate):
                    return candidate
            except (requests.RequestException, OllamaError) as exc:
                logger.warning("Failed to obtain model '%s': %s", candidate, exc)

        raise OllamaError(
            f"Failed to pull model '{model_name}'"
            + (f" and fallbacks {fallback_models}" if fallback_models else "")
        )

    def match_equivalents(self, list_a: list[str], list_b: list[str]) -> list[dict]:
        """Match each item in list_a to its semantic equivalent in list_b.

        Sends a single grammar-constrained chat request to Ollama's native
        /api/chat and runs the shared parse/validate/sanitise pipeline on
        the response.

        Raises:
            OllamaError: On empty or schema-invalid responses.
            requests.RequestException: On transport failures.
        """
        payload = {
            "model": self._resolved_model or self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(list_a, list_b)},
            ],
            "format": MATCH_OUTPUT_SCHEMA,
            "options": {
                "temperature": self.temperature,
                "num_ctx": estimate_num_ctx(list_a, list_b),
            },
            "stream": False,
        }
        response = requests.post(
            f"{self.host}/api/chat", json=payload, timeout=self._timeout
        )
        response.raise_for_status()
        content = (response.json().get("message") or {}).get("content")

        try:
            return parse_and_sanitise(content, list_a, list_b)
        except MatchingError as exc:
            raise OllamaError(str(exc)) from exc
