"""Provider interface for LLM mapping-suggestion backends.

Every backend (local or cloud) implements LLMProvider. The suggestion
service is provider-agnostic: it calls ensure_ready() once per job and
match_equivalents() per chunk, and never sees transport details.
"""

import abc


class LLMProviderError(RuntimeError):
    """Raised when a provider is unreachable, misconfigured, or returns bad data."""


class LLMProvider(abc.ABC):
    """One configured LLM backend. The model is owned by the instance.

    Attributes:
        name: Provider identifier ("ollama", "openai", "anthropic").
        model: Configured model identifier.
    """

    name: str
    model: str

    @abc.abstractmethod
    def is_reachable(self) -> bool:
        """Return True when the backend responds. Never raises.

        Used only by the status endpoint; must be cheap.
        """

    @abc.abstractmethod
    def ensure_ready(self) -> str:
        """Ensure the provider can serve requests.

        May be slow (e.g. an Ollama model pull) — jobs report the
        "pulling_model" status while this runs.

        Returns:
            The resolved model name (may differ from ``self.model`` when a
            fallback model was selected).

        Raises:
            LLMProviderError: When the backend is unusable.
        """

    @abc.abstractmethod
    def match_equivalents(self, list_a: list[str], list_b: list[str]) -> list[dict]:
        """Match each item in list_a to its semantic equivalent in list_b.

        Returns:
            One dict per list_a item, in list_a order:
            ``{"item", "match", "confidence", "reason"}``.

        Raises:
            LLMProviderError: On transport or output validation failures.
        """
