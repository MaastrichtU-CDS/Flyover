"""Anthropic (Claude) provider for LLM mapping suggestions.

Uses the official ``anthropic`` SDK with structured outputs
(``output_config.format``), which guarantees the response's first text
block is valid JSON per the match schema. The SDK's synchronous httpx
transport cooperates with gevent monkey-patching like ``requests`` does.
"""

import copy
import logging
import time

import anthropic

from services.llm.base import LLMProvider, LLMProviderError
from services.llm.matching import (
    MATCH_OUTPUT_SCHEMA,
    SYSTEM_PROMPT,
    build_user_prompt,
    estimate_output_tokens,
    parse_and_sanitise,
)

logger = logging.getLogger(__name__)

_REACHABILITY_CACHE_S = 300


def _anthropic_schema() -> dict:
    """Return the match schema in Anthropic's supported JSON-schema subset.

    Anthropic structured outputs document basic types plus anyOf; the
    shared schema's ``{"type": ["string", "null"]}`` leaf is rewritten to
    the equivalent anyOf form. The shared schema itself stays untouched —
    Ollama's grammar converter handles type arrays and the working default
    path must not change.
    """
    schema = copy.deepcopy(MATCH_OUTPUT_SCHEMA)
    match_leaf = schema["properties"]["pairs"]["items"]["properties"]["match"]
    match_leaf.pop("type", None)
    match_leaf["anyOf"] = [{"type": "string"}, {"type": "null"}]
    return schema


class AnthropicProvider(LLMProvider):
    """Provider for Anthropic's Claude models via the official SDK.

    Attributes:
        model: Claude model id (default "claude-opus-4-8").
    """

    name = "anthropic"

    def __init__(
        self,
        model: str = "claude-opus-4-8",
        api_key: str | None = None,
        timeout: float = 180.0,
        client: anthropic.Anthropic | None = None,
    ):
        """Create the provider.

        Args:
            model: Claude model id.
            api_key: API key; the SDK also honors ANTHROPIC_API_KEY.
            timeout: Request timeout in seconds.
            client: Injectable SDK client for tests.
        """
        self.model = model
        self._api_key = api_key
        self._client = client or anthropic.Anthropic(
            api_key=api_key, timeout=timeout, max_retries=1
        )
        self._schema = _anthropic_schema()
        self._reachable: bool | None = None
        self._reachable_at = 0.0

    def is_reachable(self) -> bool:
        """Probe the configured model, caching the result for 5 minutes.

        A models.retrieve round-trip validates both the API key and the
        model id — exactly what "ready" means for a cloud provider — while
        the cache keeps status polls cheap.
        """
        now = time.monotonic()
        if (
            self._reachable is not None
            and now - self._reachable_at < _REACHABILITY_CACHE_S
        ):
            return self._reachable
        try:
            self._client.models.retrieve(self.model)
            self._reachable = True
        except Exception as exc:
            logger.debug("Anthropic reachability probe failed: %s", exc)
            self._reachable = False
        self._reachable_at = now
        return self._reachable

    def ensure_ready(self) -> str:
        """Check a credential is available; there is no pull concept.

        Returns:
            The configured model id.

        Raises:
            LLMProviderError: When no API key is resolvable.
        """
        if not (self._api_key or self._client.api_key):
            raise LLMProviderError(
                "FLYOVER_LLM_API_KEY (or ANTHROPIC_API_KEY) is required for "
                "the anthropic provider."
            )
        return self.model

    def match_equivalents(self, list_a: list[str], list_b: list[str]) -> list[dict]:
        """Match each item in list_a to its semantic equivalent in list_b.

        Sends a single structured-output request; current Claude models
        reject sampling parameters, so no temperature is passed (the schema
        constraint makes it unnecessary anyway).

        Raises:
            LLMProviderError: On refusals, truncation, transport errors, or
                invalid output.
        """
        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=estimate_output_tokens(list_a),
                system=SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": build_user_prompt(list_a, list_b)}
                ],
                output_config={
                    "format": {"type": "json_schema", "schema": self._schema}
                },
            )
        except (anthropic.APIConnectionError, anthropic.APIStatusError) as exc:
            raise LLMProviderError(f"Anthropic request failed: {exc}") from exc

        if response.stop_reason in ("refusal", "max_tokens"):
            raise LLMProviderError(
                f"Anthropic stopped with stop_reason={response.stop_reason}."
            )

        content = next(
            (block.text for block in response.content if block.type == "text"), ""
        )
        return parse_and_sanitise(content, list_a, list_b)
