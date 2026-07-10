"""OpenAI-compatible chat-completions provider for LLM mapping suggestions.

Speaks the de-facto standard ``POST {base_url}/v1/chat/completions`` wire
format, which covers virtually every local inference server (vLLM,
LM Studio, llama.cpp server, TGI, Ollama's compat endpoint) and most cloud
APIs (OpenAI, Azure OpenAI, Mistral, Groq, OpenRouter, Gemini-compat).

Uses ``requests`` directly rather than the openai SDK: one endpoint, one
non-streaming shape, and the repo's established mocked-HTTP test pattern —
an SDK would add a fast-moving dependency for zero benefit here.
"""

import json
import logging

import requests

from services.llm.base import LLMProvider, LLMProviderError
from services.llm.matching import (
    MATCH_OUTPUT_SCHEMA,
    SYSTEM_PROMPT,
    build_user_prompt,
    estimate_output_tokens,
    parse_and_sanitise,
)

logger = logging.getLogger(__name__)


def _normalise_base_url(base_url: str) -> str:
    """Strip trailing slashes and ensure the URL ends with /v1."""
    url = base_url.rstrip("/")
    if not url.endswith("/v1"):
        url = f"{url}/v1"
    return url


class OpenAICompatProvider(LLMProvider):
    """Provider for any OpenAI-compatible chat-completions endpoint.

    Structured output is negotiated: the strict ``json_schema`` response
    format is tried first; servers that reject it fall back (stickily) to
    ``json_object`` mode with the schema embedded in the prompt. The shared
    validate+sanitise pipeline guards the output identically either way.
    """

    name = "openai"

    def __init__(
        self,
        base_url: str,
        model: str,
        api_key: str | None = None,
        connect_timeout: float = 2.0,
        read_timeout: float = 180.0,
        temperature: float = 0.1,
        structured_mode: str = "auto",
    ):
        """Create the provider.

        Args:
            base_url: Server base URL, with or without the /v1 suffix.
            model: Model identifier as the server knows it.
            api_key: Bearer token; omit for local servers without auth.
            connect_timeout: Seconds before an unreachable host fails.
            read_timeout: Seconds before a hung generation fails.
            temperature: Sampling temperature.
            structured_mode: "auto" (try json_schema, fall back to
                json_object), or pin "json_schema" / "json_object".
        """
        self.base_url = _normalise_base_url(base_url)
        self.model = model
        self.api_key = api_key
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.temperature = temperature
        self._mode = "json_schema" if structured_mode == "auto" else structured_mode
        self._may_downgrade = structured_mode == "auto"

    @property
    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def is_reachable(self) -> bool:
        """Return True when the server answers at all (401/403 included)."""
        try:
            requests.get(
                f"{self.base_url}/models",
                headers=self._headers,
                timeout=self.connect_timeout,
            )
            return True
        except requests.RequestException:
            return False

    def ensure_ready(self) -> str:
        """Check the endpoint is configured and reachable.

        Returns:
            The configured model name.

        Raises:
            LLMProviderError: When no model is configured or the server is
                unreachable.
        """
        if not self.model:
            raise LLMProviderError(
                "FLYOVER_LLM_MODEL is required for the openai provider."
            )
        if not self.is_reachable():
            raise LLMProviderError(
                f"Can't reach the OpenAI-compatible server at {self.base_url}."
            )
        return self.model

    def _response_format(self) -> dict:
        if self._mode == "json_schema":
            return {
                "type": "json_schema",
                "json_schema": {
                    "name": "match_output",
                    "schema": MATCH_OUTPUT_SCHEMA,
                    "strict": True,
                },
            }
        return {"type": "json_object"}

    def _user_prompt(self, list_a: list[str], list_b: list[str]) -> str:
        prompt = build_user_prompt(list_a, list_b)
        if self._mode == "json_object":
            prompt += (
                "\n\nYour JSON must conform to this JSON schema:\n"
                f"{json.dumps(MATCH_OUTPUT_SCHEMA)}"
            )
        return prompt

    def _post_chat(self, list_a: list[str], list_b: list[str]) -> requests.Response:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": self._user_prompt(list_a, list_b)},
            ],
            "temperature": self.temperature,
            "max_tokens": estimate_output_tokens(list_a),
            "response_format": self._response_format(),
        }
        return requests.post(
            f"{self.base_url}/chat/completions",
            json=payload,
            headers=self._headers,
            timeout=(self.connect_timeout, self.read_timeout),
        )

    def match_equivalents(self, list_a: list[str], list_b: list[str]) -> list[dict]:
        """Match each item in list_a to its semantic equivalent in list_b.

        Raises:
            LLMProviderError: On HTTP errors or invalid model output.
            requests.RequestException: On transport failures.
        """
        response = self._post_chat(list_a, list_b)

        if (
            response.status_code == 400
            and self._may_downgrade
            and self._mode == "json_schema"
            and "response_format" in response.text.lower()
        ):
            logger.info(
                "Server at %s rejected json_schema response_format; "
                "falling back to json_object mode.",
                self.base_url,
            )
            self._mode = "json_object"
            response = self._post_chat(list_a, list_b)

        if response.status_code >= 400:
            raise LLMProviderError(
                f"OpenAI-compatible server returned HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

        try:
            content = response.json()["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise LLMProviderError(
                "OpenAI-compatible server returned an unexpected response shape."
            ) from exc

        return parse_and_sanitise(content, list_a, list_b)
