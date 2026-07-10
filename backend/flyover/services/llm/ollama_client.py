"""HTTP client for a local Ollama server.

Ported from the standalone llm-mapper prototype (mapping_recommendation.py
and pull_ollama_model.py). Provides model management (reachability check,
pull with fallbacks) and a structured-output list matcher that maps items
from one string list to semantically equivalent items in another.
"""

import json
import logging

import requests
from jsonschema import ValidationError, validate

logger = logging.getLogger(__name__)


MATCH_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["pairs"],
    "properties": {
        "pairs": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["a", "match", "confidence", "reason"],
                "properties": {
                    "a": {"type": "string"},
                    "match": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                    "reason": {"type": "string"},
                },
            },
        }
    },
}

SYSTEM_PROMPT = """
You are a careful data-matching assistant.
You receive two lists of strings: list_a and list_b.
Your job is to determine semantic equivalence between items.
You must find the best match in list_b for each item in list_a, or state that no match exists.

Rules:
- Consider synonyms, abbreviations, pluralization, and domain-specific naming.
- Only match items that refer to the same real-world concept.
- IMPORTANT: If you find a match, set "match" to the EXACT string from list_b (character-for-character).
- If there is no equivalent in list_b for an item in list_a, set "match" to null and briefly explain why in "reason".
- If multiple items in list_b could match, choose the single best one and explain briefly in "reason".
- The "item" field in the output MUST correspond exactly to the items in list_a.
- For every item, "reason" MUST be a non-empty natural-language explanation (at least one sentence) of why you chose that match, or why no match exists.
- Return ONLY JSON according to the provided schema. Do not include any text outside JSON.
- CRITICAL: For every matched item, "match" MUST be an exact element of list_b. Do NOT leave "match" as null if a match exist.
"""


class OllamaError(RuntimeError):
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


def _build_user_prompt(list_a: list[str], list_b: list[str]) -> str:
    """Build the user prompt embedding both lists as JSON."""
    return (
        "Match items from list_a to semantically equivalent items in list_b.\n\n"
        f"list_a = {json.dumps(list_a, ensure_ascii=False)}\n"
        f"list_b = {json.dumps(list_b, ensure_ascii=False)}\n\n"
        "Return ONLY the JSON as specified."
    )


class OllamaClient:
    """Client for the Ollama REST API with model management and matching.

    Attributes:
        host: Base URL of the Ollama server, e.g. "http://ollama:11434".
        connect_timeout: Seconds before an unreachable host fails.
        read_timeout: Seconds before a hung generation or pull fails.
    """

    def __init__(
        self,
        host: str,
        connect_timeout: float = 2.0,
        read_timeout: float = 180.0,
    ):
        self.host = host.rstrip("/")
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

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
        """Ensure a usable model is present, pulling it if necessary.

        Tries the requested model first, then each fallback in order.

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

    def match_equivalents(
        self,
        list_a: list[str],
        list_b: list[str],
        model: str,
        temperature: float = 0.1,
    ) -> list[dict]:
        """Match each item in list_a to its semantic equivalent in list_b.

        Sends a single schema-constrained chat request. The response is
        validated and sanitised: confidences are clamped to [0, 1], matches
        not literally present in list_b are nulled (hallucination guard),
        pairs for unknown list_a items are dropped, duplicate matches within
        the call are nulled (first wins), and items the model omitted are
        returned with a null match.

        Args:
            list_a: Items to find matches for (e.g. CSV column names).
            list_b: Candidate targets (e.g. semantic variable keys).
            model: Ollama model name to use.
            temperature: Sampling temperature.

        Returns:
            One dict per list_a item, in list_a order:
            ``{"item", "match", "confidence", "reason"}``.

        Raises:
            OllamaError: On empty or schema-invalid responses.
            requests.RequestException: On transport failures.
        """
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": _build_user_prompt(list_a, list_b)},
            ],
            "format": MATCH_OUTPUT_SCHEMA,
            "options": {
                "temperature": temperature,
                "num_ctx": estimate_num_ctx(list_a, list_b),
            },
            "stream": False,
        }
        response = requests.post(
            f"{self.host}/api/chat", json=payload, timeout=self._timeout
        )
        response.raise_for_status()
        content = (response.json().get("message") or {}).get("content")
        if not isinstance(content, str) or not content.strip():
            raise OllamaError("Ollama returned an empty or malformed response.")

        try:
            data = json.loads(content)
            validate(data, MATCH_OUTPUT_SCHEMA)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise OllamaError(f"Ollama response failed validation: {exc}") from exc

        return self._sanitise_pairs(data["pairs"], list_a, list_b)

    @staticmethod
    def _sanitise_pairs(
        pairs: list[dict], list_a: list[str], list_b: list[str]
    ) -> list[dict]:
        """Clamp, deduplicate and guard raw model output pairs."""
        valid_targets = set(list_b)
        by_item: dict[str, dict] = {}
        used_matches: set[str] = set()

        for pair in pairs:
            item = pair["a"]
            if item not in set(list_a) or item in by_item:
                continue

            match = pair["match"]
            reason = pair["reason"]
            try:
                confidence = float(pair["confidence"])
            except (TypeError, ValueError):
                confidence = 0.0
            confidence = max(0.0, min(1.0, confidence))

            if match is not None and match not in valid_targets:
                reason = (
                    f"{reason} | Note: model proposed '{match}', which is not "
                    "a valid target."
                ).strip()
                match = None
                confidence = 0.0

            if match is not None and match in used_matches:
                reason = (
                    f"{reason} | Note: candidate already matched to another item."
                ).strip()
                match = None
            elif match is not None:
                used_matches.add(match)

            by_item[item] = {
                "item": item,
                "match": match,
                "confidence": confidence,
                "reason": reason,
            }

        return [
            by_item.get(
                item,
                {
                    "item": item,
                    "match": None,
                    "confidence": 0.0,
                    "reason": "No response from model for this item.",
                },
            )
            for item in list_a
        ]
