"""
Backend unit: the Anthropic LLM provider.

Tests cover the structured-output request shape (no sampling params,
sized max_tokens, anyOf-transformed schema), refusal/truncation handling,
readiness without a key, and cached reachability — with an injected mock
SDK client; the network is never touched.
"""

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import anthropic

from services.llm.anthropic_provider import AnthropicProvider, _anthropic_schema
from services.llm.base import LLMProviderError
from services.llm.matching import MATCH_OUTPUT_SCHEMA


def _sdk_response(pairs, stop_reason="end_turn"):
    return SimpleNamespace(
        stop_reason=stop_reason,
        content=[SimpleNamespace(type="text", text=json.dumps({"pairs": pairs}))],
    )


AGE_PAIR = [{"a": "age", "match": "patient_age", "confidence": 0.9, "reason": "r"}]


def _provider(**kwargs):
    client = MagicMock()
    client.api_key = kwargs.pop("client_api_key", "sk-test")
    provider = AnthropicProvider(api_key="sk-test", client=client, **kwargs)
    return provider, client


class TestSchemaTransform(unittest.TestCase):
    """Test the anyOf rewrite for Anthropic's schema subset."""

    def test_type_array_becomes_anyof(self):
        schema = _anthropic_schema()
        match_leaf = schema["properties"]["pairs"]["items"]["properties"]["match"]
        self.assertNotIn("type", match_leaf)
        self.assertEqual(
            match_leaf["anyOf"], [{"type": "string"}, {"type": "null"}]
        )

    def test_shared_schema_is_untouched(self):
        _anthropic_schema()
        match_leaf = MATCH_OUTPUT_SCHEMA["properties"]["pairs"]["items"][
            "properties"
        ]["match"]
        self.assertEqual(match_leaf["type"], ["string", "null"])

    def test_additional_properties_false_preserved(self):
        schema = _anthropic_schema()
        self.assertFalse(schema["additionalProperties"])
        self.assertFalse(schema["properties"]["pairs"]["items"]["additionalProperties"])


class TestMatchEquivalents(unittest.TestCase):
    """Test the structured-output request and response handling."""

    def test_request_shape(self):
        provider, client = _provider()
        client.messages.create.return_value = _sdk_response(AGE_PAIR)

        result = provider.match_equivalents(["age"], ["patient_age"])
        self.assertEqual(result[0]["match"], "patient_age")

        kwargs = client.messages.create.call_args.kwargs
        self.assertEqual(kwargs["model"], "claude-opus-4-8")
        self.assertEqual(kwargs["max_tokens"], 55 * 1 + 1000)
        self.assertNotIn("temperature", kwargs)
        self.assertEqual(kwargs["output_config"]["format"]["type"], "json_schema")
        match_leaf = kwargs["output_config"]["format"]["schema"]["properties"][
            "pairs"
        ]["items"]["properties"]["match"]
        self.assertIn("anyOf", match_leaf)

    def test_hallucinated_match_still_sanitised(self):
        provider, client = _provider()
        client.messages.create.return_value = _sdk_response(
            [{"a": "age", "match": "invented", "confidence": 0.9, "reason": "r"}]
        )
        result = provider.match_equivalents(["age"], ["patient_age"])
        self.assertIsNone(result[0]["match"])

    def test_refusal_raises(self):
        provider, client = _provider()
        client.messages.create.return_value = _sdk_response(
            AGE_PAIR, stop_reason="refusal"
        )
        with self.assertRaises(LLMProviderError):
            provider.match_equivalents(["age"], ["patient_age"])

    def test_truncation_raises(self):
        provider, client = _provider()
        client.messages.create.return_value = _sdk_response(
            AGE_PAIR, stop_reason="max_tokens"
        )
        with self.assertRaises(LLMProviderError):
            provider.match_equivalents(["age"], ["patient_age"])

    def test_connection_error_wrapped(self):
        provider, client = _provider()
        client.messages.create.side_effect = anthropic.APIConnectionError(
            request=MagicMock()
        )
        with self.assertRaises(LLMProviderError):
            provider.match_equivalents(["age"], ["patient_age"])


class TestReadinessAndReachability(unittest.TestCase):
    """Test ensure_ready and the cached reachability probe."""

    def test_ensure_ready_returns_model_with_key(self):
        provider, _ = _provider()
        self.assertEqual(provider.ensure_ready(), "claude-opus-4-8")

    def test_ensure_ready_raises_without_any_key(self):
        client = MagicMock()
        client.api_key = None
        provider = AnthropicProvider(api_key=None, client=client)
        with self.assertRaises(LLMProviderError):
            provider.ensure_ready()

    def test_reachability_probe_is_cached(self):
        provider, client = _provider()
        client.models.retrieve.return_value = SimpleNamespace(id="claude-opus-4-8")

        self.assertTrue(provider.is_reachable())
        self.assertTrue(provider.is_reachable())
        self.assertEqual(client.models.retrieve.call_count, 1)

    def test_probe_failure_is_unreachable(self):
        provider, client = _provider()
        client.models.retrieve.side_effect = anthropic.APIConnectionError(
            request=MagicMock()
        )
        self.assertFalse(provider.is_reachable())


if __name__ == "__main__":
    unittest.main()
