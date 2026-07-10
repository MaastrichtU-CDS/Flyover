"""
Backend unit: the OpenAI-compatible LLM provider.

Tests cover base-url normalisation, auth header behavior, the strict
json_schema request shape, the sticky json_object downgrade on servers
that reject it, reachability semantics, and failure paths — all with
mocked HTTP.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import requests

from services.llm.base import LLMProviderError
from services.llm.matching import MATCH_OUTPUT_SCHEMA
from services.llm.openai_provider import OpenAICompatProvider, _normalise_base_url


def _chat_response(pairs, status_code=200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = {
        "choices": [{"message": {"content": json.dumps({"pairs": pairs})}}]
    }
    return response


def _error_response(status_code, text):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    return response


AGE_PAIR = [{"a": "age", "match": "patient_age", "confidence": 0.9, "reason": "r"}]


class TestBaseUrlNormalisation(unittest.TestCase):
    """Test /v1 suffix handling."""

    def test_variants_normalise_to_v1(self):
        self.assertEqual(_normalise_base_url("http://vllm:8000"), "http://vllm:8000/v1")
        self.assertEqual(
            _normalise_base_url("http://vllm:8000/v1"), "http://vllm:8000/v1"
        )
        self.assertEqual(
            _normalise_base_url("https://api.openai.com/v1/"),
            "https://api.openai.com/v1",
        )


class TestRequestShape(unittest.TestCase):
    """Test the strict json_schema request and auth behavior."""

    @patch("services.llm.openai_provider.requests.post")
    def test_json_schema_payload_with_bearer_auth(self, mock_post):
        mock_post.return_value = _chat_response(AGE_PAIR)
        provider = OpenAICompatProvider(
            "https://api.openai.com", model="gpt-4o-mini", api_key="sk-test"
        )
        result = provider.match_equivalents(["age"], ["patient_age"])

        self.assertEqual(result[0]["match"], "patient_age")
        self.assertEqual(
            mock_post.call_args.args[0], "https://api.openai.com/v1/chat/completions"
        )
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["model"], "gpt-4o-mini")
        self.assertEqual(payload["response_format"]["type"], "json_schema")
        self.assertEqual(
            payload["response_format"]["json_schema"]["schema"], MATCH_OUTPUT_SCHEMA
        )
        self.assertTrue(payload["response_format"]["json_schema"]["strict"])
        self.assertEqual(payload["max_tokens"], 55 * 1 + 1000)
        headers = mock_post.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer sk-test")

    @patch("services.llm.openai_provider.requests.post")
    def test_no_auth_header_without_api_key(self, mock_post):
        mock_post.return_value = _chat_response(AGE_PAIR)
        provider = OpenAICompatProvider("http://lmstudio:1234", model="local")
        provider.match_equivalents(["age"], ["patient_age"])
        self.assertNotIn("Authorization", mock_post.call_args.kwargs["headers"])


class TestStructuredOutputDowngrade(unittest.TestCase):
    """Test the sticky json_object fallback."""

    @patch("services.llm.openai_provider.requests.post")
    def test_downgrades_on_response_format_rejection_and_sticks(self, mock_post):
        mock_post.side_effect = [
            _error_response(400, "Invalid parameter: 'response_format' json_schema"),
            _chat_response(AGE_PAIR),
            _chat_response(AGE_PAIR),
        ]
        provider = OpenAICompatProvider("http://llamacpp:8080", model="local")

        result = provider.match_equivalents(["age"], ["patient_age"])
        self.assertEqual(result[0]["match"], "patient_age")

        retry_payload = mock_post.call_args_list[1].kwargs["json"]
        self.assertEqual(retry_payload["response_format"], {"type": "json_object"})
        self.assertIn("JSON schema", retry_payload["messages"][1]["content"])

        provider.match_equivalents(["age"], ["patient_age"])
        third_payload = mock_post.call_args_list[2].kwargs["json"]
        self.assertEqual(third_payload["response_format"], {"type": "json_object"})
        self.assertEqual(mock_post.call_count, 3)

    @patch("services.llm.openai_provider.requests.post")
    def test_unrelated_400_raises_without_downgrade(self, mock_post):
        mock_post.return_value = _error_response(400, "model not found")
        provider = OpenAICompatProvider("http://vllm:8000", model="missing")
        with self.assertRaises(LLMProviderError):
            provider.match_equivalents(["age"], ["patient_age"])
        self.assertEqual(mock_post.call_count, 1)

    @patch("services.llm.openai_provider.requests.post")
    def test_pinned_json_object_mode_never_sends_schema_format(self, mock_post):
        mock_post.return_value = _chat_response(AGE_PAIR)
        provider = OpenAICompatProvider(
            "http://vllm:8000", model="local", structured_mode="json_object"
        )
        provider.match_equivalents(["age"], ["patient_age"])
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["response_format"], {"type": "json_object"})


class TestReachabilityAndReadiness(unittest.TestCase):
    """Test is_reachable and ensure_ready semantics."""

    @patch("services.llm.openai_provider.requests.get")
    def test_reachable_on_any_http_response(self, mock_get):
        mock_get.return_value = MagicMock(status_code=401)
        provider = OpenAICompatProvider("https://api.openai.com", model="gpt-4o-mini")
        self.assertTrue(provider.is_reachable())

    @patch("services.llm.openai_provider.requests.get")
    def test_unreachable_on_connection_error(self, mock_get):
        mock_get.side_effect = requests.ConnectionError("refused")
        provider = OpenAICompatProvider("http://vllm:8000", model="local")
        self.assertFalse(provider.is_reachable())

    def test_ensure_ready_requires_model(self):
        provider = OpenAICompatProvider("http://vllm:8000", model="")
        with self.assertRaises(LLMProviderError):
            provider.ensure_ready()

    @patch.object(OpenAICompatProvider, "is_reachable", return_value=True)
    def test_ensure_ready_returns_model(self, _reachable):
        provider = OpenAICompatProvider("http://vllm:8000", model="local")
        self.assertEqual(provider.ensure_ready(), "local")

    @patch.object(OpenAICompatProvider, "is_reachable", return_value=False)
    def test_ensure_ready_raises_when_unreachable(self, _reachable):
        provider = OpenAICompatProvider("http://vllm:8000", model="local")
        with self.assertRaises(LLMProviderError):
            provider.ensure_ready()


class TestResponseParsing(unittest.TestCase):
    """Test malformed-response handling."""

    @patch("services.llm.openai_provider.requests.post")
    def test_unexpected_shape_raises(self, mock_post):
        response = MagicMock(status_code=200)
        response.json.return_value = {"unexpected": True}
        mock_post.return_value = response
        provider = OpenAICompatProvider("http://vllm:8000", model="local")
        with self.assertRaises(LLMProviderError):
            provider.match_equivalents(["age"], ["patient_age"])

    @patch("services.llm.openai_provider.requests.post")
    def test_invalid_content_raises_via_matching_pipeline(self, mock_post):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "choices": [{"message": {"content": "not json"}}]
        }
        mock_post.return_value = response
        provider = OpenAICompatProvider("http://vllm:8000", model="local")
        with self.assertRaises(LLMProviderError):
            provider.match_equivalents(["age"], ["patient_age"])


if __name__ == "__main__":
    unittest.main()
