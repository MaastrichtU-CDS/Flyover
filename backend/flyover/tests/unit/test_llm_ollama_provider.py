"""
Backend unit: the Ollama LLM provider.

Tests cover request payload construction, response sanitisation
(confidence clamping, hallucination guard, duplicate handling), model
management with fallbacks, and failure behaviour — all with mocked HTTP.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import requests

from services.llm.ollama_provider import (
    MATCH_OUTPUT_SCHEMA,
    OllamaProvider,
    OllamaError,
    estimate_num_ctx,
)


def _chat_response(pairs):
    """Build a mocked /api/chat response carrying the given pairs."""
    response = MagicMock()
    response.json.return_value = {
        "message": {"content": json.dumps({"pairs": pairs})}
    }
    return response


class TestEstimateNumCtx(unittest.TestCase):
    """Test context window estimation."""

    def test_small_lists_floor_at_4096(self):
        self.assertEqual(estimate_num_ctx(["a"] * 8, ["b"] * 50), 4096)

    def test_large_lists_scale_in_powers_of_two(self):
        num_ctx = estimate_num_ctx(["a"] * 8, ["b"] * 600)
        self.assertEqual(num_ctx, 8192)

    def test_result_always_covers_estimate(self):
        for n_b in (10, 100, 500, 2000):
            num_ctx = estimate_num_ctx(["a"] * 8, ["b"] * n_b)
            self.assertGreaterEqual(num_ctx, 4096)
            self.assertEqual(num_ctx & (num_ctx - 1), 0)


class TestMatchEquivalents(unittest.TestCase):
    """Test the structured-output matcher with mocked HTTP."""

    def setUp(self):
        self.client = OllamaProvider("http://ollama:11434", model="m")

    @patch("services.llm.ollama_provider.requests.post")
    def test_sends_schema_constrained_payload(self, mock_post):
        mock_post.return_value = _chat_response(
            [{"a": "age", "match": "patient_age", "confidence": 0.9, "reason": "r"}]
        )
        self.client.match_equivalents(["age"], ["patient_age"])

        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["model"], "m")
        self.assertEqual(payload["format"], MATCH_OUTPUT_SCHEMA)
        self.assertFalse(payload["stream"])
        self.assertEqual(payload["options"]["num_ctx"], 4096)
        self.assertIn("list_a", payload["messages"][1]["content"])
        self.assertEqual(
            mock_post.call_args.kwargs["timeout"],
            (self.client.connect_timeout, self.client.read_timeout),
        )

    @patch("services.llm.ollama_provider.requests.post")
    def test_clamps_confidence(self, mock_post):
        mock_post.return_value = _chat_response(
            [
                {"a": "age", "match": "patient_age", "confidence": 1.7, "reason": "r"},
                {"a": "sex", "match": "biological_sex", "confidence": -2, "reason": "r"},
            ]
        )
        result = self.client.match_equivalents(
            ["age", "sex"], ["patient_age", "biological_sex"]
        )
        self.assertEqual(result[0]["confidence"], 1.0)
        self.assertEqual(result[1]["confidence"], 0.0)

    @patch("services.llm.ollama_provider.requests.post")
    def test_nulls_hallucinated_match(self, mock_post):
        mock_post.return_value = _chat_response(
            [{"a": "age", "match": "invented_var", "confidence": 0.9, "reason": "r"}]
        )
        result = self.client.match_equivalents(["age"], ["patient_age"])
        self.assertIsNone(result[0]["match"])
        self.assertEqual(result[0]["confidence"], 0.0)
        self.assertIn("invented_var", result[0]["reason"])

    @patch("services.llm.ollama_provider.requests.post")
    def test_nulls_duplicate_match_first_wins(self, mock_post):
        mock_post.return_value = _chat_response(
            [
                {"a": "age", "match": "patient_age", "confidence": 0.9, "reason": "r"},
                {"a": "leeftijd", "match": "patient_age", "confidence": 0.8, "reason": "r"},
            ]
        )
        result = self.client.match_equivalents(
            ["age", "leeftijd"], ["patient_age"]
        )
        self.assertEqual(result[0]["match"], "patient_age")
        self.assertIsNone(result[1]["match"])
        self.assertIn("already matched", result[1]["reason"])

    @patch("services.llm.ollama_provider.requests.post")
    def test_drops_unknown_items_and_fills_missing(self, mock_post):
        mock_post.return_value = _chat_response(
            [{"a": "bogus", "match": "patient_age", "confidence": 0.9, "reason": "r"}]
        )
        result = self.client.match_equivalents(["age"], ["patient_age"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["item"], "age")
        self.assertIsNone(result[0]["match"])

    @patch("services.llm.ollama_provider.requests.post")
    def test_preserves_list_a_order(self, mock_post):
        mock_post.return_value = _chat_response(
            [
                {"a": "b_col", "match": None, "confidence": 0, "reason": "r"},
                {"a": "a_col", "match": None, "confidence": 0, "reason": "r"},
            ]
        )
        result = self.client.match_equivalents(["a_col", "b_col"], ["x"])
        self.assertEqual([r["item"] for r in result], ["a_col", "b_col"])

    @patch("services.llm.ollama_provider.requests.post")
    def test_empty_content_raises(self, mock_post):
        response = MagicMock()
        response.json.return_value = {"message": {"content": "  "}}
        mock_post.return_value = response
        with self.assertRaises(OllamaError):
            self.client.match_equivalents(["age"], ["x"])

    @patch("services.llm.ollama_provider.requests.post")
    def test_invalid_json_raises(self, mock_post):
        response = MagicMock()
        response.json.return_value = {"message": {"content": "not json"}}
        mock_post.return_value = response
        with self.assertRaises(OllamaError):
            self.client.match_equivalents(["age"], ["x"])

    @patch("services.llm.ollama_provider.requests.post")
    def test_schema_violation_raises(self, mock_post):
        response = MagicMock()
        response.json.return_value = {
            "message": {"content": json.dumps({"pairs": [{"a": "age"}]})}
        }
        mock_post.return_value = response
        with self.assertRaises(OllamaError):
            self.client.match_equivalents(["age"], ["x"])


class TestEnsureModel(unittest.TestCase):
    """Test model availability management with fallbacks."""

    def setUp(self):
        self.client = OllamaProvider("http://ollama:11434", model="m")

    @patch("services.llm.ollama_provider.requests.get")
    def test_unreachable_server_raises_fast(self, mock_get):
        mock_get.side_effect = requests.ConnectionError("refused")
        with self.assertRaises(OllamaError) as ctx:
            self.client.ensure_model("llama3.2:3b")
        self.assertIn("Can't reach Ollama", str(ctx.exception))

    @patch.object(OllamaProvider, "has_model", return_value=True)
    @patch.object(OllamaProvider, "is_reachable", return_value=True)
    def test_present_model_returned_without_pull(self, _reachable, _has):
        self.assertEqual(self.client.ensure_model("llama3.2:3b"), "llama3.2:3b")

    @patch.object(OllamaProvider, "pull_model")
    @patch.object(OllamaProvider, "has_model")
    @patch.object(OllamaProvider, "is_reachable", return_value=True)
    def test_fallback_chain_on_pull_failure(self, _reachable, mock_has, mock_pull):
        mock_has.side_effect = lambda name: name == "llama3.2:1b"
        mock_pull.side_effect = OllamaError("no such model")
        chosen = self.client.ensure_model("llama3.2:3b", ["llama3.2:1b"])
        self.assertEqual(chosen, "llama3.2:1b")

    @patch.object(OllamaProvider, "pull_model", side_effect=OllamaError("fail"))
    @patch.object(OllamaProvider, "has_model", return_value=False)
    @patch.object(OllamaProvider, "is_reachable", return_value=True)
    def test_all_models_failing_raises(self, _reachable, _has, _pull):
        with self.assertRaises(OllamaError):
            self.client.ensure_model("llama3.2:3b", ["llama3.2:1b"])

    @patch("services.llm.ollama_provider.requests.post")
    @patch.object(OllamaProvider, "ensure_model", return_value="fallback:1b")
    def test_ensure_ready_pins_resolved_model_for_matching(self, _ensure, mock_post):
        provider = OllamaProvider(
            "http://ollama:11434", model="wanted:3b", fallback_models=["fallback:1b"]
        )
        self.assertEqual(provider.ensure_ready(), "fallback:1b")

        mock_post.return_value = _chat_response(
            [{"a": "age", "match": None, "confidence": 0, "reason": "r"}]
        )
        provider.match_equivalents(["age"], ["x"])
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["model"], "fallback:1b")


if __name__ == "__main__":
    unittest.main()
