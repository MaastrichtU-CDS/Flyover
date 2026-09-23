"""
Backend unit: the provider-neutral matching kernel.

Tests cover the schema's strict-mode readiness, output-size estimation,
and the parse/validate/sanitise pipeline shared by every provider.
"""

import json
import sys
import unittest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.llm.matching import (
    MATCH_OUTPUT_SCHEMA,
    MatchingError,
    build_user_prompt,
    estimate_output_tokens,
    parse_and_sanitise,
    sanitise_pairs,
)


def _content(pairs):
    return json.dumps({"pairs": pairs})


class TestSchema(unittest.TestCase):
    """Test strict-structured-output readiness of the shared schema."""

    def test_additional_properties_false_at_both_levels(self):
        self.assertFalse(MATCH_OUTPUT_SCHEMA["additionalProperties"])
        items = MATCH_OUTPUT_SCHEMA["properties"]["pairs"]["items"]
        self.assertFalse(items["additionalProperties"])

    def test_required_lists_are_complete(self):
        self.assertEqual(MATCH_OUTPUT_SCHEMA["required"], ["pairs"])
        items = MATCH_OUTPUT_SCHEMA["properties"]["pairs"]["items"]
        self.assertEqual(items["required"], ["a", "match", "confidence", "reason"])


class TestEstimateOutputTokens(unittest.TestCase):
    """Test max_tokens sizing for cloud providers."""

    def test_scales_with_items_plus_headroom(self):
        self.assertEqual(estimate_output_tokens(["a"]), 55 + 1000)
        self.assertEqual(estimate_output_tokens(["a"] * 8), 55 * 8 + 1000)


class TestBuildUserPrompt(unittest.TestCase):
    """Test prompt construction."""

    def test_embeds_both_lists_as_json(self):
        prompt = build_user_prompt(["leeftijd"], ["age"])
        self.assertIn('["leeftijd"]', prompt)
        self.assertIn('["age"]', prompt)


class TestParseAndSanitise(unittest.TestCase):
    """Test the shared pipeline's failure paths and sanitisation."""

    def test_valid_content_round_trips(self):
        content = _content(
            [{"a": "age", "match": "patient_age", "confidence": 0.9, "reason": "r"}]
        )
        result = parse_and_sanitise(content, ["age"], ["patient_age"])
        self.assertEqual(result[0]["match"], "patient_age")

    def test_empty_content_raises(self):
        with self.assertRaises(MatchingError):
            parse_and_sanitise("  ", ["age"], ["x"])
        with self.assertRaises(MatchingError):
            parse_and_sanitise(None, ["age"], ["x"])

    def test_invalid_json_raises(self):
        with self.assertRaises(MatchingError):
            parse_and_sanitise("not json", ["age"], ["x"])

    def test_schema_violation_raises(self):
        with self.assertRaises(MatchingError):
            parse_and_sanitise(_content([{"a": "age"}]), ["age"], ["x"])

    def test_sanitise_guards_hallucinations_and_duplicates(self):
        pairs = [
            {"a": "age", "match": "invented", "confidence": 0.9, "reason": "r"},
            {"a": "sex", "match": "biological_sex", "confidence": 2.0, "reason": "r"},
            {"a": "sexe", "match": "biological_sex", "confidence": 0.8, "reason": "r"},
        ]
        result = sanitise_pairs(pairs, ["age", "sex", "sexe"], ["biological_sex"])
        self.assertIsNone(result[0]["match"])
        self.assertEqual(result[1]["confidence"], 1.0)
        self.assertIsNone(result[2]["match"])

    def test_duplicate_matches_resolved_by_confidence_not_order(self):
        # "sexe" appears first but is the weaker claim — the higher-confidence
        # "sex" claim must win regardless of output order.
        pairs = [
            {"a": "sexe", "match": "biological_sex", "confidence": 0.4, "reason": "r"},
            {"a": "sex", "match": "biological_sex", "confidence": 0.95, "reason": "r"},
        ]
        result = sanitise_pairs(pairs, ["sex", "sexe"], ["biological_sex"])
        by_item = {r["item"]: r for r in result}
        self.assertEqual(by_item["sex"]["match"], "biological_sex")
        self.assertIsNone(by_item["sexe"]["match"])

    def test_duplicate_matches_tie_keeps_first_seen(self):
        pairs = [
            {"a": "sexe", "match": "biological_sex", "confidence": 0.8, "reason": "r"},
            {"a": "sex", "match": "biological_sex", "confidence": 0.8, "reason": "r"},
        ]
        result = sanitise_pairs(pairs, ["sex", "sexe"], ["biological_sex"])
        by_item = {r["item"]: r for r in result}
        self.assertEqual(by_item["sexe"]["match"], "biological_sex")
        self.assertIsNone(by_item["sex"]["match"])

    def test_model_reported_no_match_carries_zero_confidence(self):
        # The prompt asks the model to abstain when unsure, so it returns
        # null matches with a confidence of its own. That number described
        # the *absence* of a mapping and reached the UI badge as e.g. "50%"
        # next to an empty description field.
        pairs = [
            {"a": "Age_diag", "match": None, "confidence": 0.5, "reason": "unsure"},
        ]
        result = sanitise_pairs(pairs, ["Age_diag"], ["age_at_diagnosis"])
        self.assertIsNone(result[0]["match"])
        self.assertEqual(result[0]["confidence"], 0.0)
        self.assertEqual(result[0]["reason"], "unsure")

    def test_deduplicated_loser_carries_zero_confidence(self):
        pairs = [
            {"a": "sex", "match": "biological_sex", "confidence": 0.95, "reason": "r"},
            {"a": "sexe", "match": "biological_sex", "confidence": 0.4, "reason": "r"},
        ]
        result = sanitise_pairs(pairs, ["sex", "sexe"], ["biological_sex"])
        by_item = {r["item"]: r for r in result}
        self.assertIsNone(by_item["sexe"]["match"])
        self.assertEqual(by_item["sexe"]["confidence"], 0.0)
        self.assertEqual(by_item["sex"]["confidence"], 0.95)


if __name__ == "__main__":
    unittest.main()
