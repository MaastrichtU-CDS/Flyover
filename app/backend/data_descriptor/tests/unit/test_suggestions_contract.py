"""
Unit tests for the suggestion record contract and ``sanitise_pairs``.

Covers schema validation, confidence clamping, non-member match nulling
(the hallucination guard), and missing-item fill-ins.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from jsonschema import ValidationError

from services.suggestions.contract import (
    SUGGESTION_RECORD_SCHEMA,
    SuggestionRecord,
    is_valid_record,
    sanitise_pairs,
    validate_record,
)


class TestSuggestionRecordSchema(unittest.TestCase):
    def test_valid_record_passes_validation(self):
        record = {
            "item": "morf",
            "match": "tumour_morphology_icd_o",
            "confidence": 0.92,
            "reason": "Alias hit.",
            "source": "alias",
            "tier": 1,
            "status": "done",
        }
        validate_record(record)  # does not raise
        self.assertTrue(is_valid_record(record))

    def test_match_may_be_null(self):
        record = {
            "item": "surv1",
            "match": None,
            "confidence": 0.0,
            "reason": "Abstain.",
            "source": "string",
            "tier": 1,
            "status": "done",
        }
        self.assertTrue(is_valid_record(record))

    def test_unknown_source_rejected(self):
        record = {
            "item": "x",
            "match": None,
            "confidence": 0.0,
            "reason": "r",
            "source": "magic",
            "tier": 1,
            "status": "done",
        }
        with self.assertRaises(ValidationError):
            validate_record(record)

    def test_unknown_tier_rejected(self):
        record = {
            "item": "x",
            "match": None,
            "confidence": 0.0,
            "reason": "r",
            "source": "alias",
            "tier": 4,
            "status": "done",
        }
        with self.assertRaises(ValidationError):
            validate_record(record)

    def test_missing_required_field_rejected(self):
        record = {"item": "x", "match": None}
        with self.assertRaises(ValidationError):
            validate_record(record)

    def test_alternatives_block_valid(self):
        record = {
            "item": "x",
            "match": "biological_sex",
            "confidence": 0.9,
            "reason": "r",
            "source": "alias",
            "tier": 1,
            "status": "done",
            "alternatives": [
                {"match": "sex", "confidence": 0.7, "source": "string", "tier": 1},
            ],
        }
        self.assertTrue(is_valid_record(record))


class TestSanitisePairs(unittest.TestCase):
    def test_clamps_confidence_to_unit_interval(self):
        out = sanitise_pairs(
            [{"item": "a", "match": "v1", "confidence": 1.5, "reason": "r"}],
            items=["a"],
            valid_targets=["v1"],
            source="alias",
            tier=1,
        )
        self.assertEqual(out[0]["confidence"], 1.0)
        out = sanitise_pairs(
            [{"item": "a", "match": "v1", "confidence": -0.3, "reason": "r"}],
            items=["a"],
            valid_targets=["v1"],
            source="alias",
            tier=1,
        )
        self.assertEqual(out[0]["confidence"], 0.0)

    def test_non_member_match_is_nulled(self):
        out = sanitise_pairs(
            [{"item": "a", "match": "hallucination", "confidence": 0.9, "reason": "r"}],
            items=["a"],
            valid_targets=["v1", "v2"],
            source="alias",
            tier=1,
        )
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)
        self.assertIn("hallucination", out[0]["reason"])

    def test_drops_unknown_items_and_fills_missing(self):
        out = sanitise_pairs(
            [{"item": "a", "match": "v1", "confidence": 0.9, "reason": "r"}],
            items=["a", "b"],
            valid_targets=["v1"],
            source="alias",
            tier=1,
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["item"], "a")
        self.assertEqual(out[1]["item"], "b")
        self.assertIsNone(out[1]["match"])
        self.assertEqual(out[1]["confidence"], 0.0)

    def test_stamps_source_tier_status(self):
        out = sanitise_pairs(
            [{"item": "a", "match": "v1", "confidence": 0.9, "reason": "r"}],
            items=["a"],
            valid_targets=["v1"],
            source="value_regex",
            tier=1,
            status="done",
        )
        self.assertEqual(out[0]["source"], "value_regex")
        self.assertEqual(out[0]["tier"], 1)
        self.assertEqual(out[0]["status"], "done")

    def test_empty_reason_gets_default(self):
        out = sanitise_pairs(
            [{"item": "a", "match": "v1", "confidence": 0.9, "reason": ""}],
            items=["a"],
            valid_targets=["v1"],
            source="alias",
            tier=1,
        )
        self.assertNotEqual(out[0]["reason"], "")

    def test_duplicate_items_first_wins(self):
        out = sanitise_pairs(
            [
                {"item": "a", "match": "v1", "confidence": 0.9, "reason": "first"},
                {"item": "a", "match": "v2", "confidence": 0.99, "reason": "second"},
            ],
            items=["a"],
            valid_targets=["v1", "v2"],
            source="alias",
            tier=1,
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["match"], "v1")

    def test_garbage_confidence_treated_as_zero(self):
        out = sanitise_pairs(
            [{"item": "a", "match": "v1", "confidence": "high", "reason": "r"}],
            items=["a"],
            valid_targets=["v1"],
            source="alias",
            tier=1,
        )
        self.assertEqual(out[0]["confidence"], 0.0)


class TestSuggestionRecordDataclass(unittest.TestCase):
    def test_roundtrip(self):
        r = SuggestionRecord(
            item="x", match="v1", confidence=0.5, reason="r", source="alias", tier=1
        )
        d = r.to_dict()
        r2 = SuggestionRecord.from_dict(d)
        self.assertEqual(r2.item, "x")
        self.assertEqual(r2.match, "v1")
        self.assertEqual(r2.source, "alias")


if __name__ == "__main__":
    unittest.main()
