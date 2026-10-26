"""
Unit tests for :mod:`services.suggestions.pasted_answer`.

The parser must accept what real LLM clients hand back: fenced JSON,
surrounding prose, trailing commas, the ``databases`` wrapper dropped or
the whole document echoed, and the flat ``[{item, match, ...}]`` record array.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.suggestions.pasted_answer import (
    AnswerParseError,
    extract_column_entries,
    parse_answer_text,
    records_from_answer,
)

ANSWER = {
    "databases": {
        "nki": {
            "tables": {
                "data": {
                    "columns": {
                        "administered_prom_language": {
                            "mapsTo": "schema:variable/administered_prom_language",
                            "localColumn": "taal",
                            "confidence": 0.9,
                            "reason": "Dutch 'taal' = language.",
                        },
                        "age_at_initial_diagnosis": {
                            "mapsTo": "schema:variable/age_at_initial_diagnosis",
                            "localColumn": "leeft",
                        },
                    }
                }
            }
        }
    }
}


class TestParseAnswerText(unittest.TestCase):
    def test_plain_json(self):
        self.assertEqual(parse_answer_text('{"a": 1}'), {"a": 1})

    def test_code_fences_and_prose(self):
        text = (
            "Sure! Here is the mapping you asked for:\n\n```json\n"
            '{"databases": {"nki": {}}}\n```\n\nLet me know if you need anything else.'
        )
        self.assertEqual(parse_answer_text(text), {"databases": {"nki": {}}})

    def test_prose_without_fences(self):
        text = 'Here you go: {"databases": {"nki": {}}} — hope this helps.'
        self.assertEqual(parse_answer_text(text), {"databases": {"nki": {}}})

    def test_trailing_commas(self):
        text = '{"databases": {"nki": {"tables": {},},},}'
        self.assertEqual(
            parse_answer_text(text), {"databases": {"nki": {"tables": {}}}}
        )

    def test_flat_array_in_prose(self):
        text = (
            'Answer:\n[{"item": "taal", "match": "administered_prom_language"}]\nDone.'
        )
        self.assertEqual(
            parse_answer_text(text),
            [{"item": "taal", "match": "administered_prom_language"}],
        )

    def test_already_parsed_object_passes_through(self):
        self.assertEqual(parse_answer_text({"x": 1}), {"x": 1})

    def test_empty_and_garbage(self):
        with self.assertRaises(AnswerParseError):
            parse_answer_text("   ")
        with self.assertRaises(AnswerParseError) as ctx:
            parse_answer_text("I cannot map these columns, sorry.")
        self.assertIn("Could not find valid JSON", str(ctx.exception))
        with self.assertRaises(AnswerParseError):
            parse_answer_text('{"unterminated": ')


class TestExtractColumnEntries(unittest.TestCase):
    def test_finds_entries_at_any_depth(self):
        entries = extract_column_entries(ANSWER)
        self.assertEqual(
            [k for k, _ in entries],
            ["administered_prom_language", "age_at_initial_diagnosis"],
        )

    def test_bare_columns_object_and_list(self):
        bare = {"taal_var": {"mapsTo": "schema:variable/x", "localColumn": "taal"}}
        self.assertEqual(len(extract_column_entries(bare)), 1)
        as_list = [{"mapsTo": "schema:variable/x", "localColumn": "taal"}]
        self.assertEqual(extract_column_entries(as_list)[0][0], None)

    def test_local_mappings_are_not_entries(self):
        entry = {
            "x": {
                "mapsTo": "schema:variable/x",
                "localColumn": "c",
                "localMappings": {"male": ["M"]},
            }
        }
        self.assertEqual(len(extract_column_entries(entry)), 1)


class TestRecordsFromAnswer(unittest.TestCase):
    def test_variables_phase(self):
        records = records_from_answer("variables", ANSWER, default_confidence=0.7)
        self.assertEqual(
            records,
            [
                {
                    "item": "taal",
                    "match": "administered_prom_language",
                    "confidence": 0.9,
                    "reason": "Dutch 'taal' = language.",
                },
                {
                    "item": "leeft",
                    "match": "age_at_initial_diagnosis",
                    "confidence": 0.7,
                    "reason": "",
                },
            ],
        )

    def test_variable_key_fallbacks(self):
        # mapsTo without the prefix, then "variable", then the entry key.
        data = {
            "columns": {
                "a": {"mapsTo": "age_at_initial_diagnosis", "localColumn": "c1"},
                "b": {"variable": "identifier", "localColumn": "c2"},
                "eortc_qlq_c30_q6": {"localColumn": "c3"},
                "d": {"mapsTo": "schema:variable/x", "localColumn": ["c4"]},
                "e": {"mapsTo": "schema:variable/x"},
            }
        }
        records = records_from_answer("variables", data, default_confidence=0.5)
        self.assertEqual(
            [(r["item"], r["match"]) for r in records],
            [
                ("c1", "age_at_initial_diagnosis"),
                ("c2", "identifier"),
                ("c3", "eortc_qlq_c30_q6"),
                ("c4", "x"),
            ],
        )

    def test_values_phase_with_value_notes(self):
        data = {
            "databases": {
                "nki": {
                    "tables": {
                        "data": {
                            "columns": {
                                "biological_sex": {
                                    "mapsTo": "schema:variable/biological_sex",
                                    "localColumn": "geslacht",
                                    "localMappings": {
                                        "female": ["F"],
                                        "male": "M",
                                        "unknown": None,
                                    },
                                    "valueNotes": {
                                        "F": {
                                            "confidence": 0.95,
                                            "reason": "F = female",
                                        }
                                    },
                                    "confidence": 0.6,
                                    "reason": "column level",
                                }
                            }
                        }
                    }
                }
            }
        }
        records = records_from_answer("values", data, default_confidence=0.7)
        self.assertEqual(
            records,
            [
                {
                    "item": "F",
                    "match": "female",
                    "confidence": 0.95,
                    "reason": "F = female",
                    "column": "geslacht",
                    "variable": "biological_sex",
                },
                {
                    "item": "M",
                    "match": "male",
                    "confidence": 0.6,
                    "reason": "column level",
                    "column": "geslacht",
                    "variable": "biological_sex",
                },
            ],
        )

    def test_flat_records_form(self):
        data = [
            {
                "item": "taal",
                "match": "administered_prom_language",
                "confidence": "0.9",
                "reason": "x",
            },
            {"item": "taal", "match": "identifier", "confidence": 0.2},
            {"item": "leeft", "match": None},
        ]
        records = records_from_answer("variables", data, default_confidence=0.7)
        self.assertEqual(
            [(r["item"], r["match"], r["confidence"]) for r in records],
            [
                ("taal", "administered_prom_language", 0.9),
                ("leeft", None, 0.7),
            ],
        )
        wrapped = records_from_answer(
            "variables", {"records": data}, default_confidence=0.7
        )
        self.assertEqual(len(wrapped), 2)

    def test_dedupe_keeps_highest_confidence(self):
        data = [
            {"mapsTo": "schema:variable/a", "localColumn": "c", "confidence": 0.3},
            {"mapsTo": "schema:variable/b", "localColumn": "c", "confidence": 0.8},
        ]
        records = records_from_answer("variables", data, default_confidence=0.7)
        self.assertEqual(
            records, [{"item": "c", "match": "b", "confidence": 0.8, "reason": ""}]
        )

    def test_unknown_phase(self):
        with self.assertRaises(ValueError):
            records_from_answer("nope", ANSWER, default_confidence=0.7)


if __name__ == "__main__":
    unittest.main()
