"""
Unit tests for :mod:`services.suggestions.prompt_export`.

Covers composition order, schema-slice completeness, the no-data-rows
guarantee, chunking, every mapped column being asked, hint rendering and
the values phase's "only the mapped variables' terms" rule.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders import JSONLDMapping
from services.suggestions.prompt_export import (
    DEFAULT_CHUNK,
    PromptExport,
    answer_schema,
    chunk_size_from_env,
    clamp_chunk,
)

MAPPING_DATA = {
    "@context": {"schema": "mapping:schema/", "mapping": "http://example.org/mapping#"},
    "@id": "mapping:root",
    "@type": "mapping:SemanticMapping",
    "schema": {
        "@id": "schema:root",
        "@type": "mapping:Schema",
        "variables": {
            "identifier": {
                "@type": "schema:IdentifierVariable",
                "dataType": "identifier",
                "description": "pseudonymised patient id",
            },
            "biological_sex": {
                "@type": "schema:CategoricalVariable",
                "dataType": "categorical",
                "valueMapping": {
                    "terms": {
                        "male": {"targetClass": "ncit:C20197"},
                        "female": {"targetClass": "ncit:C16576"},
                    }
                },
            },
            "age_at_initial_diagnosis": {
                "@type": "schema:ContinuousVariable",
                "dataType": "continuous",
            },
            "administered_prom_language": {
                "@type": "schema:CategoricalVariable",
                "dataType": "categorical",
                "valueMapping": {
                    "terms": {
                        "dutch": {"targetClass": "x:nl"},
                        "english": {"targetClass": "x:en"},
                    }
                },
            },
            "eortc_qlq_c30_q6": {
                "@type": "schema:CategoricalVariable",
                "dataType": "categorical",
                "valueMapping": {"terms": {"not_at_all": {"targetClass": "x:1"}}},
            },
        },
    },
    "databases": {
        "nki": {
            "@id": "mapping:database/nki",
            "@type": "mapping:Database",
            "name": "nki",
            "tables": {
                "data": {
                    "@id": "mapping:table/nki/data",
                    "@type": "mapping:Table",
                    "sourceFile": "nki",
                    "columns": {
                        "biological_sex": {
                            "mapsTo": "schema:variable/biological_sex",
                            "localColumn": "geslacht",
                            "localMappings": {"male": ["M"]},
                        }
                    },
                }
            },
        }
    },
}

COLUMNS = [
    "Rnnummer",
    "geslacht",
    "taal",
    "leeft",
    "jaar_van_diagnose",
    "surv70",
    "opmerking",
]

# Distinct values per column; the identifier and free-text columns hold
# "cell values" that must never appear in a prompt.
ROW_LIKE_IDS = [f"P{i:04d}" for i in range(1, 151)]
FREE_TEXT = [
    f"patient reported feeling {w} after the third cycle of treatment"
    for w in ("tired", "fine", "dizzy", "well", "sick")
] + [f"note number {i} about the visit" for i in range(60)]
VALUES = {
    "Rnnummer": ROW_LIKE_IDS,
    "geslacht": ["M", "F", "U"],
    "taal": ["nl_NL", "en_GB"],
    "leeft": [str(n) for n in range(18, 60)],
    "jaar_van_diagnose": [str(n) for n in range(1990, 2024)],
    "surv70": ["1", "2", "3", "4"],
    "opmerking": FREE_TEXT,
}

RECORDS = {
    "nki_jaar_van_diagnose": {
        "item": "jaar_van_diagnose",
        "match": "age_at_initial_diagnosis",
        "confidence": 0.86,
        "source": "string",
        "tier": 1,
        "status": "done",
    },
    "nki_surv70": {
        "item": "surv70",
        "match": None,
        "confidence": 0.0,
        "source": "string",
        "tier": 1,
        "status": "done",
    },
    "nki_geslacht_U": {
        "item": "U",
        "match": None,
        "confidence": 0.0,
        "source": "string",
        "tier": 1,
        "status": "done",
    },
}


def _export(phase="variables", **overrides):
    mapping = JSONLDMapping.from_dict(MAPPING_DATA)
    kwargs = dict(
        columns=COLUMNS,
        distinct_values=lambda col: VALUES.get(col, []),
        records=RECORDS,
        mapping_data=MAPPING_DATA,
    )
    kwargs.update(overrides)
    return PromptExport(phase, "nki", mapping, **kwargs)


class TestVariablesPrompt(unittest.TestCase):
    def test_sections_in_order_and_only_unmapped_columns(self):
        result = _export().build()
        prompt = result["prompt"]
        positions = [prompt.index(h) for h in ("## 1.", "## 2.", "## 3.", "## 4.")]
        self.assertEqual(positions, sorted(positions))
        # Every unmapped column is an item; the mapped one is context only.
        self.assertEqual(result["item_count"], len(COLUMNS) - 1)
        section3 = prompt.split("## 3.")[1].split("## 4.")[0]
        for column in COLUMNS:
            if column == "geslacht":
                self.assertNotIn(f"- {column}", section3)
            else:
                self.assertIn(f"- {column}", section3)
        self.assertEqual(result["already_mapped"], 1)

    def test_schema_slice_lists_free_variables_and_marks_taken_in_section_2(self):
        prompt = _export().build()["prompt"]
        section1 = prompt.split("## 1.")[1].split("## 2.")[0]
        section2 = prompt.split("## 2.")[1].split("## 3.")[0]
        for key in (
            "identifier",
            "age_at_initial_diagnosis",
            "administered_prom_language",
            "eortc_qlq_c30_q6",
        ):
            self.assertIn(f"- {key}:", section1)
        # biological_sex is already used by "geslacht": shown as taken, not offered.
        self.assertNotIn("- biological_sex:", section1)
        self.assertIn('"mapsTo": "schema:variable/biological_sex"', section2)
        self.assertIn('"localColumn": "geslacht"', section2)
        # Description from the raw JSON-LD reaches the slice.
        self.assertIn("pseudonymised patient id", section1)

    def test_prompt_contains_no_data_rows(self):
        prompt = _export().build()["prompt"]
        for cell in ROW_LIKE_IDS:
            self.assertNotIn(cell, prompt)
        for cell in FREE_TEXT:
            self.assertNotIn(cell, prompt)
        # The variables phase shares column names only — never a value.
        self.assertIn("- Rnnummer\n", prompt + "\n")
        self.assertNotIn("nl_NL", prompt)
        self.assertNotIn("en_GB", prompt)
        self.assertNotIn("150 distinct", prompt)

    def test_hints_render_matches_and_abstains(self):
        prompt = _export().build()["prompt"]
        self.assertIn("jaar_van_diagnose", prompt)
        self.assertIn("hint: age_at_initial_diagnosis (0.86, string)", prompt)
        self.assertIn("- surv70    hint: no candidate", prompt)

    def test_answer_format_is_the_jsonld_section(self):
        result = _export().build()
        prompt = result["prompt"]
        self.assertIn('"databases": {', prompt)
        self.assertIn('"nki": {', prompt)
        self.assertIn('"tables": {', prompt)
        self.assertIn('"data": {', prompt)
        self.assertIn('"mapsTo": "schema:variable/<variable_key>"', prompt)
        self.assertEqual(result["answer_schema"]["required"], ["databases"])

    def test_chunking_repeats_schema_and_context_per_chunk(self):
        result = _export(chunk_size=5).build()
        self.assertEqual(result["chunk_hint"], 5)
        self.assertEqual(len(result["chunks"]), 2)
        self.assertEqual(result["chunks"][0]["item_count"], 5)
        self.assertEqual(result["chunks"][1]["item_count"], 1)
        for chunk in result["chunks"]:
            self.assertIn("## 1.", chunk["prompt"])
            self.assertIn('"localColumn": "geslacht"', chunk["prompt"])
            self.assertIn(f"Part {chunk['index']} of 2", chunk["prompt"])
        self.assertEqual(result["prompt"], result["chunks"][0]["prompt"])
        self.assertEqual(
            [c for chunk in result["chunks"] for c in chunk["items"]],
            [c for c in COLUMNS if c != "geslacht"],
        )

    def test_nothing_to_map_gives_no_chunks(self):
        result = _export(columns=["geslacht"]).build()
        self.assertEqual(result["item_count"], 0)
        self.assertEqual(result["chunks"], [])
        self.assertEqual(result["prompt"], "")

    def test_contains_and_privacy_notice(self):
        result = _export().build()
        self.assertIn("column names", result["contains"])
        self.assertIn("variable keys", result["contains"])
        self.assertIn("no data rows", result["privacy"])
        # The variables phase shares no values at all: neither the contains
        # list nor the notice may claim any.
        self.assertFalse(any("distinct values" in c for c in result["contains"]))
        self.assertNotIn("distinct values", result["privacy"])
        # The values phase does share values (that is its purpose).
        values = _export("values").build()
        self.assertTrue(any("distinct values" in c for c in values["contains"]))
        self.assertIn("distinct values", values["privacy"])


class TestValuesPrompt(unittest.TestCase):
    def test_only_mapped_variables_terms_and_unmapped_values(self):
        result = _export("values").build()
        prompt = result["prompt"]
        # Only geslacht is mapped (to biological_sex); "M" is already mapped.
        self.assertEqual(result["item_count"], 2)
        self.assertIn("- biological_sex:", prompt)
        self.assertIn("terms: male (ncit:C20197), female (ncit:C16576)", prompt)
        self.assertNotIn("- administered_prom_language:", prompt)
        self.assertIn('already mapped: "M" → male', prompt)
        self.assertIn('- "F"', prompt)
        self.assertIn('- "U"    hint: no candidate', prompt)
        self.assertNotIn('- "M"', prompt)
        self.assertIn('"localMappings": {', prompt)
        self.assertIn("term keys", result["contains"])

    def test_every_mapped_column_is_asked_no_free_text_exclusion(self):
        # A mapped categorical column may hold many string values, but it
        # is never treated as free text: all of them are asked.
        data = _with_wide_column_mapped()
        mapping = JSONLDMapping.from_dict(data)
        kwargs = dict(
            columns=COLUMNS,
            distinct_values=lambda c: VALUES.get(c, []),
            mapping_data=data,
        )
        result = PromptExport("values", "nki", mapping, **kwargs).build()
        all_prompts = "\n".join(c["prompt"] for c in result["chunks"])
        self.assertIn(FREE_TEXT[0], all_prompts)
        self.assertIn('Column "opmerking"', all_prompts)
        self.assertNotIn("free-text", " ".join(result["contains"]))
        self.assertNotIn("free-text", result["privacy"])
        self.assertIn("categorical columns", result["privacy"])

    def test_values_chunks_keep_whole_columns(self):
        data = _with_wide_column_mapped()
        mapping = JSONLDMapping.from_dict(data)
        result = PromptExport(
            "values",
            "nki",
            mapping,
            columns=COLUMNS,
            distinct_values=lambda c: VALUES.get(c, []),
            chunk_size=10,
        ).build()
        # geslacht (2 values) fits a chunk; opmerking (65) exceeds the size
        # and gets a chunk of its own instead of being split.
        self.assertEqual(
            [c["items"] for c in result["chunks"]], [["geslacht"], ["opmerking"]]
        )
        self.assertEqual(result["chunks"][1]["item_count"], 65)


def _with_wide_column_mapped():
    import copy

    data = copy.deepcopy(MAPPING_DATA)
    data["schema"]["variables"]["remark"] = {
        "@type": "schema:CategoricalVariable",
        "dataType": "categorical",
        "valueMapping": {"terms": {"tired": {"targetClass": "x:t"}}},
    }
    data["databases"]["nki"]["tables"]["data"]["columns"]["remark"] = {
        "mapsTo": "schema:variable/remark",
        "localColumn": "opmerking",
    }
    return data


class TestHelpers(unittest.TestCase):
    def test_chunk_size_clamped(self):
        self.assertEqual(clamp_chunk(1), 5)
        self.assertEqual(clamp_chunk(10_000), 1000)
        self.assertEqual(clamp_chunk("x"), DEFAULT_CHUNK)
        with unittest.mock.patch.dict(
            "os.environ", {"FLYOVER_SUGGESTION_PROMPT_CHUNK": "12"}
        ):
            self.assertEqual(chunk_size_from_env(), 12)
        with unittest.mock.patch.dict(
            "os.environ", {"FLYOVER_SUGGESTION_PROMPT_CHUNK": "abc"}
        ):
            self.assertEqual(chunk_size_from_env(), DEFAULT_CHUNK)

    def test_answer_schema_values_requires_local_mappings(self):
        entry = answer_schema("values")["properties"]["databases"][
            "additionalProperties"
        ]["properties"]["tables"]["additionalProperties"]["properties"]["columns"][
            "additionalProperties"
        ]
        self.assertIn("localMappings", entry["required"])
        self.assertNotIn(
            "localMappings",
            answer_schema("variables")["properties"]["databases"][
                "additionalProperties"
            ]["properties"]["tables"]["additionalProperties"]["properties"]["columns"][
                "additionalProperties"
            ][
                "required"
            ],
        )

    def test_unknown_phase(self):
        with self.assertRaises(ValueError):
            _export("nope")


import unittest.mock  # noqa: E402  (used by TestHelpers)

if __name__ == "__main__":
    unittest.main()
