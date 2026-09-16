"""
Unit tests for tier-1 rule-based matchers (alias, value_regex, string).

Uses a fixture JSON-LD with two databases (an English-header site and an
NKI-style Dutch slice) to exercise alias memory with leave-one-site-out,
value-type regexes, and string similarity with margin abstain.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders import JSONLDMapping
from services.suggestions.tiers import SuggestionContext
from services.suggestions.tiers.rules import (
    AliasMatcher,
    StringMatcher,
    ValueRegexMatcher,
    build_alias_memory,
    jaro_winkler,
    load_rules,
    normalise_label,
    tokenise,
)

_RULES = load_rules()


def _make_mapping() -> JSONLDMapping:
    """Two databases: christie (English headers) and nki (Dutch)."""
    return JSONLDMapping.from_dict({
        "@context": {"schema": "mapping:schema/", "mapping": "http://example.org/mapping#"},
        "@id": "mapping:root",
        "@type": "mapping:SemanticMapping",
        "schema": {
            "@id": "schema:root",
            "@type": "mapping:Schema",
            "variables": {
                "biological_sex": {
                    "@type": "schema:CategoricalVariable",
                    "dataType": "categorical",
                    "predicate": "sio:has_sex",
                    "class": "ncit:C28421",
                    "valueMapping": {
                        "terms": {
                            "male": {"targetClass": "ncit:C20197"},
                            "female": {"targetClass": "ncit:C16576"},
                        }
                    },
                },
                "tumour_morphology_icd_o": {
                    "@type": "schema:StandardisedVariable",
                    "dataType": "standardised",
                    "predicate": "sio:has_morphology",
                    "class": "ncit:C94812",
                },
                "year_of_initial_diagnosis": {
                    "@type": "schema:ContinuousVariable",
                    "dataType": "continuous",
                    "predicate": "sio:has_year",
                    "class": "ncit:C81206",
                },
                "year_of_last_followup": {
                    "@type": "schema:ContinuousVariable",
                    "dataType": "continuous",
                    "predicate": "sio:has_year",
                    "class": "ncit:C81206",
                },
                "yes_no_response": {
                    "@type": "schema:CategoricalVariable",
                    "dataType": "categorical",
                    "predicate": "sio:has_response",
                    "class": "ncit:C25526",
                    "valueMapping": {
                        "terms": {
                            "yes": {"targetClass": "ncit:C25227"},
                            "no": {"targetClass": "ncit:C25225"},
                        }
                    },
                },
            },
        },
        "databases": {
            "christie": {
                "@id": "mapping:database/christie",
                "@type": "mapping:Database",
                "name": "christie",
                "tables": {
                    "data": {
                        "@id": "mapping:table/christie/data",
                        "@type": "mapping:Table",
                        "sourceFile": "christie",
                        "columns": {
                            "morph": {
                                "mapsTo": "schema:variable/tumour_morphology_icd_o",
                                "localColumn": "morph",
                            },
                            "sex": {
                                "mapsTo": "schema:variable/biological_sex",
                                "localColumn": "sex",
                                "localMappings": {
                                    "male": ["M"],
                                    "female": ["F"],
                                },
                            },
                        },
                    }
                },
            },
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
                            "geslacht": {
                                "mapsTo": "schema:variable/biological_sex",
                                "localColumn": "geslacht",
                                "localMappings": {
                                    "male": ["man"],
                                    "female": ["vrouw"],
                                },
                            },
                        },
                    }
                },
            },
        },
    })


VARIABLE_KEYS = [
    "biological_sex",
    "tumour_morphology_icd_o",
    "year_of_initial_diagnosis",
    "year_of_last_followup",
    "yes_no_response",
]


class TestNormalisation(unittest.TestCase):
    def test_normalise_label_splits_snake_and_camel(self):
        self.assertEqual(normalise_label("jaar_van_diagnose"), "jaar van diagnose")
        self.assertEqual(normalise_label("MorphCode"), "morph code")
        self.assertEqual(normalise_label("ICD-O-3"), "icd o 3")

    def test_tokenise_expands_abbreviations_and_drops_stopwords(self):
        self.assertEqual(tokenise("jaar_van_diagnose", _RULES), ["year", "diagnosis"])
        self.assertEqual(tokenise("leeft", _RULES), ["age"])

    def test_jaro_winkler_identity_and_empty(self):
        self.assertEqual(jaro_winkler("abc", "abc"), 1.0)
        self.assertEqual(jaro_winkler("", "abc"), 0.0)


class TestAliasMemory(unittest.TestCase):
    def test_build_alias_memory_excludes_described_database(self):
        mapping = _make_mapping()
        memory = build_alias_memory(mapping, described_database="nki")
        # christie's 'morph' -> tumour_morphology_icd_o should be present
        self.assertIn("morph", memory)
        self.assertEqual(memory["morph"][0], "tumour_morphology_icd_o")
        # nki's 'geslacht' should be excluded
        self.assertNotIn("geslacht", memory)

    def test_build_alias_memory_includes_all_when_no_described_db(self):
        mapping = _make_mapping()
        memory = build_alias_memory(mapping, None)
        self.assertIn("morph", memory)
        self.assertIn("geslacht", memory)


class TestAliasMatcher(unittest.TestCase):
    def test_exact_normalised_hit_scores_one(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database="nki",
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        out = AliasMatcher().run(
            ["morph"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertEqual(out[0]["match"], "tumour_morphology_icd_o")
        self.assertEqual(out[0]["confidence"], 1.0)
        self.assertIn("christie", out[0]["reason"])

    def test_near_hit_uses_similarity(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database="christie",
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        # 'morf' is near 'morph' (christie excluded, so no exact hit; nki has
        # no morphology column, so this abstains).
        out = AliasMatcher().run(
            ["morf"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertIsNone(out[0]["match"])

    def test_values_phase_uses_value_alias_memory(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="values", mapping=mapping, described_database="nki",
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        out = AliasMatcher().run(
            ["M"], {"M": ["male", "female"]}, ctx,
        )
        self.assertEqual(out[0]["match"], "male")
        self.assertEqual(out[0]["confidence"], 1.0)


class TestValueRegexMatcher(unittest.TestCase):
    def test_yes_no_value_set_suggests_yes_no_variable(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
            column_values={"db": {"resp": ["ja", "nee"]}},
        )
        out = ValueRegexMatcher().run(
            ["resp"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertEqual(out[0]["match"], "yes_no_response")
        self.assertEqual(out[0]["confidence"], 0.9)

    def test_sex_values_suggest_biological_sex(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
            column_values={"db": {"sex": ["M", "V"]}},
        )
        out = ValueRegexMatcher().run(
            ["sex"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertEqual(out[0]["match"], "biological_sex")

    def test_year_column_abstains_when_multiple_year_variables(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
            column_values={"db": {"year_col": ["2018", "2019", "2020"]}},
        )
        out = ValueRegexMatcher().run(
            ["year_col"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertIn("matches 2 variables", out[0]["reason"])

    def test_values_phase_maps_yes_to_term(self):
        ctx = SuggestionContext(
            phase="values", mapping=_make_mapping(), described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        out = ValueRegexMatcher().run(
            ["ja"], {"ja": ["yes", "no"]}, ctx,
        )
        self.assertEqual(out[0]["match"], "yes")

    def test_no_pattern_matched_abstains(self):
        ctx = SuggestionContext(
            phase="variables", mapping=_make_mapping(), described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
            column_values={"db": {"col": ["abc", "def"]}},
        )
        out = ValueRegexMatcher().run(
            ["col"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertIsNone(out[0]["match"])


class TestStringMatcher(unittest.TestCase):
    def test_jaar_van_diagnose_matches_year_variable_above_margin(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database=None,
            rules=_RULES, threshold=0.8, margin=0.05,
        )
        out = StringMatcher().run(
            ["jaar_van_diagnose"], {"*": VARIABLE_KEYS}, ctx,
        )
        self.assertEqual(out[0]["match"], "year_of_initial_diagnosis")
        self.assertGreaterEqual(out[0]["confidence"], 0.8)

    def test_near_identical_numbered_candidates_abstain(self):
        mapping = _make_mapping()
        # Add many similar eortc-style candidates so top1-top2 gap is tiny.
        keys = [f"eortc_qlq_c30_q{i}" for i in range(1, 31)] + VARIABLE_KEYS
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        out = StringMatcher().run(
            ["surv1"], {"*": keys}, ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertIn("margin", out[0]["reason"])

    def test_alg_v7_abstains(self):
        mapping = _make_mapping()
        keys = [
            "alg_v1", "alg_v2", "alg_v3", "alg_v4", "alg_v5",
            "alg_v6", "alg_v7", "alg_v8", "alg_v9",
        ]
        ctx = SuggestionContext(
            phase="variables", mapping=mapping, described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        out = StringMatcher().run(
            ["alg_v7"], {"*": keys}, ctx,
        )
        self.assertIsNone(out[0]["match"])

    def test_empty_label_abstains(self):
        ctx = SuggestionContext(
            phase="variables", mapping=_make_mapping(), described_database=None,
            rules=_RULES, threshold=0.8, margin=0.1,
        )
        out = StringMatcher().run([""], {"*": VARIABLE_KEYS}, ctx)
        self.assertIsNone(out[0]["match"])


if __name__ == "__main__":
    unittest.main()
