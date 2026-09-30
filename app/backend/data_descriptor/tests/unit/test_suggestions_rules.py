"""
Unit tests for tier-1 rule-based matchers (alias, value_regex, string).

Uses a fixture JSON-LD with two databases (an English-header site and an
NKI-style Dutch slice) to exercise alias memory with leave-one-site-out,
value-type regexes, and string similarity with margin abstain.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

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
# The production margin (services.suggestions.DEFAULT_MARGIN); tests must
# run at the settings the app ships with, not a looser one.
PRODUCTION_MARGIN = 0.05


def _mapping_dict() -> dict:
    """The raw JSON-LD fixture: christie (English headers) and nki (Dutch)."""
    return {
        "@context": {
            "schema": "mapping:schema/",
            "mapping": "http://example.org/mapping#",
        },
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
                "tumour_topography_icd_o": {
                    "@type": "schema:StandardisedVariable",
                    "dataType": "standardised",
                    "predicate": "sio:has_topography",
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
    }


def _make_mapping() -> JSONLDMapping:
    return JSONLDMapping.from_dict(_mapping_dict())


def _variable_stub(name: str) -> dict:
    return {
        "@type": "schema:ContinuousVariable",
        "dataType": "continuous",
        "predicate": f"sio:has_{name}",
        "class": "ncit:C00000",
    }


def _make_mapping_with_site(site_columns: dict) -> JSONLDMapping:
    """The standard two-site fixture plus a 'leeds' site.

    ``site_columns`` maps a local column label to the schema variable key
    it is remembered as at leeds, e.g. ``{"surv7": "eortc_qlq_c30_q6"}``.
    """
    data = _mapping_dict()
    data["schema"]["variables"].update(
        {key: _variable_stub(key) for key in site_columns.values()}
    )
    data["databases"]["leeds"] = {
        "@id": "mapping:database/leeds",
        "@type": "mapping:Database",
        "name": "leeds",
        "tables": {
            "data": {
                "@id": "mapping:table/leeds/data",
                "@type": "mapping:Table",
                "sourceFile": "leeds",
                "columns": {
                    label: {
                        "mapsTo": f"schema:variable/{var_key}",
                        "localColumn": label,
                    }
                    for label, var_key in site_columns.items()
                },
            }
        },
    }
    return JSONLDMapping.from_dict(data)


VARIABLE_KEYS = [
    "biological_sex",
    "tumour_morphology_icd_o",
    "tumour_topography_icd_o",
    "year_of_initial_diagnosis",
    "year_of_last_followup",
    "yes_no_response",
    "eortc_qlq_c30_q6",
    "eortc_qlq_c30_q12",
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
            phase="variables",
            mapping=mapping,
            described_database="nki",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = AliasMatcher().run(
            ["morph"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "tumour_morphology_icd_o")
        self.assertEqual(out[0]["confidence"], 1.0)
        self.assertIn("christie", out[0]["reason"])

    def test_near_hit_uses_similarity(self):
        """The design doc's headline example: NKI's 'morf' fuzzy-matches
        christie's 'morph' above the similarity floor and is suggested with
        confidence 0.9 x similarity (Jaro-Winkler('morf', 'morph') = 0.848,
        so confidence lands near 0.76)."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="nki",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = AliasMatcher().run(
            ["morf"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "tumour_morphology_icd_o")
        self.assertGreater(out[0]["confidence"], 0.7)
        self.assertLess(out[0]["confidence"], 0.9)
        self.assertIn("christie", out[0]["reason"])

    def test_below_floor_abstains(self):
        """A label near an alias key but below the similarity floor abstains:
        describing christie leaves only nki's 'geslacht' in memory, which
        'morf' does not resemble closely enough."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="christie",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = AliasMatcher().run(
            ["morf"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)

    def test_values_phase_uses_value_alias_memory(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="values",
            mapping=mapping,
            described_database="nki",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = AliasMatcher().run(
            ["M"],
            {"M": ["male", "female"]},
            ctx,
        )
        self.assertEqual(out[0]["match"], "male")
        self.assertEqual(out[0]["confidence"], 1.0)


_ENABLE_VALUE_BASED = patch(
    "services.suggestions.tiers.rules.VALUE_BASED_VARIABLE_SUGGESTIONS", True
)


class TestAliasMatcherGuards(unittest.TestCase):
    """WS3.1/3.2/3.3: the fuzzy alias matcher must not confidently
    mis-map, and its reasons must name what they matched."""

    def _run_alias(self, mapping, items, schema_slice=None, phase="variables"):
        ctx = SuggestionContext(
            phase=phase,
            mapping=mapping,
            described_database="nki",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        return AliasMatcher().run(items, schema_slice or {"*": VARIABLE_KEYS}, ctx)

    def test_surv1_does_not_fuzzy_match_surv7(self):
        """The README's own warning case: Jaro-Winkler('surv1','surv7') is
        0.93 (confidence 0.84, above the 0.8 threshold), but the digits are
        exactly what distinguishes the two labels, so the fuzzy hit must be
        rejected."""
        mapping = _make_mapping_with_site({"surv7": "eortc_qlq_c30_q6"})
        out = self._run_alias(mapping, ["surv1"])
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)

    def test_alg_v7_abstains_between_two_similar_remembered_columns(self):
        """Two remembered labels near 'alg_v7' but with distinct targets:
        the best and second-best sit within the margin, so the matcher
        abstains instead of coin-flipping."""
        mapping = _make_mapping_with_site(
            {"alg_v1b": "eortc_qlq_c30_q6", "alg_v2b": "eortc_qlq_c30_q12"}
        )
        out = self._run_alias(mapping, ["alg_v7"])
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)
        self.assertIn("margin", out[0]["reason"])

    def test_morf_still_matches_morph(self):
        """The guards must not kill the headline near-miss: 'morf' against
        a single remembered 'morph' still fuzzy-matches above the 0.84
        floor and escalates below the threshold."""
        mapping = _make_mapping()
        out = self._run_alias(mapping, ["morf"])
        self.assertEqual(out[0]["match"], "tumour_morphology_icd_o")
        self.assertGreater(out[0]["confidence"], 0.7)

    def test_conflicting_aliases_abstain_and_name_both(self):
        """The same normalised label mapped to different variables at two
        sites must abstain (setdefault used to let the first site win
        silently) and name both candidates in the reason."""
        mapping = _make_mapping_with_site({"morph": "eortc_qlq_c30_q6"})
        out = self._run_alias(mapping, ["morph"])
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)
        self.assertIn("tumour_morphology_icd_o", out[0]["reason"])
        self.assertIn("eortc_qlq_c30_q6", out[0]["reason"])

    def test_reason_names_the_matched_alias(self):
        """The plan's reason format: name the matched column/value and its
        database, not just the database."""
        mapping = _make_mapping()
        out = self._run_alias(mapping, ["morph"])
        self.assertEqual(
            out[0]["reason"],
            "Alias: column 'morph' in database 'christie' is mapped to this variable.",
        )

    def test_fuzzy_reason_names_the_remembered_column_not_the_item(self):
        """On a fuzzy hit the item and the remembered column differ: the
        reason must name christie's 'morph', not claim christie has a
        column called 'morf'."""
        mapping = _make_mapping()
        out = self._run_alias(mapping, ["morf"])
        self.assertEqual(out[0]["match"], "tumour_morphology_icd_o")
        self.assertEqual(
            out[0]["reason"],
            "Alias: column 'morph' in database 'christie' is mapped to this variable.",
        )

    def test_values_phase_reason_names_the_matched_value(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="values",
            mapping=mapping,
            described_database="nki",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = AliasMatcher().run(["M"], {"M": ["male", "female"]}, ctx)
        self.assertEqual(out[0]["match"], "male")
        self.assertEqual(
            out[0]["reason"],
            "Alias: value 'M' in database 'christie' is mapped to this term.",
        )


class TestValueRegexMatcher(unittest.TestCase):
    def test_variables_phase_abstains_while_disabled(self):
        """Value-based variable suggestions are disabled by default: the
        matcher abstains in the variables phase even when distinct values
        are available, so no RDF-store value queries are needed."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"resp": ["ja", "nee"]}},
        )
        out = ValueRegexMatcher().run(
            ["resp"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)
        self.assertIn("disabled", out[0]["reason"])

    @_ENABLE_VALUE_BASED
    def test_yes_no_value_set_suggests_yes_no_variable(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"resp": ["ja", "nee"]}},
        )
        out = ValueRegexMatcher().run(
            ["resp"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "yes_no_response")
        self.assertEqual(out[0]["confidence"], 0.9)

    @_ENABLE_VALUE_BASED
    def test_sex_values_suggest_biological_sex(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"sex": ["M", "V"]}},
        )
        out = ValueRegexMatcher().run(
            ["sex"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "biological_sex")

    @_ENABLE_VALUE_BASED
    def test_morphology_codes_suggest_morphology_not_topography(self):
        """Morphology codes (8500/3) must not be grabbed by the topography
        rule: its pattern list used to include the morphology regex, so a
        morphology column was suggested as tumour_topography_icd_o."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"morfo": ["8500/3", "8010/3"]}},
        )
        out = ValueRegexMatcher().run(
            ["morfo"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "tumour_morphology_icd_o")

    @_ENABLE_VALUE_BASED
    def test_topography_codes_suggest_topography(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"topo": ["C50.9", "C18.5"]}},
        )
        out = ValueRegexMatcher().run(
            ["topo"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "tumour_topography_icd_o")

    @_ENABLE_VALUE_BASED
    def test_variables_phase_uses_only_described_database_values(self):
        """A column with the same name in two databases must be scored
        against the described database's values, not whichever database
        happens to be iterated first."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="nki",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={
                "christie": {"sex": ["M", "F"]},
                "nki": {"sex": ["abc", "def"]},
            },
        )
        out = ValueRegexMatcher().run(
            ["sex"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        # nki's values match no pattern, so the matcher abstains instead of
        # suggesting biological_sex from christie's M/F values.
        self.assertIsNone(out[0]["match"])

    @_ENABLE_VALUE_BASED
    def test_year_column_abstains_when_multiple_year_variables(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"year_col": ["2018", "2019", "2020"]}},
        )
        out = ValueRegexMatcher().run(
            ["year_col"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertIn("matches 2 variables", out[0]["reason"])

    def test_values_phase_maps_yes_to_term(self):
        ctx = SuggestionContext(
            phase="values",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = ValueRegexMatcher().run(
            ["ja"],
            {"ja": ["yes", "no"]},
            ctx,
        )
        self.assertEqual(out[0]["match"], "yes")

    def test_values_phase_value_set_only_fires_for_a_whole_column(self):
        """WS3.6: a value-set rule describes a coding scheme, not single
        values. 'ja' in a column whose other value 'y' is outside the
        yes/no sets must not get a yes-suggestion; in a clean {ja, nee}
        column it must."""
        ctx = SuggestionContext(
            phase="values",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            item_column_values={"ja": ["ja", "y"], "nee": ["ja", "y"]},
        )
        out = ValueRegexMatcher().run(
            ["ja", "nee"],
            {"ja": ["yes", "no"], "nee": ["yes", "no"]},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertIsNone(out[1]["match"])

        ctx_clean = SuggestionContext(
            phase="values",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            item_column_values={"ja": ["ja", "nee"], "nee": ["ja", "nee"]},
        )
        out = ValueRegexMatcher().run(
            ["ja", "nee"],
            {"ja": ["yes", "no"], "nee": ["yes", "no"]},
            ctx_clean,
        )
        self.assertEqual(out[0]["match"], "yes")
        self.assertEqual(out[1]["match"], "no")

    def test_values_phase_missing_code_maps_to_missing_term(self):
        """WS3.6: a value that is a missing code maps to the term naming
        the missing/unknown category, driven by the rules file's
        missing_code_rule term_predicates."""
        ctx = SuggestionContext(
            phase="values",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = ValueRegexMatcher().run(
            ["999"],
            {"999": ["missing_or_unspecified", "male", "female"]},
            ctx,
        )
        self.assertEqual(out[0]["match"], "missing_or_unspecified")
        self.assertEqual(out[0]["confidence"], 0.9)
        self.assertIn("missing code", out[0]["reason"].lower())

    def test_values_phase_missing_code_abstains_between_matching_terms(self):
        ctx = SuggestionContext(
            phase="values",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = ValueRegexMatcher().run(
            ["999"],
            {"999": ["missing", "unknown"]},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertIn("matches 2 terms", out[0]["reason"])

    def test_values_phase_maps_one_to_yes_and_zero_to_no(self):
        """The {1, 0} yes/no set is positional; 1 must map to the yes term
        and 0 to the no term (the old {0, 1} order inverted both)."""
        ctx = SuggestionContext(
            phase="values",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = ValueRegexMatcher().run(
            ["1", "0"],
            {"1": ["yes", "no"], "0": ["yes", "no"]},
            ctx,
        )
        self.assertEqual(out[0]["match"], "yes")
        self.assertEqual(out[1]["match"], "no")

    @_ENABLE_VALUE_BASED
    def test_no_pattern_matched_abstains(self):
        ctx = SuggestionContext(
            phase="variables",
            mapping=_make_mapping(),
            described_database="db",
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
            column_values={"db": {"col": ["abc", "def"]}},
        )
        out = ValueRegexMatcher().run(
            ["col"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertIsNone(out[0]["match"])


class TestStringMatcher(unittest.TestCase):
    def test_jaar_van_diagnose_matches_year_variable_above_margin(self):
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = StringMatcher().run(
            ["jaar_van_diagnose"],
            {"*": VARIABLE_KEYS},
            ctx,
        )
        self.assertEqual(out[0]["match"], "year_of_initial_diagnosis")
        self.assertGreaterEqual(out[0]["confidence"], 0.8)

    def test_near_identical_numbered_candidates_abstain(self):
        mapping = _make_mapping()
        # Add many similar eortc-style candidates so top1-top2 gap is tiny.
        keys = [f"eortc_qlq_c30_q{i}" for i in range(1, 31)] + VARIABLE_KEYS
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = StringMatcher().run(
            ["surv1"],
            {"*": keys},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        # Abstains carry confidence 0 (raw scores in the reason) so they
        # never beat a real match in the merge and always escalate.
        self.assertEqual(out[0]["confidence"], 0.0)
        self.assertIn("margin", out[0]["reason"])

    def test_alg_v7_abstains(self):
        """A column 'alg_v7' against numbered sibling variables without an
        exact key: the siblings sit within the margin of each other, so
        the string matcher abstains. (When the schema does contain the
        exact key, the exact match wins — that is correct and covered by
        the production-margin behaviour.)"""
        mapping = _make_mapping()
        keys = [
            "alg_v1",
            "alg_v2",
            "alg_v3",
            "alg_v4",
            "alg_v5",
            "alg_v6",
            "alg_v8",
            "alg_v9",
        ]
        ctx = SuggestionContext(
            phase="variables",
            mapping=mapping,
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = StringMatcher().run(
            ["alg_v7"],
            {"*": keys},
            ctx,
        )
        self.assertIsNone(out[0]["match"])
        self.assertEqual(out[0]["confidence"], 0.0)

    def test_empty_label_abstains(self):
        ctx = SuggestionContext(
            phase="variables",
            mapping=_make_mapping(),
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = StringMatcher().run([""], {"*": VARIABLE_KEYS}, ctx)
        self.assertIsNone(out[0]["match"])

    def test_values_phase_matches_value_to_term(self):
        """In the values phase the schema_slice maps each value to its list
        of terms (no '*' key). The StringMatcher must collect targets from
        all value entries, not from a missing '*' key — otherwise it has
        no candidates and every value abstains."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="values",
            mapping=mapping,
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        terms = ["male", "female", "missing_or_unspecified"]
        schema_slice = {
            "Male": terms,
            "Female": terms,
        }
        out = StringMatcher().run(["Male", "Female"], schema_slice, ctx)
        self.assertEqual(out[0]["match"], "male")
        self.assertGreaterEqual(out[0]["confidence"], 0.95)
        self.assertEqual(out[1]["match"], "female")
        self.assertGreaterEqual(out[1]["confidence"], 0.95)

    def test_values_phase_no_candidates_when_slice_empty(self):
        """When the schema_slice is empty (no terms at all), the matcher
        must abstain with no candidates rather than crash."""
        mapping = _make_mapping()
        ctx = SuggestionContext(
            phase="values",
            mapping=mapping,
            described_database=None,
            rules=_RULES,
            threshold=0.8,
            margin=PRODUCTION_MARGIN,
        )
        out = StringMatcher().run(["unknown"], {}, ctx)
        self.assertIsNone(out[0]["match"])


if __name__ == "__main__":
    unittest.main()
