"""
Unit tests for :class:`SuggestionService`.

Covers job lifecycle, fingerprint reuse, ``force``, cascade merge and
tie-breaking, one-variable-per-database conflict downgrade, disabled state,
and unknown-phase handling. A fake tier producer replaces the real matchers
so the service logic is tested in isolation.
"""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders import JSONLDMapping
from services.suggestions import (
    SuggestionConfig,
    SuggestionRequestError,
    SuggestionService,
    VARIABLES_PHASE,
    VALUES_PHASE,
)
from services.suggestions.prompt_export import (
    chunk_size_from_env,
    min_value_count_from_env,
)

# ---------------------------------------------------------------------------
# Fake tier producer
# ---------------------------------------------------------------------------


class FakeProducer:
    """Minimal :class:`TierProducer` that returns canned records."""

    def __init__(self, tier: int, source: str, records: dict[str, dict]):
        self.tier = tier
        self.source = source
        self._records = records

    def run(self, items, schema_slice, ctx):
        out = []
        for item in items:
            if item in self._records:
                rec = dict(self._records[item])
                rec.setdefault("item", item)
                out.append(rec)
            else:
                out.append(
                    {
                        "item": item,
                        "match": None,
                        "confidence": 0.0,
                        "reason": "No match.",
                    }
                )
        return out


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_mapping() -> JSONLDMapping:
    return JSONLDMapping.from_dict(
        {
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
                    "year_of_initial_diagnosis": {
                        "@type": "schema:ContinuousVariable",
                        "dataType": "continuous",
                        "predicate": "sio:has_year",
                        "class": "ncit:C81206",
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
                                },
                            },
                        }
                    },
                },
            },
        }
    )


def _make_session_cache(mapping=None, columns_by_db=None):
    """Return a mock session_cache with the attributes the service reads."""
    cache = MagicMock()
    cache.jsonld_mapping = mapping if mapping is not None else _make_mapping()
    cache.suggestion_jobs = None  # service will lazily initialise
    cache.suggestion_ingested = None
    return cache


def _make_rdf_store(columns_by_db=None, categories=None):
    """Return a mock rdf_store_service."""
    rdf = MagicMock()
    rdf.get_column_info_by_database.return_value = columns_by_db or {}
    rdf.get_categories.return_value = categories or ""
    return rdf


def _add_nki_site(
    data: dict,
    sex_local_mappings: dict = None,
    maps_to: str = "schema:variable/biological_sex",
) -> dict:
    """Add an 'nki' site to a mapping dict from _make_mapping().to_dict().

    The base service fixture only has christie; the leave-one-site-out and
    fallback tests need a second remembered site.
    """
    data["databases"]["nki"] = {
        "@id": "mapping:database/nki",
        "@type": "mapping:Database",
        "name": "nki",
        "tables": {
            "data": {
                "@id": "mapping:table/nki/data",
                "@type": "mapping:Table",
                "sourceFile": "nki",
                "columns": {
                    "sex": {
                        "mapsTo": maps_to,
                        "localColumn": "sex",
                        "localMappings": sex_local_mappings or {},
                    }
                },
            }
        },
    }
    return data


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _config(tiers=(1,), threshold=0.8, compute="host"):
    cfg = SuggestionConfig.__new__(SuggestionConfig)
    cfg.tiers = list(tiers)
    cfg.compute = compute
    cfg.threshold = threshold
    cfg.margin = 0.05
    return cfg


VARIABLE_KEYS = [
    "biological_sex",
    "tumour_morphology_icd_o",
    "year_of_initial_diagnosis",
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStatusHonesty(unittest.TestCase):
    """WS4.1: /status must never claim an unimplemented tier is active."""

    def test_enabled_but_unimplemented_tiers_report_inactive(self):
        svc = SuggestionService(_config(tiers=(1, 2, 3)))
        status = svc.status()
        self.assertEqual(status["tiers"][1]["state"], "active")
        self.assertEqual(status["tiers"][2]["state"], "inactive")
        self.assertIn("not implemented yet (issue 3)", status["tiers"][2]["reason"])
        self.assertEqual(status["tiers"][3]["state"], "inactive")
        self.assertIn("not implemented yet (issue 4)", status["tiers"][3]["reason"])


class TestValuesLeaveOneSiteOut(unittest.TestCase):
    """WS4.3: every value group must exclude ITS OWN database from the
    alias memory. The groups used to fall back to the payload-level
    described_database (the FIRST database), so every group excluded the
    same one and a site could suggest from its own remembered values."""

    def test_two_databases_each_exclude_their_own_values(self):
        data = _make_mapping().to_dict()
        # christie remembers 'man_en' -> male; nki remembers 'M' -> male.
        data["databases"]["christie"]["tables"]["data"]["columns"]["sex"][
            "localMappings"
        ] = {"male": ["man_en"], "female": ["vrouw_en"]}
        _add_nki_site(data, {"male": ["M"], "female": ["F"]})
        mapping = JSONLDMapping.from_dict(data)
        cache = _make_session_cache(mapping)
        cache.DescriptiveInfoDetails = {
            "christie": [{'Biological Sex (or "sex")': [{"value": "M"}]}],
            "nki": [{'Biological Sex (or "sex")': [{"value": "M"}]}],
        }
        svc = SuggestionService(_config())
        svc.start(VALUES_PHASE, cache, None)
        records = svc.get_state(cache, VALUES_PHASE)["records"]

        # christie's group excludes christie, so nki's remembered 'M' ->
        # male is a legitimate alias hit.
        christie_rec = records["christie_sex_M"]
        self.assertEqual(christie_rec["match"], "male")
        self.assertEqual(christie_rec["source"], "alias")

        # nki's group excludes nki: its own 'M' -> male must not leak back
        # as an alias suggestion.
        nki_rec = records["nki_sex_M"]
        self.assertFalse(
            nki_rec["source"] == "alias" and nki_rec["match"] == "male",
            "the nki group saw nki's own remembered M -> male",
        )


class TestValuesColumnContextPerColumn(unittest.TestCase):
    """The whole-column value-set rule must judge each column by its OWN
    distinct values. The column context used to be one payload-wide
    ``value -> column values`` map where the first column containing a
    value won, so '1' in a 1/2/3 grade column inherited a yes/no column's
    coding (or the other way round, depending on column order)."""

    def _records(self, column_order):
        data = _make_mapping().to_dict()
        yes_no_terms = {
            "yes": {"targetClass": "ncit:C49488"},
            "no": {"targetClass": "ncit:C49487"},
        }
        data["schema"]["variables"]["has_chemo"] = {
            "@type": "schema:CategoricalVariable",
            "dataType": "categorical",
            "predicate": "sio:has_chemo",
            "class": "ncit:C15632",
            "valueMapping": {"terms": dict(yes_no_terms)},
        }
        data["schema"]["variables"]["grade"] = {
            "@type": "schema:CategoricalVariable",
            "dataType": "categorical",
            "predicate": "sio:has_grade",
            "class": "ncit:C28076",
            "valueMapping": {
                "terms": {**yes_no_terms, "grade_2": {"targetClass": "ncit:C28078"}}
            },
        }
        columns = {
            "chemo": ('Has Chemo (or "chemo")', ["1", "0"]),
            "grade": ('Grade (or "grade")', ["1", "2", "3"]),
        }
        cache = _make_session_cache(JSONLDMapping.from_dict(data))
        cache.DescriptiveInfoDetails = {
            "nki": [
                {columns[c][0]: [{"value": v} for v in columns[c][1]]}
                for c in column_order
            ]
        }
        svc = SuggestionService(_config())
        svc.start(VALUES_PHASE, cache, None)
        return svc.get_state(cache, VALUES_PHASE)["records"]

    def test_result_does_not_depend_on_column_order(self):
        for order in (["chemo", "grade"], ["grade", "chemo"]):
            with self.subTest(order=order):
                records = self._records(order)
                # The 1/0 column is a yes/no coding: 1 -> yes.
                self.assertEqual(records["nki_chemo_1"]["match"], "yes")
                self.assertEqual(records["nki_chemo_1"]["source"], "value_regex")
                # The 1/2/3 column is not; its '1' must not become 'yes'.
                self.assertNotEqual(records["nki_grade_1"]["match"], "yes")


class TestValuesFallback(unittest.TestCase):
    """WS4.4: the values-phase fallback must resolve a column's variable by
    (database, local column), not by local name across all databases."""

    def test_same_named_column_in_two_databases_maps_to_its_own_variable(self):
        data = _make_mapping().to_dict()
        # nki's 'sex' column maps to a different variable than christie's.
        data["schema"]["variables"]["other_sex"] = {
            "@type": "schema:CategoricalVariable",
            "dataType": "categorical",
            "predicate": "sio:has_sex",
            "class": "ncit:C28421",
            "valueMapping": {
                "terms": {
                    "man": {"targetClass": "ncit:C20197"},
                    "vrouw": {"targetClass": "ncit:C16576"},
                }
            },
        }
        _add_nki_site(
            data,
            {"male": ["M"], "female": ["F"]},
            maps_to="schema:variable/other_sex",
        )
        mapping = JSONLDMapping.from_dict(data)
        cache = _make_session_cache(mapping)
        cache.DescriptiveInfoDetails = {}
        rdf = _make_rdf_store(
            columns_by_db={"christie": ["sex"], "nki": ["sex"]},
            categories="value,count\nM,80\nF,70\n",
        )
        svc = SuggestionService(_config())
        result = svc.start(VALUES_PHASE, cache, rdf)
        self.assertEqual(result["status"], "started")
        records = svc.get_state(cache, VALUES_PHASE)["records"]

        # christie's sex is biological_sex; nki's remembered 'M' -> male
        # is a valid alias hit for it.
        self.assertEqual(records["christie_sex_M"]["match"], "male")

        # nki's sex is other_sex (terms man/vrouw). The old local-name
        # lookup resolved it to christie's biological_sex and suggested
        # 'male' from nki's own remembered values; now the christie
        # variable's terms are not valid targets for this group, so the
        # sanitiser nulls any such hit.
        self.assertNotEqual(records["nki_sex_M"]["match"], "male")


class TestFingerprintExpiry(unittest.TestCase):
    """WS4.5: the fingerprint covers the rules version and the alias
    memory, so a rules bump or another site's mapping change expires the
    cached job instead of serving stale suggestions."""

    def setUp(self):
        self.cache = _make_session_cache()
        self.rdf = _make_rdf_store(columns_by_db={"christie": ["morph"]})

    @patch("services.suggestions.tier1_producers")
    def test_rules_version_change_expires_cached_job(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {})]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        self.assertEqual(svc.get_state(self.cache, VARIABLES_PHASE)["status"], "done")
        # Same items and mapping: reused.
        self.assertEqual(
            svc.start(VARIABLES_PHASE, self.cache, self.rdf)["status"],
            "already_done",
        )
        # A rules bump changes the fingerprint.
        svc._rules["version"] = "999.0.0"
        self.assertEqual(
            svc.start(VARIABLES_PHASE, self.cache, self.rdf)["status"], "started"
        )

    @patch("services.suggestions.tier1_producers")
    def test_alias_memory_change_expires_cached_job(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {})]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        self.assertEqual(
            svc.start(VARIABLES_PHASE, self.cache, self.rdf)["status"],
            "already_done",
        )
        # Another site reviews another column: the alias memory hash
        # changes, so the cached job must not be served.
        data = _add_nki_site(_make_mapping().to_dict(), {"male": ["M"]})
        data["databases"]["nki"]["tables"]["data"]["columns"]["extra_col"] = {
            "mapsTo": "schema:variable/biological_sex",
            "localColumn": "extra_col",
        }
        self.cache.jsonld_mapping = JSONLDMapping.from_dict(data)
        self.assertEqual(
            svc.start(VARIABLES_PHASE, self.cache, self.rdf)["status"], "started"
        )


class TestIngest(unittest.TestCase):
    """``ingest()`` turns a pasted LLM answer into pasted_llm
    records through the same gate as every producer, merges them into the
    phase's job without touching the JSON-LD, and survives a rebuild."""

    CATEGORIES = "value,count\nM,10\nF,12\nman_en,3\n?,1\n"

    def setUp(self):
        data = _make_mapping().to_dict()
        data["databases"]["christie"]["tables"]["data"]["columns"]["sex"][
            "localMappings"
        ] = {"male": ["man_en"]}
        self.mapping = JSONLDMapping.from_dict(data)
        self.cache = _make_session_cache(self.mapping)
        self.rdf = _make_rdf_store(
            columns_by_db={"christie": ["morph", "sex", "yr", "free"]},
            categories=self.CATEGORIES,
        )

    # -- variables ----------------------------------------------------------

    def test_variables_answer_is_gated_and_merged(self):
        answer = (
            "Here is the mapping:\n```json\n"
            + json.dumps(
                {
                    "databases": {
                        "christie": {
                            "tables": {
                                "data": {
                                    "columns": {
                                        "year_of_initial_diagnosis": {
                                            "mapsTo": "schema:variable/year_of_initial_diagnosis",
                                            "localColumn": "yr",
                                            "confidence": 0.9,
                                            "reason": "yr = year",
                                        },
                                        "free": {
                                            "mapsTo": "schema:variable/no_such_variable",
                                            "localColumn": "free",
                                        },
                                        "biological_sex": {
                                            "mapsTo": "schema:variable/biological_sex",
                                            "localColumn": "sex",
                                        },
                                        "ghost": {
                                            "mapsTo": "schema:variable/year_of_initial_diagnosis",
                                            "localColumn": "ghost",
                                        },
                                    }
                                }
                            }
                        }
                    }
                }
            )
            + "\n```\nHope this helps!"
        )
        svc = SuggestionService(_config(tiers=()))
        result = svc.ingest(
            VARIABLES_PHASE, self.cache, self.rdf, database="christie", answer=answer
        )
        self.assertEqual(
            {k: result[k] for k in ("accepted", "nulled", "rejected", "skipped")},
            {"accepted": 1, "nulled": 1, "rejected": 1, "skipped": 1},
        )
        records = result["job"]["records"]
        yr = records["christie_yr"]
        self.assertEqual(yr["match"], "year_of_initial_diagnosis")
        self.assertEqual(yr["confidence"], 0.9)
        self.assertEqual(yr["source"], "pasted_llm")
        self.assertEqual(yr["tier"], 3)
        self.assertEqual(yr["status"], "done")
        self.assertEqual((yr["database"], yr["column"]), ("christie", "yr"))
        free = records["christie_free"]
        self.assertIsNone(free["match"])
        self.assertTrue(free["reason"].startswith("[invalid key from LLM]"))
        self.assertNotIn("christie_sex", records)
        self.assertNotIn("christie_ghost", records)
        self.assertTrue(any("ghost" in m for m in result["messages"]))
        # The job is visible through the normal poll, with a fingerprint.
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertEqual(state["status"], "done")
        self.assertIn("christie_yr", state["records"])
        self.assertTrue(state["fingerprint"])
        # Nothing was written to the JSON-LD.
        self.assertIsNone(self.mapping.get_local_column("year_of_initial_diagnosis"))

    def test_variable_already_mapped_in_jsonld_is_not_reused(self):
        svc = SuggestionService(_config(tiers=()))
        result = svc.ingest(
            VARIABLES_PHASE,
            self.cache,
            self.rdf,
            database="christie",
            records=[{"item": "yr", "match": "biological_sex", "confidence": 0.95}],
        )
        self.assertEqual(result["nulled"], 1)
        rec = result["job"]["records"]["christie_yr"]
        self.assertIsNone(rec["match"])
        self.assertIn(
            "[already mapped] biological_sex is mapped to column 'sex'", rec["reason"]
        )

    def test_canned_answer_from_the_design_document(self):
        data = _make_mapping().to_dict()
        data["schema"]["variables"] = {
            key: {"@type": "schema:ContinuousVariable", "dataType": "continuous"}
            for key in (
                "identifier",
                "administered_prom_language",
                "age_at_initial_diagnosis",
                "eortc_qlq_c30_q6",
            )
        }
        data["databases"] = {}
        cache = _make_session_cache(JSONLDMapping.from_dict(data))
        rdf = _make_rdf_store(
            columns_by_db={"nki": ["Rnnummer", "taal", "leeft", "surv70"]}
        )
        canned = [
            {
                "item": "taal",
                "match": "administered_prom_language",
                "confidence": 0.9,
                "reason": "Dutch 'taal' = language; values are locale codes.",
            },
            {
                "item": "leeft",
                "match": "age_at_initial_diagnosis",
                "confidence": 0.85,
                "reason": "'leeft' abbreviates 'leeftijd' (age).",
            },
            {
                "item": "Rnnummer",
                "match": "identifier",
                "confidence": 0.7,
                "reason": "Looks like a registration number.",
            },
            {
                "item": "surv70",
                "match": "eortc_qlq_c30_question_6",
                "confidence": 0.4,
                "reason": "Guessing a questionnaire item.",
            },
        ]
        svc = SuggestionService(_config(tiers=()))
        result = svc.ingest(
            VARIABLES_PHASE, cache, rdf, database="nki", answer=json.dumps(canned)
        )
        self.assertEqual(
            (result["accepted"], result["nulled"], result["rejected"]), (3, 1, 0)
        )
        records = result["job"]["records"]
        for key, match in (
            ("nki_taal", "administered_prom_language"),
            ("nki_leeft", "age_at_initial_diagnosis"),
            ("nki_Rnnummer", "identifier"),
        ):
            self.assertEqual(records[key]["match"], match)
            self.assertEqual(records[key]["source"], "pasted_llm")
            self.assertEqual(records[key]["tier"], 3)
        self.assertIsNone(records["nki_surv70"]["match"])
        self.assertTrue(
            records["nki_surv70"]["reason"].startswith("[invalid key from LLM]")
        )

    @patch("services.suggestions.tier1_producers")
    def test_merge_with_tier1_job_and_survive_rebuild(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                    "yr": {
                        "match": "year_of_initial_diagnosis",
                        "confidence": 0.5,
                        "reason": "weak",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        before = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertEqual(before["records"]["christie_free"]["match"], None)

        result = svc.ingest(
            VARIABLES_PHASE,
            self.cache,
            self.rdf,
            database="christie",
            records=[
                # Stronger than tier 1's weak record: replaces it.
                {"item": "yr", "match": "year_of_initial_diagnosis", "confidence": 0.9},
                # Fills a tier-1 abstain, but with the variable 'yr' now
                # holds at 0.9: a conflict loser (decision D2).
                {
                    "item": "free",
                    "match": "year_of_initial_diagnosis",
                    "confidence": 0.6,
                },
            ],
        )
        after = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertNotEqual(after["fingerprint"], before["fingerprint"])
        self.assertEqual(after["records"]["christie_yr"]["source"], "pasted_llm")
        self.assertEqual(after["records"]["christie_yr"]["confidence"], 0.9)
        free = after["records"]["christie_free"]
        self.assertIsNone(free["match"])
        self.assertIn("'yr'", free["reason"])
        self.assertEqual(free["alternatives"][0]["match"], "year_of_initial_diagnosis")
        self.assertEqual(free["alternatives"][0]["source"], "pasted_llm")
        self.assertEqual(after["records"]["christie_morph"]["source"], "alias")
        self.assertEqual(result["accepted"], 2)

        # A forced re-run rebuilds the tier-1 job and re-applies the paste.
        svc.start(VARIABLES_PHASE, self.cache, self.rdf, force=True)
        rebuilt = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertEqual(rebuilt["records"]["christie_yr"]["source"], "pasted_llm")
        self.assertEqual(rebuilt["fingerprint"], after["fingerprint"])

    @patch("services.suggestions.tier1_producers")
    def test_paste_reopens_a_dismissed_field_and_survives_rebuild(self, mock_producers):
        """A dismissal judges one suggestion, not the field: for a key the
        browser dismissed, the pasted record takes the field whatever the
        confidences, the dismissed candidate stays an alternative, and the
        key is reported as reopened. A nulled paste reopens nothing, and a
        key that was not dismissed still follows the cascade rule."""
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "yr": {
                        "match": "year_of_initial_diagnosis",
                        "confidence": 1.0,
                        "reason": "Alias hit (poor).",
                    },
                    "free": {
                        "match": "year_of_initial_diagnosis",
                        "confidence": 0.95,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        cache = self._cache_with_free_variable()
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, self.rdf)
        # Decision D2 left 'free' a conflict loser; the user dismissed 'yr'.
        result = svc.ingest(
            VARIABLES_PHASE,
            cache,
            self.rdf,
            database="christie",
            records=[
                {"item": "yr", "match": "age_at_diagnosis", "confidence": 0.7},
                {
                    "item": "free",
                    "match": "year_of_initial_diagnosis",
                    "confidence": 0.6,
                },
            ],
            dismissed=["christie_yr", "christie_free"],
        )
        self.assertEqual(result["reopened"], ["christie_yr", "christie_free"])
        records = result["job"]["records"]
        yr = records["christie_yr"]
        self.assertEqual(
            (yr["match"], yr["source"], yr["confidence"]),
            ("age_at_diagnosis", "pasted_llm", 0.7),
        )
        self.assertTrue(yr.get("reopens"))
        self.assertEqual(yr["alternatives"][0]["match"], "year_of_initial_diagnosis")
        self.assertEqual(yr["alternatives"][0]["source"], "alias")
        # 'free' now holds year_of_initial_diagnosis unopposed: the paste
        # took the dismissed loser's place and the conflict is gone.
        self.assertEqual(records["christie_free"]["match"], "year_of_initial_diagnosis")
        self.assertEqual(records["christie_free"]["source"], "pasted_llm")

        # A forced re-run rebuilds tier 1 (yr -> year at 1.0 again) and the
        # stored paste still takes the dismissed field.
        svc.start(VARIABLES_PHASE, cache, self.rdf, force=True)
        rebuilt = svc.get_state(cache, VARIABLES_PHASE)["records"]
        self.assertEqual(rebuilt["christie_yr"]["match"], "age_at_diagnosis")
        self.assertEqual(rebuilt["christie_yr"]["source"], "pasted_llm")

    @staticmethod
    def _cache_with_free_variable():
        """The base mapping plus one variable no column uses yet."""
        data = _make_mapping().to_dict()
        data["schema"]["variables"]["age_at_diagnosis"] = {
            "@type": "schema:ContinuousVariable",
            "dataType": "continuous",
            "predicate": "sio:has_age",
            "class": "ncit:C25150",
        }
        return _make_session_cache(JSONLDMapping.from_dict(data))

    @patch("services.suggestions.tier1_producers")
    def test_paste_without_dismissal_or_match_keeps_the_cascade(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "yr": {
                        "match": "year_of_initial_diagnosis",
                        "confidence": 1.0,
                        "reason": "x",
                    }
                },
            )
        ]
        cache = self._cache_with_free_variable()
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, self.rdf)
        # Not dismissed: the stronger tier-1 record keeps the field.
        result = svc.ingest(
            VARIABLES_PHASE,
            cache,
            self.rdf,
            database="christie",
            records=[{"item": "yr", "match": "age_at_diagnosis", "confidence": 0.7}],
        )
        self.assertEqual(result["reopened"], [])
        yr = result["job"]["records"]["christie_yr"]
        self.assertEqual(
            (yr["match"], yr["source"]), ("year_of_initial_diagnosis", "alias")
        )
        self.assertEqual(yr["alternatives"][0]["match"], "age_at_diagnosis")
        # Dismissed, but the paste was nulled: nothing to show, not reopened.
        result = svc.ingest(
            VARIABLES_PHASE,
            cache,
            self.rdf,
            database="christie",
            records=[{"item": "yr", "match": "no_such_variable", "confidence": 0.9}],
            dismissed=["christie_yr"],
        )
        self.assertEqual(result["reopened"], [])
        self.assertEqual(result["nulled"], 1)
        yr = result["job"]["records"]["christie_yr"]
        self.assertEqual(
            (yr["match"], yr["source"]), ("year_of_initial_diagnosis", "alias")
        )

    def test_two_pasted_columns_same_variable_keep_the_stronger(self):
        svc = SuggestionService(_config(tiers=()))
        result = svc.ingest(
            VARIABLES_PHASE,
            self.cache,
            self.rdf,
            database="christie",
            records=[
                {"item": "yr", "match": "year_of_initial_diagnosis", "confidence": 0.6},
                {
                    "item": "free",
                    "match": "year_of_initial_diagnosis",
                    "confidence": 0.8,
                },
            ],
        )
        records = result["job"]["records"]
        self.assertEqual(records["christie_free"]["match"], "year_of_initial_diagnosis")
        self.assertIsNone(records["christie_yr"]["match"])
        self.assertIn("free", records["christie_yr"]["reason"])

    # -- values -------------------------------------------------------------

    def test_values_answer_is_gated_per_column(self):
        answer = {
            "columns": {
                "biological_sex": {
                    "mapsTo": "schema:variable/biological_sex",
                    "localColumn": "sex",
                    "localMappings": {
                        "female": ["F"],
                        "male": ["M", "man_en"],
                        "unknown_term": ["?"],
                    },
                    "valueNotes": {"F": {"confidence": 0.95, "reason": "F = female"}},
                },
                "morph": {
                    "mapsTo": "schema:variable/tumour_morphology_icd_o",
                    "localColumn": "morph",
                    "localMappings": {"x": ["1"]},
                },
                "yr": {
                    "mapsTo": "schema:variable/year_of_initial_diagnosis",
                    "localColumn": "yr",
                    "localMappings": {"x": ["1"]},
                },
            }
        }
        svc = SuggestionService(_config(tiers=()))
        result = svc.ingest(
            VALUES_PHASE,
            self.cache,
            self.rdf,
            database="christie",
            answer=json.dumps(answer),
        )
        # F, M accepted; ? nulled; man_en skipped (already mapped); morph's
        # variable has no terms and yr is unmapped: rejected.
        self.assertEqual(
            {k: result[k] for k in ("accepted", "nulled", "rejected", "skipped")},
            {"accepted": 2, "nulled": 1, "rejected": 2, "skipped": 1},
        )
        records = result["job"]["records"]
        f = records["christie_sex_F"]
        self.assertEqual(
            (f["match"], f["confidence"], f["reason"]), ("female", 0.95, "F = female")
        )
        self.assertEqual(
            (f["database"], f["column"], f["value"]), ("christie", "sex", "F")
        )
        self.assertEqual(records["christie_sex_M"]["match"], "male")
        self.assertEqual(records["christie_sex_M"]["confidence"], 0.7)
        q = records["christie_sex_?"]
        self.assertIsNone(q["match"])
        self.assertTrue(q["reason"].startswith("[invalid key from LLM]"))
        self.assertNotIn("christie_sex_man_en", records)
        self.rdf.get_categories.assert_called_with("sex", "christie")

    def test_values_unknown_value_is_rejected(self):
        svc = SuggestionService(_config(tiers=()))
        result = svc.ingest(
            VALUES_PHASE,
            self.cache,
            self.rdf,
            database="christie",
            records=[
                {"item": "Z", "match": "female", "column": "sex"},
                {"item": "F", "match": "female"},
            ],
        )
        self.assertEqual((result["accepted"], result["rejected"]), (0, 2))

    # -- errors ---------------------------------------------------------------

    def test_request_errors(self):
        svc = SuggestionService(_config(tiers=()))
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.ingest("bogus", self.cache, self.rdf, database="christie", answer="{}")
        self.assertEqual(ctx.exception.kind, "unknown_phase")
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.ingest(
                VARIABLES_PHASE, self.cache, self.rdf, database="nope", answer="{}"
            )
        self.assertEqual(ctx.exception.kind, "unknown_database")
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.ingest(
                VARIABLES_PHASE,
                self.cache,
                self.rdf,
                database="christie",
                answer="no json here",
            )
        self.assertEqual(ctx.exception.kind, "bad_answer")
        self.assertIn("Could not find valid JSON", ctx.exception.message)
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.ingest(
                VARIABLES_PHASE,
                self.cache,
                self.rdf,
                database="christie",
                answer='{"databases": {}}',
            )
        self.assertEqual(ctx.exception.kind, "bad_answer")
        self.assertIn("no column entries", ctx.exception.message)
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.ingest(
                VARIABLES_PHASE,
                self.cache,
                self.rdf,
                database="christie",
                answer="{}",
                source="alias",
            )
        self.assertEqual(ctx.exception.kind, "bad_source")
        cache = _make_session_cache()
        cache.jsonld_mapping = None
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.ingest(
                VARIABLES_PHASE, cache, self.rdf, database="christie", answer="{}"
            )
        self.assertEqual(ctx.exception.kind, "no_semantic_map")


class TestBuildPrompt(unittest.TestCase):
    def setUp(self):
        self.cache = _make_session_cache()
        self.rdf = _make_rdf_store(
            columns_by_db={"christie": ["morph", "sex", "yr"]},
            categories="value,count\n2019,3\n2020,4\n",
        )

    @patch("services.suggestions.tier1_producers")
    def test_prompt_uses_job_records_as_hints(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "string",
                {
                    "yr": {
                        "match": "year_of_initial_diagnosis",
                        "confidence": 0.86,
                        "reason": "x",
                    }
                },
            )
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        result = svc.build_prompt(
            VARIABLES_PHASE, self.cache, self.rdf, database="christie"
        )
        self.assertEqual(result["item_count"], 1)
        self.assertIn(
            "- yr    hint: year_of_initial_diagnosis (0.86, string)",
            result["prompt"],
        )
        self.assertIn('"localColumn": "morph"', result["prompt"])
        self.assertEqual(result["already_mapped"], 2)

    def test_prompt_works_with_tiers_disabled_and_chunk_override(self):
        svc = SuggestionService(_config(tiers=()))
        result = svc.build_prompt(
            VARIABLES_PHASE,
            self.cache,
            self.rdf,
            database="christie",
            chunk=7,
        )
        self.assertEqual(result["chunk_hint"], 7)
        self.assertIn("- yr", result["prompt"])
        # The variables phase shares no values and queries none.
        self.assertNotIn("2019", result["prompt"])
        self.rdf.get_categories.assert_not_called()

    def test_prompt_errors(self):
        svc = SuggestionService(_config(tiers=()))
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.build_prompt(VARIABLES_PHASE, self.cache, self.rdf, database="nope")
        self.assertEqual(ctx.exception.kind, "unknown_database")
        with self.assertRaises(SuggestionRequestError) as ctx:
            svc.build_prompt("bogus", self.cache, self.rdf, database="christie")
        self.assertEqual(ctx.exception.kind, "unknown_phase")

    def test_status_lists_prompt_export_active_regardless_of_tiers(self):
        for tiers in ((), (1,)):
            status = SuggestionService(_config(tiers=tiers)).status()
            self.assertEqual(
                status["prompt_export"],
                {
                    "state": "active",
                    "chunk": chunk_size_from_env(),
                    "min_value_count": min_value_count_from_env(),
                },
            )
            self.assertIsInstance(status["prompt_export"]["chunk"], int)


class TestSuggestionStatus(unittest.TestCase):
    def test_status_returns_tier_shape(self):
        svc = SuggestionService(_config(tiers=(1,)))
        status = svc.status()
        self.assertEqual(status["compute"], "host")
        self.assertIn("tiers", status)
        self.assertEqual(status["tiers"][1]["state"], "active")
        self.assertEqual(status["tiers"][2]["state"], "inactive")
        self.assertEqual(status["tiers"][3]["state"], "inactive")
        self.assertEqual(status["threshold"], 0.8)
        self.assertIsNotNone(status["rules_version"])

    def test_status_when_disabled(self):
        svc = SuggestionService(_config(tiers=()))
        status = svc.status()
        self.assertEqual(status["tiers"][1]["state"], "inactive")
        self.assertIn("disabled", status["tiers"][1]["reason"])


class TestStartLifecycle(unittest.TestCase):
    def setUp(self):
        self.mapping = _make_mapping()
        self.cache = _make_session_cache(self.mapping)
        self.rdf = _make_rdf_store(
            columns_by_db={"christie": ["morph", "sex", "year_col"]},
        )

    @patch("services.suggestions.tier1_producers")
    def test_start_runs_job_and_records_present(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                    "sex": {
                        "match": "biological_sex",
                        "confidence": 0.9,
                        "reason": "Alias hit.",
                    },
                    "year_col": {
                        "match": None,
                        "confidence": 0.0,
                        "reason": "No match.",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        result = svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        self.assertEqual(result["status"], "started")

        state = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertEqual(state["status"], "done")
        self.assertEqual(state["progress"]["done"], 3)
        self.assertEqual(state["progress"]["total"], 3)
        # Keys are prefixed with described_database.
        self.assertIn("christie_morph", state["records"])
        rec = state["records"]["christie_morph"]
        self.assertEqual(rec["match"], "tumour_morphology_icd_o")
        self.assertEqual(rec["confidence"], 1.0)
        self.assertEqual(rec["source"], "alias")
        self.assertEqual(rec["tier"], 1)
        self.assertEqual(rec["status"], "done")

    @patch("services.suggestions.tier1_producers")
    def test_variables_job_skips_category_queries(self, mock_producers):
        """Value-based variable suggestions are disabled, so building the
        variables payload must not issue a distinct-values query per
        column."""
        mock_producers.return_value = [FakeProducer(1, "alias", {})]
        rdf = _make_rdf_store(columns_by_db={"christie": ["morph"]})
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, rdf)
        rdf.get_categories.assert_not_called()

    @patch("services.suggestions.tier1_producers")
    def test_fingerprint_reuse_returns_already_done(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        result = svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        self.assertEqual(result["status"], "already_done")

    @patch("services.suggestions.tier1_producers")
    def test_force_reruns_job(self, mock_producers):
        call_count = [0]

        def producer_factory():
            call_count[0] += 1
            return [
                FakeProducer(
                    1,
                    "alias",
                    {
                        "morph": {
                            "match": "tumour_morphology_icd_o",
                            "confidence": 1.0,
                            "reason": "Alias hit.",
                        },
                    },
                )
            ]

        mock_producers.side_effect = producer_factory
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        svc.start(VARIABLES_PHASE, self.cache, self.rdf, force=True)
        self.assertEqual(call_count[0], 2)

    @patch("services.suggestions.tier1_producers")
    def test_no_mapping_returns_unavailable(self, mock_producers):
        mock_producers.return_value = []
        cache = _make_session_cache(mapping=None)
        svc = SuggestionService(_config())
        result = svc.start(VARIABLES_PHASE, cache, self.rdf)
        self.assertEqual(result["status"], "unavailable")

    @patch("services.suggestions.tier1_producers")
    def test_no_columns_returns_unavailable(self, mock_producers):
        mock_producers.return_value = []
        rdf = _make_rdf_store(columns_by_db={})
        svc = SuggestionService(_config())
        result = svc.start(VARIABLES_PHASE, self.cache, rdf)
        self.assertEqual(result["status"], "unavailable")

    def test_unknown_phase_returns_error(self):
        svc = SuggestionService(_config())
        result = svc.start("bogus", self.cache, self.rdf)
        self.assertEqual(result["status"], "error")

    def test_disabled_returns_disabled(self):
        svc = SuggestionService(_config(tiers=()))
        result = svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        self.assertEqual(result["status"], "disabled")


class TestCascadeMerge(unittest.TestCase):
    def setUp(self):
        self.mapping = _make_mapping()
        self.cache = _make_session_cache(self.mapping)
        self.rdf = _make_rdf_store(
            columns_by_db={"christie": ["col_a"]},
        )

    @patch("services.suggestions.tier1_producers")
    def test_highest_confidence_wins(self, mock_producers):
        """When two producers disagree, highest confidence wins."""
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "col_a": {
                        "match": "biological_sex",
                        "confidence": 0.7,
                        "reason": "Weak alias hit.",
                    },
                },
            ),
            FakeProducer(
                1,
                "string",
                {
                    "col_a": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 0.9,
                        "reason": "Strong string hit.",
                    },
                },
            ),
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        rec = state["records"]["christie_col_a"]
        self.assertEqual(rec["match"], "tumour_morphology_icd_o")
        self.assertEqual(rec["confidence"], 0.9)
        self.assertEqual(rec["source"], "string")
        # Loser is kept in alternatives.
        self.assertTrue(rec["alternatives"])
        self.assertEqual(rec["alternatives"][0]["match"], "biological_sex")
        self.assertEqual(rec["alternatives"][0]["source"], "alias")

    @patch("services.suggestions.tier1_producers")
    def test_tie_goes_to_lower_tier(self, mock_producers):
        """When confidence ties, the lower (cheaper) tier wins."""
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "col_a": {
                        "match": "biological_sex",
                        "confidence": 0.85,
                        "reason": "Alias hit.",
                    },
                },
            ),
            FakeProducer(
                2,
                "embedding",
                {
                    "col_a": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 0.85,
                        "reason": "Embedding hit.",
                    },
                },
            ),
        ]
        svc = SuggestionService(_config(tiers=(1, 2)))
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        rec = state["records"]["christie_col_a"]
        self.assertEqual(rec["match"], "biological_sex")
        self.assertEqual(rec["source"], "alias")
        self.assertEqual(rec["tier"], 1)


class TestConflictDowngrade(unittest.TestCase):
    @patch("services.suggestions.tier1_producers")
    def test_two_columns_same_variable_winner_keeps_match(self, mock_producers):
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(
            columns_by_db={"christie": ["col_a", "col_b"]},
        )
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "col_a": {
                        "match": "biological_sex",
                        "confidence": 0.95,
                        "reason": "Alias hit.",
                    },
                    "col_b": {
                        "match": "biological_sex",
                        "confidence": 0.90,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)
        state = svc.get_state(cache, VARIABLES_PHASE)
        rec_a = state["records"]["christie_col_a"]
        rec_b = state["records"]["christie_col_b"]
        # The higher-confidence column keeps its match.
        self.assertEqual(rec_a["match"], "biological_sex")
        self.assertEqual(rec_a["confidence"], 0.95)
        # The loser is nulled with a reason naming the winning column.
        self.assertIsNone(rec_b["match"])
        self.assertEqual(rec_b["confidence"], 0.0)
        self.assertIn("col_a", rec_b["reason"])
        self.assertIn("conflict", rec_b["reason"])
        # D2: the loser keeps the contested variable as an alternative so
        # the user can still pick it; the winner gains nothing.
        self.assertEqual(
            rec_b["alternatives"],
            [
                {
                    "match": "biological_sex",
                    "confidence": 0.90,
                    "reason": "Alias hit.",
                    "source": "alias",
                    "tier": 1,
                }
            ],
        )
        self.assertNotIn("alternatives", rec_a)


class TestGetState(unittest.TestCase):
    def test_idle_when_no_job(self):
        cache = _make_session_cache()
        svc = SuggestionService(_config())
        state = svc.get_state(cache, VARIABLES_PHASE)
        self.assertEqual(state["status"], "idle")
        self.assertEqual(state["records"], {})


class TestBumpPriority(unittest.TestCase):
    def test_no_job_returns_no_job(self):
        cache = _make_session_cache()
        svc = SuggestionService(_config())
        result = svc.bump_priority(cache, VARIABLES_PHASE, ["col_a"])
        self.assertEqual(result["status"], "no_job")

    @patch("services.suggestions.tier1_producers")
    def test_priority_is_noop_for_tier1(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {})]
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(columns_by_db={"christie": ["col_a"]})
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)
        result = svc.bump_priority(cache, VARIABLES_PHASE, ["col_a"])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["moved"], 0)


class TestRecordsConformToContract(unittest.TestCase):
    @patch("services.suggestions.tier1_producers")
    def test_all_records_have_required_fields(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                    "sex": {"match": None, "confidence": 0.0, "reason": "No match."},
                },
            )
        ]
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(columns_by_db={"christie": ["morph", "sex"]})
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)
        state = svc.get_state(cache, VARIABLES_PHASE)
        for key, rec in state["records"].items():
            for field in (
                "item",
                "match",
                "confidence",
                "reason",
                "source",
                "tier",
                "status",
            ):
                self.assertIn(field, rec, f"record {key} missing {field}")
            self.assertIn(rec["source"], ("alias", "value_regex", "string", "manual"))
            self.assertIn(rec["status"], ("done",))
            self.assertIsInstance(rec["confidence"], (int, float))
            self.assertIsInstance(rec["tier"], int)


class TestSchemaByteIdentical(unittest.TestCase):
    """Running a suggestion job must not mutate the mapping's schema section."""

    @patch("services.suggestions.tier1_producers")
    def test_schema_unchanged_after_job(self, mock_producers):
        import copy
        import json

        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                    "sex": {
                        "match": "biological_sex",
                        "confidence": 0.9,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(columns_by_db={"christie": ["morph", "sex"]})

        schema_before = copy.deepcopy(mapping.to_dict()["schema"])
        schema_before_json = json.dumps(schema_before, sort_keys=True)

        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)

        schema_after_json = json.dumps(mapping.to_dict()["schema"], sort_keys=True)
        self.assertEqual(schema_before_json, schema_after_json)


class TestMultiDatabaseVariables(unittest.TestCase):
    """The variables phase must produce per-database groups so each
    database's columns get keys prefixed with the correct database name.
    """

    def setUp(self):
        self.mapping = _make_mapping()
        self.cache = _make_session_cache(self.mapping)
        self.rdf = _make_rdf_store(
            columns_by_db={
                "christie": ["morph", "sex", "year_col"],
                "nki": ["morfo", "geslacht", "jaar"],
            }
        )

    @patch("services.suggestions.tier1_producers")
    def test_records_have_correct_database_prefix(self, mock_producers):
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                    "morfo": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 0.9,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        records = state["records"]
        # Each database's columns should be keyed with its own prefix.
        self.assertIn("christie_morph", records)
        self.assertIn("christie_sex", records)
        self.assertIn("christie_year_col", records)
        self.assertIn("nki_morfo", records)
        self.assertIn("nki_geslacht", records)
        self.assertIn("nki_jaar", records)
        # No ghost keys with the wrong database prefix.
        for key in records:
            self.assertTrue(
                key.startswith("christie_") or key.startswith("nki_"),
                f"Unexpected key: {key}",
            )

    @patch("services.suggestions.tier1_producers")
    def test_records_carry_explicit_location_fields(self, mock_producers):
        """Records must expose database/column(/value) as separate fields so
        the frontend never has to recover them by splitting the composite
        key (database names may contain underscores)."""
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "morph": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                    "morfo": {
                        "match": "tumour_morphology_icd_o",
                        "confidence": 0.9,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        rec = state["records"]["christie_morph"]
        self.assertEqual(rec["database"], "christie")
        self.assertEqual(rec["column"], "morph")
        rec_nki = state["records"]["nki_morfo"]
        self.assertEqual(rec_nki["database"], "nki")
        self.assertEqual(rec_nki["column"], "morfo")
        # The job snapshot exposes its fingerprint so clients can expire
        # per-job state (e.g. the frontend's suggestion marks).
        self.assertTrue(state["fingerprint"])

    @patch("services.suggestions.tier1_producers")
    def test_values_records_carry_database_column_value(self, mock_producers):
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        cache.DescriptiveInfoDetails = {
            "christie": [
                {'Biological Sex (or "sex")': [{"value": "M"}, {"value": "F"}]},
            ]
        }
        mock_producers.return_value = [
            FakeProducer(
                1,
                "alias",
                {
                    "M": {
                        "match": "male",
                        "confidence": 1.0,
                        "reason": "Alias hit.",
                    },
                },
            )
        ]
        svc = SuggestionService(_config())
        result = svc.start(VALUES_PHASE, cache, None)
        self.assertEqual(result["status"], "started")
        state = svc.get_state(cache, VALUES_PHASE)
        rec = state["records"]["christie_sex_M"]
        self.assertEqual(rec["database"], "christie")
        self.assertEqual(rec["column"], "sex")
        self.assertEqual(rec["value"], "M")
        self.assertEqual(rec["match"], "male")

    @patch("services.suggestions.tier1_producers")
    def test_total_progress_covers_all_databases(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {})]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertEqual(state["progress"]["total"], 6)


class TestProductionAbstains(unittest.TestCase):
    """WS3.7: the README's warning cases must hold end-to-end, through the
    real tier-1 producers, the sanitiser, and the cascade — not only in
    the matcher unit tests."""

    @staticmethod
    def _mapping_with_site(site_columns: dict) -> JSONLDMapping:
        """The standard fixture plus a 'leeds' site whose columns are
        remembered as ``{label: variable_key}``."""
        data = _make_mapping().to_dict()
        stub = {
            "@type": "schema:ContinuousVariable",
            "dataType": "continuous",
            "predicate": "sio:has_x",
            "class": "ncit:C00000",
        }
        for key in site_columns.values():
            data["schema"]["variables"].setdefault(key, dict(stub))
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

    def _records_for(self, mapping, columns_by_db):
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(columns_by_db=columns_by_db)
        svc = SuggestionService(_config())
        result = svc.start(VARIABLES_PHASE, cache, rdf)
        self.assertEqual(result["status"], "started")
        return svc.get_state(cache, VARIABLES_PHASE)["records"]

    def test_surv1_is_never_confidently_mapped_from_another_sites_surv7(self):
        """The alias matcher used to map surv1 to whatever another site's
        surv7 maps to at confidence 0.84 — above the 0.8 threshold. Through
        the full service the alias hit must be gone: whatever survives may
        only be a below-threshold hint, never the confident mis-map."""
        mapping = self._mapping_with_site({"surv7": "eortc_qlq_c30_q6"})
        records = self._records_for(mapping, {"nki": ["surv1"]})
        rec = records["nki_surv1"]
        self.assertNotEqual(rec["match"], "eortc_qlq_c30_q6")
        self.assertLess(rec["confidence"], 0.8)
        # The confident alias record must not hide in alternatives either.
        for alt in rec.get("alternatives", []):
            self.assertNotEqual(alt.get("match"), "eortc_qlq_c30_q6")

    def test_alg_v7_abstains_between_similar_remembered_columns(self):
        mapping = self._mapping_with_site(
            {"alg_v1b": "eortc_qlq_c30_q6", "alg_v2b": "eortc_qlq_c30_q12"}
        )
        records = self._records_for(mapping, {"nki": ["alg_v7"]})
        rec = records["nki_alg_v7"]
        self.assertIsNone(rec["match"])
        self.assertEqual(rec["confidence"], 0.0)
        # Every matcher abstained: the cascade merge keeps the most
        # informative reason, which mentions the margin.
        self.assertIn("margin", rec["reason"])
        # No abstain pollutes the alternatives list.
        self.assertEqual(rec.get("alternatives", []), [])


if __name__ == "__main__":
    unittest.main()
