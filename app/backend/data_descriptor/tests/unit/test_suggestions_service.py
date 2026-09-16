"""
Unit tests for :class:`SuggestionService`.

Covers job lifecycle, fingerprint reuse, ``force``, cascade merge and
tie-breaking, one-variable-per-database conflict downgrade, disabled state,
and unknown-phase handling. A fake tier producer replaces the real matchers
so the service logic is tested in isolation.
"""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders import JSONLDMapping
from services.suggestions import (
    SuggestionConfig,
    SuggestionService,
    VARIABLES_PHASE,
    VALUES_PHASE,
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
                out.append({
                    "item": item, "match": None, "confidence": 0.0,
                    "reason": "No match.",
                })
        return out


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_mapping() -> JSONLDMapping:
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
    })


def _make_session_cache(mapping=None, columns_by_db=None):
    """Return a mock session_cache with the attributes the service reads."""
    cache = MagicMock()
    cache.jsonld_mapping = mapping if mapping is not None else _make_mapping()
    cache.suggestion_jobs = None  # service will lazily initialise
    return cache


def _make_rdf_store(columns_by_db=None, categories=None):
    """Return a mock rdf_store_service."""
    rdf = MagicMock()
    rdf.get_column_info_by_database.return_value = columns_by_db or {}
    rdf.get_categories.return_value = categories or ""
    return rdf


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
        mock_producers.return_value = [FakeProducer(1, "alias", {
            "morph": {"match": "tumour_morphology_icd_o", "confidence": 1.0,
                      "reason": "Alias hit."},
            "sex": {"match": "biological_sex", "confidence": 0.9,
                    "reason": "Alias hit."},
            "year_col": {"match": None, "confidence": 0.0,
                         "reason": "No match."},
        })]
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
    def test_fingerprint_reuse_returns_already_done(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {
            "morph": {"match": "tumour_morphology_icd_o", "confidence": 1.0,
                      "reason": "Alias hit."},
        })]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        result = svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        self.assertEqual(result["status"], "already_done")

    @patch("services.suggestions.tier1_producers")
    def test_force_reruns_job(self, mock_producers):
        call_count = [0]

        def producer_factory():
            call_count[0] += 1
            return [FakeProducer(1, "alias", {
                "morph": {"match": "tumour_morphology_icd_o", "confidence": 1.0,
                          "reason": "Alias hit."},
            })]

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
            FakeProducer(1, "alias", {
                "col_a": {"match": "biological_sex", "confidence": 0.7,
                          "reason": "Weak alias hit."},
            }),
            FakeProducer(1, "string", {
                "col_a": {"match": "tumour_morphology_icd_o", "confidence": 0.9,
                          "reason": "Strong string hit."},
            }),
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
            FakeProducer(1, "alias", {
                "col_a": {"match": "biological_sex", "confidence": 0.85,
                          "reason": "Alias hit."},
            }),
            FakeProducer(2, "embedding", {
                "col_a": {"match": "tumour_morphology_icd_o", "confidence": 0.85,
                          "reason": "Embedding hit."},
            }),
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
    def test_two_columns_same_variable_both_nulled(self, mock_producers):
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(
            columns_by_db={"christie": ["col_a", "col_b"]},
        )
        mock_producers.return_value = [FakeProducer(1, "alias", {
            "col_a": {"match": "biological_sex", "confidence": 0.95,
                      "reason": "Alias hit."},
            "col_b": {"match": "biological_sex", "confidence": 0.90,
                      "reason": "Alias hit."},
        })]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)
        state = svc.get_state(cache, VARIABLES_PHASE)
        rec_a = state["records"]["christie_col_a"]
        rec_b = state["records"]["christie_col_b"]
        self.assertIsNone(rec_a["match"])
        self.assertEqual(rec_a["confidence"], 0.0)
        self.assertIn("conflict", rec_a["reason"])
        self.assertIn("2 columns", rec_a["reason"])
        self.assertIsNone(rec_b["match"])
        self.assertIn("conflict", rec_b["reason"])


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
        mock_producers.return_value = [FakeProducer(1, "alias", {
            "morph": {"match": "tumour_morphology_icd_o", "confidence": 1.0,
                      "reason": "Alias hit."},
            "sex": {"match": None, "confidence": 0.0, "reason": "No match."},
        })]
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(columns_by_db={"christie": ["morph", "sex"]})
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)
        state = svc.get_state(cache, VARIABLES_PHASE)
        for key, rec in state["records"].items():
            for field in ("item", "match", "confidence", "reason", "source",
                          "tier", "status"):
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

        mock_producers.return_value = [FakeProducer(1, "alias", {
            "morph": {"match": "tumour_morphology_icd_o", "confidence": 1.0,
                      "reason": "Alias hit."},
            "sex": {"match": "biological_sex", "confidence": 0.9,
                    "reason": "Alias hit."},
        })]
        mapping = _make_mapping()
        cache = _make_session_cache(mapping)
        rdf = _make_rdf_store(columns_by_db={"christie": ["morph", "sex"]})

        schema_before = copy.deepcopy(mapping.to_dict()["schema"])
        schema_before_json = json.dumps(schema_before, sort_keys=True)

        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, cache, rdf)

        schema_after_json = json.dumps(mapping.to_dict()["schema"],
                                       sort_keys=True)
        self.assertEqual(schema_before_json, schema_after_json)


class TestMultiDatabaseVariables(unittest.TestCase):
    """The variables phase must produce per-database groups so each
    database's columns get keys prefixed with the correct database name.
    """

    def setUp(self):
        self.mapping = _make_mapping()
        self.cache = _make_session_cache(self.mapping)
        self.rdf = _make_rdf_store(columns_by_db={
            "christie": ["morph", "sex", "year_col"],
            "nki": ["morfo", "geslacht", "jaar"],
        })

    @patch("services.suggestions.tier1_producers")
    def test_records_have_correct_database_prefix(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {
            "morph": {"match": "tumour_morphology_icd_o", "confidence": 1.0,
                      "reason": "Alias hit."},
            "morfo": {"match": "tumour_morphology_icd_o", "confidence": 0.9,
                      "reason": "Alias hit."},
        })]
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
    def test_total_progress_covers_all_databases(self, mock_producers):
        mock_producers.return_value = [FakeProducer(1, "alias", {})]
        svc = SuggestionService(_config())
        svc.start(VARIABLES_PHASE, self.cache, self.rdf)
        state = svc.get_state(self.cache, VARIABLES_PHASE)
        self.assertEqual(state["progress"]["total"], 6)


if __name__ == "__main__":
    unittest.main()
