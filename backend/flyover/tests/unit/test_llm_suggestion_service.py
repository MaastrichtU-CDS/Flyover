"""
Backend unit: the LLM suggestion job orchestration.

Tests cover chunking and ordering, preselected-pair exclusion, cross-chunk
deduplication, priority bumps and retries, fingerprint idempotency,
generation-based cancellation, failure escalation, and the values-phase
extraction from DescriptiveInfoDetails — with a fake Ollama client and a
controllable spawn.
"""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.llm.suggestion_service import (
    VALUES_PHASE,
    VARIABLES_PHASE,
    LLMConfig,
    LLMSuggestionService,
)


class FakeMapping:
    """Minimal stand-in for JSONLDMapping."""

    def __init__(self, variable_keys, local_columns=None, value_mappings=None):
        self._keys = variable_keys
        self._local_columns = local_columns or {}
        self._value_mappings = value_mappings or {}

    def get_all_variable_keys(self):
        return list(self._keys)

    def get_local_column(self, key, database_key=None, table_key=None):
        return self._local_columns.get(key)

    def get_variable(self, key):
        if key not in self._keys:
            return None
        return SimpleNamespace(value_mappings=self._value_mappings.get(key, {}))


class FakeClient:
    """Fake OllamaClient with programmable match results."""

    def __init__(self, matcher=None):
        self.calls = []
        self.matcher = matcher or (lambda items, candidates: [])
        self.ensure_model_error = None

    def ensure_model(self, model, fallbacks=None):
        if self.ensure_model_error:
            raise self.ensure_model_error
        return model

    def match_equivalents(self, list_a, list_b, model):
        self.calls.append((list(list_a), list(list_b)))
        result = self.matcher(list_a, list_b)
        if isinstance(result, Exception):
            raise result
        return result


def _match_all_to(target):
    """Matcher mapping every item to the same target."""

    def matcher(items, candidates):
        return [
            {"item": i, "match": target, "confidence": 0.9, "reason": "r"}
            for i in items
        ]

    return matcher


def _match_by(table):
    """Matcher using a lookup dict; unmatched items get null."""

    def matcher(items, candidates):
        return [
            {
                "item": i,
                "match": table.get(i),
                "confidence": 0.9 if table.get(i) else 0.0,
                "reason": "r",
            }
            for i in items
        ]

    return matcher


def _cache(mapping=None, details=None):
    return SimpleNamespace(
        jsonld_mapping=mapping, DescriptiveInfoDetails=details, llm_jobs=None
    )


def _rdf(columns_by_db):
    return SimpleNamespace(get_column_info_by_database=lambda: columns_by_db)


def _service(client, enabled=True, chunk_size=2, spawn=None):
    config = LLMConfig(enabled=enabled, model="test-model", chunk_size=chunk_size)
    return LLMSuggestionService(
        config, client, spawn=spawn or (lambda fn, *args: fn(*args))
    )


class TestStartVariableJob(unittest.TestCase):
    """Test the variables-phase job build and worker run."""

    def test_disabled_config_short_circuits(self):
        service = _service(FakeClient(), enabled=False)
        result = service.start_variable_job(_cache(FakeMapping(["v"])), _rdf({}))
        self.assertEqual(result, {"status": "disabled"})

    def test_no_mapping_is_unavailable(self):
        service = _service(FakeClient())
        cache = _cache(mapping=None)
        result = service.start_variable_job(cache, _rdf({"db": ["a"]}))
        self.assertEqual(result["reason"], "no_semantic_map")
        self.assertEqual(cache.llm_jobs[VARIABLES_PHASE].status, "unavailable")

    def test_chunks_follow_column_order(self):
        client = FakeClient(_match_by({}))
        service = _service(client, chunk_size=2)
        cache = _cache(FakeMapping(["v1", "v2"]))
        service.start_variable_job(cache, _rdf({"db": ["a", "b", "c"]}))
        self.assertEqual([call[0] for call in client.calls], [["a", "b"], ["c"]])

    def test_preselected_pairs_excluded(self):
        client = FakeClient(_match_by({}))
        service = _service(client, chunk_size=8)
        mapping = FakeMapping(["age", "sex"], local_columns={"age": "leeftijd"})
        cache = _cache(mapping)
        service.start_variable_job(cache, _rdf({"db": ["leeftijd", "geslacht"]}))
        self.assertEqual(client.calls, [(["geslacht"], ["sex"])])

    def test_all_preselected_is_nothing_to_suggest(self):
        service = _service(FakeClient())
        mapping = FakeMapping(["age"], local_columns={"age": "leeftijd"})
        cache = _cache(mapping)
        result = service.start_variable_job(cache, _rdf({"db": ["leeftijd"]}))
        self.assertEqual(result["reason"], "nothing_to_suggest")

    def test_results_recorded_and_public_shape(self):
        client = FakeClient(_match_by({"a": "v1"}))
        service = _service(client, chunk_size=8)
        cache = _cache(FakeMapping(["v1", "v2"]))
        service.start_variable_job(cache, _rdf({"db": ["a", "b"]}))

        state = service.get_state(cache, VARIABLES_PHASE)
        self.assertEqual(state["status"], "done")
        self.assertEqual(state["progress"], {"chunks_done": 1, "chunks_total": 1})
        self.assertEqual(state["suggestions"]["db"]["a"]["variable_key"], "v1")
        self.assertIsNone(state["suggestions"]["db"]["b"]["variable_key"])

    def test_cross_chunk_dedup_first_wins(self):
        client = FakeClient(_match_all_to("v1"))
        service = _service(client, chunk_size=1)
        cache = _cache(FakeMapping(["v1", "v2"]))
        service.start_variable_job(cache, _rdf({"db": ["a", "b"]}))

        suggestions = service.get_state(cache, VARIABLES_PHASE)["suggestions"]["db"]
        self.assertEqual(suggestions["a"]["variable_key"], "v1")
        self.assertIsNone(suggestions["b"]["variable_key"])
        self.assertIn("already suggested", suggestions["b"]["reason"])

    def test_fingerprint_idempotency_and_force(self):
        client = FakeClient(_match_by({}))
        service = _service(client)
        cache = _cache(FakeMapping(["v1"]))
        rdf = _rdf({"db": ["a"]})

        self.assertEqual(service.start_variable_job(cache, rdf)["status"], "started")
        self.assertEqual(
            service.start_variable_job(cache, rdf)["status"], "already_done"
        )
        self.assertEqual(
            service.start_variable_job(cache, rdf, force=True)["status"], "started"
        )

    def test_changed_inputs_rebuild_job(self):
        client = FakeClient(_match_by({}))
        service = _service(client)
        cache = _cache(FakeMapping(["v1"]))
        service.start_variable_job(cache, _rdf({"db": ["a"]}))
        result = service.start_variable_job(cache, _rdf({"db": ["a", "b"]}))
        self.assertEqual(result["status"], "started")

    def test_model_failure_marks_job_failed(self):
        client = FakeClient()
        client.ensure_model_error = RuntimeError("unreachable")
        service = _service(client)
        cache = _cache(FakeMapping(["v1"]))
        service.start_variable_job(cache, _rdf({"db": ["a"]}))

        state = service.get_state(cache, VARIABLES_PHASE)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["error"]["kind"], "ollama_unavailable")

    def test_chunk_failure_continues_then_three_strikes_fails(self):
        client = FakeClient(lambda items, candidates: RuntimeError("boom"))
        service = _service(client, chunk_size=1)
        cache = _cache(FakeMapping(["v1"]))
        service.start_variable_job(cache, _rdf({"db": ["a", "b", "c", "d"]}))

        state = service.get_state(cache, VARIABLES_PHASE)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["error"]["kind"], "chunk_failures")
        self.assertEqual(state["progress"]["chunks_done"], 3)
        self.assertEqual(state["suggestions"]["db"]["a"], {"status": "error"})
        self.assertEqual(state["suggestions"]["db"]["d"], {"status": "pending"})

    def test_generation_bump_cancels_running_worker(self):
        service_holder = {}

        def matcher(items, candidates):
            job = service_holder["cache"].llm_jobs[VARIABLES_PHASE]
            job.generation += 1
            return [
                {"item": i, "match": None, "confidence": 0.0, "reason": "r"}
                for i in items
            ]

        client = FakeClient(matcher)
        service = _service(client, chunk_size=1)
        cache = _cache(FakeMapping(["v1"]))
        service_holder["cache"] = cache
        service.start_variable_job(cache, _rdf({"db": ["a", "b"]}))

        job = cache.llm_jobs[VARIABLES_PHASE]
        self.assertEqual(len(client.calls), 1)
        self.assertNotEqual(job.status, "done")
        self.assertEqual(job.results["db"]["a"], {"status": "pending"})


class TestPriorityAndRetry(unittest.TestCase):
    """Test queue reordering and retries."""

    def _deferred_service(self, client, chunk_size=1):
        deferred = []
        service = _service(
            client,
            chunk_size=chunk_size,
            spawn=lambda fn, *args: deferred.append((fn, args)),
        )
        return service, deferred

    def test_bump_moves_matching_chunks_to_front(self):
        client = FakeClient(_match_by({}))
        service, deferred = self._deferred_service(client)
        cache = _cache(FakeMapping(["v1"]))
        service.start_variable_job(cache, _rdf({"db": ["a", "b", "c"]}))

        result = service.bump_priority(cache, VARIABLES_PHASE, "db", ["c"])
        self.assertEqual(result, {"status": "ok", "moved": 1})
        job = cache.llm_jobs[VARIABLES_PHASE]
        self.assertEqual([c["items"] for c in job.pending], [["c"], ["a"], ["b"]])

    def test_bump_without_job_reports_no_job(self):
        service = _service(FakeClient())
        result = service.bump_priority(_cache(), VARIABLES_PHASE, "db", ["a"])
        self.assertEqual(result, {"status": "no_job"})

    def test_retry_discards_result_releases_match_and_respawns(self):
        client = FakeClient(_match_all_to("v1"))
        service = _service(client, chunk_size=8)
        cache = _cache(FakeMapping(["v1", "v2"]))
        service.start_variable_job(cache, _rdf({"db": ["a", "b"]}))
        job = cache.llm_jobs[VARIABLES_PHASE]
        self.assertEqual(job.status, "done")
        self.assertEqual(job.results["db"]["a"]["variable_key"], "v1")

        client.matcher = _match_by({})
        result = service.bump_priority(
            cache, VARIABLES_PHASE, "db", ["a"], retry=True
        )
        self.assertEqual(result["moved"], 1)
        self.assertEqual(job.status, "done")
        self.assertNotIn("v1", job.used_matches["db"])
        self.assertIsNone(job.results["db"]["a"]["variable_key"])
        self.assertEqual(client.calls[-1], (["a"], ["v1", "v2"]))


class TestStartValueJob(unittest.TestCase):
    """Test the values-phase extraction and job run."""

    def _mapping(self):
        return FakeMapping(
            ["biological_sex", "age"],
            value_mappings={"biological_sex": {"male": "c1", "female": "c2"}},
        )

    def _details(self, rows):
        return {"db": [{'Biological Sex (or "geslacht")': rows}, "ignored-continuous"]}

    def test_extracts_targets_and_matches_values(self):
        client = FakeClient(_match_by({"M": "male", "V": "female"}))
        service = _service(client)
        rows = [
            {"value": "M", "count": "10"},
            {"value": "V", "count": "12"},
            {"value": "", "count": "1"},
            {"value": "M", "count": "3"},
        ]
        cache = _cache(self._mapping(), self._details(rows))
        result = service.start_value_job(cache)
        self.assertEqual(result["status"], "started")
        self.assertEqual(client.calls, [(["M", "V"], ["male", "female"])])

        state = service.get_state(cache, VALUES_PHASE)
        entry = state["suggestions"]["db"]["geslacht"]
        self.assertEqual(entry["status"], "done")
        self.assertEqual(entry["variable_key"], "biological_sex")
        self.assertEqual(entry["values"]["M"]["term_key"], "male")
        self.assertNotIn("_chunks_left", entry)

    def test_missing_description_and_unknown_variables_skipped(self):
        client = FakeClient(_match_by({}))
        service = _service(client)
        details = {
            "db": [
                {'Missing Description (or "vrijveld")': [{"value": "x"}]},
                {'Unknown Var (or "kolom")': [{"value": "x"}]},
                {'Age (or "leeftijd")': [{"value": "1"}]},
            ]
        }
        cache = _cache(self._mapping(), details)
        result = service.start_value_job(cache)
        self.assertEqual(result["reason"], "nothing_to_suggest")

    def test_no_details_is_unavailable(self):
        service = _service(FakeClient())
        cache = _cache(self._mapping(), details=None)
        result = service.start_value_job(cache)
        self.assertEqual(result["reason"], "no_data")

    def test_wide_variables_split_and_complete_after_all_chunks(self):
        client = FakeClient(_match_by({}))
        deferred = []
        service = _service(
            client, spawn=lambda fn, *args: deferred.append((fn, args))
        )
        rows = [{"value": str(i)} for i in range(20)]
        cache = _cache(self._mapping(), self._details(rows))
        service.start_value_job(cache)

        job = cache.llm_jobs[VALUES_PHASE]
        self.assertEqual(len(job.pending), 2)
        entry = job.results["db"]["geslacht"]
        self.assertEqual(entry["_chunks_left"], 2)

        fn, args = deferred[0]
        fn(*args)
        self.assertEqual(entry["status"], "done")
        self.assertEqual(len(entry["values"]), 20)


if __name__ == "__main__":
    unittest.main()
