"""
Unit tests for the suggestions Flask blueprint.

Tests cover ``/status``, ``/<phase>/start``, ``GET /<phase>``,
``/<phase>/priority``, 400 on unknown phase, and the disabled state. A mock
``SuggestionService`` is injected so no real tier producers run.
"""

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask

from controllers import suggestions_bp
from services.suggestions import SuggestionRequestError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app(suggestion_service=None, session_cache=None):
    app = Flask(__name__)
    app.secret_key = "test-secret"
    mock_session = session_cache or MagicMock()
    app.config["APP_CONTEXT"] = {
        "session_cache": mock_session,
        "rdf_store_service": MagicMock(),
        "suggestion_service": suggestion_service,
    }
    app.register_blueprint(suggestions_bp)
    return app


def _make_mock_service(enabled=True, compute="host", threshold=0.8):
    svc = MagicMock()
    svc.config.enabled = enabled
    svc.config.compute = compute
    svc.config.threshold = threshold
    svc.status.return_value = {
        "compute": compute,
        "tiers": {
            1: {"state": "active"},
            2: {"state": "inactive", "reason": "not enabled"},
            3: {"state": "inactive", "reason": "not enabled"},
        },
        "threshold": threshold,
        "rules_version": "1.0.0",
    }
    svc.get_state.return_value = {
        "status": "done",
        "progress": {"done": 2, "total": 2},
        "error": None,
        "records": {
            "christie_morph": {
                "item": "morph",
                "match": "tumour_morphology_icd_o",
                "confidence": 1.0,
                "reason": "Alias hit.",
                "source": "alias",
                "tier": 1,
                "status": "done",
            }
        },
    }
    svc.start.return_value = {"status": "started"}
    svc.bump_priority.return_value = {"status": "ok", "moved": 0}
    return svc


# A minimal mapping that passes MappingValidator (the full @context and
# the mapping:DataMapping @type are required).
_VALID_MAPPING = {
    "@context": {
        "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/",
        "sio": "http://semanticscience.org/resource/",
        "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
        "schema": "schema/",
        "mapping": "mapping/",
        "variables": {"@id": "schema:hasVariable", "@container": "@index"},
        "valueMapping": {"@id": "schema:hasValueMapping"},
        "terms": {"@id": "schema:hasTerms", "@container": "@index"},
        "targetClass": {"@id": "schema:mapsToClass", "@type": "@id"},
        "databases": {"@id": "mapping:hasDatabase", "@container": "@index"},
        "tables": {"@id": "mapping:hasTable", "@container": "@index"},
        "columns": {"@id": "mapping:hasColumn", "@container": "@index"},
        "localMappings": {"@id": "mapping:hasLocalMappings", "@container": "@index"},
        "mapsTo": {"@id": "mapping:mapsToVariable", "@type": "@id"},
    },
    "@id": "mapping:test",
    "@type": "mapping:DataMapping",
    "schema": {
        "@id": "schema:root",
        "@type": "schema:SemanticSchema",
        "variables": {
            "biological_sex": {
                "@type": "schema:CategoricalVariable",
                "dataType": "categorical",
                "predicate": "sio:has_sex",
                "class": "ncit:C28421",
                "valueMapping": {"terms": {"male": {"targetClass": "ncit:C20197"}}},
            }
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
                        "sex": {
                            "mapsTo": "schema:variable/biological_sex",
                            "localColumn": "sex",
                        }
                    },
                }
            },
        }
    },
}


# ---------------------------------------------------------------------------
# Status tests
# ---------------------------------------------------------------------------


class TestStatus(unittest.TestCase):
    def test_status_enabled(self):
        svc = _make_mock_service(enabled=True)
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/status")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertTrue(data["enabled"])
            self.assertEqual(data["compute"], "host")
            self.assertEqual(data["tiers"]["1"]["state"], "active")
            self.assertEqual(data["tiers"]["2"]["state"], "inactive")
            self.assertEqual(data["threshold"], 0.8)
            self.assertEqual(data["rules_version"], "1.0.0")

    def test_status_disabled(self):
        svc = _make_mock_service(enabled=False)
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/status")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertFalse(data["enabled"])
            self.assertEqual(data["tiers"]["1"]["state"], "inactive")
            self.assertIn("disabled", data["tiers"]["1"]["reason"])

    def test_status_no_service(self):
        app = _make_app(suggestion_service=None)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/status")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertFalse(data["enabled"])
            self.assertEqual(data["compute"], "host")


# ---------------------------------------------------------------------------
# Start tests
# ---------------------------------------------------------------------------


class TestStart(unittest.TestCase):
    def test_start_variables(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/start",
                json={},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "started")
            svc.start.assert_called_once()

    def test_start_values(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/values/start",
                json={},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "started")

    def test_start_with_force(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/start",
                json={"force": True},
            )
            self.assertEqual(resp.status_code, 200)
            _, kwargs = svc.start.call_args
            self.assertTrue(kwargs.get("force"))

    def test_start_unknown_phase_returns_400(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post("/api/v1/suggestions/bogus/start", json={})
            self.assertEqual(resp.status_code, 400)

    def test_start_no_service_returns_disabled(self):
        app = _make_app(suggestion_service=None)
        with app.test_client() as client:
            resp = client.post("/api/v1/suggestions/variables/start", json={})
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "disabled")

    def test_start_adopts_valid_body_mapping_when_session_has_none(self):
        """The mapping may only survive in the browser's IndexedDB (e.g.
        after a container restart): when the session has no mapping and the
        body carries a VALID one (MappingValidator passes), it is adopted
        so the job does not return no_semantic_map."""
        svc = _make_mock_service()
        session_cache = MagicMock()
        session_cache.jsonld_mapping = None
        app = _make_app(svc, session_cache=session_cache)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/start",
                json={"mapping": _VALID_MAPPING},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "started")
        self.assertIsNotNone(session_cache.jsonld_mapping)

    def test_start_rejects_invalid_body_mapping(self):
        """An unvalidated request body must never reach the session or the
        job: a mapping that fails MappingValidator is dropped."""
        svc = _make_mock_service()
        session_cache = MagicMock()
        session_cache.jsonld_mapping = None
        app = _make_app(svc, session_cache=session_cache)
        invalid = dict(_VALID_MAPPING)
        invalid["@type"] = "mapping:NotAThing"
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/start",
                json={"mapping": invalid},
            )
            self.assertEqual(resp.status_code, 200)
        self.assertIsNone(session_cache.jsonld_mapping)
        # The job ran on the session mapping (None), not the rejected body.
        self.assertIs(svc.start.call_args.kwargs.get("mapping"), None)

    def test_start_variables_uses_body_mapping_without_overwriting_session(self):
        """The describe pages work on the browser's semantic map, and the
        session may hold an older one (the describe-landing upload never
        reaches it). The variables job therefore runs on the body mapping,
        job-locally, and the session's own mapping is left untouched."""
        svc = _make_mock_service()
        existing = MagicMock(name="existing-session-mapping")
        session_cache = MagicMock()
        session_cache.jsonld_mapping = existing
        app = _make_app(svc, session_cache=session_cache)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/start",
                json={"mapping": _VALID_MAPPING},
            )
            self.assertEqual(resp.status_code, 200)
        self.assertIs(session_cache.jsonld_mapping, existing)
        # The variables job runs on the browser's map, not the session's.
        job_mapping = svc.start.call_args.kwargs.get("mapping")
        self.assertIsNotNone(job_mapping)
        self.assertEqual(job_mapping.get_all_variable_keys(), ["biological_sex"])

    def test_start_values_phase_uses_body_mapping_job_locally(self):
        """The values phase needs the browser's latest variable
        selections, so the body mapping is parsed, validated, and passed
        to the service for THIS JOB only — never assigned to the session
        cache."""
        svc = _make_mock_service()
        existing = MagicMock(name="existing-session-mapping")
        session_cache = MagicMock()
        session_cache.jsonld_mapping = existing
        app = _make_app(svc, session_cache=session_cache)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/values/start",
                json={"mapping": _VALID_MAPPING},
            )
            self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "started")
        # The session mapping is untouched...
        self.assertIs(session_cache.jsonld_mapping, existing)
        # ...and the job received the parsed body mapping.
        job_mapping = svc.start.call_args.kwargs.get("mapping")
        self.assertIsNotNone(job_mapping)
        self.assertEqual(job_mapping.get_all_variable_keys(), ["biological_sex"])


# ---------------------------------------------------------------------------
# GET phase tests
# ---------------------------------------------------------------------------


class TestGetSuggestions(unittest.TestCase):
    def test_get_variables_returns_records(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/variables")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(data["status"], "done")
            self.assertTrue(data["enabled"])
            self.assertIn("christie_morph", data["records"])

    def test_get_values(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/values")
            self.assertEqual(resp.status_code, 200)

    def test_get_unknown_phase_returns_400(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/bogus")
            self.assertEqual(resp.status_code, 400)

    def test_get_no_service_returns_idle(self):
        app = _make_app(suggestion_service=None)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/variables")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertFalse(data["enabled"])
            self.assertEqual(data["status"], "idle")
            self.assertEqual(data["records"], {})


# ---------------------------------------------------------------------------
# Priority tests
# ---------------------------------------------------------------------------


class TestPriority(unittest.TestCase):
    def test_priority_returns_ok(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/priority",
                json={"items": ["morph", "sex"]},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "ok")

    def test_priority_unknown_phase_returns_400(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/bogus/priority",
                json={"items": ["x"]},
            )
            self.assertEqual(resp.status_code, 400)

    def test_priority_without_items_returns_400(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/priority",
                json={},
            )
            self.assertEqual(resp.status_code, 400)

    def test_priority_accepts_columns_alias(self):
        svc = _make_mock_service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/priority",
                json={"columns": ["morph"]},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "ok")

    def test_priority_no_service_returns_no_job(self):
        app = _make_app(suggestion_service=None)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/priority",
                json={"items": ["morph"]},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "no_job")


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Prompt export / ingest (issue 2)
# ---------------------------------------------------------------------------


class TestPromptRoute(unittest.TestCase):
    def _service(self):
        svc = _make_mock_service()
        svc.build_prompt.return_value = {
            "prompt": "PROMPT",
            "chunks": [
                {"index": 1, "items": ["yr"], "item_count": 1, "prompt": "PROMPT"}
            ],
            "item_count": 1,
            "chunk_hint": 40,
            "contains": ["variable keys"],
            "answer_schema": {"type": "object"},
        }
        return svc

    def test_get_prompt(self):
        svc = self._service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/prompt?phase=variables&database=nki")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(data["prompt"], "PROMPT")
            self.assertEqual(data["item_count"], 1)
            self.assertIn("answer_schema", data)
            self.assertIn("contains", data)
        kwargs = svc.build_prompt.call_args.kwargs
        self.assertEqual(kwargs["database"], "nki")
        self.assertTrue(kwargs["include_values"])
        self.assertTrue(kwargs["exclude_free_text"])
        self.assertIsNone(kwargs["chunk"])
        self.assertIsNone(kwargs["mapping"])

    def test_post_prompt_with_options_and_mapping(self):
        svc = self._service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/prompt",
                data=json.dumps(
                    {
                        "phase": "values",
                        "database": "christie",
                        "include_values": False,
                        "exclude_free_text": "false",
                        "chunk": "20",
                        "mapping": _VALID_MAPPING,
                    }
                ),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
        args, kwargs = svc.build_prompt.call_args
        self.assertEqual(args[0], "values")
        self.assertFalse(kwargs["include_values"])
        self.assertFalse(kwargs["exclude_free_text"])
        self.assertEqual(kwargs["chunk"], 20)
        self.assertIsNotNone(kwargs["mapping"])
        self.assertEqual(kwargs["mapping_data"], _VALID_MAPPING)

    def test_prompt_errors(self):
        svc = self._service()
        svc.build_prompt.side_effect = SuggestionRequestError(
            "unknown_database", "unknown database 'nope'"
        )
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.get("/api/v1/suggestions/prompt?phase=bogus&database=nki")
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "unknown_phase")
            resp = client.get("/api/v1/suggestions/prompt?phase=variables")
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "unknown_database")
            resp = client.get(
                "/api/v1/suggestions/prompt?phase=variables&database=nope"
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "unknown_database")
            self.assertIn("nope", resp.get_json()["error"])
            # A body that parses to a JSON array is a caller mistake: a
            # readable 400, not an unhandled AttributeError (a 500).
            resp = client.post(
                "/api/v1/suggestions/prompt",
                data=json.dumps(["not", "a", "dict"]),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "unknown_phase")
        with _make_app(None).test_client() as client:
            resp = client.get("/api/v1/suggestions/prompt?phase=variables&database=nki")
            self.assertEqual(resp.status_code, 503)

    def test_status_lists_prompt_export(self):
        svc = _make_mock_service(enabled=False)
        with _make_app(svc).test_client() as client:
            data = client.get("/api/v1/suggestions/status").get_json()
            self.assertFalse(data["enabled"])
            self.assertEqual(data["prompt_export"]["state"], "active")
            # The site's chunk default reaches the panel through /status.
            with patch.dict(os.environ, {"FLYOVER_SUGGESTION_PROMPT_CHUNK": "160"}):
                data = client.get("/api/v1/suggestions/status").get_json()
            self.assertEqual(data["prompt_export"]["chunk"], 160)
        with _make_app(None).test_client() as client:
            data = client.get("/api/v1/suggestions/status").get_json()
            self.assertEqual(data["prompt_export"]["state"], "inactive")


class TestIngestRoute(unittest.TestCase):
    def _service(self):
        svc = _make_mock_service(enabled=False)
        svc.ingest.return_value = {
            "accepted": 3,
            "nulled": 1,
            "rejected": 0,
            "skipped": 0,
            "messages": [],
            "job": {"status": "done", "fingerprint": "x", "records": {}},
        }
        return svc

    def test_ingest_answer_text(self):
        svc = self._service()
        app = _make_app(svc)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/ingest",
                data=json.dumps(
                    {
                        "database": "nki",
                        "source": "pasted_llm",
                        "answer": "```json\n[]\n```",
                        "mapping": _VALID_MAPPING,
                    }
                ),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual((data["accepted"], data["nulled"]), (3, 1))
            self.assertIn("job", data)
        args, kwargs = svc.ingest.call_args
        self.assertEqual(args[0], "variables")
        self.assertEqual(kwargs["database"], "nki")
        self.assertEqual(kwargs["answer"], "```json\n[]\n```")
        self.assertIsNone(kwargs["records"])
        self.assertEqual(kwargs["source"], "pasted_llm")
        self.assertIsNotNone(kwargs["mapping"])

    def test_ingest_records_list(self):
        svc = self._service()
        with _make_app(svc).test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/values/ingest",
                data=json.dumps(
                    {"database": "nki", "records": [{"item": "M", "match": "male"}]}
                ),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
        kwargs = svc.ingest.call_args.kwargs
        self.assertIsNone(kwargs["answer"])
        self.assertEqual(kwargs["records"], [{"item": "M", "match": "male"}])

    def test_ingest_errors(self):
        svc = self._service()
        with _make_app(svc).test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/bogus/ingest",
                data="{}",
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            resp = client.post(
                "/api/v1/suggestions/variables/ingest",
                data=json.dumps({"answer": "{}"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "unknown_database")
            resp = client.post(
                "/api/v1/suggestions/variables/ingest",
                data=json.dumps({"database": "nki"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "bad_answer")
            # A body that parses to a JSON array is a caller mistake: a
            # readable 400, not an unhandled AttributeError (a 500).
            resp = client.post(
                "/api/v1/suggestions/variables/ingest",
                data=json.dumps(["not", "a", "dict"]),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["kind"], "unknown_database")
            svc.ingest.side_effect = SuggestionRequestError(
                "bad_answer", "Could not find valid JSON in the pasted text."
            )
            resp = client.post(
                "/api/v1/suggestions/variables/ingest",
                data=json.dumps({"database": "nki", "answer": "nope"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            self.assertIn("Could not find valid JSON", resp.get_json()["error"])
        with _make_app(None).test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/ingest",
                data=json.dumps({"database": "nki", "answer": "{}"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 503)

    def test_snapshot_reports_enabled_when_a_pasted_job_exists(self):
        svc = _make_mock_service(enabled=False)
        with _make_app(svc).test_client() as client:
            data = client.get("/api/v1/suggestions/variables").get_json()
            self.assertTrue(data["enabled"])
        svc.get_state.return_value = {
            "status": "idle",
            "progress": {"done": 0, "total": 0},
            "error": None,
            "records": {},
        }
        with _make_app(svc).test_client() as client:
            data = client.get("/api/v1/suggestions/variables").get_json()
            self.assertFalse(data["enabled"])
