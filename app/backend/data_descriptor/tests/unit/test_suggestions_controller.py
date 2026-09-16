"""
Unit tests for the suggestions Flask blueprint.

Tests cover ``/status``, ``/<phase>/start``, ``GET /<phase>``,
``/<phase>/priority``, 400 on unknown phase, and the disabled state. A mock
``SuggestionService`` is injected so no real tier producers run.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask

from controllers import suggestions_bp


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
            resp = client.post(
                "/api/v1/suggestions/variables/start", json={}
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "disabled")

    def test_start_restores_mapping_from_body_when_missing(self):
        """When session_cache.jsonld_mapping is None and the frontend sends
        a mapping dict, the controller should populate jsonld_mapping before
        calling service.start — so the service doesn't return
        no_semantic_map.
        """
        svc = _make_mock_service()
        session_cache = MagicMock()
        session_cache.jsonld_mapping = None
        app = _make_app(svc, session_cache=session_cache)
        mapping_dict = {
            "@context": {"schema": "mapping:schema/"},
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
                                "sex": {
                                    "mapsTo": "schema:variable/biological_sex",
                                    "localColumn": "sex",
                                },
                            },
                        }
                    },
                }
            },
        }
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/variables/start",
                json={"mapping": mapping_dict},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["status"], "started")
            # jsonld_mapping should now be populated on the session cache.
            self.assertIsNotNone(session_cache.jsonld_mapping)

    def test_start_updates_mapping_from_body_when_provided(self):
        """When the frontend sends a mapping in the body, the controller should
        always update jsonld_mapping — even if one is already set. The values
        phase relies on this to send the UPDATED mapping (reflecting the user's
        variable selections) so value suggestions can resolve column→variable.
        """
        svc = _make_mock_service()
        existing = MagicMock()
        session_cache = MagicMock()
        session_cache.jsonld_mapping = existing
        app = _make_app(svc, session_cache=session_cache)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/suggestions/values/start",
                json={"mapping": {"schema": {"variables": {}}}},
            )
            self.assertEqual(resp.status_code, 200)
            # The existing mapping should be replaced, not preserved.
            self.assertIsNot(session_cache.jsonld_mapping, existing)


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
