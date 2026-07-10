"""
Backend unit: the LLM controller/route layer.

Tests cover /api/v1/llm/status and the suggestions start/snapshot/priority
endpoints for both phases, with the suggestion service fully mocked.
"""

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

# Add parent (flyover package) to path so blueprint imports resolve
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask

from controllers import llm_bp

_MINIMAL_VALID_JSONLD = {
    "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
    "@type": "mapping:DataMapping",
    "name": "Test Mapping",
    "schema": {
        "@type": "schema:SemanticSchema",
        "variables": {
            "identifier": {
                "@type": "schema:IdentifierVariable",
                "dataType": "identifier",
                "predicate": "sio:SIO_000673",
                "class": "ncit:C25364",
            }
        },
    },
    "databases": {
        "test_db": {
            "@type": "mapping:Database",
            "name": "Test Database",
            "tables": {
                "test_table": {
                    "@type": "mapping:Table",
                    "columns": {
                        "identifier": {
                            "mapsTo": "schema:variable/identifier",
                            "localColumn": "id",
                        }
                    },
                }
            },
        }
    },
}


def _make_llm_app(enabled=True, reachable=True):
    """Return a minimal Flask app with llm_bp and a mocked llm_service."""
    app = Flask(__name__)
    app.secret_key = "test-secret"

    mock_session = MagicMock()
    mock_session.jsonld_mapping = None

    llm_service = MagicMock()
    llm_service.config = SimpleNamespace(enabled=enabled, model="llama3.2:3b")
    llm_service.client.is_reachable.return_value = reachable
    llm_service.get_state.return_value = {
        "status": "running",
        "model": "llama3.2:3b",
        "progress": {"chunks_done": 1, "chunks_total": 3},
        "error": None,
        "suggestions": {},
    }
    llm_service.start_variable_job.return_value = {"status": "started"}
    llm_service.start_value_job.return_value = {"status": "started"}
    llm_service.bump_priority.return_value = {"status": "ok", "moved": 1}

    app.config["APP_CONTEXT"] = {
        "session_cache": mock_session,
        "rdf_store_service": MagicMock(),
        "llm_service": llm_service,
    }
    app.register_blueprint(llm_bp)
    return app, llm_service, mock_session


class TestLLMStatus(unittest.TestCase):
    """Tests for GET /api/v1/llm/status."""

    def test_disabled_reports_enabled_false(self):
        app, _, _ = _make_llm_app(enabled=False)
        response = app.test_client().get("/api/v1/llm/status")
        self.assertEqual(
            json.loads(response.data),
            {"enabled": False, "model": None, "ollama": None},
        )

    def test_enabled_and_reachable(self):
        app, _, _ = _make_llm_app(enabled=True, reachable=True)
        body = json.loads(app.test_client().get("/api/v1/llm/status").data)
        self.assertEqual(body["ollama"], "ready")
        self.assertEqual(body["model"], "llama3.2:3b")

    def test_enabled_and_unreachable(self):
        app, _, _ = _make_llm_app(enabled=True, reachable=False)
        body = json.loads(app.test_client().get("/api/v1/llm/status").data)
        self.assertEqual(body["ollama"], "unreachable")


class TestVariableSuggestionRoutes(unittest.TestCase):
    """Tests for the variables-phase endpoints."""

    def setUp(self):
        self.app, self.service, self.session = _make_llm_app()
        self.client = self.app.test_client()

    def test_start_delegates_and_returns_service_result(self):
        response = self.client.post(
            "/api/v1/llm/suggestions/variables/start", json={"force": True}
        )
        self.assertEqual(json.loads(response.data), {"status": "started"})
        _, kwargs = self.service.start_variable_job.call_args
        self.assertTrue(kwargs["force"])

    def test_start_adopts_valid_mapping_when_backend_has_none(self):
        response = self.client.post(
            "/api/v1/llm/suggestions/variables/start",
            json={"mapping": _MINIMAL_VALID_JSONLD},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(self.session.jsonld_mapping)

    def test_start_rejects_invalid_mapping(self):
        response = self.client.post(
            "/api/v1/llm/suggestions/variables/start",
            json={"mapping": {"not": "a mapping"}},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("validation_errors", json.loads(response.data))
        self.service.start_variable_job.assert_not_called()

    def test_snapshot_includes_enabled_flag(self):
        body = json.loads(self.client.get("/api/v1/llm/suggestions/variables").data)
        self.assertTrue(body["enabled"])
        self.assertEqual(body["status"], "running")

    def test_priority_requires_database_and_columns(self):
        response = self.client.post(
            "/api/v1/llm/suggestions/variables/priority", json={"database": "db"}
        )
        self.assertEqual(response.status_code, 400)

    def test_priority_delegates(self):
        response = self.client.post(
            "/api/v1/llm/suggestions/variables/priority",
            json={"database": "db", "columns": ["a"], "retry": True},
        )
        self.assertEqual(json.loads(response.data)["moved"], 1)
        args, kwargs = self.service.bump_priority.call_args
        self.assertEqual(args[1:], ("variables", "db", ["a"]))
        self.assertTrue(kwargs["retry"])


class TestValueSuggestionRoutes(unittest.TestCase):
    """Tests for the values-phase endpoints."""

    def setUp(self):
        self.app, self.service, _ = _make_llm_app()
        self.client = self.app.test_client()

    def test_start_delegates(self):
        response = self.client.post("/api/v1/llm/suggestions/values/start", json={})
        self.assertEqual(json.loads(response.data), {"status": "started"})

    def test_snapshot_includes_enabled_flag(self):
        body = json.loads(self.client.get("/api/v1/llm/suggestions/values").data)
        self.assertTrue(body["enabled"])

    def test_priority_requires_database_and_column(self):
        response = self.client.post(
            "/api/v1/llm/suggestions/values/priority", json={"database": "db"}
        )
        self.assertEqual(response.status_code, 400)

    def test_priority_wraps_single_column(self):
        self.client.post(
            "/api/v1/llm/suggestions/values/priority",
            json={"database": "db", "column": "geslacht"},
        )
        args, _ = self.service.bump_priority.call_args
        self.assertEqual(args[1:], ("values", "db", ["geslacht"]))


if __name__ == "__main__":
    unittest.main()
