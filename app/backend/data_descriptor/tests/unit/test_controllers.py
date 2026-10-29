"""
Unit tests for the Flask controller/route layer.

Tests cover each Blueprint in controllers/:
- ShareController   — generate-mock-data, share_landing, share_mock

Strategy: A minimal Flask app is created for each test class with
APP_CONTEXT populated by MagicMock objects so no real RDF store or
external service is contacted.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add parent (data_descriptor) to path so blueprint imports resolve
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask

from controllers import share_bp

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _make_app(*blueprints):
    """Return a minimal Flask app with the given blueprints and a mock context."""
    app = Flask(
        __name__, template_folder=str(Path(__file__).parent.parent.parent / "templates")
    )
    app.secret_key = "test-secret"
    mock_session = MagicMock()
    mock_session.databases = []
    mock_session.jsonld_mapping = None
    mock_rdf = MagicMock()
    mock_rdf.check_data_exists.return_value = False
    mock_rdf.get_databases.return_value = []
    mock_rdf.get_column_info_by_database.return_value = {}
    app.config["APP_CONTEXT"] = {
        "session_cache": mock_session,
        "rdf_store_service": mock_rdf,
        "upload_folder": "/tmp",
        "formulate_local_map": MagicMock(return_value={}),
        "run_triplifier": MagicMock(return_value=(True, "ok")),
        "upload_func": MagicMock(return_value=(True, [])),
        "start_background": MagicMock(),
        "name_matcher": MagicMock(return_value=True),
    }
    for bp in blueprints:
        app.register_blueprint(bp)
    return app


# ---------------------------------------------------------------------------
# ShareController tests
# ---------------------------------------------------------------------------


class TestShareControllerGenerateMockData(unittest.TestCase):
    """Tests for POST /api/generate-mock-data."""

    def setUp(self):
        self.app = _make_app(share_bp)
        self.client = self.app.test_client()

    def test_no_json_body_returns_400(self):
        """Request with null JSON body returns 400."""
        response = self.client.post(
            "/api/generate-mock-data",
            data=json.dumps(None),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertFalse(body["success"])

    def test_missing_jsonld_map_returns_400(self):
        """Request without jsonld_map key returns 400."""
        response = self.client.post(
            "/api/generate-mock-data",
            data=json.dumps({"num_rows": 5}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertFalse(body["success"])

    def test_valid_request_returns_200(self):
        """Valid request with a JSON-LD map returns 200 with data."""
        payload = {
            "jsonld_map": {
                "schema": {"variables": {}},
                "databases": {},
            },
            "num_rows": 5,
        }
        response = self.client.post(
            "/api/generate-mock-data",
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body["success"])

    def test_service_error_returns_400(self):
        """Client input error (table_id without database_id) returns 400."""
        payload = {
            "jsonld_map": {
                "schema": {"variables": {}},
                "databases": {},
            },
            "num_rows": 5,
            "table_id": "some_table",  # missing database_id → client input error
        }
        response = self.client.post(
            "/api/generate-mock-data",
            data=json.dumps(payload),
            content_type="application/json",
        )
        # table_id without database_id is a client input error → 400
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertFalse(body["success"])


class TestShareControllerShareLanding(unittest.TestCase):
    """Tests for GET /share_landing."""

    def setUp(self):
        self.app = _make_app(share_bp)
        self.client = self.app.test_client()

    def test_share_landing_redirects_to_spa(self):
        """GET /share_landing redirects to the SPA share page."""
        response = self.client.get("/share_landing")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/share")


class TestShareControllerShareMock(unittest.TestCase):
    """Tests for GET /share_mock."""

    def setUp(self):
        self.app = _make_app(share_bp)
        self.client = self.app.test_client()

    def test_share_mock_redirects_to_spa(self):
        """GET /share_mock redirects to the SPA share-mock page."""
        response = self.client.get("/share_mock")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/share/mock")


if __name__ == "__main__":
    unittest.main()
