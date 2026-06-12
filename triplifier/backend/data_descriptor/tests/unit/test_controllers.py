"""
Unit tests for the Flask controller/route layer.

Tests cover each Blueprint in controllers/:
- IngestController  — upload-semantic-map, submit-indexeddb-semantic-map,
                      rdf-store-databases, check-graph-exists, landing
- ShareController   — generate-mock-data, share_landing, share_mock
- DescribeController — describe_landing (service layer fully mocked)
- AnnotateController — annotation_landing (service layer fully mocked)

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

from controllers import annotate_bp, describe_bp, ingest_bp, share_bp

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
# IngestController tests
# ---------------------------------------------------------------------------


class TestIngestControllerUploadSemanticMap(unittest.TestCase):
    """Tests for POST /upload-semantic-map."""

    def setUp(self):
        self.app = _make_app(ingest_bp)
        self.client = self.app.test_client()

    def test_no_file_returns_400(self):
        """Request with no file returns 400."""
        response = self.client.post("/upload-semantic-map")
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertIn("error", body)

    def test_wrong_extension_returns_400(self):
        """Uploading a .json file (not .jsonld) returns 400."""
        from io import BytesIO

        data = {"semanticMapFile": (BytesIO(b'{"key": "val"}'), "mapping.json")}
        response = self.client.post(
            "/upload-semantic-map", data=data, content_type="multipart/form-data"
        )
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertIn("error", body)

    def test_invalid_json_returns_400(self):
        """Uploading a .jsonld file with invalid JSON returns 400."""
        from io import BytesIO

        data = {"semanticMapFile": (BytesIO(b"this is not json"), "mapping.jsonld")}
        response = self.client.post(
            "/upload-semantic-map", data=data, content_type="multipart/form-data"
        )
        self.assertEqual(response.status_code, 400)

    def test_valid_jsonld_returns_200(self):
        """Uploading a valid JSON-LD mapping returns 200 with success."""
        from io import BytesIO

        payload = json.dumps(_MINIMAL_VALID_JSONLD).encode()
        data = {"semanticMapFile": (BytesIO(payload), "mapping.jsonld")}
        response = self.client.post(
            "/upload-semantic-map", data=data, content_type="multipart/form-data"
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body.get("success"))

    def test_invalid_mapping_structure_returns_400(self):
        """Uploading a JSON-LD file that fails schema validation returns 400."""
        from io import BytesIO

        # Valid JSON but missing required fields
        bad_mapping = {"@context": {}, "name": "Incomplete"}
        payload = json.dumps(bad_mapping).encode()
        data = {"semanticMapFile": (BytesIO(payload), "mapping.jsonld")}
        response = self.client.post(
            "/upload-semantic-map", data=data, content_type="multipart/form-data"
        )
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertIn("validation_errors", body)


class TestIngestControllerSubmitIndexedDB(unittest.TestCase):
    """Tests for POST /submit-indexeddb-semantic-map."""

    def setUp(self):
        self.app = _make_app(ingest_bp)
        self.client = self.app.test_client()

    def test_no_json_body_returns_400(self):
        """Request with no JSON body returns 400."""
        response = self.client.post(
            "/submit-indexeddb-semantic-map", content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_invalid_mapping_returns_400(self):
        """Posting a mapping that fails validation returns 400."""
        response = self.client.post(
            "/submit-indexeddb-semantic-map",
            data=json.dumps({"name": "incomplete"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        body = json.loads(response.data)
        self.assertIn("error", body)

    def test_valid_mapping_returns_200_with_redirect_url(self):
        """Posting a valid mapping returns 200 with redirect_url."""
        response = self.client.post(
            "/submit-indexeddb-semantic-map",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body.get("success"))
        self.assertIn("redirect_url", body)


class TestIngestControllerValidateMapping(unittest.TestCase):
    """Tests for POST /api/v1/validate-mapping (pure validation + CSV cross-check)."""

    def setUp(self):
        self.app = _make_app(ingest_bp)
        self.client = self.app.test_client()

    def test_no_body_returns_400(self):
        response = self.client.post(
            "/api/v1/validate-mapping", content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_valid_mapping_no_csv_returns_valid(self):
        """A schema-valid mapping returns valid=True when no CSV is loaded."""
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body["valid"])
        self.assertEqual(body["validation_errors"], [])
        self.assertFalse(body["csv_checked"])

    def test_schema_invalid_mapping_returns_invalid(self):
        """A mapping missing required fields returns valid=False with errors."""
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps({"name": "incomplete"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertFalse(body["valid"])
        self.assertTrue(len(body["validation_errors"]) > 0)

    def test_valid_mapping_with_csv_columns_present_returns_valid(self):
        """Schema-valid mapping whose localColumns exist in the CSV returns valid."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].get_column_info_by_database.return_value = {
            "test_db": ["id", "other_col"]
        }
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body["valid"])
        self.assertTrue(body["csv_checked"])

    def test_valid_mapping_with_orphan_column_returns_invalid(self):
        """A localColumn not in the loaded CSV produces an orphan error."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].get_column_info_by_database.return_value = {
            "test_db": ["other_col"]  # no "id"
        }
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertFalse(body["valid"])
        self.assertTrue(body["csv_checked"])
        orphan = next(
            (e for e in body["validation_errors"] if e.get("value") == "id"), None
        )
        self.assertIsNotNone(orphan)
        self.assertEqual(orphan["severity"], "error")
        self.assertIn("not present", orphan["message"].lower())

    def test_wrapped_mapping_payload_accepted(self):
        """Accepts {"mapping": {...}} as well as the bare mapping object."""
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps({"mapping": _MINIMAL_VALID_JSONLD}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body["valid"])

    def test_does_not_mutate_session_cache(self):
        """The endpoint must not touch session_cache.jsonld_mapping."""
        ctx = self.app.config["APP_CONTEXT"]
        sentinel = object()
        ctx["session_cache"].jsonld_mapping = sentinel
        self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertIs(ctx["session_cache"].jsonld_mapping, sentinel)


class TestIngestControllerRdfStoreDatabases(unittest.TestCase):
    """Tests for GET /api/rdf-store-databases."""

    def setUp(self):
        self.app = _make_app(ingest_bp)
        self.client = self.app.test_client()

    def test_returns_200_with_empty_list_when_no_databases(self):
        """Returns 200 with empty databases list when RDF store has none."""
        response = self.client.get("/api/rdf-store-databases")
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertIn("databases", body)

    def test_returns_databases_when_present(self):
        """Returns list of database names when RDF store has data."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].get_databases.return_value = ["db_a", "db_b"]
        response = self.client.get("/api/rdf-store-databases")
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertIn("db_a", body["databases"])

    def test_returns_500_on_service_exception(self):
        """Returns 500 when RDF store service raises an exception."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].get_databases.side_effect = RuntimeError("conn failed")
        response = self.client.get("/api/rdf-store-databases")
        self.assertEqual(response.status_code, 500)


class TestIngestControllerCheckGraphExists(unittest.TestCase):
    """Tests for GET /api/check-graph-exists."""

    def setUp(self):
        self.app = _make_app(ingest_bp)
        self.client = self.app.test_client()

    def test_returns_exists_false_when_no_data(self):
        """Returns {'exists': False} when no graph data is present."""
        response = self.client.get("/api/check-graph-exists")
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertFalse(body["exists"])

    def test_returns_exists_true_when_data_present(self):
        """Returns {'exists': True} when graph data exists."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].check_data_exists.return_value = True
        response = self.client.get("/api/check-graph-exists")
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body["exists"])


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


# ---------------------------------------------------------------------------
# DescribeController tests
# ---------------------------------------------------------------------------


class TestDescribeControllerLanding(unittest.TestCase):
    """Tests for GET /describe_landing."""

    def setUp(self):
        self.app = _make_app(describe_bp)
        self.client = self.app.test_client()

    def test_describe_landing_redirects_to_spa(self):
        """GET /describe_landing redirects to the SPA describe page."""
        response = self.client.get("/describe_landing")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/describe")

    def test_describe_landing_redirect_is_independent_of_data_state(self):
        """Redirect happens regardless of whether RDF data already exists."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].check_data_exists.return_value = True
        response = self.client.get("/describe_landing")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/describe")


# ---------------------------------------------------------------------------
# AnnotateController tests
# ---------------------------------------------------------------------------


class TestAnnotateControllerLanding(unittest.TestCase):
    """Tests for GET /annotation_landing."""

    def setUp(self):
        self.app = _make_app(annotate_bp, ingest_bp)
        self.client = self.app.test_client()

    def test_annotation_landing_redirects_to_spa(self):
        """GET /annotation_landing redirects to the SPA annotate page."""
        response = self.client.get("/annotation_landing")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/annotate")

    def test_annotation_landing_redirect_is_independent_of_data_state(self):
        """Redirect happens regardless of whether RDF data already exists."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].check_data_exists.return_value = True
        response = self.client.get("/annotation_landing")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/annotate")


if __name__ == "__main__":
    unittest.main()
