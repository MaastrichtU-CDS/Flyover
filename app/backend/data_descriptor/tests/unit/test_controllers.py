"""
Unit tests for the Flask controller/route layer.

Tests cover each Blueprint in controllers/:
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


class TestAnnotateControllerStartAnnotationFiltering(unittest.TestCase):
    """Tests for POST /start-annotation with a filtered semantic map.

    The key invariant under test: when a filtered semantic map is submitted the
    session_cache.jsonld_mapping must be restored to its original value after
    the request completes — regardless of whether annotation succeeds or fails.
    This ensures that the Share page (and every other route) always sees the
    full, unfiltered map.
    """

    # A minimal valid JSON-LD map used as the "full" session map.
    _FULL_MAP = _MINIMAL_VALID_JSONLD

    # A filtered subset that removes the only database.
    _FILTERED_MAP = {
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
        "databases": {},  # empty — only selected table removed
    }

    def _make_annotate_app(self, prepare_return=None, execute_return=None):
        """Build a minimal Flask app whose AnnotateService is fully mocked."""
        from unittest.mock import patch, MagicMock

        app = Flask(
            __name__,
            template_folder=str(Path(__file__).parent.parent.parent / "templates"),
        )
        app.secret_key = "test-secret"

        original_mapping = MagicMock()
        mock_session = MagicMock()
        mock_session.jsonld_mapping = original_mapping
        mock_session.annotation_status = {}
        mock_session.repo = "test_repo"

        mock_rdf = MagicMock()
        mock_rdf.get_databases.return_value = ["test_db"]

        app.config["APP_CONTEXT"] = {
            "session_cache": mock_session,
            "rdf_store_service": mock_rdf,
            "rdf_store_url": "http://localhost:7200",
            "upload_folder": "/tmp",
            "formulate_local_map": MagicMock(return_value={}),
            "get_semantic_map": MagicMock(return_value={}),
            "get_table_names": MagicMock(return_value=["test_table"]),
            "has_semantic_map": MagicMock(return_value=True),
            "name_matcher": MagicMock(return_value=True),
        }
        app.register_blueprint(annotate_bp)
        return app, mock_session, original_mapping

    def test_filtered_map_does_not_permanently_replace_session_mapping(self):
        """After a filtered annotation, the original mapping must be restored."""
        from unittest.mock import patch, MagicMock

        app, mock_session, original_mapping = self._make_annotate_app()

        with app.test_client() as client:
            with patch(
                "controllers.annotate_controller.AnnotateService.prepare_annotation_data",
                return_value={},  # no annotation data → early return, still hits finally
            ), patch(
                "controllers.annotate_controller.MappingValidator"
            ) as MockValidator:
                mock_result = MagicMock()
                mock_result.is_valid = True
                MockValidator.return_value.validate.return_value = mock_result

                response = client.post(
                    "/start-annotation",
                    data=json.dumps(self._FILTERED_MAP),
                    content_type="application/json",
                )

        # The request should complete (success=False is fine because there is
        # no annotation data), but crucially the mapping must be restored.
        self.assertIsNotNone(response)
        self.assertIs(
            mock_session.jsonld_mapping,
            original_mapping,
            "session_cache.jsonld_mapping was not restored to the original after "
            "a filtered annotation run — this would break the Share page.",
        )

    def test_no_filtered_map_leaves_session_mapping_unchanged(self):
        """When no filtered map is sent, the session mapping must not be touched."""
        from unittest.mock import patch, MagicMock

        app, mock_session, original_mapping = self._make_annotate_app()

        with app.test_client() as client:
            with patch(
                "controllers.annotate_controller.AnnotateService.prepare_annotation_data",
                return_value={},
            ):
                # POST with no body → no filtered map
                response = client.post("/start-annotation")

        self.assertIs(
            mock_session.jsonld_mapping,
            original_mapping,
            "session_cache.jsonld_mapping should never be altered when no filtered "
            "map is provided.",
        )


if __name__ == "__main__":
    unittest.main()
