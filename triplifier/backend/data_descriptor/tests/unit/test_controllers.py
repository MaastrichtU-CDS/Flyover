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
        # Set loaded databases in session cache - use the database name from the mapping
        ctx["session_cache"].databases = ["test_db"]
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        self.assertTrue(body["valid"])
        self.assertTrue(body["csv_checked"])
        self.assertEqual(body["validation_warnings"], [])
        # databases_checked contains the database name from the mapping, not the key
        self.assertIn("Test Database", body["databases_checked"])

    def test_valid_mapping_with_orphan_column_returns_valid_with_warning(self):
        """A localColumn not in the loaded CSV produces a warning, not an error."""
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].get_column_info_by_database.return_value = {
            "test_db": ["other_col"]  # no "id"
        }
        # Set loaded databases in session cache
        ctx["session_cache"].databases = ["test_db"]
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        # Should be valid (schema is valid), but have warnings for column mismatch
        self.assertTrue(body["valid"])
        self.assertTrue(body["csv_checked"])
        # Check that the orphan column is now a warning, not an error
        orphan_warning = next(
            (w for w in body["validation_warnings"] if "id" in w.get("values", [])),
            None,
        )
        self.assertIsNotNone(orphan_warning)
        self.assertEqual(orphan_warning["severity"], "warning")
        # Should have no schema errors
        self.assertEqual(body["validation_errors"], [])
        # databases_checked contains the database name from the mapping, not the key
        self.assertIn("Test Database", body["databases_checked"])

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

    def test_unloaded_database_skipped_silently(self):
        """Databases not in loaded list are skipped silently (no errors, no warnings)."""
        ctx = self.app.config["APP_CONTEXT"]
        # Data exists for test_db in RDF store
        ctx["rdf_store_service"].get_column_info_by_database.return_value = {
            "test_db": ["id", "other_col"]
        }
        # But a different database is loaded in session cache (not test_db)
        ctx["session_cache"].databases = ["different_db"]
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(_MINIMAL_VALID_JSONLD),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        # Should be valid (schema is valid, test_db is not loaded so not checked)
        self.assertTrue(body["valid"])
        self.assertTrue(body["csv_checked"])
        # No warnings because the database is not loaded
        self.assertEqual(body["validation_warnings"], [])
        self.assertEqual(body["databases_checked"], [])

    def test_mixed_loaded_unloaded_databases(self):
        """Test with mapping containing both loaded and unloaded databases."""
        # Create a mapping with multiple databases
        mixed_mapping = {
            "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
            "@type": "mapping:DataMapping",
            "name": "Mixed Test Mapping",
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
                "loaded_db": {
                    "@type": "mapping:Database",
                    "name": "Loaded Database",
                    "tables": {
                        "test_table": {
                            "@type": "mapping:Table",
                            "columns": {
                                "identifier": {
                                    "mapsTo": "schema:variable/identifier",
                                    "localColumn": "missing_col",
                                }
                            },
                        }
                    },
                },
                "unloaded_db": {
                    "@type": "mapping:Database",
                    "name": "Unloaded Database",
                    "tables": {
                        "test_table": {
                            "@type": "mapping:Table",
                            "columns": {
                                "identifier": {
                                    "mapsTo": "schema:variable/identifier",
                                    "localColumn": "missing_col",
                                }
                            },
                        }
                    },
                },
            },
        }
        ctx = self.app.config["APP_CONTEXT"]
        ctx["rdf_store_service"].get_column_info_by_database.return_value = {
            "loaded_db": ["id", "other_col"],
            "unloaded_db": ["id", "other_col"],
        }
        # Only loaded_db is loaded in session
        ctx["session_cache"].databases = ["loaded_db"]
        response = self.client.post(
            "/api/v1/validate-mapping",
            data=json.dumps(mixed_mapping),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.data)
        # Should be valid (schema is valid)
        self.assertTrue(body["valid"])
        # Should have warning for loaded_db's missing column
        self.assertEqual(len(body["validation_warnings"]), 1)
        self.assertIn("missing_col", body["validation_warnings"][0]["values"])
        self.assertEqual(body["validation_warnings"][0]["severity"], "warning")
        # Should have checked loaded_db but not unloaded_db
        # databases_checked contains the database name from the mapping
        self.assertIn("Loaded Database", body["databases_checked"])
        self.assertNotIn("Unloaded Database", body["databases_checked"])

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
