"""
API tests for the ingest routes (``controllers/ingest_controller.py``).

The ingest service, the triplifier run, the graph upload and the background
jobs are doubles; what is under test is how each route reads the request,
which service answer leads to which response, and what lands in the
session. Mapping uploads run the real ``MappingValidator`` and orphan-column
clean-up, because those decide the HTTP answer before any service is called.
"""

import io
import json
from urllib.parse import parse_qs, urlsplit

import pytest

from tests.api.support import PARSED_FRAME, TABLE, valid_mapping
from loaders import JSONLDMapping

JSON = "application/json"


def _jsonld_upload(content, filename="mapping.jsonld"):
    """A multipart file part; dicts are serialised as JSON."""
    if isinstance(content, dict) or isinstance(content, list):
        content = json.dumps(content).encode("utf-8")
    elif isinstance(content, str):
        content = content.encode("utf-8")
    return (io.BytesIO(content), filename)


def _error_from_redirect(response) -> str:
    """The error message an /ingest redirect carries in its query string."""
    location = urlsplit(response.headers["Location"])
    assert location.path == "/ingest"
    return parse_qs(location.query)["error"][0]


@pytest.fixture
def storeless_client(make_app):
    """An app with no store service and no store URL to build one from.

    The ingest controller builds a store client from the app config when the
    context has none, so only without both does it take its no-store path.
    """
    app = make_app(rdf_store_service=None)
    del app.config["rdf_store_url"]
    return app.test_client()


def _mapping_without_column(column: str) -> dict:
    mapping = valid_mapping()
    columns = mapping["databases"]["centre_x"]["tables"][TABLE]["columns"]
    del columns[column]
    return mapping


# ---------------------------------------------------------------------------
# POST /upload-semantic-map
# ---------------------------------------------------------------------------


class TestUploadSemanticMap:
    @pytest.mark.route("POST", "/upload-semantic-map", "success")
    def test_a_valid_map_is_adopted_and_echoed_back(self, client, session_cache):
        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.status_code == 200
        assert response.mimetype == JSON
        body = response.get_json()
        assert body["success"] is True
        assert body["message"] == "JSON-LD semantic mapping uploaded successfully"
        assert body["cleaned_mapping"] == valid_mapping()
        assert body["statistics"]["databases"] == 1
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)
        assert session_cache.jsonld_mapping.name == "Shared fixture mapping"

    def test_the_extension_check_is_the_ingest_services(self, client, ingest_service):
        client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping(), "Map.JSONLD")},
        )

        ingest_service.allowed_file.assert_called_once_with("Map.JSONLD", {"jsonld"})

    @pytest.mark.route("POST", "/upload-semantic-map", "failure")
    def test_a_request_without_a_file_part_is_refused(self, client, session_cache):
        response = client.post("/upload-semantic-map", data={})

        assert response.status_code == 400
        assert response.get_json() == {"error": "No semantic map file provided"}
        assert session_cache.jsonld_mapping is None

    @pytest.mark.route("POST", "/upload-semantic-map", "failure")
    def test_a_file_part_without_a_filename_is_refused(self, client):
        response = client.post(
            "/upload-semantic-map", data={"semanticMapFile": _jsonld_upload({}, "")}
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "No semantic map file provided"}

    @pytest.mark.route("POST", "/upload-semantic-map", "failure")
    def test_a_json_body_instead_of_a_file_is_refused(self, client):
        response = client.post("/upload-semantic-map", json=valid_mapping())

        assert response.status_code == 400
        assert response.get_json() == {"error": "No semantic map file provided"}

    @pytest.mark.route("POST", "/upload-semantic-map", "failure")
    def test_a_file_the_service_does_not_allow_is_refused(
        self, client, ingest_service, session_cache
    ):
        ingest_service.allowed_file.return_value = False

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping(), "map.json")},
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "Please upload a valid .jsonld file"}
        assert session_cache.jsonld_mapping is None

    @pytest.mark.route("POST", "/upload-semantic-map", "failure")
    def test_a_syntax_error_is_located_by_line_and_column(self, client):
        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload('{\n  "name": "x",\n  oops\n}')},
        )

        assert response.status_code == 400
        body = response.get_json()
        assert body["error"].startswith("Invalid JSON-LD syntax at line 3, column 3")
        (issue,) = body["validation_errors"]
        assert issue["path"] == "(line 3, column 3)"
        assert issue["severity"] == "error"
        assert issue["message"].startswith("JSON-LD syntax error: ")
        assert issue["suggestion"] == "Check for missing commas, brackets, or quotes"

    @pytest.mark.parametrize(
        "content",
        [b"", b"   \n", b"\xef\xbb\xbf" + json.dumps(valid_mapping()).encode()],
        ids=["empty", "whitespace", "utf8-bom"],
    )
    def test_empty_files_and_byte_order_marks_are_syntax_errors_at_the_start(
        self, client, content
    ):
        # The loader reads JSON-LD as plain UTF-8 too (see test_loaders.py),
        # so a BOM written by e.g. Notepad is refused the same way here.
        response = client.post(
            "/upload-semantic-map", data={"semanticMapFile": _jsonld_upload(content)}
        )

        assert response.status_code == 400
        assert response.get_json()["validation_errors"][0]["path"] in (
            "(line 1, column 1)",
            "(line 2, column 1)",
        )

    def test_bytes_that_are_not_utf8_are_a_readable_client_error(self, client):
        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(b'{"name": "caf\xe9"}')},
        )

        assert response.status_code == 400
        assert response.get_json()["error"].startswith("Unexpected error: 'utf-8'")

    @pytest.mark.route("POST", "/upload-semantic-map", "failure")
    def test_a_map_that_fails_validation_is_refused_with_the_issues(
        self, client, session_cache
    ):
        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload({"@context": {}, "name": "x"})},
        )

        assert response.status_code == 400
        body = response.get_json()
        assert body["error"] == "Mapping file validation failed"
        assert body["validation_errors"]
        assert all(issue["severity"] == "error" for issue in body["validation_errors"])
        assert session_cache.jsonld_mapping is None

    @pytest.mark.parametrize("document", [[], [1, 2], "a string", 42, None, True])
    def test_a_document_that_is_not_an_object_is_refused(self, client, document):
        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(json.dumps(document))},
        )

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_only_the_first_of_several_files_is_read(self, client, session_cache):
        other = valid_mapping()
        other["name"] = "Second file"

        response = client.post(
            "/upload-semantic-map",
            data={
                "semanticMapFile": [
                    _jsonld_upload(valid_mapping(), "first.jsonld"),
                    _jsonld_upload(other, "second.jsonld"),
                ]
            },
        )

        assert response.status_code == 200
        assert session_cache.jsonld_mapping.name == "Shared fixture mapping"

    def test_a_newer_upload_replaces_the_adopted_map(self, client, session_cache):
        newer = valid_mapping()
        newer["name"] = "Newer map"

        client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )
        client.post(
            "/upload-semantic-map", data={"semanticMapFile": _jsonld_upload(newer)}
        )

        assert session_cache.jsonld_mapping.name == "Newer map"


class TestUploadSemanticMapOrphanColumns:
    """Columns the store does not hold are removed before validation."""

    def test_a_column_missing_from_a_loaded_table_is_dropped(
        self, client, store, session_cache
    ):
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex"]}

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.status_code == 200
        cleaned = response.get_json()["cleaned_mapping"]
        columns = cleaned["databases"]["centre_x"]["tables"][TABLE]["columns"]
        assert set(columns) == {"identifier", "biological_sex"}
        assert session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") is None

    def test_tables_and_databases_left_empty_are_dropped_with_them(
        self, client, store, session_cache
    ):
        mapping = valid_mapping()
        mapping["databases"]["other_db"] = {
            "@id": "mapping:database/other_db",
            "@type": "mapping:Database",
            "name": "other",
            "tables": {
                "other": {
                    "@id": "mapping:table/other",
                    "@type": "mapping:Table",
                    "sourceFile": "other",
                    "columns": {
                        "identifier": {
                            "mapsTo": "schema:variable/identifier",
                            "localColumn": "not_there",
                        }
                    },
                }
            },
        }
        session_cache.databases = [TABLE, "other"]
        store.get_column_info_by_database.return_value = {
            TABLE: ["id", "sex", "age"],
            "other": ["something_else"],
        }

        response = client.post(
            "/upload-semantic-map", data={"semanticMapFile": _jsonld_upload(mapping)}
        )

        assert response.status_code == 200
        assert set(response.get_json()["cleaned_mapping"]["databases"]) == {"centre_x"}

    def test_databases_the_store_does_not_hold_are_left_alone(
        self, client, store, session_cache
    ):
        session_cache.databases = ["another_table"]
        store.get_column_info_by_database.return_value = {"another_table": ["x"]}

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.get_json()["cleaned_mapping"] == valid_mapping()

    def test_without_a_session_database_list_the_store_columns_decide(
        self, client, store, session_cache
    ):
        session_cache.databases = None
        store.get_column_info_by_database.return_value = {TABLE: ["id"]}

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        columns = response.get_json()["cleaned_mapping"]["databases"]["centre_x"][
            "tables"
        ][TABLE]["columns"]
        assert set(columns) == {"identifier"}

    def test_a_session_without_a_database_list_asks_the_store(
        self, client, store, session_cache
    ):
        del session_cache.databases
        store.get_databases.return_value = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.status_code == 200
        assert session_cache.databases == [TABLE]

    def test_a_store_failure_listing_databases_skips_only_the_listing(
        self, client, store, session_cache
    ):
        del session_cache.databases
        store.get_databases.side_effect = ConnectionError("store down")
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.status_code == 200

    def test_a_store_failure_skips_the_clean_up_but_not_the_upload(
        self, client, store, session_cache
    ):
        store.get_column_info_by_database.side_effect = ConnectionError("store down")

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.status_code == 200
        assert response.get_json()["cleaned_mapping"] == valid_mapping()
        assert session_cache.jsonld_mapping is not None

    def test_without_a_store_service_there_is_no_clean_up(self, storeless_client):
        response = storeless_client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": _jsonld_upload(valid_mapping())},
        )

        assert response.status_code == 200
        assert response.get_json()["cleaned_mapping"] == valid_mapping()


def _with_database(database, key="centre_z"):
    """The valid mapping plus one more (possibly malformed) database entry."""
    mapping = valid_mapping()
    mapping["databases"][key] = database
    return mapping


def _column_entries(columns):
    """A loaded database (named like the table) holding the given column entries."""
    return {
        "@id": "mapping:database/patients_2",
        "@type": "mapping:Database",
        "name": TABLE,
        "tables": {
            "t": {
                "@id": "mapping:table/t",
                "@type": "mapping:Table",
                "sourceFile": TABLE,
                "columns": columns,
            }
        },
    }


class TestOrphanCheckOnMalformedShapes:
    """The orphan check walks whatever the upload contains without crashing.

    Each shape below is malformed in a way the walk has to step over; the
    request still gets a deliberate answer (200 with the store-backed check,
    or the validator's 400), never a server error from the walk itself.
    """

    @pytest.fixture(autouse=True)
    def _loaded_table(self, store, session_cache):
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}

    @pytest.mark.parametrize(
        "mapping",
        [
            _with_database({}, key="broken"),
            _with_database("text", key="x"),
            _with_database({"name": "", "tables": {}}, key="unnamed"),
            _with_database({"name": TABLE, "tables": {}}),
            _with_database({"name": TABLE, "tables": {"t": "not a table"}}),
            _with_database({"name": TABLE, "tables": {"t": {"columns": "text"}}}),
            _with_database(_column_entries({"c": "not a column"})),
            _with_database(_column_entries({"c": {"mapsTo": "schema:variable/age"}})),
        ],
        ids=[
            "empty-database",
            "database-is-text",
            "database-without-a-name",
            "loaded-database-without-tables",
            "table-is-text",
            "columns-are-text",
            "column-is-text",
            "column-without-local-column",
        ],
    )
    def test_entries_the_walk_cannot_read_are_skipped(self, client, mapping):
        response = client.post("/api/v1/validate-mapping", json=mapping)

        assert response.status_code == 200
        assert response.get_json()["csv_checked"] is True
        assert response.get_json()["validation_warnings"] == []

    def test_a_table_without_a_source_file_is_matched_on_the_database(self, client):
        mapping = valid_mapping()
        del mapping["databases"]["centre_x"]["tables"][TABLE]["sourceFile"]

        body = client.post("/api/v1/validate-mapping", json=mapping).get_json()

        assert body["databases_checked"] == [TABLE]
        assert body["validation_warnings"] == []

    def test_unloaded_databases_are_skipped_when_only_the_store_decides(
        self, client, store, session_cache
    ):
        session_cache.databases = None
        store.get_column_info_by_database.return_value = {"elsewhere": ["id"]}

        body = client.post("/api/v1/validate-mapping", json=valid_mapping()).get_json()

        assert body["csv_checked"] is True
        assert body["databases_checked"] == []

    @pytest.mark.parametrize(
        "section, value",
        [("databases", []), ("databases", "text"), ("tables", []), ("tables", "t")],
    )
    def test_list_or_text_sections_are_refused_by_the_upload(
        self, client, section, value
    ):
        mapping = valid_mapping()
        if section == "databases":
            mapping["databases"] = value
        else:
            mapping["databases"]["centre_x"]["tables"] = value

        response = client.post(
            "/upload-semantic-map", data={"semanticMapFile": _jsonld_upload(mapping)}
        )

        assert response.status_code == 400
        assert "error" in response.get_json()

    @pytest.mark.parametrize("value", [[], "text", ["centre_x"], 7])
    def test_list_or_text_sections_are_reported_by_the_validation_endpoint(
        self, client, value
    ):
        mapping = valid_mapping()
        mapping["databases"] = value

        response = client.post("/api/v1/validate-mapping", json=mapping)

        assert response.status_code == 200
        assert response.get_json()["valid"] is False


# ---------------------------------------------------------------------------
# POST /submit-indexeddb-semantic-map
# ---------------------------------------------------------------------------


class TestSubmitIndexedDbSemanticMap:
    @pytest.mark.route("POST", "/submit-indexeddb-semantic-map", "success")
    def test_a_valid_map_is_adopted_and_points_to_the_review(
        self, client, session_cache
    ):
        response = client.post("/submit-indexeddb-semantic-map", json=valid_mapping())

        assert response.status_code == 200
        body = response.get_json()
        assert body["success"] is True
        assert body["message"] == "Semantic map loaded from browser storage"
        assert body["redirect_url"] == "/annotate/review"
        assert body["cleaned_mapping"] == valid_mapping()
        assert body["statistics"]["variables"] == 3
        assert session_cache.jsonld_mapping.name == "Shared fixture mapping"

    @pytest.mark.route("POST", "/submit-indexeddb-semantic-map", "failure")
    @pytest.mark.parametrize("body", [{}, None, [], 0, False, ""])
    def test_an_empty_body_is_refused(self, client, session_cache, body):
        response = client.post(
            "/submit-indexeddb-semantic-map",
            data=json.dumps(body),
            content_type=JSON,
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "No semantic map data provided"}
        assert session_cache.jsonld_mapping is None

    @pytest.mark.route("POST", "/submit-indexeddb-semantic-map", "failure")
    def test_a_map_that_fails_validation_is_refused_with_the_issues(
        self, client, session_cache
    ):
        response = client.post("/submit-indexeddb-semantic-map", json={"name": "x"})

        assert response.status_code == 400
        body = response.get_json()
        assert body["error"] == "Semantic map validation failed"
        assert body["validation_errors"]
        assert session_cache.jsonld_mapping is None

    @pytest.mark.parametrize("data", ["", "{not json"], ids=["no-body", "malformed"])
    def test_a_body_that_does_not_parse_is_a_client_error(self, client, data):
        response = client.post(
            "/submit-indexeddb-semantic-map", data=data, content_type=JSON
        )

        assert response.status_code == 400
        assert response.get_json()["error"].startswith(
            "Unexpected error: 400 Bad Request"
        )

    def test_a_body_that_is_not_json_is_a_client_error(self, client):
        response = client.post(
            "/submit-indexeddb-semantic-map",
            data=json.dumps(valid_mapping()),
            content_type="text/plain",
        )

        assert response.status_code == 400
        assert "415 Unsupported Media Type" in response.get_json()["error"]

    @pytest.mark.parametrize("body", [[1, 2], "a string", 42])
    def test_a_body_that_is_not_an_object_is_a_client_error(self, client, body):
        response = client.post("/submit-indexeddb-semantic-map", json=body)

        assert response.status_code == 400
        assert "error" in response.get_json()

    def test_the_browser_map_replaces_the_adopted_one(self, client, session_cache):
        newer = valid_mapping()
        newer["name"] = "From the browser"
        client.post("/submit-indexeddb-semantic-map", json=valid_mapping())

        client.post("/submit-indexeddb-semantic-map", json=newer)

        assert session_cache.jsonld_mapping.name == "From the browser"


# ---------------------------------------------------------------------------
# POST /api/v1/validate-mapping
# ---------------------------------------------------------------------------


class TestValidateMapping:
    @pytest.mark.route("POST", "/api/v1/validate-mapping", "success")
    def test_a_valid_map_without_store_data_is_valid_and_unchecked(self, client):
        response = client.post("/api/v1/validate-mapping", json=valid_mapping())

        assert response.status_code == 200
        assert response.get_json() == {
            "valid": True,
            "validation_errors": [],
            "validation_warnings": [],
            "statistics": response.get_json()["statistics"],
            "csv_checked": False,
            "databases_checked": [],
            "cleaned_mapping": None,
        }

    def test_a_map_wrapped_in_a_mapping_key_is_accepted(self, client):
        response = client.post(
            "/api/v1/validate-mapping", json={"mapping": valid_mapping()}
        )

        assert response.get_json()["valid"] is True

    def test_a_mapping_key_that_is_not_an_object_is_validated_as_is(self, client):
        response = client.post(
            "/api/v1/validate-mapping", json={"mapping": "not an object"}
        )

        assert response.status_code == 200
        assert response.get_json()["valid"] is False

    @pytest.mark.route("POST", "/api/v1/validate-mapping", "failure")
    @pytest.mark.parametrize(
        "data, content_type",
        [
            ("", JSON),
            ("null", JSON),
            ("[]", JSON),
            ("[1, 2]", JSON),
            ('"a string"', JSON),
            ("42", JSON),
            ("{}", JSON),
            ("{broken", JSON),
            (json.dumps(valid_mapping()), "text/plain"),
        ],
        ids=[
            "no-body",
            "null",
            "empty-list",
            "list",
            "string",
            "number",
            "empty-object",
            "malformed",
            "not-json-content-type",
        ],
    )
    def test_anything_but_a_mapping_object_is_refused(self, client, data, content_type):
        response = client.post(
            "/api/v1/validate-mapping", data=data, content_type=content_type
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "No mapping data provided"}

    def test_schema_errors_are_reported_with_200_and_valid_false(self, client):
        response = client.post("/api/v1/validate-mapping", json={"name": "x"})

        assert response.status_code == 200
        body = response.get_json()
        assert body["valid"] is False
        assert body["validation_errors"]

    def test_columns_the_store_holds_raise_no_warning(
        self, client, store, session_cache
    ):
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}

        body = client.post("/api/v1/validate-mapping", json=valid_mapping()).get_json()

        assert body["valid"] is True
        assert body["csv_checked"] is True
        assert body["validation_warnings"] == []
        assert body["databases_checked"] == [TABLE]
        assert body["cleaned_mapping"] is None

    def test_a_missing_column_is_a_warning_with_a_cleaned_copy(
        self, client, store, session_cache
    ):
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex"]}

        body = client.post("/api/v1/validate-mapping", json=valid_mapping()).get_json()

        assert body["valid"] is True
        (warning,) = body["validation_warnings"]
        assert warning["severity"] == "warning"
        assert warning["values"] == ["age"]
        assert f"({TABLE}: age)" in warning["message"]
        assert body["cleaned_mapping"] == _mapping_without_column("age_at_diagnosis")

    def test_unloaded_databases_are_skipped_silently(
        self, client, store, session_cache
    ):
        session_cache.databases = ["different_db"]
        store.get_column_info_by_database.return_value = {TABLE: ["id"]}

        body = client.post("/api/v1/validate-mapping", json=valid_mapping()).get_json()

        assert body["csv_checked"] is True
        assert body["validation_warnings"] == []
        assert body["databases_checked"] == []

    def test_only_the_loaded_one_of_two_databases_is_checked(
        self, client, store, session_cache
    ):
        mapping = valid_mapping()
        unloaded = json.loads(json.dumps(mapping["databases"]["centre_x"]))
        unloaded["name"] = "unloaded"
        unloaded["tables"][TABLE]["sourceFile"] = "unloaded"
        mapping["databases"]["centre_y"] = unloaded
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {
            TABLE: ["id"],
            "unloaded": ["id"],
        }

        body = client.post("/api/v1/validate-mapping", json=mapping).get_json()

        assert body["databases_checked"] == [TABLE]
        (warning,) = body["validation_warnings"]
        # Listed in request-body order; the test client sorts JSON keys.
        assert sorted(warning["values"]) == ["age", "sex"]

    def test_a_store_failure_skips_the_cross_check_only(self, client, store):
        store.get_column_info_by_database.side_effect = ConnectionError("store down")

        response = client.post("/api/v1/validate-mapping", json=valid_mapping())

        assert response.status_code == 200
        assert response.get_json()["valid"] is True
        assert response.get_json()["csv_checked"] is False

    def test_a_session_without_a_database_list_asks_the_store(
        self, client, store, session_cache
    ):
        del session_cache.databases
        store.get_databases.return_value = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}

        body = client.post("/api/v1/validate-mapping", json=valid_mapping()).get_json()

        assert body["databases_checked"] == [TABLE]
        assert session_cache.databases == [TABLE]

    def test_a_store_failure_listing_databases_falls_back_to_the_store_columns(
        self, client, store, session_cache
    ):
        del session_cache.databases
        store.get_databases.side_effect = ConnectionError("store down")
        store.get_column_info_by_database.return_value = {TABLE: ["id"]}

        body = client.post("/api/v1/validate-mapping", json=valid_mapping()).get_json()

        assert body["databases_checked"] == [TABLE]
        assert sorted(body["validation_warnings"][0]["values"]) == ["age", "sex"]

    def test_the_session_map_is_never_touched(self, client, session_cache, store):
        sentinel = object()
        session_cache.jsonld_mapping = sentinel
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id"]}

        client.post("/api/v1/validate-mapping", json=valid_mapping())

        assert session_cache.jsonld_mapping is sentinel

    def test_without_a_store_service_nothing_is_cross_checked(self, storeless_client):
        response = storeless_client.post(
            "/api/v1/validate-mapping", json=valid_mapping()
        )

        body = response.get_json()

        assert body["valid"] is True
        assert body["csv_checked"] is False


# ---------------------------------------------------------------------------
# POST /upload
# ---------------------------------------------------------------------------


def _csv_form(*names, file_type="CSV", **fields):
    form = {"fileType": file_type, **fields}
    form["csvFile"] = [(io.BytesIO(b"id,sex\nP1,M\n"), name) for name in names]
    return form


class TestUploadCsv:
    @pytest.mark.route("POST", "/upload", "success")
    def test_a_csv_is_parsed_triplified_uploaded_and_linked(
        self, client, session_cache, ingest_service, helpers
    ):
        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert response.status_code == 302
        assert response.headers["Location"] == "/describe"
        (files,), _ = ingest_service.validate_csv_files.call_args
        assert [f.filename for f in files] == ["patients.csv"]
        ingest_service.parse_csv_files.assert_called_once()
        assert session_cache.csvData == [PARSED_FRAME]
        assert session_cache.csvTableNames == [TABLE]
        helpers["run_triplifier"].assert_called_once_with("triplifierCSV.properties")
        assert session_cache.StatusToDisplay == "Triplification completed"
        helpers["upload_func"].assert_called_once_with(
            "CSV", session_cache.output_files
        )
        helpers["start_background"].assert_called_once_with(session_cache)

    def test_the_separator_and_decimal_sign_reach_the_parser(
        self, client, ingest_service
    ):
        client.post(
            "/upload",
            data=_csv_form(
                "a.csv", "b.csv", csv_separator_sign=";", csv_decimal_sign=","
            ),
        )

        (files, separator, decimal), _ = ingest_service.parse_csv_files.call_args
        assert [f.filename for f in files] == ["a.csv", "b.csv"]
        assert (separator, decimal) == (";", ",")

    def test_the_parser_defaults_to_comma_and_full_stop(self, client, ingest_service):
        client.post("/upload", data=_csv_form("a.csv"))

        _, separator, decimal = ingest_service.parse_csv_files.call_args.args
        assert (separator, decimal) == (",", ".")

    @pytest.mark.route("POST", "/upload", "failure")
    def test_files_the_service_refuses_go_back_to_ingest_with_its_message(
        self, client, ingest_service, helpers, session_cache
    ):
        ingest_service.validate_csv_files.return_value = (False, "Upload a .csv file.")

        response = client.post("/upload", data=_csv_form("data.txt"))

        assert response.status_code == 302
        assert _error_from_redirect(response) == "Upload a .csv file."
        ingest_service.parse_csv_files.assert_not_called()
        helpers["run_triplifier"].assert_not_called()
        assert session_cache.csvData is None

    @pytest.mark.route("POST", "/upload", "failure")
    def test_a_parse_error_goes_back_to_ingest_with_its_message(
        self, client, ingest_service, helpers
    ):
        ingest_service.parse_csv_files.return_value = ([], [], "Error parsing CSV")

        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert _error_from_redirect(response) == "Error parsing CSV"
        helpers["run_triplifier"].assert_not_called()

    @pytest.mark.route("POST", "/upload", "failure")
    def test_a_triplifier_failure_uploads_nothing(self, client, helpers, session_cache):
        helpers["run_triplifier"].return_value = (False, "Java heap space")

        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert _error_from_redirect(response) == "Error: Java heap space"
        helpers["upload_func"].assert_not_called()
        helpers["start_background"].assert_not_called()
        assert session_cache.StatusToDisplay is None

    def test_an_exception_in_the_triplifier_is_a_server_error(self, client, helpers):
        helpers["run_triplifier"].side_effect = RuntimeError("triplifier crashed")

        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert response.status_code == 500

    def test_a_failed_graph_upload_goes_back_to_ingest(self, client, helpers):
        helpers["upload_func"].return_value = (
            False,
            ["HTTP 503 from the store", "data graph not loaded"],
        )

        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert _error_from_redirect(response) == (
            "Error: the RDF store did not accept the triplified data: "
            "HTTP 503 from the store; data graph not loaded"
        )
        helpers["start_background"].assert_not_called()

    def test_a_failed_upload_without_messages_still_says_what_failed(
        self, client, helpers
    ):
        helpers["upload_func"].return_value = (False, [])

        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert _error_from_redirect(response) == (
            "Error: the RDF store did not accept the triplified data"
        )


class TestUploadRedirectMessages:
    """The /ingest?error= redirect carries any message safely."""

    @pytest.mark.parametrize(
        "message",
        [
            "line one\nline two",
            "carriage\r\nreturn",
            "Set-Cookie injection\r\nSet-Cookie: session=evil",
            "ümlauts, ünïcödé and 漢字",
            "symbols & = ? # % + /",
            "  padded  ",
        ],
    )
    def test_the_message_survives_without_breaking_the_header(
        self, client, ingest_service, message
    ):
        ingest_service.validate_csv_files.return_value = (False, message)

        response = client.post("/upload", data=_csv_form("x.csv"))

        assert response.status_code == 302
        assert "\r" not in response.headers["Location"]
        assert "\n" not in response.headers["Location"]
        assert response.headers.get("Set-Cookie") is None
        expected = message.replace("\n", " ").replace("\r", " ").strip()
        assert _error_from_redirect(response) == expected


class TestUploadSpreadsheetAndDatabase:
    def test_a_workbook_is_parsed_triplified_uploaded_and_linked(
        self, client, ingest_service, helpers, session_cache
    ):
        response = client.post(
            "/upload", data=_csv_form("book.xlsx", file_type="Excel")
        )

        assert response.headers["Location"] == "/describe"
        (files,), _ = ingest_service.parse_excel_files.call_args
        assert [f.filename for f in files] == ["book.xlsx"]
        assert session_cache.csvTableNames == [f"{TABLE}_Sheet1"]
        helpers["run_triplifier"].assert_called_once_with("triplifierCSV.properties")
        helpers["upload_func"].assert_called_once_with(
            "Excel", session_cache.output_files
        )
        helpers["start_background"].assert_called_once_with(session_cache)

    def test_a_workbook_the_service_refuses_goes_back_to_ingest(
        self, client, ingest_service, helpers
    ):
        ingest_service.validate_excel_files.return_value = (False, "Not a workbook")

        response = client.post("/upload", data=_csv_form("a.csv", file_type="Excel"))

        assert _error_from_redirect(response) == "Not a workbook"
        ingest_service.parse_excel_files.assert_not_called()

    def test_an_unreadable_workbook_goes_back_to_ingest(
        self, client, ingest_service, helpers
    ):
        ingest_service.parse_excel_files.return_value = ([], [], "Corrupt workbook")

        response = client.post("/upload", data=_csv_form("a.xlsx", file_type="Excel"))

        assert _error_from_redirect(response) == "Corrupt workbook"
        helpers["run_triplifier"].assert_not_called()

    def test_a_database_connection_is_handed_over_and_triplified(
        self, client, helpers, session_cache
    ):
        response = client.post(
            "/upload",
            data={
                "fileType": "Postgres",
                "username": "flyover",
                "password": "s3cret",
                "POSTGRES_URL": "db.example",
                "POSTGRES_DB": "clinic",
                "table": "patients",
            },
        )

        assert response.headers["Location"] == "/describe"
        helpers["handle_postgres"].assert_called_once_with(
            "flyover", "s3cret", "db.example", "clinic", "patients"
        )
        helpers["run_triplifier"].assert_called_once_with("triplifierSQL.properties")
        helpers["upload_func"].assert_called_once_with(
            "Postgres", session_cache.output_files
        )
        # The relationship jobs only follow file uploads.
        helpers["start_background"].assert_not_called()

    def test_a_database_ingest_without_the_connection_helper_still_triplifies(
        self, make_app, helpers
    ):
        client = make_app(handle_postgres=None).test_client()

        response = client.post("/upload", data={"fileType": "Postgres"})

        assert response.headers["Location"] == "/describe"
        helpers["run_triplifier"].assert_called_once_with("triplifierSQL.properties")

    def test_without_an_upload_helper_nothing_is_uploaded(self, make_app, helpers):
        client = make_app(upload_func=None).test_client()

        response = client.post("/upload", data=_csv_form("patients.csv"))

        assert response.headers["Location"] == "/describe"
        helpers["start_background"].assert_not_called()


class TestUploadWithoutNewData:
    @pytest.mark.parametrize(
        "form",
        [
            {},
            {"fileType": ""},
            {"fileType": "CSV"},
            {"fileType": "none", "csvFile": (io.BytesIO(b""), "")},
        ],
        ids=["no-fields", "empty-type", "csv-without-files", "empty-file-input"],
    )
    def test_submitting_no_data_moves_on_to_describe(
        self, client, form, helpers, session_cache
    ):
        response = client.post("/upload", data=form)

        assert response.headers["Location"] == "/describe"
        assert session_cache.StatusToDisplay.startswith(
            "You have opted to not submit any new data."
        )
        helpers["run_triplifier"].assert_not_called()
        helpers["upload_func"].assert_not_called()

    def test_a_json_body_counts_as_no_data(self, client, helpers):
        response = client.post("/upload", json={"fileType": "CSV"})

        assert response.headers["Location"] == "/describe"
        helpers["run_triplifier"].assert_not_called()

    def test_an_unknown_source_type_with_a_file_is_an_error(self, client, helpers):
        response = client.post("/upload", data=_csv_form("data.xml", file_type="XML"))

        assert _error_from_redirect(response) == (
            "Error: An unexpected error occurred. Please try again."
        )
        helpers["run_triplifier"].assert_not_called()


class TestUploadRelationships:
    def test_relationship_fields_are_parsed_into_the_session(
        self, client, ingest_service, session_cache
    ):
        ingest_service.parse_pk_fk_data.return_value = [{"pk": "id"}]
        ingest_service.parse_cross_graph_data.return_value = {"link": "id"}

        client.post(
            "/upload",
            data=_csv_form(
                "patients.csv",
                pkFkData='[{"pk": "id"}]',
                crossGraphLinkData='{"link": "id"}',
            ),
        )

        ingest_service.parse_pk_fk_data.assert_called_once_with('[{"pk": "id"}]')
        ingest_service.parse_cross_graph_data.assert_called_once_with('{"link": "id"}')
        assert session_cache.pk_fk_data == [{"pk": "id"}]
        assert session_cache.cross_graph_link_data == {"link": "id"}

    def test_without_relationship_fields_the_session_keeps_its_own(
        self, client, ingest_service, session_cache
    ):
        session_cache.pk_fk_data = ["earlier"]

        client.post("/upload", data=_csv_form("patients.csv"))

        ingest_service.parse_pk_fk_data.assert_not_called()
        assert session_cache.pk_fk_data == ["earlier"]

    def test_relationships_are_kept_even_when_the_files_are_refused(
        self, client, ingest_service, session_cache
    ):
        # Parsed before the files are validated: a refused upload leaves them.
        ingest_service.parse_pk_fk_data.return_value = [{"pk": "id"}]
        ingest_service.validate_csv_files.return_value = (False, "bad file")

        client.post("/upload", data=_csv_form("x.csv", pkFkData="[]"))

        assert session_cache.pk_fk_data == [{"pk": "id"}]


class TestUploadSizes:
    """No MAX_CONTENT_LENGTH is set; Werkzeug's per-form limits still apply."""

    def test_a_large_file_reaches_the_parser_whole(self, client, ingest_service):
        payload = b"id,value\n" + b"P,1\n" * (2 * 1024 * 1024)
        received = []

        def parse(files, separator, decimal):
            # Read during the call: Werkzeug closes the upload streams when
            # the request ends.
            received.append(files[0].read())
            return [], [TABLE], None

        ingest_service.parse_csv_files.side_effect = parse

        response = client.post(
            "/upload",
            data={"fileType": "CSV", "csvFile": (io.BytesIO(payload), "big.csv")},
        )

        assert response.headers["Location"] == "/describe"
        assert received == [payload]

    def test_a_relationship_field_over_500_kb_is_too_large(self, client, helpers):
        response = client.post(
            "/upload",
            data=_csv_form("patients.csv", pkFkData="x" * 600_000),
        )

        assert response.status_code == 413
        helpers["run_triplifier"].assert_not_called()

    def test_more_than_1000_form_parts_is_too_large(self, client, helpers):
        form = {
            "fileType": "CSV",
            "csvFile": [
                (io.BytesIO(b"id\n1\n"), f"table_{i}.csv") for i in range(1001)
            ],
        }

        response = client.post("/upload", data=form)

        assert response.status_code == 413
        helpers["run_triplifier"].assert_not_called()


# ---------------------------------------------------------------------------
# GET /data-submission
# ---------------------------------------------------------------------------


class TestDataSubmission:
    @pytest.mark.route("GET", "/data-submission", "success")
    def test_the_legacy_page_redirects_to_describe(self, client):
        response = client.get("/data-submission")

        assert response.status_code == 302
        assert response.headers["Location"] == "/describe"

    @pytest.mark.route("POST", "/data-submission", "failure")
    def test_posting_to_the_legacy_page_is_not_allowed(self, client):
        response = client.post("/data-submission")

        assert response.status_code == 405


# ---------------------------------------------------------------------------
# GET /api/rdf-store-databases
# ---------------------------------------------------------------------------


class TestRdfStoreDatabases:
    @pytest.mark.route("GET", "/api/rdf-store-databases", "success")
    def test_the_store_databases_are_listed_and_remembered(
        self, client, store, session_cache
    ):
        store.get_databases.return_value = ["patients", "visits"]

        response = client.get("/api/rdf-store-databases")

        assert response.status_code == 200
        assert response.get_json() == {
            "success": True,
            "databases": ["patients", "visits"],
        }
        assert session_cache.databases == ["patients", "visits"]

    def test_an_empty_store_is_reported_without_an_error_status(
        self, client, session_cache
    ):
        session_cache.databases = ["stale"]

        response = client.get("/api/rdf-store-databases")

        assert response.status_code == 200
        assert response.get_json() == {
            "success": False,
            "databases": [],
            "message": "No databases found",
        }
        assert session_cache.databases == []

    @pytest.mark.route("GET", "/api/rdf-store-databases", "failure")
    def test_a_store_failure_is_a_server_error_with_the_reason(self, client, store):
        store.get_databases.side_effect = ConnectionError("store unreachable")

        response = client.get("/api/rdf-store-databases")

        assert response.status_code == 500
        assert response.get_json() == {"success": False, "error": "store unreachable"}

    def test_without_a_store_service_or_url_the_list_is_empty(
        self, storeless_client, session_cache
    ):
        response = storeless_client.get("/api/rdf-store-databases")

        assert response.get_json() == {
            "success": False,
            "databases": [],
            "message": "No databases found",
        }
        assert session_cache.databases == []

    def test_a_missing_store_service_is_built_from_the_app_config(
        self, make_app, monkeypatch
    ):
        # Without a service in the context the controller builds one from the
        # app config; stop that one from reaching for a real store.
        import services

        built = []

        class _Unreachable:
            def __init__(self, url, repo):
                built.append((url, repo))

            def get_databases(self):
                return []

        monkeypatch.setattr(services, "RDFStoreService", _Unreachable)
        app = make_app(rdf_store_service=None)

        response = app.test_client().get("/api/rdf-store-databases")

        assert response.get_json()["databases"] == []
        assert built == [(app.config["rdf_store_url"], app.config["repo"])]
        assert isinstance(app.config["APP_CONTEXT"]["rdf_store_service"], _Unreachable)


# ---------------------------------------------------------------------------
# GET /api/check-graph-exists and /get-existing-graph-structure
# ---------------------------------------------------------------------------


class TestCheckGraphExists:
    @pytest.mark.route("GET", "/api/check-graph-exists", "success")
    @pytest.mark.parametrize("exists", [True, False])
    def test_the_store_answer_is_passed_on(self, client, store, exists):
        store.check_data_exists.return_value = exists

        response = client.get("/api/check-graph-exists")

        assert response.status_code == 200
        assert response.get_json() == {"exists": exists}

    @pytest.mark.route("GET", "/api/check-graph-exists", "failure")
    def test_a_store_failure_reads_as_no_data_with_the_reason(self, client, store):
        store.check_data_exists.side_effect = ConnectionError("store unreachable")

        response = client.get("/api/check-graph-exists")

        assert response.status_code == 200
        assert response.get_json() == {"exists": False, "error": "store unreachable"}

    def test_without_a_store_service_there_is_no_data(self, storeless_client):
        response = storeless_client.get("/api/check-graph-exists")

        assert response.get_json() == {"exists": False}


class TestExistingGraphStructure:
    @pytest.mark.route("GET", "/get-existing-graph-structure", "success")
    def test_the_store_structure_is_passed_on(self, client, store):
        structure = {"tables": [TABLE], "tableColumns": {TABLE: ["id", "sex"]}}
        store.get_graph_structure.return_value = structure

        response = client.get("/get-existing-graph-structure")

        assert response.status_code == 200
        assert response.get_json() == structure

    @pytest.mark.route("GET", "/get-existing-graph-structure", "failure")
    def test_a_store_failure_reads_as_an_empty_structure(self, client, store):
        store.get_graph_structure.side_effect = ConnectionError("store unreachable")

        response = client.get("/get-existing-graph-structure")

        assert response.status_code == 200
        assert response.get_json() == {"tables": [], "tableColumns": {}}

    def test_without_a_store_service_the_structure_is_empty(self, storeless_client):
        response = storeless_client.get("/get-existing-graph-structure")

        assert response.get_json() == {"tables": [], "tableColumns": {}}
