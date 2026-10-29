"""
API tests for the annotate routes (``controllers/annotate_controller.py``).

The annotate service (preparation, execution, verification), the store and
the semantic-map helpers are doubles. Under test: how an uploaded map is
saved, matched against the store's tables and cleaned; how a filtered map
is applied for one run and restored; how per-database annotation results
become the session's status and the response; and the error contract of
each route (several report failures in a 200 body, which the SPA reads).
"""

import io
import json
import os
import tempfile
import threading
import time

import pytest

from loaders import JSONLDMapping
from tests.api.support import REPO, STORE_URL, TABLE, valid_mapping

VERIFY_STATE = "/api/v1/annotation-verify-state"


def _upload(content, filename="mapping.jsonld"):
    if isinstance(content, (dict, list)):
        content = json.dumps(content).encode("utf-8")
    elif isinstance(content, str):
        content = content.encode("utf-8")
    return {"annotationJsonFile": (io.BytesIO(content), filename)}


def _saved_files(app):
    folder = app.config["APP_CONTEXT"]["upload_folder"]
    return sorted(os.listdir(folder))


# ---------------------------------------------------------------------------
# Legacy page routes
# ---------------------------------------------------------------------------


class TestLegacyAnnotatePages:
    @pytest.mark.parametrize(
        "path, target",
        [
            pytest.param(
                "/annotation_landing",
                "/annotate",
                marks=pytest.mark.route("GET", "/annotation_landing", "success"),
            ),
            pytest.param(
                "/annotation-review",
                "/annotate/review",
                marks=pytest.mark.route("GET", "/annotation-review", "success"),
            ),
            pytest.param(
                "/annotation-verify",
                "/annotate/verify",
                marks=pytest.mark.route("GET", "/annotation-verify", "success"),
            ),
        ],
    )
    def test_the_legacy_pages_redirect_into_the_spa(self, client, path, target):
        response = client.get(path)

        assert response.status_code == 302
        assert response.headers["Location"] == target

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param(
                "/annotation_landing",
                marks=pytest.mark.route("POST", "/annotation_landing", "failure"),
            ),
            pytest.param(
                "/annotation-review",
                marks=pytest.mark.route("POST", "/annotation-review", "failure"),
            ),
            pytest.param(
                "/annotation-verify",
                marks=pytest.mark.route("POST", "/annotation-verify", "failure"),
            ),
        ],
    )
    def test_posting_to_a_legacy_page_is_not_allowed(self, client, path):
        assert client.post(path).status_code == 405

    def test_the_redirects_ignore_the_session_state(self, client, store):
        store.check_data_exists.return_value = True

        assert client.get("/annotation_landing").headers["Location"] == "/annotate"


# ---------------------------------------------------------------------------
# POST /upload-annotation-json
# ---------------------------------------------------------------------------


class TestUploadAnnotationMap:
    @pytest.fixture(autouse=True)
    def _ingested(self, store):
        store.get_databases.return_value = [TABLE]

    @pytest.mark.route("POST", "/upload-annotation-json", "success")
    def test_a_matching_map_is_saved_adopted_and_reported(
        self, app, client, session_cache
    ):
        response = client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert response.status_code == 200
        assert response.get_json() == {
            "success": True,
            "message": "JSON-LD file validated successfully",
            "filename": "mapping.jsonld",
            "rdf_store_databases": [TABLE],
            "jsonld_databases": [TABLE],
            "matching_databases": [{"jsonld": TABLE, "rdf_store": TABLE}],
            "non_matching_jsonld": [],
            "removed_columns": [],
            "cleaned_mapping": valid_mapping(),
        }
        assert _saved_files(app) == ["mapping.jsonld"]
        assert session_cache.annotation_json_path == os.path.join(
            app.config["APP_CONTEXT"]["upload_folder"], "mapping.jsonld"
        )
        assert session_cache.global_semantic_map == valid_mapping()
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)
        assert session_cache.databases == [TABLE]

    def test_the_extension_check_ignores_case(self, client):
        response = client.post(
            "/upload-annotation-json", data=_upload(valid_mapping(), "Map.JSONLD")
        )

        assert response.status_code == 200
        assert response.get_json()["filename"] == "Map.JSONLD"

    def test_a_map_that_fails_validation_is_kept_only_as_the_legacy_map(
        self, client, session_cache
    ):
        mapping = valid_mapping()
        mapping["@type"] = "mapping:NotAMapping"

        response = client.post("/upload-annotation-json", data=_upload(mapping))

        assert response.get_json()["success"] is True
        assert session_cache.global_semantic_map == mapping
        assert session_cache.jsonld_mapping is None

    def test_a_legacy_map_is_matched_on_its_database_name(self, client, session_cache):
        legacy = {"database_name": TABLE, "variable_info": {}}

        body = client.post("/upload-annotation-json", data=_upload(legacy)).get_json()

        assert body["jsonld_databases"] == [TABLE]
        assert body["matching_databases"] == [{"jsonld": TABLE, "rdf_store": TABLE}]
        assert session_cache.jsonld_mapping is None

    def test_tables_without_a_source_file_are_matched_on_their_key(self, client):
        mapping = valid_mapping()
        del mapping["databases"]["centre_x"]["tables"][TABLE]["sourceFile"]

        body = client.post("/upload-annotation-json", data=_upload(mapping)).get_json()

        assert body["jsonld_databases"] == [TABLE]

    def test_tables_and_databases_that_are_not_objects_are_handled(self, client):
        mapping = valid_mapping()
        mapping["databases"]["centre_x"]["tables"]["visits"] = "not a table"
        mapping["databases"]["text_db"] = "not a database"

        body = client.post("/upload-annotation-json", data=_upload(mapping)).get_json()

        assert body["jsonld_databases"] == [TABLE, "visits"]
        assert body["non_matching_jsonld"] == ["visits"]

    def test_tables_the_store_does_not_hold_are_listed_as_not_matching(
        self, client, helpers
    ):
        mapping = valid_mapping()
        visits = json.loads(
            json.dumps(mapping["databases"]["centre_x"]["tables"][TABLE])
        )
        visits["sourceFile"] = "visits"
        mapping["databases"]["centre_x"]["tables"]["visits"] = visits

        body = client.post("/upload-annotation-json", data=_upload(mapping)).get_json()

        assert body["matching_databases"] == [{"jsonld": TABLE, "rdf_store": TABLE}]
        assert body["non_matching_jsonld"] == ["visits"]
        helpers["name_matcher"].assert_any_call("visits", TABLE)

    def test_columns_the_store_lacks_are_removed_and_reported(
        self, client, store, session_cache
    ):
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex"]}

        body = client.post(
            "/upload-annotation-json", data=_upload(valid_mapping())
        ).get_json()

        assert body["removed_columns"] == ["age"]
        columns = body["cleaned_mapping"]["databases"]["centre_x"]["tables"][TABLE][
            "columns"
        ]
        assert "age_at_diagnosis" not in columns
        assert session_cache.global_semantic_map == body["cleaned_mapping"]

    def test_a_store_holding_every_column_removes_nothing(self, client, store):
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}

        body = client.post(
            "/upload-annotation-json", data=_upload(valid_mapping())
        ).get_json()

        assert body["removed_columns"] == []
        assert body["cleaned_mapping"] == valid_mapping()

    def test_a_refused_file_that_cannot_be_removed_is_still_refused(
        self, client, monkeypatch
    ):
        from controllers import annotate_controller

        def refuse_removal(path):
            raise PermissionError(f"cannot remove {path}")

        monkeypatch.setattr(annotate_controller.os, "remove", refuse_removal)

        response = client.post("/upload-annotation-json", data=_upload("{not json"))

        assert response.status_code == 400
        assert response.get_json() == {"error": "Invalid JSON file format"}

    def test_a_store_failure_in_the_column_check_skips_only_the_clean_up(
        self, client, store
    ):
        store.get_column_info_by_database.side_effect = ConnectionError("store down")

        response = client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert response.status_code == 200
        assert response.get_json()["removed_columns"] == []

    def test_a_path_in_the_filename_never_leaves_the_upload_folder(
        self, app, client, tmp_path
    ):
        response = client.post(
            "/upload-annotation-json",
            data=_upload(valid_mapping(), "../../../outside/evil.jsonld"),
        )

        assert response.status_code == 200
        assert response.get_json()["filename"] == "outside_evil.jsonld"
        assert _saved_files(app) == ["outside_evil.jsonld"]
        assert not (tmp_path.parent / "outside").exists()

    @pytest.mark.route("POST", "/upload-annotation-json", "failure")
    def test_a_request_without_a_file_part_is_refused(self, client):
        response = client.post("/upload-annotation-json", data={})

        assert response.status_code == 400
        assert response.get_json() == {"error": "No file uploaded"}

    def test_a_file_part_without_a_filename_is_refused(self, client):
        response = client.post("/upload-annotation-json", data=_upload({}, ""))

        assert response.status_code == 400
        assert response.get_json() == {"error": "No file selected"}

    @pytest.mark.parametrize("filename", ["map.json", "map.jsonld.txt", "jsonld"])
    def test_a_file_without_the_jsonld_extension_is_refused(
        self, app, client, filename
    ):
        response = client.post(
            "/upload-annotation-json", data=_upload(valid_mapping(), filename)
        )

        assert response.status_code == 400
        assert response.get_json() == {"error": "File must be a .jsonld file"}
        assert _saved_files(app) == []

    def test_a_file_that_is_not_json_is_refused_and_removed(self, app, client):
        response = client.post("/upload-annotation-json", data=_upload("{not json"))

        assert response.status_code == 400
        assert response.get_json() == {"error": "Invalid JSON file format"}
        assert _saved_files(app) == []

    def test_without_ingested_data_the_map_is_refused_and_removed(
        self, app, client, store, session_cache
    ):
        store.get_databases.return_value = []

        response = client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "No data found. Please complete Ingest step first.",
            "rdf_store_databases": [],
            "jsonld_databases": [],
        }
        assert _saved_files(app) == []
        assert session_cache.databases == []

    def test_a_map_without_tables_is_refused_and_removed(self, app, client):
        mapping = valid_mapping()
        mapping["databases"] = {}

        response = client.post("/upload-annotation-json", data=_upload(mapping))

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "No table definitions found in semantic map.",
            "rdf_store_databases": [TABLE],
            "jsonld_databases": [],
        }
        assert _saved_files(app) == []

    def test_a_map_matching_no_ingested_table_is_refused_and_removed(
        self, app, client, store, session_cache
    ):
        store.get_databases.return_value = ["visits"]

        response = client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "No data sources match RDF store data.",
            "rdf_store_databases": ["visits"],
            "jsonld_databases": [TABLE],
            "matching_databases": [],
            "non_matching_jsonld": [TABLE],
        }
        assert _saved_files(app) == []
        assert session_cache.global_semantic_map is None

    def test_a_store_failure_is_a_server_error(self, client, store):
        store.get_databases.side_effect = ConnectionError("store down")

        response = client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert response.status_code == 500
        assert response.get_json() == {"error": "Upload failed: store down"}

    def test_without_a_store_service_there_is_no_ingested_data(self, make_app):
        client = make_app(rdf_store_service=None).test_client()

        response = client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert response.status_code == 400
        assert response.get_json()["rdf_store_databases"] == []

    @pytest.mark.xfail(
        strict=True,
        reason="defect: a file the server cannot decode as UTF-8 raises outside the "
        "JSON error handling, so the upload answers 500 and leaves the file in "
        "the upload folder",
    )
    def test_a_file_that_is_not_utf8_is_refused_and_removed(self, app, client):
        response = client.post(
            "/upload-annotation-json", data=_upload(b'{"name": "caf\xe9"}')
        )

        assert response.status_code == 400
        assert _saved_files(app) == []

    @pytest.mark.xfail(
        strict=True,
        reason="defect: when the store fails after the file was saved, the 500 "
        "path does not remove it, so failed uploads accumulate in the folder",
    )
    def test_a_failed_upload_leaves_no_file_behind(self, app, client, store):
        store.get_databases.side_effect = ConnectionError("store down")

        client.post("/upload-annotation-json", data=_upload(valid_mapping()))

        assert _saved_files(app) == []


# ---------------------------------------------------------------------------
# POST /start-annotation
# ---------------------------------------------------------------------------


def _prepared(*databases, variables=("biological_sex",)):
    return {
        database: {
            "variables": {name: {"local_definition": name} for name in variables},
            "prefixes": "PREFIX sio: <http://semanticscience.org/resource/>",
            "is_jsonld": True,
        }
        for database in databases
    }


@pytest.fixture
def annotatable(session_cache, store, helpers, annotate_service):
    """A session with a map, an ingested table and one variable to annotate."""
    session_cache.jsonld_mapping = JSONLDMapping.from_dict(valid_mapping())
    helpers["has_semantic_map"].return_value = True
    helpers["get_table_names"].return_value = [TABLE]
    store.get_databases.return_value = [TABLE]
    annotate_service.prepare_annotation_data.return_value = _prepared(TABLE)
    annotate_service.execute_annotation.return_value = (
        {"biological_sex": {"success": True}},
        None,
    )
    return annotate_service


class TestStartAnnotation:
    @pytest.mark.route("POST", "/start-annotation", "success")
    def test_the_prepared_variables_are_annotated_and_recorded(
        self, client, session_cache, annotatable
    ):
        response = client.post("/start-annotation")

        assert response.status_code == 200
        assert response.get_json() == {
            "success": True,
            "message": "Annotated 1 variables across 1 databases",
        }
        assert session_cache.annotation_status == {
            f"{TABLE}.biological_sex": {
                "success": True,
                "message": "Annotation completed successfully",
                "database": TABLE,
                "details": {"success": True},
            }
        }
        assert session_cache.databases == [TABLE]

    def test_the_services_get_the_session_the_helpers_and_the_endpoint(
        self, client, session_cache, annotatable, helpers
    ):
        client.post("/start-annotation")

        annotatable.prepare_annotation_data.assert_called_once_with(
            [TABLE],
            session_cache,
            [TABLE],
            helpers["name_matcher"],
            helpers["get_semantic_map"],
            helpers["formulate_local_map"],
        )
        annotatable.execute_annotation.assert_called_once_with(
            f"{STORE_URL}/repositories/{REPO}/statements",
            TABLE,
            "PREFIX sio: <http://semanticscience.org/resource/>",
            {"biological_sex": {"local_definition": "biological_sex"}},
            temp_dir=os.path.join(tempfile.gettempdir(), "annotation_temp", TABLE),
        )

    @pytest.mark.parametrize(
        "database, folder",
        [("../../etc", "etc"), ("///", "db"), ("Centre B (NL)", "Centre_B_NL")],
    )
    def test_every_database_gets_its_own_safe_temporary_folder(
        self, client, store, annotatable, database, folder
    ):
        store.get_databases.return_value = [database]
        annotatable.prepare_annotation_data.return_value = _prepared(database)

        client.post("/start-annotation")

        temp_dir = annotatable.execute_annotation.call_args.kwargs["temp_dir"]
        assert temp_dir == os.path.join(
            tempfile.gettempdir(), "annotation_temp", folder
        )

    def test_databases_are_reported_one_by_one(
        self, client, session_cache, store, annotatable
    ):
        store.get_databases.return_value = [TABLE, "visits"]
        annotatable.prepare_annotation_data.return_value = _prepared(TABLE, "visits")

        def execute(endpoint, database, prefixes, variables, temp_dir):
            if database == "visits":
                return None, "the store refused the update"
            return {"biological_sex": {"success": True}}, None

        annotatable.execute_annotation.side_effect = execute

        response = client.post("/start-annotation")

        assert response.get_json() == {
            "success": True,
            "message": "Annotated 1 variables across 2 databases",
        }
        assert session_cache.annotation_status["visits.biological_sex"] == {
            "success": False,
            "error": "the store refused the update",
            "database": "visits",
        }

    def test_a_failed_variable_carries_the_databases_error(
        self, client, session_cache, annotatable
    ):
        annotatable.prepare_annotation_data.return_value = _prepared(
            TABLE, variables=("biological_sex", "age_at_diagnosis")
        )
        annotatable.execute_annotation.return_value = (
            {
                "biological_sex": {"success": True},
                "age_at_diagnosis": {"success": False},
            },
            "one update failed",
        )

        client.post("/start-annotation")

        status = session_cache.annotation_status[f"{TABLE}.age_at_diagnosis"]
        assert status["success"] is False
        assert status["message"] == "Annotation failed"
        assert status["error"] == "one update failed"

    @pytest.mark.route("POST", "/start-annotation", "failure")
    def test_without_a_map_nothing_is_annotated(
        self, client, helpers, annotate_service
    ):
        helpers["has_semantic_map"].return_value = False

        response = client.post("/start-annotation")

        assert response.status_code == 200
        assert response.get_json() == {
            "success": False,
            "error": "No semantic map available",
        }
        annotate_service.prepare_annotation_data.assert_not_called()

    def test_without_ingested_data_nothing_is_annotated(
        self, client, store, annotatable
    ):
        store.get_databases.return_value = []

        response = client.post("/start-annotation")

        assert response.get_json() == {
            "success": False,
            "error": "No databases available",
        }
        annotatable.prepare_annotation_data.assert_not_called()

    def test_without_a_store_service_nothing_is_annotated(self, make_app, annotatable):
        client = make_app(rdf_store_service=None).test_client()

        response = client.post("/start-annotation")

        assert response.get_json() == {
            "success": False,
            "error": "No databases available",
        }

    def test_a_map_without_annotatable_variables_is_reported(self, client, annotatable):
        annotatable.prepare_annotation_data.return_value = {}

        response = client.post("/start-annotation")

        assert response.get_json() == {
            "success": False,
            "error": "No variables with local definitions found",
        }
        annotatable.execute_annotation.assert_not_called()

    def test_a_run_in_which_every_variable_fails_is_a_failure(
        self, client, session_cache, annotatable
    ):
        annotatable.execute_annotation.return_value = (None, "store unreachable")

        response = client.post("/start-annotation")

        assert response.get_json() == {
            "success": False,
            "error": "No variables were successfully annotated",
        }
        assert session_cache.annotation_status[f"{TABLE}.biological_sex"]["error"] == (
            "store unreachable"
        )

    def test_an_exception_while_preparing_is_reported_in_the_body(
        self, client, annotatable
    ):
        annotatable.prepare_annotation_data.side_effect = KeyError("biological_sex")

        response = client.post("/start-annotation")

        assert response.status_code == 200
        assert response.get_json() == {"success": False, "error": "'biological_sex'"}

    def test_the_run_is_capped_at_32_databases_in_parallel(
        self, client, store, annotatable
    ):
        databases = [f"table_{i:02d}" for i in range(40)]
        store.get_databases.return_value = databases
        annotatable.prepare_annotation_data.return_value = _prepared(*databases)
        lock = threading.Lock()
        running = {"now": 0, "peak": 0}
        enough_in_flight = threading.Event()

        def execute(endpoint, database, prefixes, variables, temp_dir):
            with lock:
                running["now"] += 1
                running["peak"] = max(running["peak"], running["now"])
                if running["now"] >= 32:
                    enough_in_flight.set()
            # Hold the worker until the pool is full (or give up after 5 s).
            enough_in_flight.wait(timeout=5)
            time.sleep(0.01)
            with lock:
                running["now"] -= 1
            return {"biological_sex": {"success": True}}, None

        annotatable.execute_annotation.side_effect = execute

        response = client.post("/start-annotation")

        assert response.get_json()["message"] == (
            "Annotated 40 variables across 40 databases"
        )
        assert running["peak"] == 32


class TestStartAnnotationWithAFilteredMap:
    """A posted map narrows one run; the session's own map is always restored."""

    @staticmethod
    def _filtered():
        mapping = valid_mapping()
        mapping["name"] = "Only the selected table"
        return mapping

    def test_a_valid_filtered_map_drives_the_run_and_is_then_forgotten(
        self, client, session_cache, annotatable
    ):
        original = session_cache.jsonld_mapping
        seen = []
        annotatable.prepare_annotation_data.side_effect = (
            lambda databases, cache, *rest: seen.append(cache.jsonld_mapping.name)
            or _prepared(TABLE)
        )

        response = client.post("/start-annotation", json=self._filtered())

        assert response.get_json()["success"] is True
        assert seen == ["Only the selected table"]
        assert session_cache.jsonld_mapping is original

    def test_the_filtered_map_may_be_wrapped_in_a_semantic_map_key(
        self, client, session_cache, annotatable
    ):
        seen = []
        annotatable.prepare_annotation_data.side_effect = (
            lambda databases, cache, *rest: seen.append(cache.jsonld_mapping.name) or {}
        )

        client.post("/start-annotation", json={"semantic_map": self._filtered()})

        assert seen == ["Only the selected table"]

    def test_the_session_map_is_restored_when_the_run_fails(
        self, client, session_cache, annotatable
    ):
        original = session_cache.jsonld_mapping
        annotatable.prepare_annotation_data.side_effect = RuntimeError("boom")

        response = client.post("/start-annotation", json=self._filtered())

        assert response.get_json() == {"success": False, "error": "boom"}
        assert session_cache.jsonld_mapping is original

    def test_an_invalid_filtered_map_stops_the_run_untouched(
        self, client, session_cache, annotatable
    ):
        original = session_cache.jsonld_mapping
        invalid = self._filtered()
        invalid["@type"] = "mapping:NotAMapping"

        response = client.post("/start-annotation", json=invalid)

        assert response.get_json() == {
            "success": False,
            "error": "Filtered semantic map validation failed",
        }
        assert session_cache.jsonld_mapping is original
        annotatable.prepare_annotation_data.assert_not_called()

    @pytest.mark.parametrize(
        "data, content_type",
        [
            ("{}", "application/json"),
            ('{"semantic_map": {}}', "application/json"),
            ("{broken", "application/json"),
            ("", None),
            ("selected=patients", "application/x-www-form-urlencoded"),
        ],
        ids=["empty-object", "empty-wrapped", "malformed", "no-body", "form"],
    )
    def test_a_body_without_a_map_runs_on_the_session_map(
        self, client, session_cache, annotatable, data, content_type
    ):
        original = session_cache.jsonld_mapping
        seen = []
        annotatable.prepare_annotation_data.side_effect = (
            lambda databases, cache, *rest: seen.append(cache.jsonld_mapping) or {}
        )

        client.post("/start-annotation", data=data, content_type=content_type)

        assert seen == [original]

    @pytest.mark.xfail(
        strict=True,
        reason="defect (also pinned by the integration layer): with no map in the "
        "session, applying a valid filtered map returns the session's None, which "
        "the controller reads as 'validation failed'",
    )
    def test_a_valid_filtered_map_works_without_a_session_map(
        self, client, session_cache, annotatable
    ):
        session_cache.jsonld_mapping = None

        response = client.post("/start-annotation", json=self._filtered())

        assert response.get_json()["success"] is True

    @pytest.mark.parametrize(
        "body",
        [
            [1, 2],
            "text",
            pytest.param(
                7,
                marks=pytest.mark.xfail(
                    strict=True,
                    reason="defect: MappingValidator.validate raises on a number, "
                    "and the filtered map is validated outside the route's "
                    "error handling, so a JSON number as the body answers 500",
                ),
            ),
        ],
    )
    def test_a_body_that_is_not_an_object_is_refused_as_a_filtered_map(
        self, client, session_cache, annotatable, body
    ):
        original = session_cache.jsonld_mapping

        response = client.post("/start-annotation", json=body)

        assert response.status_code == 200
        assert response.get_json() == {
            "success": False,
            "error": "Filtered semantic map validation failed",
        }
        assert session_cache.jsonld_mapping is original


# ---------------------------------------------------------------------------
# GET /api/v1/annotation-verify-state
# ---------------------------------------------------------------------------


class TestVerifyState:
    @pytest.mark.route("GET", VERIFY_STATE, "success")
    def test_the_verification_data_and_statuses_are_returned(
        self, client, session_cache, annotatable, helpers
    ):
        annotatable.get_verification_data.return_value = (
            [f"{TABLE}.biological_sex"],
            [f"{TABLE}.age_at_diagnosis"],
            {f"{TABLE}.biological_sex": {"class": "ncit:C28421"}},
        )
        session_cache.annotation_status = {f"{TABLE}.biological_sex": {"success": True}}

        response = client.get(VERIFY_STATE)

        assert response.status_code == 200
        assert response.get_json() == {
            "annotated_variables": [f"{TABLE}.biological_sex"],
            "unannotated_variables": [f"{TABLE}.age_at_diagnosis"],
            "variable_data": {f"{TABLE}.biological_sex": {"class": "ncit:C28421"}},
            "annotation_status": {f"{TABLE}.biological_sex": {"success": True}},
            "success_message": (
                "Data processing complete. Semantic interoperability achieved."
            ),
        }
        annotatable.get_verification_data.assert_called_once_with(
            [TABLE],
            session_cache,
            [TABLE],
            helpers["name_matcher"],
            helpers["get_semantic_map"],
            helpers["formulate_local_map"],
        )

    @pytest.mark.parametrize(
        "statuses",
        [None, {}, {"a": {"success": True}, "b": {"success": False}}],
        ids=["never-run", "empty", "one-failed"],
    )
    def test_the_success_message_needs_every_variable_to_have_succeeded(
        self, client, session_cache, annotatable, statuses
    ):
        session_cache.annotation_status = statuses

        body = client.get(VERIFY_STATE).get_json()

        assert body["success_message"] is None
        assert body["annotation_status"] == (statuses or {})

    @pytest.mark.route("GET", VERIFY_STATE, "failure")
    def test_without_a_map_the_page_cannot_be_built(self, client, helpers):
        helpers["has_semantic_map"].return_value = False

        response = client.get(VERIFY_STATE)

        assert response.status_code == 400
        assert response.get_json() == {"error": "No semantic map available"}

    def test_without_ingested_data_the_page_cannot_be_built(
        self, client, store, annotatable
    ):
        store.get_databases.return_value = []

        response = client.get(VERIFY_STATE)

        assert response.status_code == 400
        assert response.get_json() == {"error": "No databases available"}

    def test_without_a_store_service_the_page_cannot_be_built(
        self, make_app, annotatable
    ):
        client = make_app(rdf_store_service=None).test_client()

        assert client.get(VERIFY_STATE).status_code == 400

    def test_a_service_failure_is_a_server_error(self, client, annotatable):
        annotatable.get_verification_data.side_effect = RuntimeError("boom")

        assert client.get(VERIFY_STATE).status_code == 500


# ---------------------------------------------------------------------------
# POST /verify-annotation-ask
# ---------------------------------------------------------------------------


class TestVerifyAnnotationAsk:
    @pytest.mark.route("POST", "/verify-annotation-ask", "success")
    @pytest.mark.parametrize("valid", [True, False])
    def test_the_ask_verdict_and_query_are_returned(
        self, client, session_cache, store, helpers, annotate_service, valid
    ):
        annotate_service.verify_single_annotation.return_value = (
            True,
            valid,
            "ASK { ?s ?p ?o }",
            None,
        )

        response = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.biological_sex"}
        )

        assert response.status_code == 200
        assert response.get_json() == {
            "success": True,
            "valid": valid,
            "query": "ASK { ?s ?p ?o }",
        }
        annotate_service.verify_single_annotation.assert_called_once_with(
            f"{TABLE}.biological_sex",
            session_cache,
            store,
            helpers["get_semantic_map"],
            helpers["formulate_local_map"],
        )

    @pytest.mark.route("POST", "/verify-annotation-ask", "failure")
    @pytest.mark.parametrize("body", [{}, {"variable": ""}, {"variable": None}])
    def test_a_request_without_a_variable_is_refused_in_the_body(
        self, client, annotate_service, body
    ):
        response = client.post("/verify-annotation-ask", json=body)

        assert response.status_code == 200
        assert response.get_json() == {
            "success": False,
            "error": "No variable specified",
        }
        annotate_service.verify_single_annotation.assert_not_called()

    def test_the_services_refusal_is_passed_on(self, client, annotate_service):
        annotate_service.verify_single_annotation.return_value = (
            False,
            False,
            None,
            "Invalid variable format",
        )

        response = client.post("/verify-annotation-ask", json={"variable": "nodot"})

        assert response.get_json() == {
            "success": False,
            "error": "Invalid variable format",
        }

    def test_a_service_failure_is_reported_in_the_body(self, client, annotate_service):
        annotate_service.verify_single_annotation.side_effect = ConnectionError(
            "store down"
        )

        response = client.post("/verify-annotation-ask", json={"variable": "a.b"})

        assert response.status_code == 200
        assert response.get_json() == {"success": False, "error": "store down"}

    @pytest.mark.parametrize(
        "data, content_type, error_start",
        [
            ("{broken", "application/json", "400 Bad Request"),
            ('{"variable": "a.b"}', "text/plain", "415 Unsupported Media Type"),
            ("[1, 2]", "application/json", "'list' object has no attribute 'get'"),
        ],
        ids=["malformed", "not-json", "not-an-object"],
    )
    def test_a_body_it_cannot_read_is_reported_in_the_body(
        self, client, data, content_type, error_start
    ):
        response = client.post(
            "/verify-annotation-ask", data=data, content_type=content_type
        )

        assert response.status_code == 200
        body = response.get_json()
        assert body["success"] is False
        assert body["error"].startswith(error_start)
