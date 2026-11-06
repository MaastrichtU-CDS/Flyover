"""
Integration tests for the ingest flow's outer edges.

    ingest_controller -> ingest_service -> curl (Graph Store Protocol uploads,
                         every strategy and failure), gevent background jobs,
                         the PostgreSQL triplifier path against a real
                         PostgreSQL (CI service container; skipped without
                         FLYOVER_TEST_POSTGRES_DSN), the orphan-column check
                         on malformed shapes, and the store URL normalisation.
"""

import os
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, urlparse

import gevent
import psycopg2
import pytest
import requests

from tests.integration.support import CSV_BYTES, TABLE
from services import IngestService
from services.rdf_store_service import RDFStoreService

POSTGRES_DSN = os.environ.get("FLYOVER_TEST_POSTGRES_DSN")


def _post_csv(client, content=CSV_BYTES, filename=f"{TABLE}.csv", **form):
    data = {"fileType": "CSV", "csvFile": (BytesIO(content), filename), **form}
    return client.post("/upload", data=data, content_type="multipart/form-data")


# ---------------------------------------------------------------------------
# Upload strategies and failures (ingest_service -> curl)
# ---------------------------------------------------------------------------


class TestUploadStrategies:
    def test_streaming_takes_over_when_data_binary_fails(self, client, fake_curl):
        fake_curl.fail_data_binary_only()

        response = _post_csv(client)

        assert response.headers["Location"].endswith("/describe")
        attempts = [(Path(u.file_path).name, u.method) for u in fake_curl.uploads]
        assert attempts == [
            (f"ontology_{TABLE}.owl", "data-binary"),
            (f"ontology_{TABLE}.owl", "streaming"),
            (f"output_{TABLE}.ttl", "data-binary"),
            (f"output_{TABLE}.ttl", "streaming"),
        ]

    def test_a_hanging_store_times_out_on_both_strategies(
        self, tmp_path, fake_curl, rdf_store
    ):
        fake_curl.hang(1)
        data_file = tmp_path / "output.ttl"
        data_file.write_text("<a> <b> <c> .\n")

        success, message, method = IngestService().upload_file_to_rdf_store(
            str(data_file),
            rdf_store.graph_store_url,
            "text/turtle",
            True,
            timeout_seconds=0.2,
        )

        assert success is False and method == "failed"
        assert "timeout" in message.lower()
        assert [u.method for u in fake_curl.uploads] == ["data-binary", "streaming"]

    def test_a_missing_file_is_refused_before_curl_runs(
        self, tmp_path, fake_curl, rdf_store
    ):
        success, message, method = IngestService().upload_file_to_rdf_store(
            str(tmp_path / "nope.ttl"), rdf_store.graph_store_url, "text/turtle"
        )

        assert (success, method) == (False, "none")
        assert "File not found" in message
        assert fake_curl.invocations == []

    @pytest.mark.parametrize("first_strategy_fails", [False, True])
    def test_background_uploads_run_as_greenlets(
        self, tmp_path, fake_curl, rdf_store, first_strategy_fails
    ):
        if first_strategy_fails:
            fake_curl.fail_data_binary_only()
        data_file = tmp_path / "output.ttl"
        data_file.write_text("<a> <b> <c> .\n")

        success, message, method = IngestService().upload_file_to_rdf_store(
            str(data_file),
            rdf_store.graph_store_url,
            "text/turtle",
            wait_for_completion=False,
        )
        gevent.wait()  # let the spawned upload finish

        assert (success, method) == (True, "background")
        assert "Background upload started" in message
        methods = [u.method for u in fake_curl.uploads]
        assert methods == (
            ["data-binary", "streaming"] if first_strategy_fails else ["data-binary"]
        )

    def test_a_background_upload_that_fails_everywhere_is_only_logged(
        self, tmp_path, fake_curl, rdf_store
    ):
        fake_curl.fail()
        data_file = tmp_path / "output.ttl"
        data_file.write_text("<a> <b> <c> .\n")

        success, _, _ = IngestService().upload_file_to_rdf_store(
            str(data_file),
            rdf_store.graph_store_url,
            "text/turtle",
            wait_for_completion=False,
        )
        gevent.wait()

        assert success is True  # the start succeeded; the outcome is in the log
        assert [u.method for u in fake_curl.uploads] == ["data-binary", "streaming"]

    def test_output_entries_without_files_are_reported_and_skipped(
        self, app, fake_curl, rdf_store
    ):
        ctx = app.config["APP_CONTEXT"]

        success, messages = ctx["upload_func"]("CSV", [{"table_name": "ghost"}])

        assert success is True
        assert messages == ["Missing files for table ghost"]
        assert fake_curl.invocations == []

    def test_a_broken_output_list_is_an_upload_error(self, app, fake_curl):
        success, messages = app.config["APP_CONTEXT"]["upload_func"]("CSV", None)

        assert success is False
        assert messages[0].startswith("Upload error:")


class TestWholeRepositoryUpload:
    """The non-CSV branch of main's upload_func: ontology.owl + output.ttl."""

    @pytest.fixture
    def root(self, app):
        return Path(app.config["APP_CONTEXT"]["root_dir"])

    def test_ontology_then_data_go_to_the_base_graphs(
        self, app, root, fake_curl, rdf_store
    ):
        (root / "ontology.owl").write_text("<rdf:RDF/>")
        (root / "output.ttl").write_text("<a> <b> <c> .\n")

        success, messages = app.config["APP_CONTEXT"]["upload_func"]("Postgres", None)

        assert success is True
        assert [m.split(" (")[0] for m in messages] == [
            "Ontology uploaded",
            "Data upload started",
        ]
        graphs = [u.url.split("graph=")[1] for u in fake_curl.uploads]
        assert graphs == [
            quote("http://ontology.local/", safe=""),
            quote("http://data.local/", safe=""),
        ]
        assert [u.content_type for u in fake_curl.uploads] == [
            "application/rdf+xml",
            "text/turtle",
        ]

    def test_a_failed_ontology_upload_stops_before_the_data(self, app, root, fake_curl):
        (root / "ontology.owl").write_text("<rdf:RDF/>")
        (root / "output.ttl").write_text("<a> <b> <c> .\n")
        fake_curl.fail()

        success, messages = app.config["APP_CONTEXT"]["upload_func"]("Postgres", None)

        assert success is False
        assert messages[0].startswith("Ontology upload failed")
        assert {Path(u.file_path).name for u in fake_curl.uploads} == {"ontology.owl"}

    def test_a_failed_data_upload_is_reported(self, app, root, fake_curl):
        (root / "ontology.owl").write_text("<rdf:RDF/>")
        (root / "output.ttl").write_text("<a> <b> <c> .\n")
        fake_curl.fail_data_binary_only()
        # streaming succeeds for the ontology, then fail everything for the data
        success, messages = app.config["APP_CONTEXT"]["upload_func"]("Postgres", None)
        assert success is True
        fake_curl.fail()

        success, messages = app.config["APP_CONTEXT"]["upload_func"]("Postgres", None)

        assert success is False
        assert messages[0].startswith("Ontology upload failed")

    def test_without_files_nothing_is_uploaded(self, app, fake_curl):
        success, messages = app.config["APP_CONTEXT"]["upload_func"]("Postgres", None)

        assert success is True
        assert messages == [
            "No ontology file found (ontology.owl)",
            "No data file found (output.ttl)",
        ]
        assert fake_curl.invocations == []


# ---------------------------------------------------------------------------
# Background relationship jobs (the greenlets main spawns after an upload)
# ---------------------------------------------------------------------------


class TestBackgroundJobs:
    @pytest.fixture(autouse=True)
    def no_sleep(self, monkeypatch):
        # The jobs wait 3 and 5 seconds for the store to settle; the fake
        # store is ready at once.
        import services.ingest_service as module

        monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    def test_pk_fk_job_writes_the_relationship_and_records_success(
        self, app, session_cache, rdf_store
    ):
        rdf_store.seed_table("patients", {"id": ["P1"]})
        rdf_store.seed_table("visits", {"patient_id": ["P1"]})
        session_cache.pk_fk_data = [
            {"fileName": "patients.csv", "primaryKey": "id"},
            {
                "fileName": "visits.csv",
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "id",
            },
        ]

        IngestService.background_pk_fk_processing(
            session_cache, app.config["APP_CONTEXT"]["rdf_store_service"]
        )

        assert session_cache.pk_fk_status == "success"
        assert len(rdf_store.updates) == 1

    def test_pk_fk_job_records_failure_when_a_column_is_unknown(
        self, app, session_cache, rdf_store
    ):
        rdf_store.seed_table("patients", {"id": ["P1"]})
        session_cache.pk_fk_data = [
            {"fileName": "patients.csv", "primaryKey": "id"},
            {
                "fileName": "visits.csv",
                "foreignKey": "x",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "id",
            },
        ]

        IngestService.background_pk_fk_processing(
            session_cache, app.config["APP_CONTEXT"]["rdf_store_service"]
        )

        assert session_cache.pk_fk_status == "failed"

    def test_incomplete_relationships_are_skipped(self, app, session_cache, rdf_store):
        session_cache.pk_fk_data = [
            {"fileName": "patients.csv", "primaryKey": "id"},
            {"fileName": "visits.csv", "foreignKey": "patient_id"},  # no target table
            {
                "fileName": "labs.csv",
                "foreignKey": "x",
                "foreignKeyTable": "nope.csv",
                "foreignKeyColumn": "id",
            },
        ]

        IngestService.background_pk_fk_processing(
            session_cache, app.config["APP_CONTEXT"]["rdf_store_service"]
        )

        assert session_cache.pk_fk_status == "success"
        assert rdf_store.calls == []

    def test_jobs_without_data_do_nothing(self, app, session_cache, rdf_store):
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        IngestService.background_pk_fk_processing(session_cache, service)
        IngestService.background_cross_graph_processing(session_cache, service)

        assert session_cache.pk_fk_status is None
        assert session_cache.cross_graph_link_status is None
        assert rdf_store.calls == []

    def test_cross_graph_job_records_its_outcome(self, app, session_cache, rdf_store):
        rdf_store.seed_table("patients", {"id": ["P1"]})
        rdf_store.seed_table("visits", {"patient_id": ["P1"]})
        service = app.config["APP_CONTEXT"]["rdf_store_service"]
        session_cache.cross_graph_link_data = {
            "newTableName": "visits",
            "newColumnName": "patient_id",
            "existingTableName": "patients",
            "existingColumnName": "id",
        }

        IngestService.background_cross_graph_processing(session_cache, service)
        assert session_cache.cross_graph_link_status == "success"

        session_cache.cross_graph_link_data["existingColumnName"] = "nope"
        IngestService.background_cross_graph_processing(session_cache, service)
        assert session_cache.cross_graph_link_status == "failed"

    def test_a_crashing_store_marks_the_jobs_failed(
        self, app, session_cache, rdf_store
    ):
        rdf_store.outage = requests.ConnectionError("down")
        service = app.config["APP_CONTEXT"]["rdf_store_service"]
        session_cache.pk_fk_data = [
            {
                "fileName": "visits.csv",
                "foreignKey": "x",
                "foreignKeyTable": "p.csv",
                "foreignKeyColumn": "id",
            },
            {"fileName": "p.csv", "primaryKey": "id"},
        ]
        session_cache.cross_graph_link_data = {
            "newTableName": "v",
            "newColumnName": "x",
            "existingTableName": "p",
            "existingColumnName": "id",
        }

        IngestService.background_pk_fk_processing(session_cache, service)
        IngestService.background_cross_graph_processing(session_cache, service)

        assert session_cache.pk_fk_status == "failed"
        assert session_cache.cross_graph_link_status == "failed"


# ---------------------------------------------------------------------------
# Form parsing edges through the controller
# ---------------------------------------------------------------------------


class TestUploadFormEdges:
    def test_unparseable_relationship_json_is_ignored(
        self, client, session_cache, fake_curl
    ):
        _post_csv(client, pkFkData="{not json", crossGraphLinkData="[also not")

        assert session_cache.pk_fk_data is None
        assert session_cache.cross_graph_link_data is None
        assert session_cache.csvTableNames == [TABLE]

    def test_a_corrupt_spreadsheet_is_a_parse_error(
        self, client, session_cache, fake_curl
    ):
        response = client.post(
            "/upload",
            data={
                "fileType": "Excel",
                "csvFile": (BytesIO(b"not a workbook"), "book.xlsx"),
            },
            content_type="multipart/form-data",
        )

        assert "Error+parsing+Excel+files" in response.headers["Location"]
        assert session_cache.csvData is None

    def test_a_triplifier_failure_is_an_error_redirect(
        self, app, client, session_cache, fake_curl
    ):
        # The triplifier's working directory is blocked by a file of that name
        ctx = app.config["APP_CONTEXT"]
        static_files = Path(ctx["root_dir"]) / ctx["child_dir"] / "static"
        static_files.write_text("in the way")

        response = _post_csv(client)

        assert response.headers["Location"].startswith("/ingest?error=")
        assert "Error+processing+CSV+data" in response.headers["Location"]
        assert session_cache.output_files is None
        assert fake_curl.invocations == []

    def test_legacy_data_submission_route_redirects(self, client):
        response = client.get("/data-submission")

        assert response.status_code == 302 and response.headers["Location"].endswith(
            "/describe"
        )

    def test_a_context_without_a_store_service_builds_one_from_the_config(
        self, app, client, session_cache, seeded_store
    ):
        app.config["APP_CONTEXT"].pop("rdf_store_service")

        body = client.get("/api/rdf-store-databases").get_json()

        assert body["databases"] == [TABLE]
        assert isinstance(
            app.config["APP_CONTEXT"]["rdf_store_service"], RDFStoreService
        )


class TestOrphanCheckOnMalformedShapes:
    """The orphan-column check must survive any shape the validator rejects."""

    @pytest.mark.parametrize(
        "databases",
        [
            {"db": "not a dict"},
            {"db": {"name": "", "tables": {}}},
            {"db": {"name": TABLE, "tables": {"t": "not a dict"}}},
            {"db": {"name": TABLE, "tables": {"t": {"columns": ["nope"]}}}},
            {
                "db": {
                    "name": TABLE,
                    "tables": {"t": {"columns": {"c": "nope", "d": {"mapsTo": "x"}}}},
                }
            },
        ],
    )
    def test_shapes_the_schema_rejects_do_not_crash_the_cross_check(
        self, client, seeded_store, mapping_dict, databases
    ):
        mapping_dict["databases"] = databases

        body = client.post("/api/v1/validate-mapping", json=mapping_dict).get_json()

        assert body["valid"] is False
        assert body["csv_checked"] is True
        assert body["validation_warnings"] == []

    @pytest.mark.parametrize(
        "databases",
        [["not", "a", "dict"], {"db": {"name": TABLE, "tables": ["not", "a", "dict"]}}],
    )
    def test_list_shaped_sections_are_validation_errors_not_crashes(
        self, client, seeded_store, mapping_dict, databases
    ):
        mapping_dict["databases"] = databases

        response = client.post("/api/v1/validate-mapping", json=mapping_dict)

        assert response.status_code == 200
        assert response.get_json()["valid"] is False

    def test_a_table_that_only_has_orphans_is_removed_from_the_cleaned_copy(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"unrelated": ["1"]})

        body = client.post("/api/v1/validate-mapping", json=mapping_dict).get_json()

        assert sorted(body["validation_warnings"][0]["values"]) == ["age", "id", "sex"]
        assert body["cleaned_mapping"]["databases"] == {}


class TestStoreUrlNormalisation:
    @pytest.mark.parametrize(
        "configured",
        [
            "http://rdf-store.test:7200",
            "http://rdf-store.test:7200/",
            "http://rdf-store.test:7200/repositories",
            "http://rdf-store.test:7200/repositories/userRepo",
        ],
    )
    def test_any_documented_base_url_reaches_the_same_endpoint(
        self, seeded_store, configured
    ):
        service = RDFStoreService(configured, "userRepo")

        assert service.get_databases() == [TABLE]
        assert seeded_store.selects[-1].url == seeded_store.repository_url


# ---------------------------------------------------------------------------
# PostgreSQL ingest path (real database; CI service container)
# ---------------------------------------------------------------------------


@pytest.fixture
def postgres(monkeypatch):
    """A PostgreSQL with one small table; the triplifier's env points at it."""
    if not POSTGRES_DSN:
        pytest.skip("FLYOVER_TEST_POSTGRES_DSN is not set")
    parsed = urlparse(POSTGRES_DSN)
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
    except psycopg2.OperationalError as exc:  # pragma: no cover - environment
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    with conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS patients")
        cur.execute("CREATE TABLE patients (id text, sex text, age text)")
        cur.execute("INSERT INTO patients VALUES ('P1', 'M', '45'), ('P2', 'F', '60')")
    conn.close()
    monkeypatch.setenv(
        "TRIPLIFIER_DB_URL",
        f"postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}",
    )
    monkeypatch.setenv("TRIPLIFIER_DB_USER", parsed.username)
    monkeypatch.setenv("TRIPLIFIER_DB_PASSWORD", parsed.password)
    # The form's POSTGRES_URL is used as the host only (no port), so libpq
    # must learn a non-default port from its environment.
    monkeypatch.setenv("PGPORT", str(parsed.port or 5432))
    return parsed


def _postgres_form(parsed, password=None, table="patients"):
    return {
        "fileType": "Postgres",
        "username": parsed.username,
        "password": parsed.password if password is None else password,
        "POSTGRES_URL": parsed.hostname,
        "POSTGRES_DB": parsed.path.lstrip("/"),
        "table": table,
    }


class TestPostgresIngest:
    def test_tables_are_triplified_one_sqlite_at_a_time(
        self, app, client, session_cache, postgres, fake_curl
    ):
        response = client.post("/upload", data=_postgres_form(postgres))

        assert response.status_code == 302, response.headers.get("Location")
        assert response.headers["Location"].endswith("/describe")
        (output,) = session_cache.output_files
        assert output["table_name"] == "patients"
        data = Path(output["data_file"]).read_text()
        assert "/rdf/ontology/patients.sex" in data
        assert '"M"' in data and '"45"' in data
        ctx = app.config["APP_CONTEXT"]
        properties = (
            Path(ctx["root_dir"]) / ctx["child_dir"] / "triplifierSQL.properties"
        )
        assert f"jdbc.user = {postgres.username}" in properties.read_text()

    @pytest.mark.xfail(
        strict=True,
        reason="The Postgres branch of upload_func uploads ontology.owl/output.ttl from the root "
        "directory, but the SQL triplifier writes ontology_<table>.owl/output_<table>.ttl per table, "
        "so a Postgres ingest never reaches the store",
    )
    def test_the_triplified_tables_are_uploaded(self, client, postgres, fake_curl):
        client.post("/upload", data=_postgres_form(postgres))

        assert len(fake_curl.uploads) == 2

    @pytest.mark.xfail(
        strict=True,
        reason="The form's connection details only end up in triplifierSQL.properties (a Java "
        "triplifier leftover); the Python triplifier reads TRIPLIFIER_DB_URL/USER/PASSWORD from the "
        "environment instead, so without those variables the ingest fails although the form is correct",
    )
    def test_the_form_credentials_drive_the_triplifier(
        self, client, monkeypatch, postgres, session_cache
    ):
        for name in (
            "TRIPLIFIER_DB_URL",
            "TRIPLIFIER_DB_USER",
            "TRIPLIFIER_DB_PASSWORD",
        ):
            monkeypatch.delenv(name)

        response = client.post("/upload", data=_postgres_form(postgres))

        assert response.headers["Location"].endswith("/describe")

    def test_wrong_form_credentials_are_logged_but_do_not_stop_the_ingest(
        self, client, postgres, session_cache
    ):
        response = client.post(
            "/upload", data=_postgres_form(postgres, password="wrong")
        )

        # handle_postgres_connection fails; the controller ignores its result
        # and the env-driven triplifier still runs
        assert response.headers["Location"].endswith("/describe")
        assert session_cache.output_files

    def test_an_empty_database_has_nothing_to_triplify(self, client, postgres):
        conn = psycopg2.connect(POSTGRES_DSN)
        with conn, conn.cursor() as cur:
            cur.execute("DROP TABLE patients")
        conn.close()

        response = client.post("/upload", data=_postgres_form(postgres))

        assert "No+tables+found" in response.headers["Location"]

    def test_an_empty_table_is_skipped(self, client, postgres, session_cache):
        conn = psycopg2.connect(POSTGRES_DSN)
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM patients")
        conn.close()

        response = client.post("/upload", data=_postgres_form(postgres))

        assert "No+tables+were+successfully+processed" in response.headers["Location"]


# ---------------------------------------------------------------------------
# Cross-check degradation, triplifier argument checks, name cleaning
# ---------------------------------------------------------------------------


class TestCrossCheckDegradation:
    def test_a_store_http_error_skips_the_cross_check_but_keeps_the_verdict(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["1"]})
        rdf_store.select_status = 500

        body = client.post("/api/v1/validate-mapping", json=mapping_dict).get_json()

        assert body["valid"] is True
        assert body["csv_checked"] is False
        assert body["databases_checked"] == []

    def test_a_store_http_error_still_lets_a_valid_map_be_adopted(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        import json

        rdf_store.seed_table(TABLE, {"id": ["1"]})
        rdf_store.select_status = 500

        response = client.post(
            "/upload-semantic-map",
            data={
                "semanticMapFile": (
                    BytesIO(json.dumps(mapping_dict).encode()),
                    "m.jsonld",
                )
            },
            content_type="multipart/form-data",
        )

        assert response.status_code == 200
        assert (
            session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") == "age"
        )

    def test_a_loaded_database_without_tables_is_checked_but_yields_nothing(
        self, client, seeded_store, mapping_dict
    ):
        mapping_dict["databases"] = {"db": {"name": TABLE, "tables": {}}}

        body = client.post("/api/v1/validate-mapping", json=mapping_dict).get_json()

        assert body["databases_checked"] == [TABLE]
        assert body["validation_warnings"] == []

    def test_the_orphan_helpers_tolerate_nothing_to_do(self, mapping_dict):
        from controllers.ingest_controller import (
            _collect_orphan_column_issues,
            _remove_orphan_columns_from_mapping,
        )

        assert _collect_orphan_column_issues(mapping_dict, {}, []) == ([], [], [])
        assert _remove_orphan_columns_from_mapping(mapping_dict, []) is mapping_dict
        odd = {"databases": ["not", "a", "dict"]}
        assert _remove_orphan_columns_from_mapping(odd, [("db", "t", "c", "x")]) is odd


class TestTriplifierArguments:
    def test_an_unknown_properties_file_is_refused_at_both_layers(self, app):
        from services.conversion_service import run_triplifier

        ctx = app.config["APP_CONTEXT"]
        assert run_triplifier(
            "weird.properties", ctx["root_dir"], ctx["child_dir"]
        ) == (
            False,
            "Unknown properties file: weird.properties",
            [],
        )
        assert IngestService().run_triplifier(
            "weird.properties", ctx["root_dir"], ctx["child_dir"]
        ) == (
            False,
            "Unknown properties file: weird.properties",
            None,
        )

    def test_the_csv_path_needs_data_and_names(self, app):
        from services.conversion_service import run_triplifier

        ctx = app.config["APP_CONTEXT"]

        assert (
            run_triplifier(
                "triplifierCSV.properties", ctx["root_dir"], ctx["child_dir"]
            )[0]
            is False
        )

    def test_a_chosen_base_uri_names_the_generated_resources(self, app):
        import polars as pl

        from services.conversion_service import PythonTriplifierIntegration

        ctx = app.config["APP_CONTEXT"]
        frame = pl.DataFrame({"id": ["P1"], "sex": ["M"]})

        success, _, files = PythonTriplifierIntegration(
            ctx["root_dir"], ctx["child_dir"]
        ).run_triplifier_csv(
            [frame], ["patients"], base_uri="http://example.org/flyover/"
        )

        assert success is True
        assert "http://example.org/flyover/" in Path(files[0]["data_file"]).read_text()

    @pytest.mark.parametrize(
        "env, expected",
        [
            ({}, "Database URL is required"),
            ({"TRIPLIFIER_DB_URL": "postgresql://h/db"}, "Database user is required"),
            (
                {"TRIPLIFIER_DB_URL": "postgresql://h/db", "TRIPLIFIER_DB_USER": "u"},
                "Database password is required",
            ),
        ],
    )
    def test_the_sql_path_names_the_missing_connection_setting(
        self, app, monkeypatch, env, expected
    ):
        from services.conversion_service import run_triplifier

        for name in (
            "TRIPLIFIER_DB_URL",
            "TRIPLIFIER_DB_USER",
            "TRIPLIFIER_DB_PASSWORD",
        ):
            monkeypatch.delenv(name, raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        ctx = app.config["APP_CONTEXT"]

        success, message, files = run_triplifier(
            "triplifierSQL.properties", ctx["root_dir"], ctx["child_dir"]
        )

        assert success is False and files == []
        assert expected in message

    def test_an_unreachable_database_is_an_error_not_a_crash(self, app, monkeypatch):
        from services.conversion_service import run_triplifier

        # Credentials inside the URL take the '@' branch; nothing listens on the port
        monkeypatch.setenv("TRIPLIFIER_DB_URL", "postgresql://u:p@127.0.0.1:1/nowhere")
        monkeypatch.setenv("TRIPLIFIER_DB_USER", "u")
        monkeypatch.setenv("TRIPLIFIER_DB_PASSWORD", "p")
        ctx = app.config["APP_CONTEXT"]

        success, message, files = run_triplifier(
            "triplifierSQL.properties", ctx["root_dir"], ctx["child_dir"]
        )

        assert success is False and files == []
        assert "Error processing PostgreSQL data" in message


class TestNameCleaning:
    def test_blank_symbol_only_and_colliding_headers_get_safe_unique_names(
        self, client, session_cache, fake_curl
    ):
        _post_csv(client, b"id,,___,a b,a_b\n1,2,3,4,5\n")

        assert session_cache.csvData[0].columns == [
            "id",
            "column_2",
            "column_3",
            "a_b",
            "a_b_1",
        ]

    @pytest.mark.parametrize(
        "filename, table",
        # werkzeug's secure_filename runs first (it strips the underscores and
        # transliterates the umlaut), then the table-name sanitiser
        [
            ("2024 export.csv", "tbl_2024_export"),
            ("___.csv", "csv"),
            ("Größe.csv", "Groe"),
        ],
    )
    def test_table_names_are_made_safe_for_sql_and_iris(
        self, client, session_cache, fake_curl, filename, table
    ):
        _post_csv(client, filename=filename)

        assert session_cache.output_files[0]["table_name"] == table

    def test_a_file_without_an_extension_is_rejected(
        self, client, session_cache, fake_curl
    ):
        response = _post_csv(client, filename="patients")

        assert response.headers["Location"].startswith("/ingest?error=")
        assert session_cache.csvData is None


class TestMappingShapeHelpers:
    def test_table_names_come_only_from_well_formed_source_files(self):
        shapes = {
            "databases": {
                "a": "not a dict",
                "b": {"tables": "not a dict"},
                "c": {
                    "tables": {
                        "t": "not a dict",
                        "u": {"sourceFile": ""},
                        "v": {"sourceFile": "visits"},
                    }
                },
            }
        }

        assert IngestService.get_table_names_from_jsonld(shapes) == ["visits"]
        assert IngestService.get_table_names_from_jsonld({"databases": []}) == []
        assert IngestService.is_jsonld_semantic_map("not a dict") is False
        assert IngestService.parse_pk_fk_data("") is None
        assert IngestService.parse_cross_graph_data(None) is None
