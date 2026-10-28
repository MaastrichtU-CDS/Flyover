"""
Integration tests for the ingest flow (issue #108, key integration point 1).

    ingest_controller -> ingest_service -> conversion_service (triplifier)
                      -> rdf_store_service -> rdf_store_repository -> HTTP

The triplifier runs for real on a temporary directory. The graph upload
reaches a fake ``curl`` on PATH, and every SPARQL request reaches the fake
RDF store at the ``requests`` level. Nothing inside ``data_descriptor`` is
mocked.
"""

import json
from io import BytesIO
from pathlib import Path
from urllib.parse import quote

import pytest
import requests

from tests.integration.support import (
    CSV_BYTES,
    TABLE,
    load_validation_fixture,
    xlsx_bytes,
)
from loaders import JSONLDMapping
from services import IngestService


def _post_csv(client, content: bytes, filename: str, **form):
    data = {"fileType": "CSV", "csvFile": (BytesIO(content), filename), **form}
    return client.post("/upload", data=data, content_type="multipart/form-data")


def _post_mapping(client, mapping: dict, filename: str = "mapping.jsonld"):
    data = {"semanticMapFile": (BytesIO(json.dumps(mapping).encode()), filename)}
    return client.post(
        "/upload-semantic-map", data=data, content_type="multipart/form-data"
    )


# ---------------------------------------------------------------------------
# CSV upload: controller -> service -> triplifier -> upload
# ---------------------------------------------------------------------------


class TestCsvUploadFlow:
    def test_csv_upload_triplifies_the_file_and_uploads_both_graphs(
        self, app, client, session_cache, fake_curl, rdf_store
    ):
        # Act
        response = _post_csv(client, CSV_BYTES, f"{TABLE}.csv")

        # Assert: the controller finished the step
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/describe")
        assert "triplified successfully" in session_cache.StatusToDisplay

        # The service parsed the CSV into the session with cleaned column names
        assert session_cache.csvTableNames == [TABLE]
        assert session_cache.csvData[0].columns == ["id", "sex", "age"]
        assert session_cache.csvData[0].height == 3

        # The triplifier produced one ontology and one data file under root_dir
        root_dir = app.config["APP_CONTEXT"]["root_dir"]
        (output,) = session_cache.output_files
        assert output["table_name"] == TABLE
        ontology = Path(output["ontology_file"])
        data = Path(output["data_file"])
        assert ontology.parent == Path(root_dir) and data.parent == Path(root_dir)
        assert "<rdf:RDF" in ontology.read_text()
        assert f"/rdf/ontology/{TABLE}.sex" in data.read_text()
        assert (
            "<http://um-cds/ontologies/databaseontology/has_cell>" in data.read_text()
        )

        # Both files went to the Graph Store Protocol endpoint, ontology first,
        # each into its own named graph, with the right media type.
        uploads = fake_curl.uploads
        assert [
            (u.content_type, Path(u.file_path).name, u.method) for u in uploads
        ] == [
            ("application/rdf+xml", f"ontology_{TABLE}.owl", "data-binary"),
            ("text/turtle", f"output_{TABLE}.ttl", "data-binary"),
        ]
        graph_store = rdf_store.graph_store_url
        assert (
            uploads[0].url
            == f"{graph_store}?graph={quote(f'http://ontology.local/{TABLE}/', safe='')}"
        )
        assert (
            uploads[1].url
            == f"{graph_store}?graph={quote(f'http://data.local/{TABLE}/', safe='')}"
        )

        # The relationship jobs were handed the session, and no SPARQL request
        # was needed for the upload itself.
        assert app.config["BACKGROUND_JOBS"] == [session_cache]
        assert rdf_store.calls == []

    def test_failed_ontology_upload_falls_back_to_streaming_and_skips_the_data(
        self, client, session_cache, fake_curl
    ):
        # Arrange: the store rejects every upload
        fake_curl.fail(exit_code=22)

        # Act
        response = _post_csv(client, CSV_BYTES, f"{TABLE}.csv")

        # Assert: the upload service tried both curl strategies for the
        # ontology and did not attempt the data file after they failed.
        assert response.status_code == 302
        attempts = [(Path(u.file_path).name, u.method) for u in fake_curl.uploads]
        assert attempts == [
            (f"ontology_{TABLE}.owl", "data-binary"),
            (f"ontology_{TABLE}.owl", "streaming"),
        ]
        # The triplifier output is still available for a retry
        assert len(session_cache.output_files) == 1

    def test_non_csv_upload_is_rejected_before_triplification(
        self, client, session_cache, fake_curl
    ):
        response = _post_csv(client, CSV_BYTES, "patients.txt")

        assert response.status_code == 302
        location = response.headers["Location"]
        assert location.startswith("/ingest?error=")
        assert "csv" in location.lower()
        assert session_cache.csvData is None
        assert session_cache.output_files is None
        assert fake_curl.invocations == []

    def test_unparseable_csv_redirects_with_the_parse_error(
        self, client, session_cache, fake_curl
    ):
        response = _post_csv(client, b"", f"{TABLE}.csv")

        assert response.status_code == 302
        assert "Error+parsing+CSV+files" in response.headers["Location"]
        assert session_cache.csvData is None
        assert fake_curl.invocations == []

    def test_relationship_data_from_the_form_lands_in_the_session(
        self, client, session_cache, fake_curl
    ):
        pk_fk = '[{"fileName": "patients.csv", "primaryKey": "id"}]'
        cross = (
            '{"newTableName": "patients", "newColumnName": "id",'
            ' "existingTableName": "visits", "existingColumnName": "patient_id"}'
        )

        _post_csv(
            client, CSV_BYTES, f"{TABLE}.csv", pkFkData=pk_fk, crossGraphLinkData=cross
        )

        assert session_cache.pk_fk_data == [
            {"fileName": "patients.csv", "primaryKey": "id"}
        ]
        assert session_cache.cross_graph_link_data["existingTableName"] == "visits"


# ---------------------------------------------------------------------------
# Semantic map upload: controller -> store columns -> validator -> loader
# ---------------------------------------------------------------------------


class TestSemanticMapUploadFlow:
    def test_valid_mapping_is_checked_against_the_store_and_adopted(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        response = _post_mapping(client, mapping_dict)

        assert response.status_code == 200, response.get_json()
        body = response.get_json()
        assert body["success"] is True
        assert body["statistics"]["databases"] == 1
        assert body["statistics"]["tables"] == 1
        assert body["statistics"]["columns"] == 3
        assert body["statistics"]["variables"] == 3
        columns = body["cleaned_mapping"]["databases"]["centre_x"]["tables"][TABLE][
            "columns"
        ]
        assert set(columns) == {"identifier", "biological_sex", "age_at_diagnosis"}

        # The loader produced a real mapping object in the session
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)
        assert session_cache.jsonld_mapping.get_local_column("biological_sex") == "sex"
        assert (
            session_cache.jsonld_mapping.get_local_term("biological_sex", "female")
            == "F"
        )

        # The column check asked the store for its column info, as CSV
        (select,) = seeded_store.selects
        assert "dbo:column ?column" in select.query
        assert select.accept == "text/csv"

    def test_column_missing_from_the_store_is_dropped_from_the_adopted_mapping(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        # Arrange: the ingested table never had an age column
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})

        response = _post_mapping(client, mapping_dict)

        assert response.status_code == 200
        cleaned = response.get_json()["cleaned_mapping"]
        columns = cleaned["databases"]["centre_x"]["tables"][TABLE]["columns"]
        assert set(columns) == {"identifier", "biological_sex"}
        assert session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") is None
        assert session_cache.jsonld_mapping.get_local_column("biological_sex") == "sex"

    def test_mapping_for_a_database_not_in_the_store_is_adopted_untouched(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        # Arrange: the store holds a different table; the mapping's database
        # is not loaded, so its columns must not be checked at all.
        rdf_store.seed_table("other_table", {"foo": ["1"]})

        response = _post_mapping(client, mapping_dict)

        assert response.status_code == 200
        columns = response.get_json()["cleaned_mapping"]["databases"]["centre_x"][
            "tables"
        ][TABLE]["columns"]
        assert len(columns) == 3
        assert (
            session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") == "age"
        )

    def test_invalid_mapping_is_rejected_and_the_session_is_untouched(
        self, client, session_cache, seeded_store
    ):
        response = _post_mapping(
            client, load_validation_fixture("invalid_data_type.jsonld")
        )

        assert response.status_code == 400
        body = response.get_json()
        assert body["error"] == "Mapping file validation failed"
        assert any("dataType" in e["path"] for e in body["validation_errors"])
        assert session_cache.jsonld_mapping is None

    def test_store_outage_does_not_block_adopting_a_valid_mapping(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        # Arrange: the column cross-check is best effort
        rdf_store.outage = requests.ConnectionError("store down")

        response = _post_mapping(client, mapping_dict)

        assert response.status_code == 200
        assert (
            session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") == "age"
        )
        assert len(rdf_store.calls) >= 1  # the check was attempted


# ---------------------------------------------------------------------------
# Store queries through the ingest API: controller -> service -> repository
# ---------------------------------------------------------------------------


class TestStoreQueriesThroughIngestApi:
    def test_database_list_comes_from_the_store_and_fills_the_session(
        self, client, session_cache, seeded_store
    ):
        response = client.get("/api/rdf-store-databases")

        assert response.get_json() == {"success": True, "databases": [TABLE]}
        assert session_cache.databases == [TABLE]
        (select,) = seeded_store.selects
        assert select.method == "POST" and select.url == seeded_store.repository_url
        assert "dbo:table ?db" in select.query
        assert "rdfs:subClassOf dbo:TableRow" in select.query

    def test_database_list_is_empty_when_the_store_is_unreachable(
        self, client, session_cache, rdf_store
    ):
        rdf_store.outage = requests.ConnectionError("store down")

        response = client.get("/api/rdf-store-databases")

        assert response.status_code == 200
        assert response.get_json()["success"] is False
        assert response.get_json()["databases"] == []
        assert session_cache.databases == []

    @pytest.mark.parametrize("exists", [True, False])
    def test_graph_existence_is_an_ask_query_over_the_data_graphs(
        self, client, rdf_store, exists
    ):
        rdf_store.data_graph_exists = exists

        response = client.get("/api/check-graph-exists")

        assert response.get_json() == {"exists": exists}
        (ask,) = rdf_store.asks
        assert ask.method == "GET"
        assert 'STRSTARTS(STR(?g), "http://data.local/")' in ask.query
        assert "application/sparql-results+json" in ask.accept

    def test_graph_structure_groups_store_columns_per_table(self, client, rdf_store):
        rdf_store.seed_table("patients", {"id": [], "sex": []})
        rdf_store.seed_table("visits", {"patient_id": []})

        response = client.get("/get-existing-graph-structure")

        body = response.get_json()
        assert sorted(body["tables"]) == ["patients", "visits"]
        assert body["tableColumns"] == {
            "patients": ["id", "sex"],
            "visits": ["patient_id"],
        }


# ---------------------------------------------------------------------------
# Relationship jobs: ingest_service -> rdf_store_service -> repository -> HTTP
# ---------------------------------------------------------------------------


class TestRelationshipProcessing:
    """The PK/FK and cross-graph jobs main runs in the background."""

    @pytest.fixture
    def two_tables(self, rdf_store):
        rdf_store.seed_table("patients", {"id": ["P1"]})
        rdf_store.seed_table("visits", {"patient_id": ["P1"], "date": ["2024"]})
        return rdf_store

    def test_fk_relationship_resolves_both_column_uris_and_inserts_the_link(
        self, app, two_tables
    ):
        rdf_store_service = app.config["APP_CONTEXT"]["rdf_store_service"]
        pk_fk_data = [
            {"fileName": "patients.csv", "primaryKey": "id"},
            {
                "fileName": "visits.csv",
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "id",
            },
        ]

        success, messages = IngestService.process_pk_fk_relationships(
            pk_fk_data, rdf_store_service
        )

        assert success is True
        assert messages == ["Created FK relationship: visits.patient_id -> patients.id"]
        # Two URI lookups, then one update into the relationships graph
        assert [c.kind for c in two_tables.calls] == ["select", "select", "update"]
        (update,) = two_tables.updates
        assert "GRAPH <http://relationships.local/>" in update
        assert "<http://um-cds/ontologies/databaseontology/fk_refers_to>" in update
        assert f"<{two_tables.column_uri('visits', 'patient_id')}>" in update
        assert f"<{two_tables.column_uri('patients', 'id')}>" in update

    def test_fk_relationship_is_not_inserted_when_a_column_is_unknown(
        self, app, two_tables
    ):
        rdf_store_service = app.config["APP_CONTEXT"]["rdf_store_service"]
        pk_fk_data = [
            {"fileName": "patients.csv", "primaryKey": "id"},
            {
                "fileName": "visits.csv",
                "foreignKey": "no_such_column",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "id",
            },
        ]

        success, messages = IngestService.process_pk_fk_relationships(
            pk_fk_data, rdf_store_service
        )

        assert success is False
        assert messages == [
            "Failed FK relationship: visits.no_such_column -> patients.id"
        ]
        assert two_tables.updates == []

    def test_cross_graph_relationship_links_the_new_table_to_the_existing_one(
        self, app, two_tables
    ):
        rdf_store_service = app.config["APP_CONTEXT"]["rdf_store_service"]
        cross_graph = {
            "newTableName": "visits.csv",
            "newColumnName": "patient_id",
            "existingTableName": "patients",
            "existingColumnName": "id",
        }

        success, message = IngestService.process_cross_graph_relationships(
            cross_graph, rdf_store_service
        )

        assert success is True
        assert (
            message
            == "Created cross-graph relationship: visits.patient_id -> patients.id"
        )
        (update,) = two_tables.updates
        assert f"<{two_tables.column_uri('visits', 'patient_id')}>" in update
        assert f"<{two_tables.column_uri('patients', 'id')}>" in update

    def test_update_rejected_by_the_store_is_reported_as_failure(self, app, two_tables):
        # Arrange: the store cannot run the update (e.g. read-only repository)
        two_tables.outage = requests.ConnectionError("store down")
        rdf_store_service = app.config["APP_CONTEXT"]["rdf_store_service"]

        success, _ = IngestService.process_cross_graph_relationships(
            {
                "newTableName": "visits",
                "newColumnName": "patient_id",
                "existingTableName": "patients",
                "existingColumnName": "id",
            },
            rdf_store_service,
        )

        assert success is False


# ---------------------------------------------------------------------------
# CSV / spreadsheet upload variants
# ---------------------------------------------------------------------------


class TestUploadVariants:
    def test_each_csv_in_a_batch_gets_its_own_table_and_graphs(
        self, client, session_cache, fake_curl, rdf_store
    ):
        response = client.post(
            "/upload",
            data={
                "fileType": "CSV",
                "csvFile": [
                    (BytesIO(CSV_BYTES), "patients.csv"),
                    (BytesIO(b"patient_id,date\nP1,2024-01-01\n"), "visits.csv"),
                ],
            },
            content_type="multipart/form-data",
        )

        assert response.status_code == 302
        assert session_cache.csvTableNames == ["patients", "visits"]
        assert [o["table_name"] for o in session_cache.output_files] == [
            "patients",
            "visits",
        ]
        graphs = [u.url.split("graph=")[1] for u in fake_curl.uploads]
        assert graphs == [
            quote(g, safe="")
            for g in (
                "http://ontology.local/patients/",
                "http://data.local/patients/",
                "http://ontology.local/visits/",
                "http://data.local/visits/",
            )
        ]

    def test_custom_separator_and_decimal_sign_are_applied_before_triplification(
        self, client, session_cache, fake_curl
    ):
        _post_csv(
            client,
            b"id;sex;age\nP1;M;45,5\nP2;F;60,0\n",
            f"{TABLE}.csv",
            csv_separator_sign=";",
            csv_decimal_sign=",",
        )

        frame = session_cache.csvData[0]
        assert frame.columns == ["id", "sex", "age"]
        assert frame["age"].to_list() == ["45.5", "60.0"]
        assert '"45.5"' in Path(session_cache.output_files[0]["data_file"]).read_text()

    def test_invalid_separator_falls_back_to_a_comma(
        self, client, session_cache, fake_curl
    ):
        _post_csv(
            client,
            CSV_BYTES,
            f"{TABLE}.csv",
            csv_separator_sign="  ",
            csv_decimal_sign="",
        )

        assert session_cache.csvData[0].columns == ["id", "sex", "age"]

    def test_column_names_are_cleaned_before_they_become_class_names(
        self, client, session_cache, fake_curl
    ):
        _post_csv(
            client,
            b"Patient ID,age (years),1st visit,Gr\xc3\xb6\xc3\x9fe\nP1,45,2024,180\n",
            f"{TABLE}.csv",
        )

        assert session_cache.csvData[0].columns == [
            "Patient_ID",
            "age_years",
            "col_1st_visit",
            "Gr_e",
        ]
        data = Path(session_cache.output_files[0]["data_file"]).read_text()
        assert f"/rdf/ontology/{TABLE}.Patient_ID" in data
        assert f"/rdf/ontology/{TABLE}.col_1st_visit" in data
        assert "Patient ID" not in data

    def test_table_name_is_sanitised_for_the_named_graphs(
        self, client, session_cache, fake_curl
    ):
        _post_csv(client, CSV_BYTES, "My Data-2024.csv")

        assert session_cache.csvTableNames == ["My_Data-2024"]  # secure_filename only
        assert session_cache.output_files[0]["table_name"] == "My_Data_2024"
        assert fake_curl.uploads[0].url.endswith(
            quote("http://ontology.local/My_Data_2024/", safe="")
        )

    def test_spreadsheet_upload_makes_one_table_per_sheet(
        self, client, session_cache, fake_curl
    ):
        workbook = xlsx_bytes(
            {
                "patients": [
                    ["id", "sex", "age"],
                    ["P1", "M", "45"],
                    ["P2", "F", "60"],
                ],
                "visits": [["patient_id", "date"], ["P1", "2024-01-01"]],
            }
        )

        response = client.post(
            "/upload",
            data={"fileType": "Excel", "csvFile": (BytesIO(workbook), "book.xlsx")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 302
        assert session_cache.csvTableNames == ["book_patients", "book_visits"]
        assert session_cache.csvData[0]["age"].to_list() == ["45", "60"]
        assert [o["table_name"] for o in session_cache.output_files] == [
            "book_patients",
            "book_visits",
        ]
        assert len(fake_curl.uploads) == 4

    def test_spreadsheet_with_wrong_extension_is_rejected(
        self, client, session_cache, fake_curl
    ):
        response = client.post(
            "/upload",
            data={"fileType": "Excel", "csvFile": (BytesIO(CSV_BYTES), "book.csv")},
            content_type="multipart/form-data",
        )

        assert response.headers["Location"].startswith("/ingest?error=")
        assert session_cache.output_files is None

    def test_no_file_means_no_new_data_and_no_upload(
        self, app, client, session_cache, fake_curl, rdf_store
    ):
        response = client.post(
            "/upload", data={"fileType": "CSV"}, content_type="multipart/form-data"
        )

        assert response.status_code == 302
        assert response.headers["Location"].endswith("/describe")
        assert "opted to not submit any new data" in session_cache.StatusToDisplay
        assert fake_curl.invocations == []
        assert app.config["BACKGROUND_JOBS"] == []
        assert rdf_store.calls == []

    def test_file_part_without_a_name_is_not_new_data_either(
        self, client, session_cache, fake_curl
    ):
        # A form that sends the file field with no file selected
        response = client.post(
            "/upload",
            data={"fileType": "CSV", "csvFile": (BytesIO(b""), "")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 302
        assert response.headers["Location"].startswith("/ingest?error=")
        assert fake_curl.invocations == []

    def test_batch_with_one_bad_file_is_rejected_entirely(
        self, client, session_cache, fake_curl
    ):
        response = client.post(
            "/upload",
            data={
                "fileType": "CSV",
                "csvFile": [
                    (BytesIO(CSV_BYTES), "patients.csv"),
                    (BytesIO(b"x"), "notes.txt"),
                ],
            },
            content_type="multipart/form-data",
        )

        assert response.headers["Location"].startswith("/ingest?error=")
        assert session_cache.csvData is None
        assert fake_curl.invocations == []

    def test_unknown_file_type_is_an_error(self, client, fake_curl):
        response = client.post(
            "/upload",
            data={"fileType": "XML", "csvFile": (BytesIO(b"<x/>"), "data.xml")},
            content_type="multipart/form-data",
        )

        assert "unexpected+error" in response.headers["Location"]
        assert fake_curl.invocations == []

    def test_postgres_without_connection_details_is_an_error(
        self, client, monkeypatch, fake_curl
    ):
        for name in (
            "TRIPLIFIER_DB_URL",
            "TRIPLIFIER_DB_USER",
            "TRIPLIFIER_DB_PASSWORD",
        ):
            monkeypatch.delenv(name, raising=False)

        response = client.post("/upload", data={"fileType": "Postgres"})

        assert response.status_code == 302
        assert "Database+URL+is+required" in response.headers["Location"]
        assert fake_curl.invocations == []


# ---------------------------------------------------------------------------
# Semantic map upload variants
# ---------------------------------------------------------------------------


class TestSemanticMapUploadVariants:
    def test_json_extension_is_rejected(self, client, seeded_store, mapping_dict):
        response = _post_mapping(client, mapping_dict, "mapping.json")

        assert response.status_code == 400
        assert ".jsonld" in response.get_json()["error"]

    def test_malformed_jsonld_reports_the_position(
        self, client, seeded_store, session_cache
    ):
        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": (BytesIO(b'{"name": "x",}'), "mapping.jsonld")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 400
        body = response.get_json()
        assert "line 1" in body["error"]
        assert body["validation_errors"][0]["path"].startswith("(line 1")
        assert session_cache.jsonld_mapping is None

    def test_only_loaded_databases_are_cross_checked(
        self, client, rdf_store, mapping_dict
    ):
        # patients is loaded without an age column; visits is not loaded at all
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})
        other = json.loads(json.dumps(mapping_dict["databases"]["centre_x"]))
        other["name"] = "visits"
        other["tables"] = {"visits": dict(other["tables"][TABLE], sourceFile="visits")}
        mapping_dict["databases"]["centre_y"] = other

        cleaned = _post_mapping(client, mapping_dict).get_json()["cleaned_mapping"]

        assert set(cleaned["databases"]["centre_x"]["tables"][TABLE]["columns"]) == {
            "identifier",
            "biological_sex",
        }
        assert len(cleaned["databases"]["centre_y"]["tables"]["visits"]["columns"]) == 3

    def test_csv_suffixed_database_name_counts_as_loaded(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})
        mapping_dict["databases"]["centre_x"]["name"] = f"{TABLE}.csv"
        mapping_dict["databases"]["centre_x"]["tables"][TABLE][
            "sourceFile"
        ] = f"{TABLE}.csv"

        cleaned = _post_mapping(client, mapping_dict).get_json()["cleaned_mapping"]

        assert (
            "age_at_diagnosis"
            not in cleaned["databases"]["centre_x"]["tables"][TABLE]["columns"]
        )

    def test_table_whose_columns_are_all_unknown_is_removed_with_its_database(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"unrelated": ["1"]})

        response = _post_mapping(client, mapping_dict)

        # Every column was an orphan, so the table and then the database went
        assert response.status_code == 200, response.get_json()
        assert response.get_json()["cleaned_mapping"]["databases"] == {}
        assert session_cache.jsonld_mapping.get_all_tables() == []


class TestIndexedDbSubmission:
    def test_valid_mapping_from_the_browser_is_adopted(
        self, client, session_cache, mapping_dict
    ):
        response = client.post("/submit-indexeddb-semantic-map", json=mapping_dict)

        assert response.status_code == 200
        body = response.get_json()
        assert body["redirect_url"] == "/annotate/review"
        assert body["statistics"]["variables"] == 3
        assert session_cache.jsonld_mapping.get_local_column("biological_sex") == "sex"

    def test_invalid_mapping_from_the_browser_is_rejected(self, client, session_cache):
        response = client.post(
            "/submit-indexeddb-semantic-map",
            json=load_validation_fixture("invalid_type.jsonld"),
        )

        assert response.status_code == 400
        assert response.get_json()["error"] == "Semantic map validation failed"
        assert session_cache.jsonld_mapping is None

    def test_empty_submission_is_rejected(self, client):
        response = client.post("/submit-indexeddb-semantic-map", json={})

        assert response.status_code == 400
        assert response.get_json()["error"] == "No semantic map data provided"


class TestValidateMappingApi:
    def test_valid_mapping_is_cross_checked_against_the_store(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        body = client.post("/api/v1/validate-mapping", json=mapping_dict).get_json()

        assert body["valid"] is True
        assert body["validation_errors"] == []
        assert body["validation_warnings"] == []
        assert body["csv_checked"] is True
        assert body["databases_checked"] == [TABLE]
        assert body["cleaned_mapping"] is None
        assert session_cache.jsonld_mapping is None

    def test_unknown_columns_are_warnings_with_a_cleaned_copy(
        self, client, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})

        body = client.post(
            "/api/v1/validate-mapping", json={"mapping": mapping_dict}
        ).get_json()

        assert body["valid"] is True
        (warning,) = body["validation_warnings"]
        assert warning["severity"] == "warning"
        assert warning["values"] == ["age"]
        assert f"{TABLE}: age" in warning["message"]
        assert (
            "age_at_diagnosis"
            not in body["cleaned_mapping"]["databases"]["centre_x"]["tables"][TABLE][
                "columns"
            ]
        )

    def test_schema_errors_are_blocking_but_the_store_is_still_checked(
        self, client, seeded_store
    ):
        body = client.post(
            "/api/v1/validate-mapping",
            json=load_validation_fixture("invalid_data_type.jsonld"),
        ).get_json()

        assert body["valid"] is False
        assert body["validation_errors"]
        assert body["csv_checked"] is True

    def test_store_outage_skips_the_cross_check(self, client, rdf_store, mapping_dict):
        rdf_store.outage = requests.ConnectionError("store down")

        body = client.post("/api/v1/validate-mapping", json=mapping_dict).get_json()

        assert body["valid"] is True
        assert body["csv_checked"] is False
        assert body["databases_checked"] == []

    @pytest.mark.parametrize("payload", [{}, [], "text"])
    def test_non_mapping_bodies_are_client_errors(self, client, payload):
        response = client.post("/api/v1/validate-mapping", json=payload)

        assert response.status_code == 400


class TestStoreChecksEdgeCases:
    def test_store_error_on_the_existence_ask_reads_as_no_graph(
        self, client, rdf_store
    ):
        rdf_store.ask_status = 500

        assert client.get("/api/check-graph-exists").get_json() == {"exists": False}

    def test_empty_store_has_no_databases(self, client, session_cache, rdf_store):
        body = client.get("/api/rdf-store-databases").get_json()

        assert body == {
            "success": False,
            "databases": [],
            "message": "No databases found",
        }
        assert session_cache.databases == []

    def test_store_html_error_on_the_database_select_reads_as_no_databases(
        self, client, rdf_store
    ):
        rdf_store.select_status = 500

        body = client.get("/api/rdf-store-databases").get_json()

        assert body["success"] is False
        assert body["databases"] == []
