"""
Integration tests for the share flow (issue #108, key integration point 4).

    share_controller -> share_service -> jsonld_loader (JSON-LD export)
                                      -> rdf_store_repository -> HTTP (ontology download)
                                      -> mock data generation -> data_preprocessing

Note: issue #108 names ``conversion_service`` here, but that module is the
triplifier integration (exercised by the ingest tests). Mock data generation
lives in ``share_service`` and is what the share endpoints call.
"""

import io
import json
import zipfile
from pathlib import Path

import pytest
import requests

from tests.integration.support import TABLE
from loaders import JSONLDMapping
from validation import MappingValidator

NTRIPLES = (
    "<http://data.local/rdf/ontology/patients.sex> "
    '<http://um-cds/ontologies/databaseontology/column> "sex" .\n'
)


# ---------------------------------------------------------------------------
# Semantic map export: controller -> service -> loader.to_dict -> JSON-LD
# ---------------------------------------------------------------------------


class TestSemanticMapDownload:
    def test_jsonld_export_is_valid_and_round_trips_through_the_loader(
        self, client, session_cache, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        response = client.get("/downloadSemanticMap")

        assert response.status_code == 200
        assert response.mimetype == "application/ld+json"
        assert (
            response.headers["Content-Disposition"]
            == "attachment;filename=semantic_mapping.jsonld"
        )
        exported = json.loads(response.data)
        assert exported == session_cache.jsonld_mapping.to_dict()
        assert exported["@context"] == mapping_dict["@context"]

        # The export validates and loads again with the same local mappings
        result = MappingValidator().validate(exported)
        assert result.is_valid, [i.message for i in result.issues]
        reloaded = JSONLDMapping.from_dict(exported)
        assert reloaded.get_local_term("biological_sex", "female") == "F"
        assert reloaded.get_local_column("age_at_diagnosis") == "age"

    def test_export_keeps_unicode_unescaped(self, client, session_cache, mapping_dict):
        mapping_dict["name"] = "Centre Ü — 患者"
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        response = client.get("/downloadSemanticMap")

        assert "Centre Ü — 患者".encode("utf-8") in response.data
        assert json.loads(response.data)["name"] == "Centre Ü — 患者"

    def test_export_can_be_imported_again_through_ingest(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        exported = client.get("/downloadSemanticMap").data
        session_cache.jsonld_mapping = None

        response = client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": (io.BytesIO(exported), "semantic_mapping.jsonld")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 200, response.get_json()
        assert session_cache.jsonld_mapping.to_dict() == json.loads(exported)

    @pytest.mark.parametrize("databases", [None, []])
    def test_without_any_mapping_the_download_is_a_server_error(
        self, client, session_cache, databases
    ):
        session_cache.databases = databases

        response = client.get("/downloadSemanticMap")

        assert response.status_code == 500

    @pytest.mark.xfail(
        strict=True,
        reason="main wires formulate_local_map as a one-argument lambda, but "
        "DescribeService.formulate_local_semantic_map needs the global map and the database, "
        "so the legacy (non JSON-LD) download always fails",
    )
    def test_legacy_mapping_is_exported_as_json(
        self, client, session_cache, legacy_mapping_data
    ):
        session_cache.global_semantic_map = legacy_mapping_data
        session_cache.databases = [TABLE]

        response = client.get("/downloadSemanticMap")

        assert response.status_code == 200
        assert response.mimetype == "application/json"


# ---------------------------------------------------------------------------
# Ontology download: controller -> service -> repository -> Graph Store Protocol
# ---------------------------------------------------------------------------


class TestOntologyDownload:
    def test_single_ontology_graph_is_found_in_the_store_and_served(
        self, client, session_cache, rdf_store
    ):
        rdf_store.seed_ontology_graph(TABLE, NTRIPLES)

        response = client.get("/downloadOntology")

        assert response.status_code == 200
        assert response.mimetype == "application/n-triples"
        assert (
            response.headers["Content-Disposition"]
            == f"attachment;filename=local_ontology_{TABLE}.nt"
        )
        assert response.data.decode() == NTRIPLES
        # The graphs were listed with a SELECT, then the one graph was fetched
        assert [c.kind for c in rdf_store.calls] == ["select", "graph"]
        assert "http://ontology.local/" in rdf_store.selects[0].query
        (download,) = [c for c in rdf_store.calls if c.kind == "graph"]
        assert download.graph == f"http://ontology.local/{TABLE}/"
        assert download.accept == "application/n-triples"

    def test_several_ontology_graphs_are_zipped_and_the_zip_is_cleaned_up(
        self, client, tmp_path, rdf_store
    ):
        rdf_store.seed_ontology_graph("patients", NTRIPLES)
        rdf_store.seed_ontology_graph("visits", NTRIPLES.replace("patients", "visits"))

        response = client.get("/downloadOntology")

        assert response.status_code == 200
        assert response.mimetype == "application/zip"
        assert (
            response.headers["Content-Disposition"]
            == "attachment;filename=local_ontologies.zip"
        )
        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            assert sorted(archive.namelist()) == [
                "local_ontology_patients.nt",
                "local_ontology_visits.nt",
            ]
            assert archive.read(
                "local_ontology_visits.nt"
            ).decode() == NTRIPLES.replace("patients", "visits")
        assert sorted(rdf_store.graph_downloads) == [
            "http://ontology.local/patients/",
            "http://ontology.local/visits/",
        ]
        # The service wrote the zip into the working directory and removed it
        assert not (tmp_path / "local_ontologies.zip").exists()

    def test_known_databases_in_the_session_skip_the_graph_listing(
        self, client, session_cache, rdf_store
    ):
        rdf_store.seed_ontology_graph("patients", NTRIPLES)
        rdf_store.seed_ontology_graph("visits", NTRIPLES)
        session_cache.databases = ["patients", "visits"]

        response = client.get("/downloadOntology")

        assert response.mimetype == "application/zip"
        assert rdf_store.selects == []
        assert len(rdf_store.graph_downloads) == 2

    def test_empty_graphs_are_left_out_of_the_zip(
        self, client, session_cache, rdf_store
    ):
        rdf_store.seed_ontology_graph("patients", NTRIPLES)
        rdf_store.seed_ontology_graph("visits", "   ")
        session_cache.databases = ["patients", "visits"]

        response = client.get("/downloadOntology")

        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            assert archive.namelist() == ["local_ontology_patients.nt"]

    @pytest.mark.xfail(
        strict=True,
        reason="ShareService.download_ontology catches every Exception, including the "
        "HTTPException raised by abort(404), and re-raises it as a 500",
    )
    def test_no_ontology_data_at_all_is_not_found(
        self, client, session_cache, rdf_store
    ):
        session_cache.databases = ["patients", "visits"]

        response = client.get("/downloadOntology")

        assert response.status_code == 404

    def test_no_ontology_data_at_all_is_an_error_response(
        self, client, session_cache, rdf_store
    ):
        session_cache.databases = ["patients", "visits"]

        response = client.get("/downloadOntology")

        assert response.status_code >= 400
        assert sorted(rdf_store.graph_downloads) == [
            "http://ontology.local/patients/",
            "http://ontology.local/visits/",
        ]

    def test_a_single_known_database_still_uses_the_store_listing(
        self, client, session_cache, rdf_store
    ):
        rdf_store.seed_ontology_graph("patients", NTRIPLES)
        session_cache.databases = ["patients"]

        response = client.get("/downloadOntology")

        assert response.mimetype == "application/n-triples"
        assert len(rdf_store.selects) == 1
        assert rdf_store.graph_downloads == ["http://ontology.local/patients/"]

    def test_without_any_listed_graph_the_base_graph_is_tried(self, client, rdf_store):
        response = client.get("/downloadOntology")

        assert response.status_code == 500
        assert rdf_store.graph_downloads == ["http://ontology.local/"]

    def test_store_outage_is_a_server_error(self, client, rdf_store):
        rdf_store.outage = requests.ConnectionError("store down")

        response = client.get("/downloadOntology")

        assert response.status_code == 500


# ---------------------------------------------------------------------------
# Mock data: controller -> service -> generator -> preprocessing
# ---------------------------------------------------------------------------


def _mock(client, **payload):
    return client.post("/api/generate-mock-data", json=payload)


class TestMockDataGeneration:
    def test_rows_follow_the_mapping_local_columns_and_local_values(
        self, client, mapping_dict
    ):
        response = _mock(client, jsonld_map=mapping_dict, num_rows=5, random_seed=1)

        assert response.status_code == 200
        body = response.get_json()
        assert body["success"] is True
        assert body["metadata"]["row_count"] == 5
        assert body["metadata"]["tables_generated"] == 1
        assert body["metadata"]["privacy_preserving"] is True
        (rows,) = body["data"].values()
        assert list(body["data"]) == [f"centre_x.{TABLE}"]
        assert len(rows) == 5
        assert all(set(row) == {"id", "sex", "age"} for row in rows)
        assert [row["id"] for row in rows] == [
            "ID_00001",
            "ID_00002",
            "ID_00003",
            "ID_00004",
            "ID_00005",
        ]
        assert all(row["sex"] in {"M", "F", ""} for row in rows)
        assert all(
            isinstance(row["age"], int) and 1 <= row["age"] <= 100 for row in rows
        )

    def test_the_same_seed_gives_the_same_data(self, client, mapping_dict):
        first = _mock(
            client, jsonld_map=mapping_dict, num_rows=20, random_seed=42
        ).get_json()
        second = _mock(
            client, jsonld_map=mapping_dict, num_rows=20, random_seed=42
        ).get_json()

        assert first["data"] == second["data"]

    def test_loader_export_is_accepted_as_input(self, client, mapping_dict):
        exported = JSONLDMapping.from_dict(mapping_dict).to_dict()

        body = _mock(client, jsonld_map=exported, num_rows=3, random_seed=0).get_json()

        assert body["success"] is True
        assert len(body["data"][f"centre_x.{TABLE}"]) == 3

    def test_grouped_local_values_are_all_used(self, client, mapping_dict):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"] = {
            "male": ["m", "man"],
            "female": "f",
        }

        body = _mock(
            client, jsonld_map=mapping_dict, num_rows=200, random_seed=7
        ).get_json()

        values = {row["sex"] for row in body["data"][f"centre_x.{TABLE}"]}
        assert values == {"m", "man", "f"}

    def test_continuous_missing_notation_shows_up_in_the_data(
        self, client, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["age_at_diagnosis"]["localMappings"] = {"missing_or_unspecified": "NA"}

        body = _mock(
            client, jsonld_map=mapping_dict, num_rows=200, random_seed=3
        ).get_json()

        ages = [row["age"] for row in body["data"][f"centre_x.{TABLE}"]]
        assert "NA" in ages
        assert all(age == "NA" or 1 <= int(age) <= 100 for age in ages)

    def test_tables_can_be_selected_per_database(self, client, mapping_dict):
        second = json.loads(json.dumps(mapping_dict["databases"]["centre_x"]))
        second["name"] = "visits"
        second["tables"] = {
            "visits": dict(second["tables"][TABLE], sourceFile="visits")
        }
        mapping_dict["databases"]["centre_y"] = second

        everything = _mock(client, jsonld_map=mapping_dict, num_rows=2).get_json()
        one_db = _mock(
            client, jsonld_map=mapping_dict, num_rows=2, database_id="centre_y"
        ).get_json()
        one_table = _mock(
            client,
            jsonld_map=mapping_dict,
            num_rows=2,
            database_id="centre_x",
            table_id=TABLE,
        ).get_json()
        unknown = _mock(
            client, jsonld_map=mapping_dict, num_rows=2, database_id="nope"
        ).get_json()

        assert sorted(everything["data"]) == [f"centre_x.{TABLE}", "centre_y.visits"]
        assert list(one_db["data"]) == ["centre_y.visits"]
        assert list(one_table["data"]) == [f"centre_x.{TABLE}"]
        assert unknown == {
            "success": True,
            "data": {},
            "metadata": {
                "generated_from": "semantic_map",
                "privacy_preserving": True,
                "row_count": 2,
                "tables_generated": 0,
            },
        }

    def test_zero_rows_gives_empty_tables(self, client, mapping_dict):
        body = _mock(client, jsonld_map=mapping_dict, num_rows=0).get_json()

        assert body["data"] == {f"centre_x.{TABLE}": []}

    def test_columns_without_a_known_variable_are_skipped(self, client, mapping_dict):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["mystery"] = {"mapsTo": "schema:variable/mystery", "localColumn": "m"}
        columns["unmapped"] = {"localColumn": "u"}

        body = _mock(
            client, jsonld_map=mapping_dict, num_rows=2, random_seed=0
        ).get_json()

        assert set(body["data"][f"centre_x.{TABLE}"][0]) == {"id", "sex", "age"}

    def test_unsupported_data_type_is_a_generation_error(self, client, mapping_dict):
        mapping_dict["schema"]["variables"]["age_at_diagnosis"]["dataType"] = "temporal"

        response = _mock(client, jsonld_map=mapping_dict, num_rows=2)

        assert response.status_code == 500
        body = response.get_json()
        assert body["success"] is False
        assert body["error"] == "Unsupported data type: temporal"

    @pytest.mark.parametrize(
        "payload, error",
        [
            ({}, "No JSON data provided"),
            ({"num_rows": 3}, "No semantic map provided"),
            (
                {"jsonld_map": {"schema": {}}, "table_id": "t"},
                "table_id requires database_id to be specified",
            ),
        ],
    )
    def test_incomplete_requests_are_client_errors(self, client, payload, error):
        response = _mock(client, **payload)

        assert response.status_code == 400
        assert response.get_json() == {"success": False, "error": error}


# ---------------------------------------------------------------------------
# Remaining branches of the share controller and service
# ---------------------------------------------------------------------------


class TestShareRoutesAndBranches:
    @pytest.mark.parametrize(
        "route, target",
        [
            ("/share_landing", "/share"),
            ("/share_mock", "/share/mock"),
            ("/share_publish", "/share/publish"),
        ],
    )
    def test_legacy_routes_redirect_into_the_spa(self, client, route, target):
        response = client.get(route)

        assert response.status_code == 302
        assert response.headers["Location"].endswith(target)

    def test_the_favicon_is_served_from_the_static_folder(self, app, client):
        ctx = app.config["APP_CONTEXT"]
        static = Path(ctx["root_dir"]) / ctx["child_dir"] / "static"
        static.mkdir(parents=True, exist_ok=True)
        (static / "favicon.ico").write_bytes(b"\x00\x00\x01\x00")

        response = client.get("/favicon.ico")

        assert response.status_code == 200
        assert response.mimetype == "image/vnd.microsoft.icon"
        assert response.data == b"\x00\x00\x01\x00"

    def test_mock_data_without_a_json_body_is_a_client_error(self, client):
        response = client.post("/api/generate-mock-data", data="jsonld_map=x")

        assert response.status_code == 400
        assert response.get_json() == {
            "success": False,
            "error": "No JSON data provided",
        }

    def test_mock_data_skips_columns_without_local_names_or_values(
        self, client, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["identifier"].pop("localColumn")
        columns["biological_sex"]["localMappings"] = {}

        body = _mock(
            client, jsonld_map=mapping_dict, num_rows=2, random_seed=0
        ).get_json()

        assert set(body["data"][f"centre_x.{TABLE}"][0]) == {"age"}

    def test_standardised_variables_are_generated_like_continuous_ones(
        self, client, mapping_dict
    ):
        mapping_dict["schema"]["variables"]["age_at_diagnosis"][
            "dataType"
        ] = "standardised"
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["age_at_diagnosis"]["localMappings"] = {
            "missing_or_unspecified": ["NULL", "NA"]
        }

        body = _mock(
            client, jsonld_map=mapping_dict, num_rows=200, random_seed=5
        ).get_json()

        ages = [row["age"] for row in body["data"][f"centre_x.{TABLE}"]]
        assert "NULL, NA" in ages  # a list of missing codes is joined
        assert all(age == "NULL, NA" or 1 <= int(age) <= 100 for age in ages)

    def test_the_legacy_single_ontology_graph_is_served_when_no_graph_is_listed(
        self, client, rdf_store
    ):
        rdf_store.seed_ontology_graph("", NTRIPLES)

        response = client.get("/downloadOntology")

        assert response.status_code == 200
        assert (
            response.headers["Content-Disposition"]
            == "attachment;filename=local_ontology.nt"
        )
        assert rdf_store.graph_downloads == ["http://ontology.local/"]

    def test_a_store_outage_during_the_download_itself_is_a_server_error(
        self, client, session_cache, rdf_store
    ):
        session_cache.databases = ["patients"]
        rdf_store.outage = requests.ConnectionError("down")

        response = client.get("/downloadOntology")

        assert response.status_code == 500


class TestMockDataService:
    def test_a_table_filter_without_a_database_is_refused_by_the_service(
        self, mapping_dict
    ):
        from services import ShareService

        result = ShareService.generate_mock_data_from_semantic_map(
            mapping_dict, table_id=TABLE
        )

        assert result["success"] is False
        assert result["error"] == "table_id requires database_id to be specified"
        assert result["metadata"]["privacy_preserving"] is True

    def test_a_table_filter_skips_the_other_tables_of_the_database(
        self, client, mapping_dict
    ):
        tables = mapping_dict["databases"]["centre_x"]["tables"]
        tables["visits"] = dict(tables[TABLE], sourceFile="visits")

        body = _mock(
            client,
            jsonld_map=mapping_dict,
            num_rows=2,
            database_id="centre_x",
            table_id="visits",
        ).get_json()

        assert list(body["data"]) == ["centre_x.visits"]
