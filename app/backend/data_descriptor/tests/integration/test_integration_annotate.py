"""
Integration tests for the annotate flow (issue #108, key integration point 3).

    annotate_controller -> annotate_service -> annotation_helper -> HTTP (SPARQL UPDATE + ASK)
                        -> rdf_store_service -> query_builder -> rdf_store_repository -> HTTP
                        -> mapping_validator / jsonld_loader (uploaded and filtered maps)

The annotation helper writes into ``http://annotation.local/<database>/``
and checks each write with an ASK query; the fake store records the writes
and answers the checks, so a test can make any single check fail.
"""

import json
from io import BytesIO
from pathlib import Path

import pytest
import requests

from tests.integration.support import TABLE, load_validation_fixture
from loaders import JSONLDMapping

ANNOTATION_GRAPH = f"http://annotation.local/{TABLE}/"
VERIFY_STATE = "/api/v1/annotation-verify-state"


def _upload_mapping(client, mapping, filename="mapping.jsonld"):
    payload = mapping if isinstance(mapping, bytes) else json.dumps(mapping).encode()
    return client.post(
        "/upload-annotation-json",
        data={"annotationJsonFile": (BytesIO(payload), filename)},
        content_type="multipart/form-data",
    )


@pytest.fixture
def annotate_ready(client, session_cache, seeded_store, mapping_dict):
    """A session that has ingested the table and adopted the mapping."""
    session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
    session_cache.databases = [TABLE]
    seeded_store.calls.clear()
    return seeded_store


# ---------------------------------------------------------------------------
# Mapping upload for annotation: controller -> store databases/columns
#                               -> validator -> loader -> session
# ---------------------------------------------------------------------------


class TestAnnotationMappingUpload:
    def test_mapping_matching_a_store_table_is_adopted_and_saved(
        self, app, client, session_cache, seeded_store, mapping_dict
    ):
        response = _upload_mapping(client, mapping_dict, "centre x.jsonld")

        assert response.status_code == 200, response.get_json()
        body = response.get_json()
        assert body["success"] is True
        assert body["rdf_store_databases"] == [TABLE]
        assert body["jsonld_databases"] == [TABLE]
        assert body["matching_databases"] == [{"jsonld": TABLE, "rdf_store": TABLE}]
        assert body["non_matching_jsonld"] == []
        assert body["removed_columns"] == []

        # Loader output in the session, raw dict kept for legacy consumers
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)
        assert session_cache.global_semantic_map["name"] == "Integration mapping"
        assert session_cache.databases == [TABLE]

        # The file was saved under the upload folder with a safe name
        saved = Path(session_cache.annotation_json_path)
        assert saved.parent == Path(app.config["APP_CONTEXT"]["upload_folder"])
        assert saved.name == "centre_x.jsonld"
        assert json.loads(saved.read_text())["name"] == "Integration mapping"

        # Databases and columns both came from the store
        assert ["dbo:table ?db" in c.query for c in seeded_store.selects] == [
            True,
            False,
        ]

    def test_mapping_with_csv_suffixed_table_name_still_matches(
        self, client, seeded_store, mapping_dict
    ):
        table = mapping_dict["databases"]["centre_x"]["tables"].pop(TABLE)
        table["sourceFile"] = f"{TABLE}.csv"
        mapping_dict["databases"]["centre_x"]["tables"][f"{TABLE}.csv"] = table

        body = _upload_mapping(client, mapping_dict).get_json()

        assert body["matching_databases"] == [
            {"jsonld": f"{TABLE}.csv", "rdf_store": TABLE}
        ]

    def test_columns_unknown_to_the_store_are_removed_and_reported(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"]})

        body = _upload_mapping(client, mapping_dict).get_json()

        assert body["removed_columns"] == ["age"]
        columns = body["cleaned_mapping"]["databases"]["centre_x"]["tables"][TABLE][
            "columns"
        ]
        assert "age_at_diagnosis" not in columns
        assert session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") is None

    def test_mapping_for_other_tables_is_rejected_with_the_store_listing(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        mapping_dict["databases"]["centre_x"]["tables"][TABLE][
            "sourceFile"
        ] = "somewhere_else"

        response = _upload_mapping(client, mapping_dict)

        assert response.status_code == 400
        body = response.get_json()
        assert body["error"] == "No data sources match RDF store data."
        assert body["rdf_store_databases"] == [TABLE]
        assert body["non_matching_jsonld"] == ["somewhere_else"]
        assert session_cache.jsonld_mapping is None

    def test_upload_before_ingest_is_rejected(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        response = _upload_mapping(client, mapping_dict)

        assert response.status_code == 400
        assert "complete Ingest step first" in response.get_json()["error"]
        assert session_cache.jsonld_mapping is None

    def test_mapping_without_tables_is_rejected(
        self, client, seeded_store, mapping_dict
    ):
        mapping_dict["databases"] = {}

        response = _upload_mapping(client, mapping_dict)

        assert response.status_code == 400
        assert "No table definitions" in response.get_json()["error"]

    def test_invalid_mapping_that_matches_is_kept_as_legacy_dict_only(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        # A matching table but a schema the validator rejects
        mapping_dict["schema"]["variables"]["identifier"]["dataType"] = "unknown_type"

        response = _upload_mapping(client, mapping_dict)

        assert response.status_code == 200
        assert session_cache.global_semantic_map is not None
        assert session_cache.jsonld_mapping is None

    @pytest.mark.parametrize(
        "payload, filename, error",
        [
            (b"{not json", "mapping.jsonld", "Invalid JSON file format"),
            (b"{}", "mapping.json", "File must be a .jsonld file"),
        ],
    )
    def test_unreadable_uploads_are_rejected(
        self, app, client, seeded_store, payload, filename, error
    ):
        response = _upload_mapping(client, payload, filename)

        assert response.status_code == 400
        assert response.get_json()["error"] == error
        assert list(Path(app.config["APP_CONTEXT"]["upload_folder"]).iterdir()) == []

    def test_missing_file_field_is_rejected(self, client, seeded_store):
        response = client.post(
            "/upload-annotation-json", data={}, content_type="multipart/form-data"
        )

        assert response.status_code == 400
        assert response.get_json()["error"] == "No file uploaded"

    def test_store_outage_reads_as_no_data(self, client, rdf_store, mapping_dict):
        rdf_store.outage = requests.ConnectionError("store down")

        response = _upload_mapping(client, mapping_dict)

        assert response.status_code == 400
        assert response.get_json()["rdf_store_databases"] == []


# ---------------------------------------------------------------------------
# Running the annotation: controller -> service -> annotation helper -> store
# ---------------------------------------------------------------------------


class TestStartAnnotation:
    def test_every_mapped_variable_is_written_to_the_annotation_graph_and_verified(
        self, app, client, session_cache, annotate_ready
    ):
        store = annotate_ready

        response = client.post("/start-annotation")

        body = response.get_json()
        assert body == {
            "success": True,
            "message": "Annotated 3 variables across 1 databases",
        }
        status = session_cache.annotation_status
        assert set(status) == {
            f"{TABLE}.identifier",
            f"{TABLE}.biological_sex",
            f"{TABLE}.age_at_diagnosis",
        }
        assert all(entry["success"] for entry in status.values())
        assert status[f"{TABLE}.biological_sex"]["details"] == {
            "success": True,
            "core_annotation_inserted": True,
            "reconstruction_inserted": True,
            "value_mappings_inserted": True,
        }

        # Writes went to the statements endpoint, checks to the query endpoint
        updates = store.updates
        assert all(
            c.url == store.statements_url for c in store.calls if c.kind == "update"
        )
        assert all(c.url == store.repository_url for c in store.asks)
        assert all("application/sparql-results+json" in c.accept for c in store.asks)

        # Every write targets this table's annotation graph
        assert all(f"GRAPH <{ANNOTATION_GRAPH}>" in u for u in updates)

        # Core annotations: local column class is equivalent to the ontology class
        core = [u for u in updates if "owl:equivalentClass ncit:" in u]
        assert any(f"db:{TABLE}.sex owl:equivalentClass ncit:C28421" in u for u in core)
        assert any(f"db:{TABLE}.id owl:equivalentClass ncit:C25364" in u for u in core)
        assert any(
            f"db:{TABLE}.age owl:equivalentClass ncit:C156420" in u for u in core
        )

        # Schema reconstruction: the demographic class and the years unit node
        assert any("demographicClass" in u and "mesh:D000091569" in u for u in updates)
        assert any(f"db:{TABLE}.years" in u and "ncit:C29848" in u for u in updates)

        # Value mappings: one OWL restriction per local term, with the mapping's prefixes expanded
        ncit = "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#"
        mappings = [u for u in updates if "owl:hasValue ?localValue" in u]
        assert sorted(u.split('BIND("')[1].split('"')[0] for u in mappings) == [
            "",
            "F",
            "M",
        ]
        assert any(
            f"BIND(<{ncit}C20197> AS ?term)" in u and 'BIND("M"' in u for u in mappings
        )
        assert any(
            f"BIND(<{ncit}C16576> AS ?term)" in u and 'BIND("F"' in u for u in mappings
        )

        # Each write was followed by its ASK check
        assert (
            len(store.asks) >= len(mappings) + 3
        )  # value mappings + 3 predicate checks

        # The generated queries were saved under the temp dir, per database
        query_dir = Path(app.config["APP_CONTEXT"]["root_dir"]).parent / "tmp"
        saved = sorted(
            p.relative_to(query_dir).as_posix() for p in query_dir.rglob("*.rq")
        )
        assert (
            f"annotation_temp/{TABLE}/generated_queries/biological_sex/biological_sex.rq"
            in saved
        )
        assert any(
            p.endswith("mappings/biological_sex_value_mapping_term_male_M.rq")
            for p in saved
        )

    def test_a_failed_value_mapping_check_fails_only_that_variable(
        self, client, session_cache, annotate_ready
    ):
        # Arrange: the store never confirms the "female" restriction
        annotate_ready.ask_answer = lambda query: not (
            "owl:hasValue" in query and '"F"^^xsd:string' in query
        )

        body = client.post("/start-annotation").get_json()

        assert body["success"] is True
        assert body["message"] == "Annotated 2 variables across 1 databases"
        status = session_cache.annotation_status
        assert status[f"{TABLE}.biological_sex"]["success"] is False
        assert (
            status[f"{TABLE}.biological_sex"]["details"]["value_mappings_inserted"]
            is False
        )
        assert (
            status[f"{TABLE}.biological_sex"]["details"]["core_annotation_inserted"]
            is True
        )
        assert status[f"{TABLE}.identifier"]["success"] is True
        assert status[f"{TABLE}.age_at_diagnosis"]["success"] is True

    def test_nothing_verified_means_no_variables_annotated(
        self, client, session_cache, annotate_ready
    ):
        annotate_ready.ask_answer = False

        body = client.post("/start-annotation").get_json()

        assert body == {
            "success": False,
            "error": "No variables were successfully annotated",
        }
        assert all(not e["success"] for e in session_cache.annotation_status.values())

    def test_rejected_updates_are_reported_as_failed_annotations(
        self, client, session_cache, annotate_ready
    ):
        annotate_ready.update_status = 500

        body = client.post("/start-annotation").get_json()

        assert body["success"] is False
        details = session_cache.annotation_status[f"{TABLE}.identifier"]["details"]
        assert details["core_annotation_inserted"] is False
        # No check is attempted for a write the store refused
        assert annotate_ready.asks == []

    def test_store_unreachable_before_the_run_means_no_databases(
        self, client, session_cache, annotate_ready
    ):
        annotate_ready.outage = requests.ConnectionError("store down")

        body = client.post("/start-annotation").get_json()

        assert body == {"success": False, "error": "No databases available"}
        assert session_cache.annotation_status is None

    def test_store_going_down_during_the_run_is_reported_per_variable(
        self, client, session_cache, annotate_ready
    ):
        # Arrange: the database listing succeeds, every later request fails
        annotate_ready.outage_after = 1

        body = client.post("/start-annotation").get_json()

        assert body == {
            "success": False,
            "error": "No variables were successfully annotated",
        }
        assert len(session_cache.annotation_status) == 3
        for entry in session_cache.annotation_status.values():
            assert entry["success"] is False
            assert "went down" in entry["error"]

    def test_filtered_map_annotates_only_its_variables_and_is_then_restored(
        self, client, session_cache, annotate_ready, mapping_dict
    ):
        original = session_cache.jsonld_mapping
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        filtered = {k: v for k, v in columns.items() if k == "identifier"}
        mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"] = filtered

        body = client.post(
            "/start-annotation", json={"semantic_map": mapping_dict}
        ).get_json()

        assert body["message"] == "Annotated 1 variables across 1 databases"
        assert list(session_cache.annotation_status) == [f"{TABLE}.identifier"]
        assert all(f"db:{TABLE}.sex" not in u for u in annotate_ready.updates)
        assert session_cache.jsonld_mapping is original

    def test_filtered_map_is_restored_even_when_the_store_is_down(
        self, client, session_cache, annotate_ready, mapping_dict
    ):
        original = session_cache.jsonld_mapping
        annotate_ready.outage = requests.ConnectionError("store down")

        client.post("/start-annotation", json=mapping_dict)

        assert session_cache.jsonld_mapping is original

    def test_invalid_filtered_map_is_rejected_before_any_write(
        self, client, session_cache, annotate_ready
    ):
        body = client.post(
            "/start-annotation",
            json={"semantic_map": load_validation_fixture("invalid_type.jsonld")},
        ).get_json()

        assert body == {
            "success": False,
            "error": "Filtered semantic map validation failed",
        }
        assert annotate_ready.updates == []

    @pytest.mark.xfail(
        strict=True,
        reason="A valid filtered map is reported as invalid when the session holds no mapping: "
        "_apply_filtered_semantic_map returns the (None) original and the controller reads None as failure",
    )
    def test_filtered_map_without_a_session_mapping_still_annotates(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.databases = [TABLE]

        body = client.post(
            "/start-annotation", json={"semantic_map": mapping_dict}
        ).get_json()

        assert body["success"] is True

    def test_without_a_mapping_nothing_is_written(self, client, seeded_store):
        body = client.post("/start-annotation").get_json()

        assert body == {"success": False, "error": "No semantic map available"}
        assert seeded_store.calls == []

    def test_without_ingested_data_nothing_is_written(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.post("/start-annotation").get_json()

        assert body == {"success": False, "error": "No databases available"}
        assert rdf_store.updates == []

    def test_mapping_for_another_table_has_nothing_to_annotate(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table("other_table", {"x": ["1"]})
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.post("/start-annotation").get_json()

        assert body == {
            "success": False,
            "error": "No variables with local definitions found",
        }
        assert rdf_store.updates == []


# ---------------------------------------------------------------------------
# Verification: controller -> service -> query builder -> repository -> ASK
# ---------------------------------------------------------------------------


class TestVerifyAnnotation:
    @pytest.mark.parametrize("answer", [True, False])
    def test_verification_asks_for_the_equivalence_and_the_value_mappings(
        self, client, annotate_ready, answer
    ):
        annotate_ready.ask_answer = answer

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.biological_sex"}
        ).get_json()

        assert body["success"] is True
        assert body["valid"] is answer
        query = body["query"]
        (ask,) = annotate_ready.asks
        assert ask.method == "GET" and ask.query == query
        assert f"GRAPH <{ANNOTATION_GRAPH}>" in query
        assert f"db:{TABLE}.sex owl:equivalentClass ncit:C28421" in query
        assert "ncit:C20197 rdfs:subClassOf ncit:C28421" in query
        assert "ncit:C16576 rdfs:subClassOf ncit:C28421" in query
        assert '"M"^^xsd:string' in query and '"F"^^xsd:string' in query
        assert 'FILTER(STRSTARTS(STR(?dataGraph), "http://data.local/"))' in query
        # Prefixes come from the mapping as well as the defaults
        assert "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>" in query
        assert "PREFIX dbo: <http://um-cds/ontologies/databaseontology/>" in query

    def test_continuous_variable_is_verified_without_value_mapping_clauses(
        self, client, annotate_ready
    ):
        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.age_at_diagnosis"}
        ).get_json()

        assert body["valid"] is True
        assert f"db:{TABLE}.age owl:equivalentClass ncit:C156420" in body["query"]
        assert "rdfs:subClassOf" not in body["query"]
        assert "?dataGraph" not in body["query"]

    def test_store_error_on_the_ask_is_a_failed_verification(
        self, client, annotate_ready
    ):
        annotate_ready.ask_status = 500

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.identifier"}
        ).get_json()

        assert body["success"] is False
        assert body["error"] == "Query execution failed"

    @pytest.mark.parametrize(
        "variable, error",
        [
            (f"{TABLE}.no_such_variable", "Variable not found"),
            ("no-dot", "Invalid variable format"),
            ("", "No variable specified"),
        ],
    )
    def test_unresolvable_variables_are_reported(
        self, client, annotate_ready, variable, error
    ):
        body = client.post(
            "/verify-annotation-ask", json={"variable": variable}
        ).get_json()

        assert body == {"success": False, "error": error}
        assert annotate_ready.asks == []

    def test_variable_without_local_column_cannot_be_verified(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        del mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "age_at_diagnosis"
        ]
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.age_at_diagnosis"}
        ).get_json()

        assert body == {"success": False, "error": "Variable has no local definition"}

    def test_without_a_mapping_verification_is_refused(self, client, seeded_store):
        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.identifier"}
        ).get_json()

        assert body == {"success": False, "error": "No semantic map available"}


# ---------------------------------------------------------------------------
# Verify page state
# ---------------------------------------------------------------------------


class TestVerifyState:
    def test_state_lists_mapped_variables_with_their_annotation_data(
        self, client, session_cache, annotate_ready
    ):
        body = client.get(VERIFY_STATE).get_json()

        assert body["annotated_variables"] == [
            f"{TABLE}.identifier",
            f"{TABLE}.biological_sex",
            f"{TABLE}.age_at_diagnosis",
        ]
        assert body["unannotated_variables"] == []
        sex = body["variable_data"][f"{TABLE}.biological_sex"]
        assert sex["database"] == TABLE
        assert sex["local_definition"] == "sex"
        assert sex["class"] == "ncit:C28421"
        assert sex["value_mapping"]["terms"]["female"] == {
            "target_class": "ncit:C16576",
            "local_term": "F",
        }
        assert "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>" in sex["prefixes"]
        assert body["annotation_status"] == {}
        assert body["success_message"] is None

    def test_unmapped_variables_are_listed_as_unannotated(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        del mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "age_at_diagnosis"
        ]
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.get(VERIFY_STATE).get_json()

        assert body["unannotated_variables"] == [f"{TABLE}.age_at_diagnosis"]
        assert f"{TABLE}.age_at_diagnosis" not in body["variable_data"]

    def test_success_message_appears_once_every_variable_is_annotated(
        self, client, annotate_ready
    ):
        client.post("/start-annotation")

        body = client.get(VERIFY_STATE).get_json()

        assert (
            body["success_message"]
            == "Data processing complete. Semantic interoperability achieved."
        )
        assert all(e["success"] for e in body["annotation_status"].values())

    def test_state_before_a_mapping_is_a_client_error(self, client, seeded_store):
        response = client.get(VERIFY_STATE)

        assert response.status_code == 400
        assert response.get_json()["error"] == "No semantic map available"

    def test_state_before_ingest_is_a_client_error(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        response = client.get(VERIFY_STATE)

        assert response.status_code == 400
        assert response.get_json()["error"] == "No databases available"
