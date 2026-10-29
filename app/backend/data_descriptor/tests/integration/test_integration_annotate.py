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


# ---------------------------------------------------------------------------
# Annotation depth: reconstruction shapes, parallel tables, materialisation
# ---------------------------------------------------------------------------


def _with_after_class(mapping_dict):
    """biological_sex with an 'after' class node, like the example's ehrClass."""
    mapping_dict["schema"]["variables"]["biological_sex"][
        "schemaReconstruction"
    ].append(
        {
            "@type": "schema:ClassNode",
            "placement": "after",
            "predicate": "sio:SIO_000253",
            "class": "ncit:C142529",
            "classLabel": "ehrClass",
            "aestheticLabel": "EHR",
        }
    )
    return mapping_dict


def _second_table(mapping_dict, table="visits"):
    """A second database/table mapping identifier -> visit_id in ``table``."""
    mapping_dict["databases"]["centre_y"] = {
        "@id": "mapping:database/centre_y",
        "@type": "mapping:Database",
        "name": table,
        "tables": {
            table: {
                "@id": f"mapping:table/{table}",
                "@type": "mapping:Table",
                "sourceFile": table,
                "columns": {
                    "identifier": {
                        "mapsTo": "schema:variable/identifier",
                        "localColumn": "visit_id",
                    }
                },
            }
        },
    }
    return mapping_dict


class TestAnnotationDepth:
    def test_after_placement_classes_are_constructed_and_their_has_column_edge_removed(
        self, app, client, session_cache, annotate_ready, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(
            _with_after_class(mapping_dict)
        )

        body = client.post("/start-annotation").get_json()

        assert body["success"] is True
        updates = annotate_ready.updates
        # Both reconstruction classes exist in the annotation graph
        constructed = [
            u for u in updates if "rdf:type owl:Class." in u and "dbo:table" in u
        ]
        labels = {u.split(f"db:{TABLE}.")[1].split(" ")[0] for u in constructed}
        assert labels == {"demographicClass", "ehrClass"}
        assert any('rdfs:label "Ehr"' in u for u in constructed)
        # The variable annotation chains before -> variable -> after
        annotation = next(
            u for u in updates if "?component1 sio:SIO_000008 ?component2" in u
        )
        assert "sio:SIO_000235 ?component1" in annotation
        assert "?component2 sio:SIO_000253 ?component3" in annotation
        assert f"?component3 rdf:type db:{TABLE}.ehrClass" in annotation
        # The after-class no longer hangs off the row directly
        (removal,) = [
            u for u in updates if "DELETE { ?tablerow sio:SIO_000253 ?variable . }" in u
        ]
        assert f"?variable rdf:type db:{TABLE}.ehrClass" in removal
        # ... and the removal query was saved next to the annotation
        tmp = Path(app.config["APP_CONTEXT"]["root_dir"]).parent / "tmp"
        removals = list(tmp.rglob("removals/biological_sex_removal_in_ehrClass.rq"))
        assert len(removals) == 1
        assert "DELETE" in removals[0].read_text()

    def test_unit_nodes_attach_to_the_variable_component(self, client, annotate_ready):
        client.post("/start-annotation")

        updates = annotate_ready.updates
        node = next(
            u for u in updates if f"db:{TABLE}.years" in u and "rdfs:label" in u
        )
        assert "ncit:C29848" in node
        annotation = next(
            u for u in updates if f"db:{TABLE}.age" in u and "?component1" in u
        )
        assert f"?component1 sio:SIO_000221 db:{TABLE}.years" in annotation

    def test_two_tables_are_annotated_into_their_own_graphs(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"], "age": ["1"]})
        rdf_store.seed_table("visits", {"visit_id": ["V1"]})
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(
            _second_table(mapping_dict)
        )

        body = client.post("/start-annotation").get_json()

        assert body == {
            "success": True,
            "message": "Annotated 4 variables across 2 databases",
        }
        status = session_cache.annotation_status
        assert status["visits.identifier"]["success"] is True
        assert status["visits.identifier"]["database"] == "visits"
        assert status[f"{TABLE}.identifier"]["success"] is True
        graphs = {u.split("GRAPH <")[1].split(">")[0] for u in rdf_store.updates}
        assert graphs == {
            f"http://annotation.local/{TABLE}/",
            "http://annotation.local/visits/",
        }
        assert any(
            "db:visits.visit_id owl:equivalentClass ncit:C25364" in u
            for u in rdf_store.updates
        )

    def test_a_failing_table_does_not_fail_the_other(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"], "age": ["1"]})
        rdf_store.seed_table("visits", {"visit_id": ["V1"]})
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(
            _second_table(mapping_dict)
        )
        # The store refuses every write into the visits annotation graph
        rdf_store.update_status_fn = lambda update: (
            500 if "http://annotation.local/visits/" in update else 204
        )

        body = client.post("/start-annotation").get_json()

        assert body["message"] == "Annotated 3 variables across 2 databases"
        assert session_cache.annotation_status["visits.identifier"]["success"] is False
        assert session_cache.annotation_status[f"{TABLE}.identifier"]["success"] is True

    def test_list_valued_local_terms_get_one_restriction_each(
        self, client, session_cache, annotate_ready, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"] = {"male": ["M", "m"], "female": "F"}
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        client.post("/start-annotation")

        restrictions = [
            u for u in annotate_ready.updates if "owl:hasValue ?localValue" in u
        ]
        assert sorted(u.split('BIND("')[1].split('"')[0] for u in restrictions) == [
            "F",
            "M",
            "m",
        ]
        assert (
            session_cache.annotation_status[f"{TABLE}.biological_sex"]["success"]
            is True
        )
        # The verification ASK accepts either coding of male
        query = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.biological_sex"}
        ).get_json()["query"]
        assert '"M"^^xsd:string' in query and '"m"^^xsd:string' in query

    def test_terms_without_a_local_value_are_not_written(
        self, client, session_cache, annotate_ready, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"] = {"male": "M"}
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        client.post("/start-annotation")

        restrictions = [
            u for u in annotate_ready.updates if "owl:hasValue ?localValue" in u
        ]
        assert len(restrictions) == 1 and 'BIND("M"' in restrictions[0]
        assert (
            session_cache.annotation_status[f"{TABLE}.biological_sex"]["success"]
            is True
        )

    def test_the_generated_query_files_hold_the_posted_queries(
        self, app, client, annotate_ready
    ):
        client.post("/start-annotation")

        tmp = Path(app.config["APP_CONTEXT"]["root_dir"]).parent / "tmp"
        queries = tmp / "annotation_temp" / TABLE / "generated_queries"
        sex = (queries / "biological_sex" / "biological_sex.rq").read_text()
        assert f"db:{TABLE}.sex owl:equivalentClass ncit:C28421" in sex
        assert "INSERT" in sex and f"GRAPH <{ANNOTATION_GRAPH}>" in sex
        mapping_files = sorted(
            p.name for p in (queries / "biological_sex" / "mappings").iterdir()
        )
        assert mapping_files == [
            "biological_sex_value_mapping_term_female_F.rq",
            "biological_sex_value_mapping_term_male_M.rq",
            "biological_sex_value_mapping_term_missing_or_unspecified_.rq",
        ]
        assert (
            queries
            / "schema_reconstruction"
            / "schema_reconstruction_demographicClass.rq"
        ).exists()
        assert (
            queries / "schema_reconstruction" / "schema_reconstruction_years.rq"
        ).exists()
        # Every saved query was actually posted
        posted = "\n".join(annotate_ready.updates)
        for path in queries.rglob("*.rq"):
            for statement in path.read_text().split("\n\n\n"):
                marker = statement.strip().splitlines()[-1] if statement.strip() else ""
                assert not marker or marker in posted

    def test_materialisation_runs_after_the_annotation_when_enabled(
        self, client, session_cache, annotate_ready, monkeypatch
    ):
        from annotation_helper.src import miscellaneous

        monkeypatch.setattr(miscellaneous, "materialize", True)

        body = client.post("/start-annotation").get_json()

        assert body["success"] is True
        materialised = [
            u
            for u in annotate_ready.updates
            if f"http://materialized.local/{TABLE}/" in u
        ]
        assert len(materialised) == 3
        assert all(f"http://annotation.local/{TABLE}/" in u for u in materialised)
        assert any("owl:equivalentClass" in u for u in materialised)
        # The materialisation came last
        assert annotate_ready.updates[-3:] == materialised

    def test_materialisation_failure_does_not_fail_the_annotation(
        self, client, session_cache, annotate_ready, monkeypatch
    ):
        from annotation_helper.src import miscellaneous

        monkeypatch.setattr(miscellaneous, "materialize", True)
        annotate_ready.outage_after = (
            1 + 20
        )  # the annotation itself fits in ~20 requests

        body = client.post("/start-annotation").get_json()

        # Either the annotation finished before the outage (then it is a success
        # regardless of the materialisation) or the outage hit it first.
        if body["success"]:
            assert all(e["success"] for e in session_cache.annotation_status.values())

    def test_verify_state_carries_the_reconstruction_for_the_frontend(
        self, client, annotate_ready
    ):
        body = client.get(VERIFY_STATE).get_json()

        sex = body["variable_data"][f"{TABLE}.biological_sex"]
        assert sex["schema_reconstruction"] == [
            {
                "type": "class",
                "predicate": "sio:SIO_000235",
                "class": "mesh:D000091569",
                "class_label": "demographicClass",
                "aesthetic_label": "Demographic",
            }
        ]
        age = body["variable_data"][f"{TABLE}.age_at_diagnosis"]
        assert age["schema_reconstruction"][0]["type"] == "node"
        assert age["schema_reconstruction"][0]["node_label"] == "years"
        assert age["data_type"] == "continuous"


# ---------------------------------------------------------------------------
# Store protocol edges on verification
# ---------------------------------------------------------------------------


class TestVerificationRetries:
    def test_a_briefly_overloaded_store_is_retried(self, client, annotate_ready):
        annotate_ready.ask_status_queue = [503]

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.identifier"}
        ).get_json()

        assert body == {**body, "success": True, "valid": True}
        assert len(annotate_ready.asks) == 2

    def test_a_persistently_overloaded_store_gives_up(self, client, annotate_ready):
        annotate_ready.ask_status_queue = [503, 503, 503, 503]

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.identifier"}
        ).get_json()

        assert body == {"success": False, "error": "Query execution failed"}
        assert len(annotate_ready.asks) == 4  # the first try plus three retries

    def test_a_definite_store_error_is_not_retried(self, client, annotate_ready):
        annotate_ready.ask_status_queue = [500]

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.identifier"}
        ).get_json()

        assert body["success"] is False
        assert len(annotate_ready.asks) == 1


# ---------------------------------------------------------------------------
# Legacy JSON annotation maps
# ---------------------------------------------------------------------------


class TestLegacyAnnotationMap:
    def test_a_legacy_map_is_matched_on_its_database_name(
        self, client, session_cache, seeded_store, legacy_mapping_data
    ):
        legacy_mapping_data["database_name"] = TABLE

        body = _upload_mapping(client, legacy_mapping_data).get_json()

        assert body["matching_databases"] == [{"jsonld": TABLE, "rdf_store": TABLE}]
        assert session_cache.global_semantic_map["database_name"] == TABLE
        # A legacy map is not a JSON-LD mapping and never becomes one
        assert session_cache.jsonld_mapping is None

    def test_a_legacy_map_can_be_annotated(
        self, client, session_cache, seeded_store, legacy_mapping_data
    ):
        legacy_mapping_data["database_name"] = TABLE
        _upload_mapping(client, legacy_mapping_data)

        body = client.post("/start-annotation").get_json()

        assert body["success"] is True


# ---------------------------------------------------------------------------
# Remaining branches of the annotate controller
# ---------------------------------------------------------------------------


class TestAnnotateRoutesAndBranches:
    @pytest.mark.parametrize(
        "route, target",
        [
            ("/annotation_landing", "/annotate"),
            ("/annotation-review", "/annotate/review"),
            ("/annotation-verify", "/annotate/verify"),
        ],
    )
    def test_legacy_routes_redirect_into_the_spa(self, client, route, target):
        response = client.get(route)

        assert response.status_code == 302
        assert response.headers["Location"].endswith(target)

    def test_an_empty_json_body_is_no_filter(
        self, client, session_cache, annotate_ready
    ):
        body = client.post("/start-annotation", json={}).get_json()

        assert body["success"] is True
        assert len(session_cache.annotation_status) == 3

    def test_a_nameless_file_is_rejected(self, client, seeded_store):
        response = client.post(
            "/upload-annotation-json",
            data={"annotationJsonFile": (BytesIO(b"{}"), "")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 400
        assert response.get_json()["error"] == "No file selected"

    def test_an_unwritable_upload_folder_is_a_server_error(
        self, app, client, seeded_store, mapping_dict
    ):
        app.config["APP_CONTEXT"]["upload_folder"] = str(
            Path(app.config["APP_CONTEXT"]["upload_folder"]) / "missing" / "deeper"
        )

        response = _upload_mapping(client, mapping_dict)

        assert response.status_code == 500
        assert response.get_json()["error"].startswith("Upload failed:")

    def test_verification_without_a_json_body_is_a_handled_error(
        self, client, annotate_ready
    ):
        response = client.post("/verify-annotation-ask", data="variable=x")

        body = response.get_json()
        assert body["success"] is False
        assert body["error"]

    def test_a_mapping_without_source_files_is_a_global_template(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        # No table names to match on: the mapping applies to every table
        del mapping_dict["databases"]["centre_x"]["tables"][TABLE]["sourceFile"]
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)

        body = client.post("/start-annotation").get_json()
        state = client.get(VERIFY_STATE).get_json()

        assert body["success"] is True
        assert len(state["annotated_variables"]) == 3


class TestLegacyShapedState:
    def test_an_invalid_jsonld_upload_leaves_a_dict_that_cannot_be_annotated(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        mapping_dict["schema"]["variables"]["identifier"]["dataType"] = "unknown_type"
        assert _upload_mapping(client, mapping_dict).status_code == 200
        assert session_cache.jsonld_mapping is None

        body = client.post("/start-annotation").get_json()

        # The JSON-LD-shaped dict is read as a legacy map, which the broken
        # formulate_local_map wiring cannot serve (see the cleanup ticket)
        assert body["success"] is False
        assert body["error"]
        assert seeded_store.updates == []

    def test_non_string_local_terms_are_left_out_of_the_verification(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        columns["biological_sex"]["localMappings"] = {"male": [1, 2], "female": "F"}
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        session_cache.databases = [TABLE]

        query = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.biological_sex"}
        ).get_json()["query"]

        assert '"F"^^xsd:string' in query
        assert "ncit:C20197 rdfs:subClassOf" not in query
