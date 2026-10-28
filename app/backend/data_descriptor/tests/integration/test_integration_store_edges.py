"""
Integration tests for the store boundary's error handling.

    rdf_store_service -> query_builder -> rdf_store_repository -> HTTP

What Flyover does when the store answers with something other than the
CSV, JSON or N-Triples it expects: HTML error pages, result sets without
the expected columns, malformed CSV, connection drops, and the query
variants the builders produce for `.csv`-suffixed names and unscoped
category lookups.
"""

import requests

from tests.integration.support import TABLE

VARIABLES_STATE = "/api/v1/describe-variables-state"


class TestResultSetsTheStoreShouldNotSend:
    def test_a_result_without_the_expected_column_lists_no_databases(
        self, client, rdf_store
    ):
        rdf_store.seed_table(TABLE, {"id": ["1"]})
        rdf_store.select_overrides["dbo:table ?db"] = "something,else\n1,2\n"

        body = client.get("/api/rdf-store-databases").get_json()

        assert body["success"] is False and body["databases"] == []

    def test_malformed_csv_is_treated_as_no_result(self, client, rdf_store):
        rdf_store.seed_table(TABLE, {"id": ["1"]})
        rdf_store.select_overrides["dbo:table ?db"] = 'db\n"unterminated\n'

        body = client.get("/api/rdf-store-databases").get_json()

        assert body["success"] is False and body["databases"] == []

    def test_column_uris_outside_the_naming_scheme_are_dropped(self, client, rdf_store):
        rdf_store.select_overrides["?uri dbo:column ?column"] = (
            "uri,column\nhttp://data.local/rdf/ontology/patients.sex,sex\nhttp://data.local/rdf/ontology/.odd,odd\n"
        )

        body = client.get(VARIABLES_STATE).get_json()

        assert body == {"column_info": {TABLE: ["sex"]}}

    def test_a_graph_structure_result_without_uris_is_empty(self, client, rdf_store):
        rdf_store.select_overrides["?uri dbo:column ?column"] = "foo,column\n1,sex\n"

        body = client.get("/get-existing-graph-structure").get_json()

        assert body == {"tables": [], "tableColumns": {}}

    def test_an_empty_store_has_no_graph_structure(self, client, rdf_store):
        body = client.get("/get-existing-graph-structure").get_json()

        assert body == {"tables": [], "tableColumns": {}}

    def test_a_column_uri_lookup_without_a_uri_column_finds_nothing(
        self, app, rdf_store
    ):
        rdf_store.select_overrides["SELECT ?uri WHERE"] = "foo\nbar\n"
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        assert service.get_existing_column_class_uri(TABLE, "sex") is None
        assert (
            service.process_pk_fk_relationship("visits", "patient_id", TABLE, "id")
            is False
        )
        assert rdf_store.updates == []


class TestConnectionDrops:
    def test_existence_check_reads_a_drop_as_no_data(self, client, rdf_store):
        rdf_store.outage = requests.ConnectionError("down")

        assert client.get("/api/check-graph-exists").get_json() == {"exists": False}

    def test_cross_graph_links_need_both_columns(self, app, rdf_store):
        rdf_store.seed_table("patients", {"id": ["P1"]})
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        success, message = service.process_cross_graph_relationships(
            {
                "newTableName": "visits",
                "newColumnName": "nope",
                "existingTableName": "patients",
                "existingColumnName": "id",
            }
        )

        assert success is False
        assert message.startswith("Failed cross-graph relationship")
        assert service.process_cross_graph_relationships({}) == (True, "")

    def test_a_malformed_cross_graph_request_is_an_error_message(self, app, rdf_store):
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        success, message = service.process_cross_graph_relationships(
            {"newTableName": "visits"}
        )

        assert success is False
        assert message.startswith("Error processing cross-graph relationship")


class TestQueryVariants:
    def test_csv_suffixed_names_are_stripped_in_the_queries(self, app, rdf_store):
        rdf_store.seed_table(TABLE, {"sex": ["M", "F"], "id": ["P1"]})
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        categories = service.get_categories("sex", f"{TABLE}.csv")
        uri = service.get_existing_column_class_uri(f"{TABLE}.csv", "id")

        assert "F,1" in categories and "M,1" in categories
        assert f"LCASE('{TABLE}.')" in rdf_store.selects[0].query
        assert uri == rdf_store.column_uri(TABLE, "id")
        assert f"LCASE('{TABLE}.')" in rdf_store.selects[1].query

    def test_categories_without_a_table_scope_span_every_table(self, app, rdf_store):
        rdf_store.seed_table("patients", {"sex": ["M"]})
        rdf_store.seed_table("visits", {"sex": ["F"]})
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        categories = service.get_categories("sex")

        assert "M,1" in categories and "F,1" in categories
        assert "LCASE(" not in rdf_store.selects[0].query

    def test_equivalencies_need_a_type_description_or_comment(self, app, rdf_store):
        service = app.config["APP_CONTEXT"]["rdf_store_service"]

        assert service.insert_equivalencies("sex", TABLE, {}) is False
        assert (
            service.insert_equivalencies(
                "sex",
                TABLE,
                {
                    "type": "Variable type: ",
                    "description": "Variable description: None",
                    "comments": "Variable comment: No comment provided",
                },
            )
            is False
        )
        assert (
            service.insert_equivalencies(
                "sex", TABLE, {"comments": "Variable comment: coded"}
            )
            is True
        )
        assert len(rdf_store.updates) == 1


class TestDroppedConnectionsOnAsk:
    def test_a_dropped_connection_is_retried_before_giving_up(
        self, client, session_cache, rdf_store, mapping_dict, monkeypatch
    ):
        import time

        from loaders import JSONLDMapping

        monkeypatch.setattr(
            time, "sleep", lambda _s: None
        )  # the backoff, not the logic
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"], "age": ["1"]})
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        rdf_store.outage_after = 0

        body = client.post(
            "/verify-annotation-ask", json={"variable": f"{TABLE}.identifier"}
        ).get_json()

        assert body == {"success": False, "error": "Query execution failed"}
        assert len(rdf_store.asks) == 4  # one attempt plus three retries
