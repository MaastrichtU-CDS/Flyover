"""
Integration tests for session state across controller calls (issue #108,
key integration point 6).

Flyover does not keep workflow state in Flask's cookie ``session``: the app
holds one server-side cache (``APP_CONTEXT["session_cache"]``) that every
controller reads and writes. These tests pin down that this state carries
forward through a workflow step, is shared by every client of the app, is
not shared between app instances, and is never touched by request-local
data (a mapping sent in a request body, a filtered annotation run).
"""

import json
from io import BytesIO

import pytest
import requests

from tests.integration.support import TABLE, build_app
from loaders import JSONLDMapping

VARIABLES_STATE = "/api/v1/describe-variables-state"
DETAILS_STATE = "/api/v1/describe-variable-details-state"
VERIFY_STATE = "/api/v1/annotation-verify-state"


def _upload_semantic_map(client, mapping):
    return client.post(
        "/upload-semantic-map",
        data={"semanticMapFile": (BytesIO(json.dumps(mapping).encode()), "m.jsonld")},
        content_type="multipart/form-data",
    )


class TestStateIsServerSide:
    def test_state_set_by_one_client_is_visible_to_another_client_of_the_app(
        self, app, client, session_cache, seeded_store
    ):
        first = client.get(VARIABLES_STATE)
        assert session_cache.databases == [TABLE]
        assert "Set-Cookie" not in first.headers  # nothing lives in the cookie

        other_client = app.test_client()
        response = other_client.post(
            "/units",
            data={f"{TABLE}_age": "continuous", f"ncit_comment_{TABLE}_age": "Age"},
        )

        assert response.status_code == 302
        assert (
            session_cache.descriptive_info[TABLE]["age"]["type"]
            == "Variable type: continuous"
        )
        # The databases known from the first client's call were reused
        assert all("dbo:table ?db" not in c.query for c in seeded_store.selects)

    def test_separate_app_instances_do_not_share_state(
        self, app, client, tmp_path, seeded_store
    ):
        client.get(VARIABLES_STATE)

        other_app = build_app(tmp_path / "other", seeded_store)

        assert app.config["APP_CONTEXT"]["session_cache"].databases == [TABLE]
        assert other_app.config["APP_CONTEXT"]["session_cache"].databases is None


class TestStateCarriesThroughAWorkflow:
    def test_ingest_describe_annotate_share_share_one_session(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        # Ingest: adopt the mapping (checked against the store's columns)
        assert _upload_semantic_map(client, mapping_dict).status_code == 200
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)

        # Describe: the pages read the databases, the mapping fills the details
        assert client.get(VARIABLES_STATE).get_json()["column_info"] == {
            TABLE: ["id", "sex", "age"]
        }
        assert session_cache.databases == [TABLE]
        details = client.get(DETAILS_STATE).get_json()
        assert details["preselected_values"][f'{TABLE}_sex_category_"M"'] == "Male"
        assert (
            session_cache.DescriptiveInfoDetails == details["descriptive_info_details"]
        )
        assert (
            client.post("/end", data={f'{TABLE}_sex_category_"M"': "Male"}).status_code
            == 302
        )
        assert "Category: M" in session_cache.descriptive_info[TABLE]["sex"]

        # Annotate: the same mapping and databases drive the run
        assert client.post("/start-annotation").get_json()["success"] is True
        assert set(session_cache.annotation_status) == {
            f"{TABLE}.identifier",
            f"{TABLE}.biological_sex",
            f"{TABLE}.age_at_diagnosis",
        }
        verify = client.get(VERIFY_STATE).get_json()
        assert verify["success_message"] is not None

        # Share: the mapping adopted at ingest is what gets exported
        exported = json.loads(client.get("/downloadSemanticMap").data)
        assert exported == session_cache.jsonld_mapping.to_dict()

    def test_a_newer_mapping_upload_replaces_the_adopted_one(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        _upload_semantic_map(client, mapping_dict)
        mapping_dict["name"] = "Second mapping"
        del mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "age_at_diagnosis"
        ]

        _upload_semantic_map(client, mapping_dict)

        assert session_cache.jsonld_mapping.name == "Second mapping"
        assert session_cache.jsonld_mapping.get_local_column("age_at_diagnosis") is None

    def test_an_empty_database_list_is_fetched_again_once_data_arrives(
        self, client, session_cache, rdf_store, mapping_dict
    ):
        # Describe before ingest: the store is empty
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        assert client.get(DETAILS_STATE).get_json()["descriptive_info_details"] == {}
        assert session_cache.databases == []

        # Data arrives (ingest in another browser, say)
        rdf_store.seed_table(TABLE, {"id": ["P1"], "sex": ["M"], "age": ["1"]})

        assert TABLE in client.get(DETAILS_STATE).get_json()["descriptive_info_details"]
        assert session_cache.databases == [TABLE]


class TestRequestLocalDataNeverLeaks:
    def test_browser_mapping_in_the_details_request_is_forgotten_afterwards(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        with_body = client.post(
            DETAILS_STATE, json={"mapping": mapping_dict}
        ).get_json()
        assert with_body["preselected_values"]  # rendered from the body mapping

        plain = client.get(DETAILS_STATE).get_json()

        assert plain["descriptive_info_details"] == {}
        assert plain["preselected_values"] == {}
        assert session_cache.jsonld_mapping is None
        assert session_cache.DescriptiveInfoDetails == {}

    def test_filtered_annotation_run_leaves_the_session_mapping_in_place(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        original = session_cache.jsonld_mapping
        columns = mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"]
        mapping_dict["databases"]["centre_x"]["tables"][TABLE]["columns"] = {
            "identifier": columns["identifier"]
        }

        client.post("/start-annotation", json={"semantic_map": mapping_dict})

        assert session_cache.jsonld_mapping is original
        exported = json.loads(client.get("/downloadSemanticMap").data)
        assert set(
            exported["databases"]["centre_x"]["tables"][TABLE]["columns"]
        ) == set(columns)

    def test_validation_endpoint_reads_the_store_without_writing_the_session(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        response = client.post("/api/v1/validate-mapping", json=mapping_dict)

        assert response.get_json()["valid"] is True
        assert response.get_json()["csv_checked"] is True
        assert session_cache.jsonld_mapping is None
        assert session_cache.databases is None


class TestOutOfOrderSteps:
    def test_annotation_steps_before_a_mapping_refuse_without_touching_state(
        self, client, session_cache, seeded_store
    ):
        assert client.get(VERIFY_STATE).status_code == 400
        assert client.post("/start-annotation").get_json()["success"] is False

        assert session_cache.annotation_status is None
        assert seeded_store.updates == []

    def test_describe_before_ingest_shows_nothing_but_does_not_fail(
        self, client, session_cache, rdf_store
    ):
        assert client.get(VARIABLES_STATE).get_json() == {"column_info": {}}
        assert client.get(DETAILS_STATE).get_json()["descriptive_info_details"] == {}
        assert session_cache.databases == []

    @pytest.mark.xfail(
        strict=True,
        reason="/end iterates session_cache.databases, which is None before any describe call, "
        "so an out-of-order details submission raises instead of redirecting",
    )
    def test_details_form_before_any_describe_call_redirects(
        self, client, seeded_store
    ):
        response = client.post("/end", data={})

        assert response.status_code == 302

    def test_store_failures_do_not_poison_later_requests(
        self, client, session_cache, seeded_store, mapping_dict
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_dict)
        seeded_store.outage = requests.ConnectionError("store down")
        assert client.post("/start-annotation").get_json()["success"] is False

        seeded_store.outage = None
        seeded_store.calls.clear()

        assert client.post("/start-annotation").get_json()["success"] is True
        assert all(e["success"] for e in session_cache.annotation_status.values())
