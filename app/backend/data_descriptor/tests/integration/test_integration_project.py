"""
Integration tests of a project's store: with a project open, every step
works on the store the project's map names, through the real services and
the real annotation helper, until the project is closed again.

Two fake stores answer side by side: the configured one (``rdf_store``) and
the project's (``project_store``). Each records the requests it gets, so a
request that went to the wrong store shows.
"""

import copy
from io import BytesIO
from urllib.parse import quote

import pytest
import requests
import responses

from tests.integration.support import (
    _MAPPING,
    CSV_BYTES,
    TABLE,
    TABLE_COLUMNS,
    FakeRdfStore,
)

PROJECT_BASE_URL = "http://project-store.test:7200"
PROJECT_REPO = "projectRepo"


def _fake_store(base_url=None, repo=None) -> FakeRdfStore:
    store = FakeRdfStore()
    if base_url:
        store.base_url = base_url
    if repo:
        store.repo = repo
    return store


@pytest.fixture
def project_store() -> FakeRdfStore:
    """The store the project's map names, with the table already ingested."""
    store = _fake_store(PROJECT_BASE_URL, PROJECT_REPO)
    store.seed_table(TABLE, TABLE_COLUMNS)
    return store


@pytest.fixture
def local_store() -> FakeRdfStore:
    """The configured store's server, holding the repository a map names
    through localhost."""
    store = _fake_store(repo="localRepo")
    store.seed_table(TABLE, TABLE_COLUMNS)
    return store


@pytest.fixture
def rdf_store(project_store, local_store):
    """The configured store (empty), with the other two answering beside it."""
    store = FakeRdfStore()
    store.data_graph_exists = False
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        for each in (store, project_store, local_store):
            each.install(mock)
        yield store


def _project_map(endpoint: str) -> dict:
    mapping = copy.deepcopy(_MAPPING)
    mapping["endpoint"] = endpoint
    return mapping


def _open(client, endpoint=f"{PROJECT_BASE_URL}/repositories/{PROJECT_REPO}"):
    response = client.put(
        "/api/v1/project",
        json={"mapping": _project_map(f"{endpoint}/statements"), "token": "t1"},
    )
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def _forget_traffic(*stores):
    for store in stores:
        store.calls.clear()


class TestAProjectsStore:
    def test_with_a_project_open_the_data_check_asks_the_projects_store(
        self, client, rdf_store, project_store
    ):
        _open(client)
        _forget_traffic(rdf_store, project_store)

        exists = client.get("/api/check-graph-exists").get_json()

        assert exists == {"exists": True}
        assert project_store.asks and not rdf_store.calls

    def test_the_describe_page_lists_the_projects_tables(
        self, client, rdf_store, project_store
    ):
        _open(client)
        _forget_traffic(rdf_store, project_store)

        state = client.get("/api/v1/describe-variables-state").get_json()

        assert TABLE in str(state)
        assert project_store.selects and not rdf_store.calls

    def test_an_ingest_uploads_to_the_projects_store(
        self, client, rdf_store, project_store, fake_curl
    ):
        _open(client)

        response = client.post(
            "/upload",
            data={"fileType": "CSV", "csvFile": (BytesIO(CSV_BYTES), f"{TABLE}.csv")},
            content_type="multipart/form-data",
        )

        assert response.status_code == 302
        graph = quote(f"http://data.local/{TABLE}/", safe="")
        assert [u.url for u in fake_curl.uploads][-1] == (
            f"{project_store.graph_store_url}?graph={graph}"
        )
        assert all(u.url.startswith(PROJECT_BASE_URL) for u in fake_curl.uploads)

    def test_the_annotations_are_written_to_the_projects_store(
        self, client, session_cache, rdf_store, project_store
    ):
        _open(client)
        _forget_traffic(rdf_store, project_store)

        body = client.post("/start-annotation").get_json()

        assert body["success"] is True, body
        updates = [c for c in project_store.calls if c.kind == "update"]
        assert updates
        assert all(c.url == project_store.statements_url for c in updates)
        assert not rdf_store.calls

    def test_closing_the_project_brings_the_configured_store_back(
        self, client, rdf_store, project_store
    ):
        _open(client)
        client.delete("/api/v1/project")
        _forget_traffic(rdf_store, project_store)

        exists = client.get("/api/check-graph-exists").get_json()

        assert exists == {"exists": False}
        assert rdf_store.asks and not project_store.calls

    def test_a_localhost_endpoint_works_on_the_configured_server(
        self, client, rdf_store, project_store, local_store
    ):
        described = _open(client, "http://localhost:7200/repositories/localRepo")
        _forget_traffic(rdf_store, project_store, local_store)

        exists = client.get("/api/check-graph-exists").get_json()

        assert described["store"]["url"] == local_store.repository_url
        assert exists == {"exists": True}
        assert local_store.asks
        assert not rdf_store.calls and not project_store.calls


class TestTheStoreStatus:
    def test_the_configured_store_answers(self, client, rdf_store):
        body = client.get("/api/v1/project/store-status").get_json()

        assert body["reachable"] is True
        assert body["store"]["url"] == rdf_store.repository_url
        assert [(c.url.split("?")[0], c.query) for c in rdf_store.asks] == [
            (rdf_store.repository_url, "ASK {}")
        ]

    def test_a_projects_store_without_its_repository_says_so(
        self, client, rdf_store, project_store
    ):
        _open(client)
        project_store.ask_status = 404

        body = client.get("/api/v1/project/store-status").get_json()

        assert body["reachable"] is False
        assert body["reason"] == "The store has no repository named 'projectRepo'."
        assert not rdf_store.calls

    def test_a_projects_store_that_is_down_says_so(self, client, project_store):
        _open(client)
        project_store.outage = requests.ConnectionError("down")

        body = client.get("/api/v1/project/store-status").get_json()

        assert body == {
            "reachable": False,
            "reason": "The store cannot be reached from Flyover.",
            "store": body["store"],
        }
        assert body["store"]["url"] == project_store.repository_url
