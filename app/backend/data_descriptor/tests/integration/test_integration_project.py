"""
Integration tests of a project's store: with a project open, every step
works on the store the project's map names, through the real services and
the real annotation helper, until the project is closed again.

Two fake stores answer side by side: the configured one (``rdf_store``) and
the project's (``project_store``). Each records the requests it gets, so a
request that went to the wrong store shows.
"""

import copy
import json
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import quote

import pytest
import requests
import responses

from services.store_lifecycle import STORE_MISSING, STORE_STOPPED
from tests.integration.support import (
    _MAPPING,
    CSV_BYTES,
    TABLE,
    TABLE_COLUMNS,
    FakeRdfStore,
    build_app,
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
def web():
    """The faked network the stores (and a launcher) answer on."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


@pytest.fixture
def rdf_store(web, project_store, local_store):
    """The configured store (empty), with the other two answering beside it."""
    store = FakeRdfStore()
    store.data_graph_exists = False
    for each in (store, project_store, local_store):
        each.install(web)
    return store


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

        # Shown as the map wrote it; reached at the configured server.
        assert (
            described["store"]["url"] == "http://localhost:7200/repositories/localRepo"
        )
        assert exists == {"exists": True}
        assert local_store.asks
        assert not rdf_store.calls and not project_store.calls


class TestTheStoreStatus:
    def test_the_configured_store_answers(self, client, rdf_store):
        client.delete("/api/v1/project")

        body = client.get("/api/v1/project/store-status").get_json()

        assert body["state"] == "reachable"
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
            "state": "unreachable",
            "reachable": False,
            "reason": "Currently, the store cannot be reached by Flyover.",
            "managed": False,
            "store": body["store"],
        }
        assert body["store"]["url"] == project_store.repository_url


class TestAMapFromAWebAddress:
    def test_a_files_page_on_a_code_hosting_site_loads_through_its_raw_file(
        self, client
    ):
        page = "https://github.com/o/r/blob/main/maps/aya.jsonld"
        raw = "https://raw.githubusercontent.com/o/r/main/maps/aya.jsonld"
        mapping = _project_map("http://localhost:7200/repositories/userRepo")
        with responses.RequestsMock() as web:
            web.get(raw, json=mapping)
            response = client.post("/api/v1/project/fetch-map", json={"url": page})

        assert response.status_code == 200
        assert response.get_json() == {
            "mapping": mapping,
            "fileName": "aya.jsonld",
            "sourceUrl": page,
        }

    def test_a_page_whose_raw_file_is_missing_says_so(self, client):
        page = "https://codeberg.org/o/r/src/branch/main/aya.jsonld"
        raw = "https://codeberg.org/o/r/raw/branch/main/aya.jsonld"
        with responses.RequestsMock() as web:
            web.get(raw, status=404)
            web.get(page, body="<!doctype html><html></html>")
            response = client.post("/api/v1/project/fetch-map", json={"url": page})

        assert response.status_code == 502
        assert response.get_json()["error"].startswith(
            f"The raw file behind that page, {raw}, answered with HTTP 404."
        )

    def test_a_zenodo_record_with_several_maps_offers_them(self, client):
        record = "https://zenodo.org/records/1486992"
        files = [
            {"key": name, "size": size}
            for name, size in (
                ("mapping_template.jsonld", 12000),
                ("mapping_centre_a.jsonld", 48000),
                ("paper.pdf", 90000),
            )
        ]
        with responses.RequestsMock() as web:
            web.get(
                "https://zenodo.org/api/records/1486992",
                json={"id": 1486992, "files": files},
            )
            response = client.post("/api/v1/project/fetch-map", json={"url": record})

        assert response.status_code == 200
        assert response.get_json() == {
            "message": "This Zenodo record holds 2 JSON-LD files.",
            "choices": [
                {
                    "name": "mapping_centre_a.jsonld",
                    "url": "https://zenodo.org/records/1486992/files/mapping_centre_a.jsonld",
                    "size": 48000,
                },
                {
                    "name": "mapping_template.jsonld",
                    "url": "https://zenodo.org/records/1486992/files/mapping_template.jsonld",
                    "size": 12000,
                },
            ],
            "sourceUrl": record,
        }

    def test_a_zenodo_doi_with_one_map_loads_it(self, client):
        doi = "https://doi.org/10.5281/zenodo.1486992"
        mapping = _project_map("http://localhost:7200/repositories/userRepo")
        with responses.RequestsMock() as web:
            web.get(
                "https://zenodo.org/api/records/1486992",
                json={"id": 1486992, "files": [{"key": "aya.jsonld", "size": 1}]},
            )
            web.get("https://zenodo.org/records/1486992/files/aya.jsonld", json=mapping)
            response = client.post("/api/v1/project/fetch-map", json={"url": doi})

        assert response.status_code == 200
        assert response.get_json() == {
            "mapping": mapping,
            "fileName": "aya.jsonld",
            "sourceUrl": doi,
        }


# ---------------------------------------------------------------------------
# Starting the local store once a user chooses it
# ---------------------------------------------------------------------------

LAUNCHER = "http://store-launcher.test:7290"


class FakeLauncher:
    """The stack's store launcher: starting the module starts the container
    of the configured store, which then takes a while to answer."""

    def __init__(self, state="stopped"):
        self.state = state
        self.started_at = None
        self.never_started = True
        self.starts = 0
        self.stops = 0

    def install(self, web):
        module = f"{LAUNCHER}/modules/rdf-store"
        web.add_callback(responses.GET, module, callback=self._report)
        web.add_callback(responses.POST, f"{module}/start", callback=self._start)
        web.add_callback(responses.POST, f"{module}/stop", callback=self._stop)

    def _report(self, request):
        body = {
            "module": "rdf-store",
            "state": self.state,
            "startedAt": self.started_at,
            "neverStarted": self.never_started and self.state != "missing",
        }
        return 200, {}, json.dumps(body)

    def _start(self, request):
        self.starts += 1
        if self.state != "missing":
            self.state = "running"
            self.started_at = datetime.now(timezone.utc).isoformat()
            self.never_started = False
        return self._report(request)

    def _stop(self, request):
        self.stops += 1
        if self.state != "missing":
            self.state, self.started_at = "stopped", None
        return self._report(request)


@pytest.fixture
def launcher(web):
    fake = FakeLauncher()
    fake.install(web)
    return fake


@pytest.fixture
def launching_client(tmp_path, rdf_store, launcher):
    """The app of a stack that runs a store launcher, before anyone chose."""
    rdf_store.outage = requests.ConnectionError("not started")
    return build_app(tmp_path, rdf_store, store_launcher_url=LAUNCHER).test_client()


def _state(client):
    return client.get("/api/v1/project/store-status").get_json()


class TestStartingTheLocalStore:
    def test_nothing_starts_before_someone_chooses(self, launching_client, launcher):
        assert _state(launching_client)["state"] == "waiting"
        launching_client.get("/api/v1/project")
        assert launcher.starts == 0

    def test_the_standard_rdf_flow_starts_the_store_which_starts_until_it_answers(
        self, launching_client, launcher, rdf_store
    ):
        launching_client.delete("/api/v1/project")

        assert launcher.starts == 1
        assert _state(launching_client) == {
            "state": "starting",
            "reachable": False,
            "reason": "",
            "managed": True,
            "store": _state(launching_client)["store"],
        }

        rdf_store.outage = None
        assert _state(launching_client)["state"] == "reachable"

    def test_a_project_on_the_local_server_starts_the_store(
        self, launching_client, launcher
    ):
        _open(launching_client, endpoint="http://localhost:7200/repositories/localRepo")

        assert launcher.starts == 1

    def test_a_project_on_another_server_starts_nothing(
        self, launching_client, launcher
    ):
        _open(launching_client)

        assert launcher.starts == 0
        assert _state(launching_client)["state"] == "reachable"

    def test_a_store_without_its_container_says_how_to_create_it(
        self, launching_client, launcher
    ):
        launcher.state = "missing"

        launching_client.delete("/api/v1/project")

        assert _state(launching_client)["state"] == "unreachable"
        assert _state(launching_client)["reason"] == STORE_MISSING

    def test_a_store_created_after_the_choice_starts_at_the_next_check(
        self, launching_client, launcher
    ):
        launcher.state = "missing"
        launching_client.delete("/api/v1/project")
        assert _state(launching_client)["reason"] == STORE_MISSING

        # The user runs docker compose create: the container exists, never ran.
        launcher.state = "stopped"
        assert _state(launching_client)["state"] == "starting"
        assert launcher.starts == 2

    def test_a_store_someone_stopped_waits_until_a_user_starts_it(
        self, launching_client, launcher, rdf_store
    ):
        launching_client.delete("/api/v1/project")
        rdf_store.outage = None
        assert _state(launching_client)["state"] == "reachable"

        # Someone stops it: Flyover leaves it stopped.
        rdf_store.outage = requests.ConnectionError("stopped")
        launcher.state, launcher.started_at = "stopped", None
        assert _state(launching_client)["state"] == "stopped"
        assert _state(launching_client)["reason"] == STORE_STOPPED
        assert launcher.starts == 1

        started = launching_client.post("/api/v1/project/store-start").get_json()

        assert launcher.starts == 2
        assert started["state"] == "starting"

    def test_a_store_a_user_stops_as_they_leave_stays_stopped_until_they_choose(
        self, launching_client, launcher, rdf_store
    ):
        launching_client.delete("/api/v1/project")
        rdf_store.outage = None
        assert _state(launching_client)["managed"] is True

        # The user leaves the workflow and asks to stop the store.
        rdf_store.outage = requests.ConnectionError("stopped")
        stopped = launching_client.post("/api/v1/project/store-stop").get_json()

        assert launcher.stops == 1
        assert stopped["state"] == "stopped"
        assert stopped["reason"] == STORE_STOPPED
        assert _state(launching_client)["state"] == "stopped"
        assert launcher.starts == 1

        # Entering the workflow again is a choice: the store starts.
        launching_client.delete("/api/v1/project")
        assert launcher.starts == 2
        assert _state(launching_client)["state"] == "starting"
