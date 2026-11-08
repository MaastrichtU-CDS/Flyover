"""
API tests of the project routes (``controllers/project_controller.py``).

The project service behind them is the real one: it only works on the
session cache, the store selection and its state file, all of which the API
layer already keeps real or inside the test's temporary folder. What crosses
the HTTP boundary, what changes in the session, which store the app works on
afterwards and what the state file holds are checked from the outside.
"""

import json
import os
import stat
from dataclasses import replace
from unittest.mock import ANY, create_autospec

import pytest

from app_factory import create_app
from services.project_service import MapFetchError, MapFileChoice
from services.store_lifecycle import StoreLifecycle
from services.store_selection import LOCALHOST_NOTE
from tests.api.support import REPO, STORE_URL, build_api_app, valid_mapping

PROJECT_STORE = "http://rdf-store.test:7200"  # what valid_mapping()'s endpoint names
CONFIGURED_STORE = {
    "kind": "rdf-store",
    "repository": REPO,
    "url": f"{STORE_URL}/repositories/{REPO}",
    "source": "configured",
    "note": "",
}


# Nobody has chosen yet: no project, and not the standard RDF flow either.
NO_PROJECT = {
    "project": None,
    "route": None,
    "store": CONFIGURED_STORE,
    "configuredStore": CONFIGURED_STORE,
}
# The standard RDF flow, chosen without a project.
STANDARD_FLOW = {**NO_PROJECT, "route": "standard"}


def _put(client, mapping=None, **fields):
    body = {"mapping": valid_mapping() if mapping is None else mapping, **fields}
    return client.put("/api/v1/project", json=body)


def _with_endpoint(endpoint):
    mapping = valid_mapping()
    if endpoint is None:
        del mapping["endpoint"]
    else:
        mapping["endpoint"] = endpoint
    return mapping


@pytest.fixture
def selection(app):
    return app.config["APP_CONTEXT"]["store_selection"]


@pytest.fixture
def state_file(doubles):
    return doubles.settings.state_file


@pytest.fixture
def lifecycle():
    """A store lifecycle that starts nothing, for the routes to call."""
    double = create_autospec(StoreLifecycle, instance=True)
    double.status.return_value = ("reachable", "")
    return double


# ---------------------------------------------------------------------------
# GET /api/v1/project
# ---------------------------------------------------------------------------


class TestTheOpenProject:
    @pytest.mark.route("GET", "/api/v1/project", "success")
    def test_without_a_project_the_configured_store_is_in_use(self, client):
        response = client.get("/api/v1/project")

        assert response.status_code == 200
        assert response.get_json() == NO_PROJECT

    def test_an_open_project_is_described_with_its_store(self, client):
        _put(client, fileName="aya.jsonld", token="browser-token-1")

        described = client.get("/api/v1/project").get_json()

        assert described["project"] == {
            "token": "browser-token-1",
            "name": "Shared fixture mapping",
            "version": "1.0.0",
            "description": "Three variables over one triplified table",
            "schemaId": "schema:shared-fixture/v1",
            "schemaName": "Shared fixture schema",
            "schemaVersion": "1.0.0",
            "fileName": "aya.jsonld",
            "sourceUrl": "",
            "openedAt": described["project"]["openedAt"],
        }
        assert described["store"] == {
            "kind": "rdf-store",
            "repository": "userRepo",
            "url": f"{PROJECT_STORE}/repositories/userRepo",
            "source": "project",
            "note": "",
        }
        # Working without a project would mean the configured store.
        assert described["configuredStore"] == CONFIGURED_STORE


# ---------------------------------------------------------------------------
# PUT /api/v1/project
# ---------------------------------------------------------------------------


class TestOpeningAProject:
    @pytest.mark.route("PUT", "/api/v1/project", "success")
    def test_the_map_becomes_the_sessions_map_and_its_store_the_apps(
        self, client, session_cache, selection
    ):
        response = _put(client)

        assert response.status_code == 200
        assert response.get_json()["project"]["name"] == "Shared fixture mapping"
        assert session_cache.jsonld_mapping.name == "Shared fixture mapping"
        assert selection.current.base_url == PROJECT_STORE
        assert selection.current.repository == "userRepo"
        assert session_cache.repo == "userRepo"

    def test_what_was_worked_out_for_the_previous_map_is_forgotten(
        self, client, session_cache
    ):
        session_cache.global_semantic_map = {"variable_info": {}}
        session_cache.annotation_status = {"patients.age": {"success": True}}
        session_cache.annotation_json_path = "/uploads/old.jsonld"
        session_cache.descriptive_info = {"patients": {"age": {}}}
        session_cache.DescriptiveInfoDetails = {"patients": {}}
        session_cache.StatusToDisplay = "Annotated"
        session_cache.suggestion_jobs = {"variables": object(), "values": None}
        session_cache.suggestion_ingested = {"variables": {"x": {}}, "values": {}}

        _put(client)

        for name in (
            "global_semantic_map",
            "annotation_status",
            "annotation_json_path",
            "descriptive_info",
            "DescriptiveInfoDetails",
            "StatusToDisplay",
            "suggestion_jobs",
            "suggestion_ingested",
        ):
            assert getattr(session_cache, name) is None, name

    def test_another_store_forgets_what_was_known_of_the_previous_ones_data(
        self, client, session_cache
    ):
        session_cache.databases = ["patients"]
        session_cache.existing_graph = True

        _put(client)

        assert session_cache.databases is None
        assert session_cache.existing_graph is False

    def test_the_same_store_keeps_what_was_known_of_its_data(
        self, client, session_cache, selection
    ):
        session_cache.databases = ["patients"]

        _put(client, _with_endpoint(""))

        assert selection.current == selection.configured
        assert session_cache.databases == ["patients"]

    @pytest.mark.parametrize("endpoint", [None, "", "   "])
    def test_a_map_naming_no_store_works_on_the_configured_one(self, client, endpoint):
        described = _put(client, _with_endpoint(endpoint)).get_json()

        assert described["store"] == CONFIGURED_STORE

    def test_a_localhost_endpoint_is_the_configured_store_with_the_maps_repository(
        self, client, selection
    ):
        described = _put(
            client, _with_endpoint("http://localhost:7200/repositories/aya/statements")
        ).get_json()

        # Shown as the map wrote it, which is how its users know the store...
        assert described["store"] == {
            "kind": "rdf-store",
            "repository": "aya",
            "url": "http://localhost:7200/repositories/aya",
            "source": "project",
            "note": LOCALHOST_NOTE,
        }
        # ...and reached at the configured store's address.
        assert selection.current.base_url == STORE_URL
        assert selection.current.repository == "aya"

    def test_the_configured_stores_public_address_is_the_one_shown(
        self, doubles, spa_dir
    ):
        # In Docker the app reaches the store as rdf-store; its users know
        # it as localhost.
        settings = replace(
            doubles.settings, rdf_store_public_url="http://localhost:7200"
        )
        app = create_app(settings, context=doubles.context(), spa_dir=str(spa_dir))
        client = app.test_client()

        before = client.get("/api/v1/project").get_json()
        opened = _put(
            client, _with_endpoint("http://127.0.0.1:7200/repositories/aya/statements")
        ).get_json()

        assert before["store"]["url"] == "http://localhost:7200/repositories/userRepo"
        assert before["configuredStore"]["url"] == before["store"]["url"]
        assert opened["store"]["url"] == "http://localhost:7200/repositories/aya"
        selection = app.config["APP_CONTEXT"]["store_selection"]
        assert selection.current.base_url == STORE_URL

    def test_the_store_is_described_without_its_credentials(self, client, selection):
        described = _put(
            client,
            _with_endpoint("https://flyover:s3cret@store.example.org/repositories/aya"),
        ).get_json()

        assert described["store"]["url"] == "https://store.example.org/repositories/aya"
        assert "s3cret" not in json.dumps(described)
        # The app itself still reaches the store with them.
        assert selection.current.base_url == "https://flyover:s3cret@store.example.org"

    def test_the_browsers_token_is_kept_and_a_strange_one_replaced(self, client):
        kept = _put(client, token="kept_Token-1").get_json()["project"]["token"]
        replaced = _put(client, token="not a token!").get_json()["project"]["token"]
        missing = _put(client).get_json()["project"]["token"]

        assert kept == "kept_Token-1"
        assert replaced != "not a token!" and len(replaced) == 32
        assert missing not in (replaced, "") and len(missing) == 32

    def test_text_fields_are_trimmed_and_kept_short(self, client):
        described = _put(
            client, fileName="  aya.jsonld  ", sourceUrl="https://x/" + "a" * 600
        ).get_json()["project"]

        assert described["fileName"] == "aya.jsonld"
        assert len(described["sourceUrl"]) == 500

    def test_the_project_is_written_for_the_next_worker_readable_by_the_app_only(
        self, client, state_file
    ):
        _put(client, fileName="aya.jsonld", token="t1")

        saved = json.loads(open(state_file, encoding="utf-8").read())
        assert saved["version"] == 1
        assert saved["token"] == "t1"
        assert saved["fileName"] == "aya.jsonld"
        assert saved["mapping"] == valid_mapping()
        assert stat.S_IMODE(os.stat(state_file).st_mode) == 0o600
        assert not os.path.exists(f"{state_file}.partial")

    def test_the_state_file_is_never_in_a_served_folder(self, app, state_file):
        assert not os.path.abspath(state_file).startswith(
            os.path.abspath(app.static_folder)
        )

    @pytest.mark.route("PUT", "/api/v1/project", "failure")
    def test_an_invalid_map_is_refused_with_the_reasons_and_nothing_changes(
        self, client, session_cache, selection, state_file
    ):
        mapping = valid_mapping()
        del mapping["schema"]

        response = _put(client, mapping)

        assert response.status_code == 400
        body = response.get_json()
        assert body["error"] == "The map is not a valid Flyover semantic map."
        assert body["validation_errors"]
        assert session_cache.jsonld_mapping is None
        assert selection.current == selection.configured
        assert not os.path.exists(state_file)

    @pytest.mark.parametrize(
        "endpoint, reason",
        [
            ("ftp://store.example.org/repositories/aya", "http:// or https://"),
            ("http://store.example.org:7200/sparql", "/repositories/<name>"),
        ],
    )
    def test_a_store_flyover_cannot_reach_is_refused_with_the_reason(
        self, client, selection, endpoint, reason
    ):
        response = _put(client, _with_endpoint(endpoint))

        assert response.status_code == 400
        [error] = response.get_json()["validation_errors"]
        assert error["path"] == "endpoint"
        assert reason in error["message"]
        assert selection.current == selection.configured

    @pytest.mark.parametrize(
        "body", [{}, {"mapping": "a map"}, {"mapping": ["a", "map"]}, ["mapping"]]
    )
    def test_a_body_without_a_map_object_is_refused(self, client, body):
        response = client.put("/api/v1/project", json=body)

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "No map was sent: the body needs a 'mapping' object."
        }

    def test_another_site_cannot_open_a_project(self, client, selection):
        response = client.put(
            "/api/v1/project",
            json={"mapping": valid_mapping()},
            headers={"Origin": "https://evil.example"},
        )

        assert response.status_code == 403
        assert selection.current == selection.configured


# ---------------------------------------------------------------------------
# DELETE /api/v1/project
# ---------------------------------------------------------------------------


class TestClosingTheProject:
    @pytest.mark.route("DELETE", "/api/v1/project", "success")
    def test_the_configured_store_is_back_and_the_projects_map_gone(
        self, client, session_cache, selection, state_file
    ):
        _put(client)
        session_cache.annotation_status = {"patients.age": {"success": True}}

        response = client.delete("/api/v1/project")

        assert response.status_code == 200
        assert response.get_json() == STANDARD_FLOW
        assert session_cache.jsonld_mapping is None
        assert session_cache.annotation_status is None
        assert session_cache.repo == REPO
        assert selection.current == selection.configured
        # The next worker must know the standard RDF flow was chosen.
        with open(state_file, encoding="utf-8") as handle:
            assert json.load(handle) == {"version": 1, "route": "standard"}

    def test_without_a_project_the_sessions_own_map_stays(self, client, session_cache):
        # A map an upload put in the session while no project was open is
        # the no-project flow's: closing "nothing" must not take it away.
        session_cache.jsonld_mapping = "a map the annotate upload set"
        session_cache.annotation_status = {"patients.age": {"success": True}}

        response = client.delete("/api/v1/project")

        assert response.get_json() == STANDARD_FLOW
        assert session_cache.jsonld_mapping == "a map the annotate upload set"
        assert session_cache.annotation_status == {"patients.age": {"success": True}}


# ---------------------------------------------------------------------------
# POST /api/v1/project/check
# ---------------------------------------------------------------------------


class TestCheckingAMap:
    @pytest.mark.route("POST", "/api/v1/project/check", "success")
    def test_it_says_what_the_map_would_open_and_changes_nothing(
        self, client, session_cache, selection, state_file
    ):
        response = client.post(
            "/api/v1/project/check",
            json={"mapping": valid_mapping(), "fileName": "aya.jsonld"},
        )

        assert response.status_code == 200
        assert response.get_json() == {
            "valid": True,
            "project": {
                "name": "Shared fixture mapping",
                "version": "1.0.0",
                "description": "Three variables over one triplified table",
                "schemaId": "schema:shared-fixture/v1",
                "schemaName": "Shared fixture schema",
                "schemaVersion": "1.0.0",
                "fileName": "aya.jsonld",
            },
            "store": {
                "kind": "rdf-store",
                "repository": "userRepo",
                "url": f"{PROJECT_STORE}/repositories/userRepo",
                "source": "project",
                "note": "",
            },
        }
        assert session_cache.jsonld_mapping is None
        assert selection.current == selection.configured
        assert not os.path.exists(state_file)

    def test_a_map_that_cannot_be_opened_is_answered_with_the_reasons(self, client):
        mapping = valid_mapping()
        mapping["@type"] = "mapping:SomethingElse"

        response = client.post("/api/v1/project/check", json={"mapping": mapping})

        assert response.status_code == 200
        body = response.get_json()
        assert body["valid"] is False
        assert body["error"] == "The map is not a valid Flyover semantic map."
        assert body["validation_errors"]

    @pytest.mark.route("POST", "/api/v1/project/check", "failure")
    def test_a_body_without_a_map_object_is_refused(self, client):
        response = client.post("/api/v1/project/check", json={"fileName": "x"})

        assert response.status_code == 400


# ---------------------------------------------------------------------------
# The state file: a project outlives the worker that opened it
# ---------------------------------------------------------------------------


class TestAProjectOutlivesItsWorker:
    def test_a_new_worker_starts_on_the_open_project(self, doubles, spa_dir, client):
        _put(client, token="t1")

        restarted = create_app(doubles.settings, spa_dir=str(spa_dir))

        described = restarted.test_client().get("/api/v1/project").get_json()
        assert described["project"]["token"] == "t1"
        assert described["store"]["url"] == f"{PROJECT_STORE}/repositories/userRepo"
        context = restarted.config["APP_CONTEXT"]
        assert context["session_cache"].jsonld_mapping.name == "Shared fixture mapping"

    def test_a_worker_built_before_the_change_catches_up_on_its_next_request(
        self, doubles, spa_dir, client
    ):
        # gunicorn's next worker starts from the app as it was built (before
        # the project opened): it must not answer from that.
        other_worker = create_app(doubles.settings, spa_dir=str(spa_dir))

        _put(client, token="t1")
        opened = other_worker.test_client().get("/api/v1/project").get_json()
        client.delete("/api/v1/project")
        closed = other_worker.test_client().get("/api/v1/project").get_json()

        assert opened["project"]["token"] == "t1"
        assert closed == STANDARD_FLOW

    def test_a_new_worker_remembers_the_standard_rdf_flow(
        self, doubles, spa_dir, client
    ):
        client.delete("/api/v1/project")

        restarted = create_app(doubles.settings, spa_dir=str(spa_dir))

        assert (
            restarted.test_client().get("/api/v1/project").get_json() == STANDARD_FLOW
        )

    def test_a_state_file_that_cannot_be_read_means_no_project(
        self, doubles, spa_dir, state_file, caplog
    ):
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w", encoding="utf-8") as handle:
            handle.write('{"version": 1, "mapping": ')

        restarted = create_app(doubles.settings, spa_dir=str(spa_dir))

        described = restarted.test_client().get("/api/v1/project").get_json()
        assert described == NO_PROJECT
        assert "could not be restored" in caplog.text

    @pytest.mark.parametrize(
        "saved",
        [
            # The map no longer passes the validator (a stricter release).
            {"version": 1, "token": "t", "mapping": {**valid_mapping(), "schema": 1}},
            # Not a map at all.
            {"version": 1, "token": "t", "mapping": ["a", "list"]},
            # Written by a release that kept it differently.
            {"version": 2, "token": "t", "mapping": valid_mapping()},
        ],
        ids=["invalid map", "not a map", "other version"],
    )
    def test_a_saved_project_this_release_cannot_open_means_no_project(
        self, doubles, spa_dir, state_file, saved
    ):
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w", encoding="utf-8") as handle:
            json.dump(saved, handle)

        restarted = create_app(doubles.settings, spa_dir=str(spa_dir))

        assert restarted.config["APP_CONTEXT"]["project_service"].project is None
        described = restarted.test_client().get("/api/v1/project").get_json()
        assert described == NO_PROJECT

    def test_an_app_with_its_own_project_service_uses_it(self, doubles, spa_dir):
        class Recording:
            def __init__(self):
                self.synced = 0

            def sync(self):
                self.synced += 1

            def describe(self):
                return {"project": None, "store": {}, "configuredStore": {}}

        service = Recording()
        app = build_api_app(doubles, spa_dir, {"project_service": service})

        app.test_client().get("/api/v1/project")

        # Once when the app is built, once before the request.
        assert service.synced == 2


# ---------------------------------------------------------------------------
# GET /api/v1/project/store-status
# ---------------------------------------------------------------------------


class TestTheStoreStatus:
    @pytest.mark.route("GET", "/api/v1/project/store-status", "success")
    def test_a_store_that_answers_is_reachable(self, client, store):
        client.delete("/api/v1/project")

        response = client.get("/api/v1/project/store-status")

        assert response.status_code == 200
        assert response.get_json() == {
            "state": "reachable",
            "reachable": True,
            "reason": "",
            "store": CONFIGURED_STORE,
        }
        store.check_reachable.assert_called_once_with()

    @pytest.mark.route("GET", "/api/v1/project/store-status", "failure")
    def test_an_unreachable_store_is_an_answer_with_the_reason(self, client, store):
        client.delete("/api/v1/project")
        store.check_reachable.return_value = (
            False,
            "The store has no repository named 'userRepo'.",
        )

        response = client.get("/api/v1/project/store-status")

        assert response.status_code == 200
        assert response.get_json()["state"] == "unreachable"
        assert response.get_json()["reachable"] is False
        assert response.get_json()["reason"] == (
            "The store has no repository named 'userRepo'."
        )

    def test_a_check_that_fails_reads_as_unreachable(self, client, store, caplog):
        client.delete("/api/v1/project")
        store.check_reachable.side_effect = RuntimeError("boom")

        body = client.get("/api/v1/project/store-status").get_json()

        assert body["state"] == "unreachable"
        assert body["reason"] == "Flyover could not check the store."
        assert "Checking the store failed" in caplog.text

    def test_before_anyone_chooses_the_status_waits_and_asks_no_store(
        self, client, store
    ):
        body = client.get("/api/v1/project/store-status").get_json()

        assert body == {
            "state": "waiting",
            "reachable": False,
            "reason": "",
            "store": CONFIGURED_STORE,
        }
        store.check_reachable.assert_not_called()

    def test_the_stores_lifecycle_says_where_it_stands(self, make_app, lifecycle):
        lifecycle.status.return_value = ("starting", "")
        client = make_app(store_lifecycle=lifecycle).test_client()
        client.delete("/api/v1/project")
        selection = client.application.config["APP_CONTEXT"]["store_selection"]

        body = client.get("/api/v1/project/store-status").get_json()

        assert body["state"] == "starting"
        assert body["reachable"] is False
        lifecycle.status.assert_called_once_with(selection.configured, True, ANY)


# ---------------------------------------------------------------------------
# Starting the store a user chooses
# ---------------------------------------------------------------------------


class TestStartingTheChosenStore:
    def test_choosing_the_standard_rdf_flow_starts_the_configured_store(
        self, make_app, lifecycle
    ):
        client = make_app(store_lifecycle=lifecycle).test_client()
        selection = client.application.config["APP_CONTEXT"]["store_selection"]

        client.delete("/api/v1/project")

        lifecycle.ensure_started.assert_called_once_with(selection.configured)

    def test_opening_a_project_starts_its_store(self, make_app, lifecycle):
        client = make_app(store_lifecycle=lifecycle).test_client()
        selection = client.application.config["APP_CONTEXT"]["store_selection"]

        _put(client)

        [(store,), _] = lifecycle.ensure_started.call_args
        assert store == selection.current
        assert store.base_url == PROJECT_STORE

    def test_looking_checking_or_a_refused_map_starts_nothing(
        self, make_app, lifecycle
    ):
        client = make_app(store_lifecycle=lifecycle).test_client()

        client.get("/api/v1/project")
        client.post("/api/v1/project/check", json={"mapping": valid_mapping()})
        _put(client, mapping={"not": "a map"})

        lifecycle.ensure_started.assert_not_called()

    def test_with_a_project_open_its_store_is_the_one_described(self, client):
        _put(client)

        body = client.get("/api/v1/project/store-status").get_json()

        assert body["store"]["url"] == f"{PROJECT_STORE}/repositories/userRepo"
        assert body["store"]["source"] == "project"


# ---------------------------------------------------------------------------
# POST /api/v1/project/fetch-map
# ---------------------------------------------------------------------------

MAP_URL = "https://example.org/projects/aya.jsonld"


class TestFetchingAMap:
    @pytest.mark.route("POST", "/api/v1/project/fetch-map", "success")
    def test_the_map_at_the_address_comes_back_unopened(
        self, client, helpers, session_cache, selection
    ):
        helpers["fetch_map"].return_value = (valid_mapping(), "aya.jsonld")

        response = client.post(
            "/api/v1/project/fetch-map", json={"url": f"  {MAP_URL}  "}
        )

        assert response.status_code == 200
        assert response.get_json() == {
            "mapping": valid_mapping(),
            "fileName": "aya.jsonld",
            "sourceUrl": MAP_URL,
        }
        helpers["fetch_map"].assert_called_once_with(f"  {MAP_URL}  ")
        assert session_cache.jsonld_mapping is None
        assert selection.current == selection.configured

    @pytest.mark.route("POST", "/api/v1/project/fetch-map", "failure")
    @pytest.mark.parametrize("body", [{}, {"url": ""}, {"url": "  "}, {"url": 7}])
    def test_a_body_without_an_address_is_refused(self, client, helpers, body):
        response = client.post("/api/v1/project/fetch-map", json=body)

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "No address was sent: the body needs a 'url'."
        }
        helpers["fetch_map"].assert_not_called()

    @pytest.mark.parametrize(
        "error, status",
        [
            (MapFetchError("Give a web address ...", upstream=False), 400),
            (MapFetchError("The address answered with HTTP 404."), 502),
        ],
    )
    def test_a_map_that_cannot_be_fetched_says_why(
        self, client, helpers, error, status
    ):
        helpers["fetch_map"].side_effect = error

        response = client.post("/api/v1/project/fetch-map", json={"url": MAP_URL})

        assert response.status_code == status
        assert response.get_json() == {"error": error.message}

    def test_a_record_with_several_maps_offers_them_to_choose_from(
        self, client, helpers, session_cache
    ):
        record = "https://doi.org/10.5281/zenodo.1234567"
        choices = [
            {
                "name": "mapping_centre_a.jsonld",
                "url": "https://zenodo.org/records/1234567/files/mapping_centre_a.jsonld",
                "size": 48000,
            },
            {
                "name": "mapping_template.jsonld",
                "url": "https://zenodo.org/records/1234567/files/mapping_template.jsonld",
                "size": None,
            },
        ]
        helpers["fetch_map"].side_effect = MapFileChoice(
            "This Zenodo record holds 2 JSON-LD files.", choices
        )

        response = client.post("/api/v1/project/fetch-map", json={"url": record})

        assert response.status_code == 200
        assert response.get_json() == {
            "message": "This Zenodo record holds 2 JSON-LD files.",
            "choices": choices,
            "sourceUrl": record,
        }
        assert session_cache.jsonld_mapping is None

    def test_another_site_cannot_make_flyover_fetch_an_address(self, client, helpers):
        response = client.post(
            "/api/v1/project/fetch-map",
            json={"url": MAP_URL},
            headers={"Origin": "https://evil.example"},
        )

        assert response.status_code == 403
        helpers["fetch_map"].assert_not_called()
