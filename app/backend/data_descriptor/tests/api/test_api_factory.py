"""
Tests of the application factory the API layer stands on
(``app_factory.py``): the settings read from the environment, the default
wiring of ``APP_CONTEXT`` (each helper delegating to the service production
uses), the guard against misspelt overrides, and the gunicorn entry point,
which is imported in a fresh process because it monkey-patches gevent.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import create_autospec

import pytest

import app_factory
from app_factory import AppSettings, SessionCache, build_app_context, create_app
from services.ingest_service import IngestService
from services.rdf_store_service import RDFStoreService
from services.suggestions import SuggestionService
from tests.api.support import REPO, STORE_URL, api_settings

PACKAGE_DIR = Path(app_factory.__file__).parent


class TestSettingsFromTheEnvironment:
    def test_the_docker_variables_point_at_the_store_and_the_image_folders(self):
        settings = AppSettings.from_env(
            {
                "FLYOVER_RDF_STORE_URL": "http://rdf-store:7200/",
                "FLYOVER_REPOSITORY_NAME": "userRepo",
            }
        )

        assert settings == AppSettings(
            rdf_store_url="http://rdf-store:7200",
            repo="userRepo",
            root_dir="/app/",
            child_dir="data_descriptor",
            upload_folder=os.path.join("data_descriptor", "static", "files"),
        )

    def test_the_older_graphdb_variable_still_works(self):
        settings = AppSettings.from_env(
            {
                "FLYOVER_GRAPHDB_URL": "http://graphdb:7200",
                "FLYOVER_REPOSITORY_NAME": "r",
            }
        )

        assert (settings.rdf_store_url, settings.repo) == ("http://graphdb:7200", "r")

    def test_a_store_url_that_includes_the_repository_is_normalised(self):
        settings = AppSettings.from_env(
            {
                "FLYOVER_RDF_STORE_URL": "http://rdf-store:7200/repositories/userRepo",
                "FLYOVER_REPOSITORY_NAME": "userRepo",
            }
        )

        assert settings.rdf_store_url == "http://rdf-store:7200"

    @pytest.mark.parametrize(
        "environ",
        [
            {},
            {"FLYOVER_RDF_STORE_URL": "http://rdf-store:7200"},
            {"FLYOVER_REPOSITORY_NAME": "userRepo"},
            {"FLYOVER_RDF_STORE_URL": "", "FLYOVER_REPOSITORY_NAME": "userRepo"},
        ],
        ids=["nothing", "no-repository", "no-store", "empty-store"],
    )
    def test_without_both_variables_the_local_defaults_apply(self, environ):
        settings = AppSettings.from_env(environ)

        assert settings == AppSettings(
            rdf_store_url="http://localhost:7200",
            repo="userRepo",
            root_dir="",
            child_dir=".",
            upload_folder=os.path.join(".", "static", "files"),
        )

    def test_the_process_environment_is_read_by_default(self, monkeypatch):
        monkeypatch.setenv("FLYOVER_RDF_STORE_URL", "http://from-env:7200")
        monkeypatch.setenv("FLYOVER_REPOSITORY_NAME", "envRepo")

        assert AppSettings.from_env().repo == "envRepo"


class TestTheDefaultWiring:
    def test_create_app_builds_the_real_services_and_folders(self, tmp_path):
        settings = api_settings(tmp_path)

        app = create_app(settings, spa_dir=str(tmp_path / "spa"))

        context = app.config["APP_CONTEXT"]
        assert isinstance(context["session_cache"], SessionCache)
        assert context["session_cache"].repo == REPO
        assert isinstance(context["rdf_store_service"], RDFStoreService)
        assert context["rdf_store_service"].repo == REPO
        assert isinstance(context["suggestion_service"], SuggestionService)
        assert context["rdf_store_url"] == STORE_URL
        assert context["upload_folder"] == settings.upload_folder
        assert os.path.isdir(settings.upload_folder)
        assert app.config["rdf_store_url"] == STORE_URL
        assert app.config["repo"] == REPO
        assert app.config["DEBUG"] is False
        assert Path(app.static_folder) == PACKAGE_DIR / "static"

    def test_an_unknown_override_is_refused(self, tmp_path):
        with pytest.raises(
            ValueError, match=r"Unknown APP_CONTEXT entries: \['rdf_store_servce'\]"
        ):
            build_app_context(api_settings(tmp_path), {"rdf_store_servce": object()})

    def test_the_defaults_act_on_the_overridden_session(self, tmp_path, monkeypatch):
        ingest = create_autospec(IngestService)
        ingest.return_value.run_triplifier.return_value = (True, "done", ["out.ttl"])
        monkeypatch.setattr(app_factory, "IngestService", ingest)
        session = SessionCache(REPO)
        session.csvData, session.csvTableNames = ["frame"], ["patients"]
        settings = api_settings(tmp_path)

        context = build_app_context(settings, {"session_cache": session})
        result = context["run_triplifier"]("triplifierCSV.properties")

        assert result == (True, "done")
        assert session.output_files == ["out.ttl"]
        ingest.return_value.run_triplifier.assert_called_once_with(
            "triplifierCSV.properties",
            settings.root_dir,
            settings.child_dir,
            csv_data_list=["frame"],
            csv_table_names=["patients"],
        )


@pytest.fixture
def wiring(tmp_path, monkeypatch):
    """The default context with the services it delegates to replaced."""
    ingest = create_autospec(IngestService)
    describe = create_autospec(app_factory.DescribeService)
    spawned = []
    monkeypatch.setattr(app_factory, "IngestService", ingest)
    monkeypatch.setattr(app_factory, "DescribeService", describe)
    monkeypatch.setattr(app_factory.gevent, "spawn", lambda *a: spawned.append(a) or a)
    settings = api_settings(tmp_path)
    store = create_autospec(RDFStoreService, instance=True)
    context = build_app_context(settings, {"rdf_store_service": store})
    return {
        "context": context,
        "settings": settings,
        "ingest": ingest,
        "describe": describe,
        "spawned": spawned,
        "store": store,
    }


class TestTheDefaultHelpers:
    @pytest.mark.parametrize("file_type", ["CSV", "Excel"])
    def test_file_uploads_send_every_triplified_graph(self, wiring, file_type):
        settings = wiring["settings"]

        wiring["context"]["upload_func"](file_type, ["a.ttl", "b.ttl"])

        wiring["ingest"].return_value.upload_multiple_graphs.assert_called_once_with(
            settings.root_dir,
            STORE_URL,
            REPO,
            ["a.ttl", "b.ttl"],
            data_background=False,
        )

    def test_a_database_ingest_uploads_the_ontology_then_the_data(self, wiring):
        settings = wiring["settings"]

        wiring["context"]["upload_func"]("Postgres", None)

        wiring["ingest"].return_value.upload_ontology_then_data.assert_called_once_with(
            settings.root_dir, STORE_URL, REPO, data_background=False
        )

    def test_the_relationship_jobs_run_as_greenlets_on_the_store(self, wiring):
        session = SessionCache(REPO)

        wiring["context"]["start_background"](session)

        assert wiring["spawned"] == [
            (wiring["ingest"].background_pk_fk_processing, session, wiring["store"]),
            (
                wiring["ingest"].background_cross_graph_processing,
                session,
                wiring["store"],
            ),
        ]

    def test_the_database_connection_is_handed_to_the_ingest_service(self, wiring):
        settings = wiring["settings"]

        wiring["context"]["handle_postgres"]("user", "pw", "db.example", "clinic", "t")

        wiring[
            "ingest"
        ].return_value.handle_postgres_connection.assert_called_once_with(
            "user",
            "pw",
            "db.example",
            "clinic",
            "t",
            settings.root_dir,
            settings.child_dir,
        )

    def test_the_lookups_delegate_to_their_services(self, wiring, monkeypatch):
        session = SessionCache(REPO)
        looked_up = []
        monkeypatch.setattr(
            app_factory,
            "get_semantic_map_for_annotation",
            lambda sc, key: looked_up.append((sc, key)) or ("map", "db", True),
        )
        context = wiring["context"]

        context["has_semantic_map"](session)
        context["get_table_names"](session)

        wiring["describe"].has_semantic_map.assert_called_once_with(session)
        wiring["ingest"].get_table_names_from_mapping.assert_called_once_with(session)
        assert context["get_semantic_map"](session) == ("map", "db", True)
        assert context["get_semantic_map"](session, "centre_x") == ("map", "db", True)
        assert looked_up == [(session, None), (session, "centre_x")]

    @pytest.mark.parametrize(
        "map_db, target, expected",
        [
            ("patients", "patients", True),
            ("patients.csv", "patients", True),
            ("patients", "visits", False),
            (None, "anything", True),
        ],
    )
    def test_the_name_matcher_is_the_store_services(
        self, wiring, map_db, target, expected
    ):
        assert wiring["context"]["name_matcher"](map_db, target) is expected

    @pytest.mark.xfail(
        strict=True,
        reason="defect (pinned by the integration layer too): formulate_local_map "
        "passes only the database to DescribeService.formulate_local_semantic_map, "
        "whose signature is (global_semantic_map, database, descriptive_info=None)",
    )
    def test_the_local_map_is_formulated_from_the_legacy_map_and_the_database(
        self, wiring
    ):
        # The autospecced service refuses a call that misses an argument.
        wiring["context"]["formulate_local_map"]("patients")

        call = wiring["describe"].formulate_local_semantic_map.call_args
        assert call.args[1] == "patients"


class TestTheGunicornEntryPoint:
    def test_the_entry_point_builds_the_app_in_a_fresh_process(self, tmp_path):
        # data_descriptor_main monkey-patches gevent at import, so it is
        # imported in its own process, as gunicorn would.
        probe = (
            "import json, data_descriptor_main as main;"
            "app = main.app;"
            "print(json.dumps({"
            "'routes': sorted(r.rule for r in app.url_map.iter_rules()),"
            "'store': app.config['rdf_store_url'], 'repo': app.config['repo'],"
            "'compress': 'COMPRESS_MIMETYPES' in app.config}))"
        )
        environment = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([str(PACKAGE_DIR), *sys.path]),
            "FLYOVER_RDF_STORE_URL": "http://rdf-store:7200",
            "FLYOVER_REPOSITORY_NAME": "userRepo",
        }

        # A fixed argv and no shell.
        result = subprocess.run(  # nosec B603
            [sys.executable, "-c", probe],
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
        )

        assert result.returncode == 0, result.stderr
        built = json.loads(result.stdout.strip().splitlines()[-1])
        assert built["store"] == "http://rdf-store:7200"
        assert built["repo"] == "userRepo"
        assert built["compress"] is True
        assert "/api/v1/suggestions/status" in built["routes"]
        assert "/<path:subpath>" in built["routes"]
        assert len(built["routes"]) == 38
