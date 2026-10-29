"""
API tests for the server-side session across requests: how a workflow moves
the session from step to step, what each step does when an earlier one never
ran, that every browser shares the one session, and what concurrent requests
do to it.

Flyover has no per-user session: ``create_app()`` puts one ``SessionCache``
in ``APP_CONTEXT`` and every request reads and writes it. Under gunicorn's
gevent worker, requests interleave wherever one waits on the store, which the
concurrency tests reproduce deterministically with threads and events.
"""

import io
import json
import threading

import pytest

from loaders import JSONLDMapping
from tests.api.support import TABLE, valid_mapping

CATEGORIES = "value,count\nF,1\nM,2\n"
JOIN_TIMEOUT = 10


def _described(data_type, description):
    return {
        "type": f"Variable type: {data_type}",
        "description": f"Variable description: {description}",
        "comments": "Variable comment: No comment provided",
    }


class TestAWorkflowThroughTheSession:
    """Ingest → describe → annotate → share, one request after another."""

    def test_each_step_leaves_the_session_ready_for_the_next(
        self,
        client,
        session_cache,
        store,
        helpers,
        describe_service,
        annotate_service,
        share_service,
    ):
        # Ingest: the CSV is parsed and triplified, the session moves on.
        response = client.post(
            "/upload",
            data={
                "fileType": "CSV",
                "csvFile": (io.BytesIO(b"id\n1\n"), "patients.csv"),
            },
        )
        assert response.headers["Location"] == "/describe"
        assert session_cache.csvTableNames == [TABLE]
        assert session_cache.StatusToDisplay == "Triplification completed"

        # Describe: the store now holds the table.
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex", "age"]}
        assert client.get("/api/v1/describe-variables-state").get_json() == {
            "column_info": {TABLE: ["id", "sex", "age"]}
        }
        assert session_cache.databases == [TABLE]

        describe_service.parse_form_data_for_database.return_value = {
            "sex": _described("categorical", "Biological sex"),
            "age": _described("continuous", "Age at diagnosis"),
        }
        store.get_categories.return_value = CATEGORIES
        response = client.post("/units", data={f"{TABLE}_sex": "categorical"})
        assert response.headers["Location"] == "/describe/variable-details"
        assert set(session_cache.descriptive_info[TABLE]) == {"sex", "age"}

        details = client.get("/api/v1/describe-variable-details-state").get_json()
        assert len(details["descriptive_info_details"][TABLE]) == 2

        describe_service.process_detailed_form_data.return_value = {
            "age": {"units": "years"}
        }
        response = client.post("/end", data={f"{TABLE}_age": "years"})
        assert response.headers["Location"] == "/annotate/review"
        assert session_cache.descriptive_info[TABLE] == {"age": {"units": "years"}}

        # Annotate: the browser's map is adopted, then the run is recorded.
        store.get_databases.return_value = [TABLE]
        response = client.post(
            "/upload-annotation-json",
            data={
                "annotationJsonFile": (
                    io.BytesIO(json.dumps(valid_mapping()).encode()),
                    "mapping.jsonld",
                )
            },
        )
        assert response.get_json()["success"] is True
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)

        helpers["has_semantic_map"].return_value = True
        annotate_service.prepare_annotation_data.return_value = {
            TABLE: {"variables": {"biological_sex": {}}, "prefixes": ""}
        }
        annotate_service.execute_annotation.return_value = (
            {"biological_sex": {"success": True}},
            None,
        )
        assert client.post("/start-annotation").get_json()["success"] is True
        assert session_cache.annotation_status[f"{TABLE}.biological_sex"]["success"]

        verify = client.get("/api/v1/annotation-verify-state").get_json()
        assert verify["success_message"].startswith("Data processing complete.")

        # Share: the download is built from the same session.
        assert client.get("/downloadSemanticMap").status_code == 200
        assert share_service.download_semantic_map.call_args.args[0] is session_cache


class TestStepsBeforeTheirPrerequisites:
    """What each later step does when the earlier ones never ran."""

    def test_describing_before_any_ingest_shows_nothing(self, client, session_cache):
        assert client.get("/api/v1/describe-variables-state").get_json() == {
            "column_info": {}
        }
        details = client.get("/api/v1/describe-variable-details-state").get_json()
        assert details["descriptive_info_details"] == {}
        assert session_cache.databases == []

    def test_submitting_the_variables_form_before_any_ingest_moves_on_to_share(
        self, client, session_cache
    ):
        response = client.post("/units", data={"anything": "categorical"})

        assert response.headers["Location"] == "/share"
        assert session_cache.descriptive_info == {}

    def test_annotation_steps_before_a_map_refuse_without_touching_state(
        self, client, session_cache, annotate_service
    ):
        assert client.get("/api/v1/annotation-verify-state").status_code == 400
        assert client.post("/start-annotation").get_json() == {
            "success": False,
            "error": "No semantic map available",
        }
        assert session_cache.annotation_status is None
        annotate_service.prepare_annotation_data.assert_not_called()

    def test_uploading_an_annotation_map_before_any_ingest_is_refused(
        self, client, session_cache
    ):
        response = client.post(
            "/upload-annotation-json",
            data={
                "annotationJsonFile": (
                    io.BytesIO(json.dumps(valid_mapping()).encode()),
                    "mapping.jsonld",
                )
            },
        )

        assert response.status_code == 400
        assert response.get_json()["error"].startswith("No data found.")
        assert session_cache.global_semantic_map is None

    def test_suggestions_before_a_map_are_the_services_call(
        self, client, session_cache, suggestions
    ):
        suggestions.start.return_value = {
            "status": "unavailable",
            "reason": "no_semantic_map",
        }

        response = client.post("/api/v1/suggestions/variables/start", json={})

        assert response.get_json()["reason"] == "no_semantic_map"
        assert suggestions.start.call_args.args[1] is session_cache

    def test_a_failed_request_leaves_the_session_usable(
        self, client, store, session_cache
    ):
        store.get_column_info_by_database.side_effect = [
            ConnectionError("store down"),
            {TABLE: ["id"]},
        ]

        assert client.get("/api/v1/describe-variables-state").status_code == 500
        response = client.get("/api/v1/describe-variables-state")

        assert response.get_json() == {"column_info": {TABLE: ["id"]}}
        assert session_cache.databases == [TABLE]


class TestOneSessionForEveryBrowser:
    def test_state_set_through_one_client_is_seen_through_another(self, app, helpers):
        # Let the map check read the session the way the real helper does.
        helpers["has_semantic_map"].side_effect = (
            lambda sc: sc.jsonld_mapping is not None
        )
        first, second = app.test_client(), app.test_client()
        before = second.post("/start-annotation").get_json()

        first.post("/submit-indexeddb-semantic-map", json=valid_mapping())

        after = second.post("/start-annotation").get_json()
        assert before["error"] == "No semantic map available"
        assert after["error"] == "No databases available"

    def test_cookies_play_no_part(self, app, session_cache):
        client = app.test_client()
        client.set_cookie("session", "forged-or-stale", domain="localhost")

        response = client.post("/submit-indexeddb-semantic-map", json=valid_mapping())

        assert response.status_code == 200
        assert session_cache.jsonld_mapping is not None

    def test_two_apps_never_share_a_session(self, make_app, doubles):
        from app_factory import SessionCache

        other_session = SessionCache(doubles.settings.repo)
        other = make_app(session_cache=other_session).test_client()

        other.post("/submit-indexeddb-semantic-map", json=valid_mapping())

        assert other_session.jsonld_mapping is not None
        assert doubles.session_cache.jsonld_mapping is None


def _run(target, *args):
    thread = threading.Thread(target=target, args=args, daemon=True)
    thread.start()
    return thread


class TestConcurrentRequests:
    @pytest.mark.xfail(
        strict=True,
        reason="defect: /start-annotation swaps a filtered map into the shared "
        "session and restores 'the original' afterwards; when two filtered runs "
        "overlap, the second saves the first's filter as its original and puts "
        "it back last, so a filtered map stays in the session for good (the "
        "Share page then exports it)",
    )
    def test_overlapping_filtered_runs_leave_the_session_map_in_place(
        self, app, session_cache, helpers, store, annotate_service
    ):
        session_cache.jsonld_mapping = JSONLDMapping.from_dict(valid_mapping())
        original = session_cache.jsonld_mapping
        helpers["has_semantic_map"].return_value = True
        store.get_databases.return_value = [TABLE]
        a_preparing, b_preparing, a_done = (threading.Event() for _ in range(3))
        calls = []

        def prepare(databases, cache, *rest):
            calls.append(cache.jsonld_mapping.name)
            if len(calls) == 1:  # run A: hold until run B has applied its filter
                a_preparing.set()
                b_preparing.wait(timeout=5)
            else:  # run B: hold until run A has finished and restored its map
                b_preparing.set()
                a_done.wait(timeout=5)
            return {}

        annotate_service.prepare_annotation_data.side_effect = prepare
        filter_a, filter_b = valid_mapping(), valid_mapping()
        filter_a["name"], filter_b["name"] = "filter A", "filter B"

        def run_a():
            app.test_client().post("/start-annotation", json=filter_a)
            a_done.set()

        thread_a = _run(run_a)
        assert a_preparing.wait(timeout=5)
        thread_b = _run(
            lambda: app.test_client().post("/start-annotation", json=filter_b)
        )
        thread_a.join(JOIN_TIMEOUT)
        thread_b.join(JOIN_TIMEOUT)

        assert calls == ["filter A", "filter B"]
        assert session_cache.jsonld_mapping is original

    def test_concurrent_validations_answer_alike_and_leave_the_session_alone(
        self, app, session_cache, store
    ):
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {TABLE: ["id", "sex"]}
        results, errors = [], []

        def validate():
            try:
                response = app.test_client().post(
                    "/api/v1/validate-mapping", json=valid_mapping()
                )
                results.append((response.status_code, response.get_json()))
            except Exception as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [_run(validate) for _ in range(16)]
        for thread in threads:
            thread.join(JOIN_TIMEOUT)

        assert errors == []
        assert len(results) == 16
        assert all(result == results[0] for result in results)
        assert results[0][0] == 200
        assert session_cache.jsonld_mapping is None

    def test_concurrent_map_uploads_leave_one_whole_map(self, app, session_cache):
        names = [f"map {i}" for i in range(12)]
        statuses = []

        def upload(name):
            mapping = valid_mapping()
            mapping["name"] = name
            response = app.test_client().post(
                "/submit-indexeddb-semantic-map", json=mapping
            )
            statuses.append(response.status_code)

        threads = [_run(upload, name) for name in names]
        for thread in threads:
            thread.join(JOIN_TIMEOUT)

        assert statuses == [200] * 12
        assert session_cache.jsonld_mapping.name in names
        assert len(session_cache.jsonld_mapping.get_all_variable_keys()) == 3
