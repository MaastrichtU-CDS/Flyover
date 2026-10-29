"""
API tests for the mapping-suggestion routes
(``controllers/suggestions_controller.py``): ``/status``, a phase's
``/start``, snapshot and ``/priority``, and the LLM round trip's
``/prompt`` and ``/ingest``.

The suggestion service is a double; the controller's own decisions are under
test: the shape of ``/status`` with the feature on, off or absent, which body
map a job runs on and when the session adopts it, how options are read from
the query string and the body, and the error contract (400 with a stable
``kind``, 503 without a service).
"""

import json

import pytest

from loaders import JSONLDMapping
from services.suggestions import SuggestionRequestError
from tests.api.support import (
    TABLE,
    idle_suggestion_state,
    make_suggestion_config,
    valid_mapping,
)

STATUS = "/api/v1/suggestions/status"
PROMPT = "/api/v1/suggestions/prompt"


def _json_post(client, path, body):
    """POST a JSON document exactly as given (the test client's json= sorts keys)."""
    return client.post(path, data=json.dumps(body), content_type="application/json")


@pytest.fixture
def no_service(make_app):
    """A client of an app with no suggestion service at all."""
    return make_app(suggestion_service=None).test_client()


@pytest.fixture
def tiers_off(suggestions):
    """The feature switched off with FLYOVER_SUGGESTION_TIERS=''."""
    suggestions.config = make_suggestion_config(tiers=(), compute="host", threshold=0.7)
    return suggestions


# ---------------------------------------------------------------------------
# GET /api/v1/suggestions/status
# ---------------------------------------------------------------------------


class TestStatus:
    @pytest.mark.route("GET", STATUS, "success")
    def test_an_enabled_feature_reports_the_services_status(self, client, suggestions):
        response = client.get(STATUS)

        assert response.status_code == 200
        body = response.get_json()
        assert body["enabled"] is True
        assert body["tiers"]["1"] == {"state": "active"}
        assert body["tiers"]["2"]["state"] == "inactive"
        assert body["rules_version"] == "1.2.0"
        assert body["prompt_export"]["state"] == "active"
        suggestions.status.assert_called_once_with()

    def test_a_disabled_feature_reports_every_tier_off_but_the_prompt_export_on(
        self, client, tiers_off, monkeypatch
    ):
        monkeypatch.setenv("FLYOVER_SUGGESTION_PROMPT_CHUNK", "160")
        monkeypatch.setenv("FLYOVER_SUGGESTION_MIN_VALUE_COUNT", "25")

        body = client.get(STATUS).get_json()

        assert body["enabled"] is False
        assert body["compute"] == "host"
        assert body["threshold"] == 0.7
        assert set(body["tiers"]) == {"1", "2", "3"}
        assert all(
            tier
            == {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"}
            for tier in body["tiers"].values()
        )
        assert body["prompt_export"] == {
            "state": "active",
            "chunk": 160,
            "min_value_count": 25,
        }
        tiers_off.status.assert_not_called()

    def test_without_a_service_every_feature_is_off(self, no_service):
        body = no_service.get(STATUS).get_json()

        assert body["enabled"] is False
        assert body["compute"] == "host"
        assert body["threshold"] == 0.8
        assert body["prompt_export"] == {
            "state": "inactive",
            "reason": "no suggestion service",
        }

    @pytest.mark.route("GET", STATUS, "failure")
    def test_a_service_failure_is_a_server_error(self, client, suggestions):
        suggestions.status.side_effect = RuntimeError("rules file unreadable")

        assert client.get(STATUS).status_code == 500


# ---------------------------------------------------------------------------
# POST /api/v1/suggestions/<phase>/start
# ---------------------------------------------------------------------------


class TestStart:
    @pytest.mark.route("POST", "/api/v1/suggestions/<phase>/start", "success")
    @pytest.mark.parametrize("phase", ["variables", "values"])
    def test_a_phase_job_is_started_on_the_session_and_the_store(
        self, client, session_cache, store, suggestions, phase
    ):
        response = client.post(f"/api/v1/suggestions/{phase}/start", json={})

        assert response.status_code == 200
        assert response.get_json() == {"status": "started"}
        suggestions.start.assert_called_once_with(
            phase, session_cache, store, force=False, mapping=None
        )

    def test_the_services_answer_is_passed_on(self, client, suggestions):
        suggestions.start.return_value = {
            "status": "unavailable",
            "reason": "no_semantic_map",
        }

        response = client.post("/api/v1/suggestions/variables/start", json={})

        assert response.get_json() == {
            "status": "unavailable",
            "reason": "no_semantic_map",
        }

    @pytest.mark.parametrize(
        "force, expected",
        [(True, True), (False, False), (1, True), ("false", True), (None, False)],
    )
    def test_any_truthy_force_value_forces_a_rerun(
        self, client, suggestions, force, expected
    ):
        # Even the string "false": the flag is read with bool().
        client.post("/api/v1/suggestions/variables/start", json={"force": force})

        assert suggestions.start.call_args.kwargs["force"] is expected

    @pytest.mark.parametrize(
        "data, content_type",
        [("", None), ("{broken", "application/json"), ("{}", "text/plain")],
        ids=["no-body", "malformed", "not-json"],
    )
    def test_a_body_it_cannot_read_starts_a_plain_job(
        self, client, suggestions, data, content_type
    ):
        response = client.post(
            "/api/v1/suggestions/variables/start", data=data, content_type=content_type
        )

        assert response.status_code == 200
        suggestions.start.assert_called_once()
        assert suggestions.start.call_args.kwargs == {"force": False, "mapping": None}

    @pytest.mark.route("POST", "/api/v1/suggestions/<phase>/start", "failure")
    @pytest.mark.parametrize("phase", ["bogus", "VARIABLES", "variable", "values "])
    def test_an_unknown_phase_is_a_client_error(self, client, suggestions, phase):
        response = client.post(f"/api/v1/suggestions/{phase}/start", json={})

        assert response.status_code == 400
        assert response.get_json() == {"error": f"unknown phase '{phase}'"}
        suggestions.start.assert_not_called()

    def test_without_a_service_the_feature_is_disabled(self, no_service):
        response = no_service.post("/api/v1/suggestions/variables/start", json={})

        assert response.status_code == 200
        assert response.get_json() == {"status": "disabled"}

    @pytest.mark.parametrize("body", [[1, 2], "text", 7])
    def test_a_body_that_is_not_an_object_starts_a_plain_job(
        self, client, suggestions, body
    ):
        response = client.post("/api/v1/suggestions/variables/start", json=body)

        assert response.status_code == 200
        call = suggestions.start.call_args
        assert call.kwargs["force"] is False
        assert call.kwargs["mapping"] is None


class TestStartWithTheBrowserMap:
    """The describe pages send the browser's map; the session keeps its own."""

    def test_the_variables_job_adopts_a_valid_map_when_the_session_has_none(
        self, client, session_cache, suggestions
    ):
        response = _json_post(
            client, "/api/v1/suggestions/variables/start", {"mapping": valid_mapping()}
        )

        assert response.status_code == 200
        assert isinstance(session_cache.jsonld_mapping, JSONLDMapping)
        job_mapping = suggestions.start.call_args.kwargs["mapping"]
        assert job_mapping is session_cache.jsonld_mapping

    def test_the_variables_job_runs_on_the_browser_map_without_replacing_the_session_map(
        self, client, session_cache, suggestions
    ):
        existing = JSONLDMapping.from_dict(valid_mapping())
        session_cache.jsonld_mapping = existing
        browser_map = valid_mapping()
        del browser_map["schema"]["variables"]["identifier"]
        del browser_map["databases"]["centre_x"]["tables"][TABLE]["columns"][
            "identifier"
        ]

        _json_post(
            client, "/api/v1/suggestions/variables/start", {"mapping": browser_map}
        )

        assert session_cache.jsonld_mapping is existing
        job_mapping = suggestions.start.call_args.kwargs["mapping"]
        assert sorted(job_mapping.get_all_variable_keys()) == [
            "age_at_diagnosis",
            "biological_sex",
        ]

    def test_the_values_job_runs_on_the_browser_map_but_never_adopts_it(
        self, client, session_cache, suggestions
    ):
        _json_post(
            client, "/api/v1/suggestions/values/start", {"mapping": valid_mapping()}
        )

        assert session_cache.jsonld_mapping is None
        assert isinstance(suggestions.start.call_args.kwargs["mapping"], JSONLDMapping)

    @pytest.mark.parametrize(
        "mapping",
        [
            {**valid_mapping(), "@type": "mapping:NotAMapping"},
            {"name": "not a mapping"},
            "text",
            [],
        ],
        ids=["wrong-type", "incomplete", "text", "list"],
    )
    def test_a_map_that_fails_validation_never_reaches_the_job_or_the_session(
        self, client, session_cache, suggestions, mapping
    ):
        response = _json_post(
            client, "/api/v1/suggestions/variables/start", {"mapping": mapping}
        )

        assert response.status_code == 200
        assert session_cache.jsonld_mapping is None
        assert suggestions.start.call_args.kwargs["mapping"] is None


# ---------------------------------------------------------------------------
# GET /api/v1/suggestions/<phase>
# ---------------------------------------------------------------------------


class TestSnapshot:
    @pytest.mark.route("GET", "/api/v1/suggestions/<phase>", "success")
    @pytest.mark.parametrize("phase", ["variables", "values"])
    def test_the_jobs_snapshot_is_returned_with_the_feature_flag(
        self, client, session_cache, suggestions, phase
    ):
        suggestions.get_state.return_value = {
            "status": "done",
            "progress": {"done": 2, "total": 2},
            "error": None,
            "records": {f"{TABLE}_sex": {"match": "biological_sex", "confidence": 1.0}},
        }

        response = client.get(f"/api/v1/suggestions/{phase}")

        assert response.status_code == 200
        body = response.get_json()
        assert body["enabled"] is True
        assert body["status"] == "done"
        assert body["records"][f"{TABLE}_sex"]["match"] == "biological_sex"
        suggestions.get_state.assert_called_once_with(session_cache, phase)

    def test_with_the_feature_off_an_idle_phase_reports_it_off(self, client, tiers_off):
        tiers_off.get_state.return_value = idle_suggestion_state()

        assert (
            client.get("/api/v1/suggestions/variables").get_json()["enabled"] is False
        )

    def test_with_the_feature_off_a_pasted_job_is_still_shown(self, client, tiers_off):
        tiers_off.get_state.return_value = {**idle_suggestion_state(), "status": "done"}

        assert client.get("/api/v1/suggestions/variables").get_json()["enabled"] is True

    @pytest.mark.route("GET", "/api/v1/suggestions/<phase>", "failure")
    def test_an_unknown_phase_is_a_client_error(self, client, suggestions):
        response = client.get("/api/v1/suggestions/bogus")

        assert response.status_code == 400
        assert response.get_json() == {"error": "unknown phase 'bogus'"}
        suggestions.get_state.assert_not_called()

    def test_without_a_service_the_phase_is_idle_and_off(self, no_service):
        response = no_service.get("/api/v1/suggestions/variables")

        assert response.get_json() == {
            "enabled": False,
            "status": "idle",
            "progress": {"done": 0, "total": 0},
            "error": None,
            "records": {},
        }


# ---------------------------------------------------------------------------
# POST /api/v1/suggestions/<phase>/priority
# ---------------------------------------------------------------------------


class TestPriority:
    @pytest.mark.route("POST", "/api/v1/suggestions/<phase>/priority", "success")
    def test_the_visible_items_are_handed_to_the_service(
        self, client, session_cache, suggestions
    ):
        response = client.post(
            "/api/v1/suggestions/variables/priority", json={"items": ["sex", "age"]}
        )

        assert response.status_code == 200
        assert response.get_json() == {"status": "ok", "moved": 0}
        suggestions.bump_priority.assert_called_once_with(
            session_cache, "variables", ["sex", "age"]
        )

    def test_columns_is_accepted_for_items(self, client, session_cache, suggestions):
        client.post("/api/v1/suggestions/values/priority", json={"columns": ["sex"]})

        suggestions.bump_priority.assert_called_once_with(
            session_cache, "values", ["sex"]
        )

    @pytest.mark.route("POST", "/api/v1/suggestions/<phase>/priority", "failure")
    @pytest.mark.parametrize(
        "body", [{}, {"items": []}, {"columns": None}], ids=["none", "empty", "null"]
    )
    def test_a_request_without_items_is_a_client_error(self, client, suggestions, body):
        response = client.post("/api/v1/suggestions/variables/priority", json=body)

        assert response.status_code == 400
        assert response.get_json() == {"error": "items are required"}
        suggestions.bump_priority.assert_not_called()

    def test_an_unknown_phase_is_a_client_error(self, client):
        response = client.post(
            "/api/v1/suggestions/bogus/priority", json={"items": ["x"]}
        )

        assert response.status_code == 400

    def test_without_a_service_there_is_no_job(self, no_service):
        response = no_service.post(
            "/api/v1/suggestions/variables/priority", json={"items": ["sex"]}
        )

        assert response.get_json() == {"status": "no_job"}

    @pytest.mark.parametrize("body", [["sex"], "sex", 7])
    def test_a_body_that_is_not_an_object_is_a_client_error(self, client, body):
        response = client.post("/api/v1/suggestions/variables/priority", json=body)

        assert response.status_code == 400
        assert response.get_json() == {"error": "items are required"}


# ---------------------------------------------------------------------------
# GET/POST /api/v1/suggestions/prompt
# ---------------------------------------------------------------------------


class TestPrompt:
    @pytest.mark.route("GET", PROMPT, "success")
    def test_a_get_composes_the_prompt_from_the_query_string(
        self, client, session_cache, store, suggestions
    ):
        suggestions.build_prompt.return_value = {"prompt": "PROMPT", "item_count": 1}

        response = client.get(f"{PROMPT}?phase=variables&database={TABLE}")

        assert response.status_code == 200
        assert response.get_json() == {"prompt": "PROMPT", "item_count": 1}
        suggestions.build_prompt.assert_called_once_with(
            "variables",
            session_cache,
            store,
            database=TABLE,
            mapping=None,
            mapping_data=None,
            chunk=None,
            include=[],
        )

    @pytest.mark.route("POST", PROMPT, "success")
    def test_a_post_carries_the_options_and_the_browser_map(self, client, suggestions):
        response = _json_post(
            client,
            PROMPT,
            {
                "phase": "values",
                "database": TABLE,
                "chunk": "20",
                "include": ["opmerking", "", None, 7],
                "mapping": valid_mapping(),
            },
        )

        assert response.status_code == 200
        args, kwargs = suggestions.build_prompt.call_args
        assert args[0] == "values"
        assert kwargs["chunk"] == 20
        assert kwargs["include"] == ["opmerking", "7"]
        assert isinstance(kwargs["mapping"], JSONLDMapping)
        assert kwargs["mapping_data"] == valid_mapping()

    def test_the_body_wins_over_the_query_string(self, client, suggestions):
        _json_post(
            client,
            f"{PROMPT}?phase=variables&database=from_query&chunk=5",
            {"phase": "values", "database": "from_body"},
        )

        args, kwargs = suggestions.build_prompt.call_args
        assert args[0] == "values"
        assert kwargs["database"] == "from_body"
        assert kwargs["chunk"] == 5

    def test_a_get_ignores_any_body(self, client, suggestions):
        client.get(
            f"{PROMPT}?phase=variables&database={TABLE}",
            data=json.dumps({"phase": "values", "mapping": valid_mapping()}),
            content_type="application/json",
        )

        args, kwargs = suggestions.build_prompt.call_args
        assert args[0] == "variables"
        assert kwargs["mapping"] is None

    @pytest.mark.parametrize(
        "chunk, expected",
        [("40", 40), ("abc", None), ("", None), ("0", 0), ("-5", -5), ("2.5", None)],
    )
    def test_the_chunk_size_is_read_as_a_whole_number_or_left_to_the_default(
        self, client, suggestions, chunk, expected
    ):
        client.get(f"{PROMPT}?phase=variables&database={TABLE}&chunk={chunk}")

        assert suggestions.build_prompt.call_args.kwargs["chunk"] == expected

    @pytest.mark.parametrize(
        "include, expected",
        [
            ("opmerking,%20notes,", ["opmerking", "notes"]),
            (",,", []),
            ("single", ["single"]),
        ],
    )
    def test_included_columns_are_comma_separated_in_the_query_string(
        self, client, suggestions, include, expected
    ):
        client.get(f"{PROMPT}?phase=values&database={TABLE}&include={include}")

        assert suggestions.build_prompt.call_args.kwargs["include"] == expected

    @pytest.mark.parametrize("include", [7, {"a": 1}, None, True])
    def test_included_columns_of_another_type_are_ignored(
        self, client, suggestions, include
    ):
        _json_post(
            client, PROMPT, {"phase": "values", "database": TABLE, "include": include}
        )

        assert suggestions.build_prompt.call_args.kwargs["include"] == []

    @pytest.mark.parametrize(
        "mapping",
        [{**valid_mapping(), "@type": "mapping:NotAMapping"}, "text", [1]],
        ids=["invalid", "text", "list"],
    )
    def test_a_browser_map_that_fails_validation_is_dropped(
        self, client, suggestions, mapping
    ):
        _json_post(
            client,
            PROMPT,
            {"phase": "variables", "database": TABLE, "mapping": mapping},
        )

        kwargs = suggestions.build_prompt.call_args.kwargs
        assert kwargs["mapping"] is None
        assert kwargs["mapping_data"] is None

    @pytest.mark.route("GET", PROMPT, "failure")
    @pytest.mark.parametrize(
        "query, kind",
        [
            ("?phase=bogus&database=x", "unknown_phase"),
            ("?database=x", "unknown_phase"),
            ("?phase=variables", "unknown_database"),
            ("?phase=variables&database=", "unknown_database"),
        ],
    )
    def test_a_missing_or_unknown_parameter_is_a_client_error(
        self, client, suggestions, query, kind
    ):
        response = client.get(f"{PROMPT}{query}")

        assert response.status_code == 400
        assert response.get_json()["kind"] == kind
        suggestions.build_prompt.assert_not_called()

    @pytest.mark.route("POST", PROMPT, "failure")
    @pytest.mark.parametrize("body", [["not", "an", "object"], "text", 7, None])
    def test_a_post_body_that_is_not_an_object_is_a_client_error(self, client, body):
        response = _json_post(client, PROMPT, body)

        assert response.status_code == 400
        assert response.get_json()["kind"] == "unknown_phase"

    def test_the_services_refusal_is_a_client_error_with_its_kind(
        self, client, suggestions
    ):
        suggestions.build_prompt.side_effect = SuggestionRequestError(
            "unknown_database", "unknown database 'nope'"
        )

        response = client.get(f"{PROMPT}?phase=variables&database=nope")

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "unknown database 'nope'",
            "kind": "unknown_database",
        }

    def test_without_a_service_the_prompt_is_unavailable(self, no_service):
        response = no_service.get(f"{PROMPT}?phase=variables&database={TABLE}")

        assert response.status_code == 503
        assert response.get_json() == {
            "error": "suggestions are not available",
            "kind": "no_service",
        }

    @pytest.mark.parametrize("chunk", ["1e999", "-1e999", "Infinity", "-Infinity"])
    def test_an_infinite_chunk_size_falls_back_to_the_default(
        self, client, suggestions, chunk
    ):
        response = client.post(
            PROMPT,
            data=f'{{"phase": "variables", "database": "{TABLE}", "chunk": {chunk}}}',
            content_type="application/json",
        )

        assert response.status_code == 200
        assert suggestions.build_prompt.call_args.kwargs["chunk"] is None


# ---------------------------------------------------------------------------
# POST /api/v1/suggestions/<phase>/ingest
# ---------------------------------------------------------------------------

INGEST = "/api/v1/suggestions/variables/ingest"


class TestIngestPastedAnswer:
    @pytest.mark.route("POST", "/api/v1/suggestions/<phase>/ingest", "success")
    def test_a_pasted_answer_is_merged_by_the_service(
        self, client, session_cache, store, suggestions
    ):
        suggestions.ingest.return_value = {"accepted": 3, "nulled": 1, "job": {}}

        response = _json_post(
            client,
            INGEST,
            {
                "database": TABLE,
                "source": "pasted_llm",
                "answer": "```json\n[]\n```",
                "mapping": valid_mapping(),
            },
        )

        assert response.status_code == 200
        assert response.get_json() == {"accepted": 3, "nulled": 1, "job": {}}
        args, kwargs = suggestions.ingest.call_args
        assert args == ("variables", session_cache, store)
        assert kwargs["database"] == TABLE
        assert kwargs["answer"] == "```json\n[]\n```"
        assert kwargs["records"] is None
        assert kwargs["source"] == "pasted_llm"
        assert isinstance(kwargs["mapping"], JSONLDMapping)
        assert kwargs["dismissed"] == []

    def test_a_record_list_is_accepted_instead_of_text(self, client, suggestions):
        records = [{"item": "M", "match": "male"}]

        _json_post(
            client,
            "/api/v1/suggestions/values/ingest",
            {"database": TABLE, "records": records},
        )

        kwargs = suggestions.ingest.call_args.kwargs
        assert kwargs["answer"] is None
        assert kwargs["records"] == records
        assert kwargs["source"] == "pasted_llm"

    def test_text_wins_over_records_when_both_are_sent(self, client, suggestions):
        _json_post(client, INGEST, {"database": TABLE, "answer": "", "records": [{}]})

        kwargs = suggestions.ingest.call_args.kwargs
        assert kwargs["answer"] == ""
        assert kwargs["records"] is None

    @pytest.mark.parametrize(
        "dismissed, expected",
        [
            ([f"{TABLE}_taal", 7], [f"{TABLE}_taal", "7"]),
            ("not a list", []),
            (None, []),
            ({"key": "value"}, []),
        ],
    )
    def test_dismissed_keys_are_passed_on_as_strings(
        self, client, suggestions, dismissed, expected
    ):
        _json_post(
            client, INGEST, {"database": TABLE, "answer": "{}", "dismissed": dismissed}
        )

        assert suggestions.ingest.call_args.kwargs["dismissed"] == expected

    @pytest.mark.parametrize("source", ["", None])
    def test_a_missing_source_defaults_to_a_pasted_answer(
        self, client, suggestions, source
    ):
        _json_post(
            client, INGEST, {"database": TABLE, "answer": "{}", "source": source}
        )

        assert suggestions.ingest.call_args.kwargs["source"] == "pasted_llm"

    @pytest.mark.route("POST", "/api/v1/suggestions/<phase>/ingest", "failure")
    @pytest.mark.parametrize(
        "body, kind",
        [
            ({"answer": "{}"}, "unknown_database"),
            ({"database": "", "answer": "{}"}, "unknown_database"),
            ({"database": TABLE}, "bad_answer"),
            ({"database": TABLE, "records": {"item": "M"}}, "bad_answer"),
            (["not", "an", "object"], "unknown_database"),
            ("text", "unknown_database"),
        ],
        ids=[
            "no-database",
            "empty-database",
            "no-answer",
            "records-not-a-list",
            "list-body",
            "text-body",
        ],
    )
    def test_an_incomplete_request_is_a_client_error(
        self, client, suggestions, body, kind
    ):
        response = _json_post(client, INGEST, body)

        assert response.status_code == 400
        assert response.get_json()["kind"] == kind
        suggestions.ingest.assert_not_called()

    def test_an_unknown_phase_is_a_client_error(self, client):
        response = _json_post(
            client,
            "/api/v1/suggestions/bogus/ingest",
            {"database": TABLE, "answer": "{}"},
        )

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "unknown phase 'bogus'",
            "kind": "unknown_phase",
        }

    def test_the_services_refusal_is_a_client_error_with_its_kind(
        self, client, suggestions
    ):
        suggestions.ingest.side_effect = SuggestionRequestError(
            "bad_answer", "Could not find valid JSON in the pasted text."
        )

        response = _json_post(client, INGEST, {"database": TABLE, "answer": "nope"})

        assert response.status_code == 400
        assert response.get_json() == {
            "error": "Could not find valid JSON in the pasted text.",
            "kind": "bad_answer",
        }

    def test_without_a_service_the_answer_cannot_be_ingested(self, no_service):
        response = _json_post(no_service, INGEST, {"database": TABLE, "answer": "{}"})

        assert response.status_code == 503
        assert response.get_json()["kind"] == "no_service"
