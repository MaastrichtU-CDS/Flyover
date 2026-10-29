"""
API tests for the route table as a whole: which routes exist, how Werkzeug
picks between overlapping ones, and what every route does with a method it
does not declare.

The inventory below is deliberately explicit: adding, removing or changing
a route fails ``test_the_route_table_is_the_documented_api`` until the
inventory (and the tests for the new route) are updated with it.
"""

import pytest

from tests.api.support import (
    RecordedRequest,
    RequestLog,
    declared_methods,
    declared_route_marks,
    rule_for,
    unmet_route_marks,
)

# (method, rule) for every route create_app() registers, without the HEAD
# and OPTIONS methods Flask adds on its own.
EXPECTED_ROUTES = {
    # ingest_controller
    ("POST", "/upload-semantic-map"),
    ("POST", "/submit-indexeddb-semantic-map"),
    ("POST", "/api/v1/validate-mapping"),
    ("POST", "/upload"),
    ("GET", "/data-submission"),
    ("GET", "/api/rdf-store-databases"),
    ("GET", "/api/check-graph-exists"),
    ("GET", "/get-existing-graph-structure"),
    # describe_controller
    ("GET", "/describe_landing"),
    ("GET", "/describe_variables"),
    ("GET", "/api/v1/describe-variables-state"),
    ("GET", "/api/v1/describe-variable-details-state"),
    ("POST", "/api/v1/describe-variable-details-state"),
    ("POST", "/units"),
    ("GET", "/describe_variable_details"),
    ("GET", "/end"),
    ("POST", "/end"),
    # annotate_controller
    ("GET", "/annotation_landing"),
    ("POST", "/upload-annotation-json"),
    ("GET", "/annotation-review"),
    ("POST", "/start-annotation"),
    ("GET", "/annotation-verify"),
    ("GET", "/api/v1/annotation-verify-state"),
    ("POST", "/verify-annotation-ask"),
    # share_controller
    ("GET", "/share_landing"),
    ("GET", "/downloadSemanticMap"),
    ("GET", "/downloadOntology"),
    ("GET", "/favicon.ico"),
    ("GET", "/share_mock"),
    ("GET", "/share_publish"),
    ("POST", "/api/generate-mock-data"),
    # suggestions_controller
    ("GET", "/api/v1/suggestions/status"),
    ("POST", "/api/v1/suggestions/<phase>/start"),
    ("GET", "/api/v1/suggestions/<phase>"),
    ("POST", "/api/v1/suggestions/<phase>/priority"),
    ("GET", "/api/v1/suggestions/prompt"),
    ("POST", "/api/v1/suggestions/prompt"),
    ("POST", "/api/v1/suggestions/<phase>/ingest"),
    # app_factory: the built SPA and Flask's static folder
    ("GET", "/"),
    ("GET", "/<path:subpath>"),
    ("GET", "/static/<path:filename>"),
}

# A concrete path for every rule with a converter.
_EXAMPLE_PATHS = {
    "/api/v1/suggestions/<phase>/start": "/api/v1/suggestions/variables/start",
    "/api/v1/suggestions/<phase>": "/api/v1/suggestions/variables",
    "/api/v1/suggestions/<phase>/priority": "/api/v1/suggestions/values/priority",
    "/api/v1/suggestions/<phase>/ingest": "/api/v1/suggestions/values/ingest",
    "/<path:subpath>": "/describe/variables",
    "/static/<path:filename>": "/static/favicon.ico",
}


def _example_path(rule: str) -> str:
    return _EXAMPLE_PATHS.get(rule, rule)


def _route_cases():
    rules = sorted({rule for _, rule in EXPECTED_ROUTES})
    return [pytest.param(rule, id=rule) for rule in rules]


class TestRouteTable:
    def test_the_route_table_is_the_documented_api(self, app):
        actual = {
            (method, rule.rule)
            for rule in app.url_map.iter_rules()
            for method in rule.methods - {"HEAD", "OPTIONS"}
        }

        assert actual == EXPECTED_ROUTES

    def test_every_route_answers_head_and_options_like_flask_does(self, app):
        for rule in app.url_map.iter_rules():
            assert {"HEAD", "OPTIONS"} <= rule.methods or "GET" not in rule.methods
            assert "OPTIONS" in rule.methods

    def test_every_example_path_routes_to_its_own_rule(self, app):
        for _, rule in EXPECTED_ROUTES:
            assert rule_for(app, _example_path(rule), rule), rule


class TestRoutingPrecedence:
    """Werkzeug prefers static rules over converters; the app relies on it."""

    @pytest.mark.parametrize(
        "path, endpoint",
        [
            ("/api/v1/suggestions/status", "suggestions.suggestions_status"),
            ("/api/v1/suggestions/prompt", "suggestions.suggestions_prompt"),
            ("/api/v1/suggestions/variables", "suggestions.get_suggestions"),
            ("/data-submission", "ingest.data_submission"),
            ("/favicon.ico", "share.favicon"),
            ("/static/favicon.ico", "static"),
            ("/describe/variables", "serve_spa"),
            ("/", "serve_spa"),
        ],
    )
    def test_get_requests_reach_the_specific_route_not_the_spa_fallback(
        self, app, path, endpoint
    ):
        adapter = app.url_map.bind("localhost")

        matched, _ = adapter.match(path, method="GET")

        assert matched == endpoint

    def test_a_get_to_a_post_only_route_falls_through_to_the_spa(self, client):
        # The browser re-requesting /upload after the form post (a reload)
        # gets the SPA shell, which renders its own not-found view.
        response = client.get("/upload")

        assert response.status_code == 200
        assert response.mimetype == "text/html"
        assert b"Flyover SPA" in response.data

    def test_a_post_to_an_unknown_path_is_method_not_allowed(self, client):
        # The SPA fallback matches every path for GET, so a POST anywhere
        # unknown is a 405 rather than a 404.
        response = client.post("/no/such/endpoint")

        assert response.status_code == 405
        assert set(response.headers["Allow"].split(", ")) == {"GET", "HEAD", "OPTIONS"}


class TestMethodsARouteDoesNotDeclare:
    """Every route refuses the methods it does not declare with a 405."""

    @pytest.mark.parametrize("rule", _route_cases())
    @pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
    def test_an_undeclared_method_is_405_with_an_allow_header(
        self, app, client, rule, method
    ):
        declared = declared_methods(app)[rule]
        if method in declared:
            pytest.skip(f"{rule} declares {method}")
        path = _example_path(rule)
        if method == "GET":
            pytest.skip("a GET to any path falls through to the SPA (tested above)")

        response = client.open(path, method=method)

        assert response.status_code == 405
        allowed = set(response.headers["Allow"].split(", "))
        assert declared <= allowed
        assert method not in allowed

    @pytest.mark.parametrize("rule", _route_cases())
    def test_options_lists_the_declared_methods(self, app, client, rule):
        response = client.options(_example_path(rule))

        assert response.status_code == 200
        assert declared_methods(app)[rule] <= set(response.headers["Allow"].split(", "))
        assert response.data == b""


class TestRouteMarkMachinery:
    """The route marks the inventory relies on are checked, not just trusted."""

    def test_a_mark_is_met_by_a_request_to_its_rule(self, app):
        log = RequestLog(app=app)
        log.requests.append(RecordedRequest("GET", "/api/v1/suggestions/variables"))
        mark = pytest.mark.route("GET", "/api/v1/suggestions/<phase>", "success").mark

        assert unmet_route_marks(log, [mark]) == []

    def test_a_mark_without_a_matching_request_is_reported(self, app):
        log = RequestLog(app=app)
        log.requests.append(RecordedRequest("GET", "/api/v1/suggestions/status"))
        mark = pytest.mark.route("GET", "/api/v1/suggestions/<phase>", "success").mark

        (problem,) = unmet_route_marks(log, [mark])

        assert "never sent that request" in problem

    def test_a_method_the_rule_refuses_still_counts_as_a_request_to_it(self, app):
        log = RequestLog(app=app)
        log.requests.append(RecordedRequest("PUT", "/upload"))
        mark = pytest.mark.route("PUT", "/upload", "failure").mark

        assert unmet_route_marks(log, [mark]) == []

    def test_malformed_marks_are_reported(self, app):
        log = RequestLog(app=app)
        log.requests.append(RecordedRequest("GET", "/"))
        wrong_outcome = pytest.mark.route("GET", "/", "maybe").mark
        too_short = pytest.mark.route("GET", "/").mark

        problems = unmet_route_marks(log, [wrong_outcome, too_short])

        assert any("unknown outcome" in p for p in problems)
        assert any("needs (method, rule, outcome)" in p for p in problems)

    def test_every_declared_mark_names_a_route_and_an_outcome(self):
        marks = declared_route_marks()

        assert all(len(mark) == 3 for mark in marks), marks
        assert all(outcome in ("success", "failure") for _, _, outcome in marks)
        assert {rule for _, rule, _ in marks} <= {rule for _, rule in EXPECTED_ROUTES}

    def test_the_source_scan_finds_decorators_and_param_marks(self, tmp_path):
        (tmp_path / "test_api_example.py").write_text(
            "import pytest\n"
            "@pytest.mark.route('GET', '/a', 'success')\n"
            "def test_a(): pass\n"
            "@pytest.mark.parametrize('x', [\n"
            "    pytest.param(1, marks=pytest.mark.route('POST', '/b', 'failure')),\n"
            "    pytest.param(2, marks=[pytest.mark.slow,\n"
            "                           pytest.mark.route('GET', '/c', 'failure')]),\n"
            "])\n"
            "def test_b(x): pass\n"
            "def test_c():\n"
            "    pytest.mark.route('GET', '/not-a-mark-on-a-test', 'success')\n",
            encoding="utf-8",
        )

        assert declared_route_marks(tmp_path) == {
            ("GET", "/a", "success"),
            ("POST", "/b", "failure"),
            ("GET", "/c", "failure"),
        }

    def test_a_mark_with_computed_arguments_is_refused_with_its_location(
        self, tmp_path
    ):
        (tmp_path / "test_api_example.py").write_text(
            "import pytest\n"
            "@pytest.mark.parametrize('path', ['/a'])\n"
            "def test_a(path):\n"
            "    pass\n"
            "def test_b():\n"
            "    pass\n"
            "test_b = pytest.mark.parametrize('p', [\n"
            "    pytest.param(1, marks=pytest.mark.route('GET', str(1), 'success'))\n"
            "])(test_b)\n",
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match=r"test_api_example.py:8: .*literals"):
            declared_route_marks(tmp_path)

    def test_module_constants_can_name_the_rule(self, tmp_path):
        (tmp_path / "test_api_example.py").write_text(
            "import pytest\n"
            "STATE = '/api/v1/state'\n"
            "@pytest.mark.route('GET', STATE, 'success')\n"
            "def test_a(): pass\n",
            encoding="utf-8",
        )

        assert declared_route_marks(tmp_path) == {("GET", "/api/v1/state", "success")}
