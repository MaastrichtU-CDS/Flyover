"""
Fixtures for the API / functional tests (issue #109).

See ``support.py`` for the doubles and the app wiring these fixtures hand to
the tests. Every test gets a fresh app, session cache and set of doubles, so
no state crosses tests.

``route`` marks: a test marked ``@pytest.mark.route(method, rule, outcome)``
must send at least one ``method`` request to ``rule``; the check runs after
the test body, so a mislabelled test fails. ``test_api_routes.py`` uses the
same marks to require a success and a failure test for every route.
"""

import os
import socket
import sys
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.api.support import (  # noqa: E402
    ApiDoubles,
    RequestLog,
    api_settings,
    build_api_app,
    build_spa,
    unmet_route_marks,
)

# ---------------------------------------------------------------------------
# Hypothesis profiles. "ci" (the default) is derandomised, so every run tries
# the same examples and a failure always reproduces; "thorough" explores new
# random examples on every run, for local bug hunting:
#   HYPOTHESIS_PROFILE=thorough pytest tests/api/test_api_fuzz.py
# The function-scoped fixtures are safe: every example resets the state it
# touches (ApiDoubles.reset_state).
# ---------------------------------------------------------------------------
_SHARED = dict(
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
    print_blob=True,
)
settings.register_profile(
    "ci", max_examples=150, derandomize=True, database=None, **_SHARED
)
settings.register_profile("thorough", max_examples=2000, **_SHARED)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))

_REQUEST_LOG = pytest.StashKey[RequestLog]()


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    result = yield
    marks = list(item.iter_markers("route"))
    log = item.stash.get(_REQUEST_LOG, None)
    if marks:
        problems = unmet_route_marks(log or RequestLog(), marks)
        if problems:
            raise AssertionError("\n".join(problems))
    return result


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """API tests never leave the process.

    Every service is a double, so any socket use is a leak (for example a
    controller building a real store client). It is refused, and recorded:
    code under test may swallow the error, the teardown check does not.
    """
    attempts = []

    def refuse(*args, **kwargs):
        attempts.append(args)
        raise ConnectionRefusedError("API tests must not open network connections")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
    yield attempts
    assert not attempts, f"the test tried to reach the network: {attempts}"


@pytest.fixture
def request_log(request) -> RequestLog:
    log = RequestLog()
    request.node.stash[_REQUEST_LOG] = log
    return log


@pytest.fixture
def spa_dir(tmp_path) -> Path:
    return build_spa(tmp_path)


@pytest.fixture
def doubles(tmp_path, monkeypatch) -> ApiDoubles:
    created = ApiDoubles.create(api_settings(tmp_path))
    for module, attribute, double in created.controller_patches():
        monkeypatch.setattr(module, attribute, double)
    return created


@pytest.fixture
def make_app(doubles, spa_dir, request_log):
    """Build another app on the same doubles (other SPA dir, context entries)."""

    def _make(spa=None, **context_overrides):
        app = build_api_app(doubles, spa or spa_dir, context_overrides)
        request_log.attach(app)
        return app

    return _make


@pytest.fixture
def app(make_app):
    return make_app()


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def session_cache(doubles):
    return doubles.session_cache


@pytest.fixture
def store(doubles):
    return doubles.store


@pytest.fixture
def suggestions(doubles):
    return doubles.suggestions


@pytest.fixture
def helpers(doubles):
    return doubles.helpers


@pytest.fixture
def ingest_service(doubles):
    return doubles.ingest


@pytest.fixture
def describe_service(doubles):
    return doubles.describe


@pytest.fixture
def annotate_service(doubles):
    return doubles.annotate


@pytest.fixture
def share_service(doubles):
    return doubles.share
