"""
Fixtures for the performance checks.

The app and doubles are the API layer's (``tests/api/support.py``), so a
check times the HTTP layer and the request-boundary validation, not the
store or the triplifier. The checks only run when selected (``-m perf``, or
pytest pointed at this folder); see ``tests/conftest.py``.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.api.support import (  # noqa: E402
    ApiDoubles,
    api_settings,
    block_network,
    build_api_app,
    build_spa,
)
from tests.perf.support import REPORT  # noqa: E402


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    attempts = block_network(monkeypatch)
    yield attempts
    assert not attempts, f"the check tried to reach the network: {attempts}"


@pytest.fixture
def doubles(tmp_path, monkeypatch) -> ApiDoubles:
    created = ApiDoubles.create(api_settings(tmp_path))
    for module, attribute, double in created.controller_patches():
        monkeypatch.setattr(module, attribute, double)
    return created


@pytest.fixture
def app(doubles, tmp_path):
    return build_api_app(doubles, build_spa(tmp_path))


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def helpers(doubles):
    return doubles.helpers


@pytest.fixture
def store(doubles):
    return doubles.store


@pytest.fixture
def session_cache(doubles):
    return doubles.session_cache


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


@pytest.fixture
def perf():
    return REPORT


def pytest_terminal_summary(terminalreporter):
    if not REPORT.rows:
        return
    terminalreporter.section("Flyover performance checks")
    terminalreporter.write(REPORT.markdown())
    target = os.environ.get("FLYOVER_PERF_REPORT")
    if target:
        Path(target).write_text(REPORT.markdown(), encoding="utf-8")
