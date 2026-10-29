"""
Fixtures for the cross-layer integration tests (issue #108).

See ``support.py`` for the test doubles (fake RDF store, fake curl) and the
app wiring these fixtures hand to the tests.
"""

import copy
import os
import sys
import tempfile
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.integration.support import (  # noqa: E402
    _FAKE_CURL_SCRIPT,
    _MAPPING,
    TABLE,
    TABLE_COLUMNS,
    FakeCurl,
    FakeRdfStore,
    SessionCache,
    build_app,
)


@pytest.fixture
def rdf_store():
    """A fake RDF store intercepting every `requests` call for its base URL.

    Any request to another host raises a ConnectionError, so a test can never
    reach the network by accident.
    """
    store = FakeRdfStore()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        store.install(mock)
        yield store


@pytest.fixture
def fake_curl(tmp_path, monkeypatch) -> FakeCurl:
    """A `curl` on PATH that records its arguments instead of uploading."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    script = bin_dir / "curl"
    script.write_text(_FAKE_CURL_SCRIPT)
    script.chmod(0o755)
    log = tmp_path / "curl.log"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    monkeypatch.setenv("FAKE_CURL_LOG", str(log))
    monkeypatch.setenv("FAKE_CURL_EXIT", "0")
    return FakeCurl(log, monkeypatch)


@pytest.fixture
def app(tmp_path, monkeypatch, rdf_store):
    """The integration app. Temp files (annotation queries, zips) stay in tmp_path."""
    scratch = tmp_path / "tmp"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    monkeypatch.chdir(tmp_path)
    return build_app(tmp_path, rdf_store)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def session_cache(app) -> SessionCache:
    return app.config["APP_CONTEXT"]["session_cache"]


@pytest.fixture
def mapping_dict() -> dict:
    """A fresh copy of the valid integration mapping."""
    return copy.deepcopy(_MAPPING)


@pytest.fixture
def seeded_store(rdf_store) -> FakeRdfStore:
    """The fake store after the integration CSV was ingested."""
    rdf_store.seed_table(TABLE, TABLE_COLUMNS)
    return rdf_store
