"""
Unit tests of Flyover's side of the store launcher
(``services.store_launcher``), with the launcher faked at the ``requests``
level.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.store_launcher import (  # noqa: E402
    LauncherError,
    ModuleState,
    StoreLauncher,
)

LAUNCHER = "http://store-launcher:7290"
MODULE_URL = f"{LAUNCHER}/modules/rdf-store"


@pytest.fixture
def web():
    with responses.RequestsMock() as mock:
        yield mock


def test_a_modules_state_comes_with_when_it_started(web):
    web.get(
        MODULE_URL,
        json={"state": "running", "startedAt": "2026-11-08T10:00:05.123456789Z"},
    )

    assert StoreLauncher(f"{LAUNCHER}/").state("rdf-store") == ModuleState(
        "running", datetime(2026, 11, 8, 10, 0, 5, 123456, tzinfo=timezone.utc)
    )


def test_starting_a_module_answers_with_its_state(web):
    web.post(f"{MODULE_URL}/start", json={"state": "running", "startedAt": None})

    assert StoreLauncher(LAUNCHER).start("rdf-store") == ModuleState("running", None)
    [call] = web.calls
    assert call.request.method == "POST"


@pytest.mark.parametrize("started", [None, "", "not a time", "2026-11-08T10:00:05"])
def test_a_start_time_that_cannot_be_read_is_none(web, started):
    web.get(MODULE_URL, json={"state": "stopped", "startedAt": started})

    assert StoreLauncher(LAUNCHER).state("rdf-store").started_at is None


@pytest.mark.parametrize(
    "answer, message",
    [
        (
            {"body": requests.ConnectionError("refused")},
            "Flyover cannot reach its store launcher.",
        ),
        (
            {"body": requests.Timeout("slow")},
            "Flyover cannot reach its store launcher.",
        ),
        (
            {"status": 502, "json": {"error": "Docker could not be reached."}},
            "The store launcher refused: Docker could not be reached.",
        ),
        ({"status": 500, "body": ""}, "The store launcher refused: HTTP 500."),
        (
            {"json": {"state": "exploded"}},
            "The store launcher gave an answer Flyover cannot read.",
        ),
        (
            {"body": "<html></html>"},
            "The store launcher gave an answer Flyover cannot read.",
        ),
    ],
)
def test_a_launcher_that_cannot_answer_says_why(web, answer, message):
    web.get(MODULE_URL, **answer)

    with pytest.raises(LauncherError) as raised:
        StoreLauncher(LAUNCHER).state("rdf-store")

    assert raised.value.message == message
