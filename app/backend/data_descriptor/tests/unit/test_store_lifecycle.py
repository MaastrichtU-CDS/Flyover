"""
Unit tests of starting the store a user chooses, and of where a store stands
(``services.store_lifecycle``), with the launcher and the clock faked.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.store_launcher import LauncherError, ModuleState  # noqa: E402
from services.store_lifecycle import (  # noqa: E402
    LAUNCHER_UNREACHABLE,
    STARTING_GRACE,
    STORE_MISSING,
    STORE_STOPPED,
    StoreLifecycle,
)
from services.store_selection import StoreAddress, StoreSelection  # noqa: E402

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
LOCAL = StoreAddress("http://rdf-store:7200", "userRepo")
LOCAL_OTHER_REPOSITORY = StoreAddress("http://rdf-store:7200/", "aya", source="project")
REMOTE = StoreAddress("https://graphdb.example.org", "aya", source="project")


class FakeLauncher:
    def __init__(self, state=None, error=None):
        self.answer = state or ModuleState("running", NOW)
        self.error = error
        self.started = []
        self.stopped = []
        self.asked = 0

    def state(self, module):
        self.asked += 1
        if self.error:
            raise self.error
        return self.answer

    def start(self, module):
        self.started.append(module)
        if self.error:
            raise self.error
        return self.answer

    def stop(self, module):
        self.stopped.append(module)
        if self.error:
            raise self.error
        return ModuleState("stopped", None)


def lifecycle(launcher=None):
    return StoreLifecycle(
        StoreSelection(LOCAL), launcher, module="rdf-store", now=lambda: NOW
    )


def answers(reachable, reason=""):
    calls = []

    def check():
        calls.append(1)
        return reachable, reason

    check.calls = calls
    return check


def test_flyover_starts_only_stores_on_the_local_server_and_only_with_a_launcher():
    with_launcher = lifecycle(FakeLauncher())

    assert with_launcher.manages(LOCAL)
    assert with_launcher.manages(LOCAL_OTHER_REPOSITORY)
    assert not with_launcher.manages(REMOTE)
    assert not lifecycle(None).manages(LOCAL)


def test_a_chosen_local_store_is_started_and_a_remote_one_left_alone():
    launcher = FakeLauncher()

    lifecycle(launcher).ensure_started(LOCAL_OTHER_REPOSITORY)
    lifecycle(launcher).ensure_started(REMOTE)

    assert launcher.started == ["rdf-store"]


def test_a_start_that_fails_is_logged_and_shows_in_the_status(caplog):
    caplog.set_level(logging.WARNING)
    launcher = FakeLauncher(
        error=LauncherError("Flyover cannot reach its store launcher.")
    )

    lifecycle(launcher).ensure_started(LOCAL)

    assert "Flyover could not start the RDF store" in caplog.text
    assert lifecycle(launcher).status(LOCAL, True, answers(False, "down")) == (
        "unreachable",
        LAUNCHER_UNREACHABLE,
    )


def test_until_someone_chooses_flyover_waits_and_asks_no_store():
    check = answers(True)

    assert lifecycle(FakeLauncher()).status(LOCAL, False, check) == ("waiting", "")
    assert check.calls == []


def test_a_store_that_answers_is_reachable_whoever_runs_it():
    launcher = FakeLauncher()

    assert lifecycle(launcher).status(LOCAL, True, answers(True)) == ("reachable", "")
    assert lifecycle(None).status(REMOTE, True, answers(True)) == ("reachable", "")
    assert launcher.asked == 0


def test_a_silent_store_flyover_does_not_start_is_unreachable_with_its_reason():
    assert lifecycle(FakeLauncher()).status(
        REMOTE, True, answers(False, "No answer.")
    ) == (
        "unreachable",
        "No answer.",
    )
    assert lifecycle(None).status(LOCAL, True, answers(False, "No answer.")) == (
        "unreachable",
        "No answer.",
    )


@pytest.mark.parametrize(
    "module, expected",
    [
        (ModuleState("starting", None), ("starting", "")),
        (ModuleState("running", NOW - timedelta(seconds=20)), ("starting", "")),
        (
            ModuleState("running", NOW - STARTING_GRACE - timedelta(seconds=1)),
            ("unreachable", "No answer."),
        ),
        (ModuleState("running", None), ("unreachable", "No answer.")),
        (ModuleState("missing", None), ("unreachable", STORE_MISSING)),
        (ModuleState("stopped", None), ("stopped", STORE_STOPPED)),
    ],
    ids=[
        "restarting",
        "just started",
        "started long ago",
        "no start time",
        "missing",
        "stopped",
    ],
)
def test_a_silent_local_store_is_starting_for_a_while_then_unreachable(
    module, expected
):
    assert (
        lifecycle(FakeLauncher(module)).status(
            LOCAL, True, answers(False, "No answer.")
        )
        == expected
    )


def test_a_chosen_store_whose_container_never_ran_is_started_at_the_check():
    # Created after the choice, with docker compose create.
    launcher = FakeLauncher(ModuleState("stopped", None, never_started=True))

    assert lifecycle(launcher).status(LOCAL, True, answers(False, "No answer.")) == (
        "starting",
        "",
    )
    assert launcher.started == ["rdf-store"]


def test_a_store_someone_stopped_is_left_alone():
    launcher = FakeLauncher(ModuleState("stopped", None, never_started=False))

    assert lifecycle(launcher).status(LOCAL, True, answers(False, "No answer.")) == (
        "stopped",
        STORE_STOPPED,
    )
    assert launcher.started == []


def test_a_never_ran_store_that_cannot_be_started_says_why():
    class Refusing(FakeLauncher):
        def start(self, module):
            self.started.append(module)
            raise LauncherError("The store launcher refused: Conflict.")

    launcher = Refusing(ModuleState("stopped", None, never_started=True))

    assert lifecycle(launcher).status(LOCAL, True, answers(False, "No answer.")) == (
        "unreachable",
        "Flyover could not start the RDF store. The store launcher refused: Conflict.",
    )


def test_nothing_is_started_before_someone_chooses_even_if_it_never_ran():
    launcher = FakeLauncher(ModuleState("stopped", None, never_started=True))

    assert lifecycle(launcher).status(LOCAL, False, answers(False)) == ("waiting", "")
    assert launcher.started == []


def test_a_user_can_stop_flyovers_store_any_repository_of_it():
    launcher = FakeLauncher()

    lifecycle(launcher).stop(LOCAL)
    lifecycle(launcher).stop(LOCAL_OTHER_REPOSITORY)

    assert launcher.stopped == ["rdf-store", "rdf-store"]


def test_a_store_flyover_does_not_run_is_never_stopped():
    launcher = FakeLauncher()

    lifecycle(launcher).stop(REMOTE)
    lifecycle(None).stop(LOCAL)

    assert launcher.stopped == []


def test_a_stop_the_launcher_refuses_says_why():
    launcher = FakeLauncher(error=LauncherError("Docker is busy"))

    with pytest.raises(LauncherError, match="Docker is busy"):
        lifecycle(launcher).stop(LOCAL)


def test_a_store_a_user_stopped_stays_stopped_until_a_user_starts_it():
    launcher = FakeLauncher(state=ModuleState("stopped", None, never_started=False))
    check = answers(False, "No answer.")

    lifecycle(launcher).stop(LOCAL)

    assert lifecycle(launcher).status(LOCAL, True, check) == ("stopped", STORE_STOPPED)
    assert launcher.started == []
