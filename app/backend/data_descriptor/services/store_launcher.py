"""
Flyover's side of the store launcher (``stores/launcher``): asking it to start
a store module, and in what state the module is.

Flyover has no access to Docker. Where the stack runs the launcher, its URL
is set (``FLYOVER_STORE_LAUNCHER_URL``) and Flyover asks it to start a store
once a user has chosen to work on it. Without that setting, Flyover counts
its stores as running already, as it always did.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# Seconds to connect, and to wait for the answer: starting a container may
# take a moment.
LAUNCHER_TIMEOUT = (3, 60)
MODULE_STATES = ("running", "starting", "stopped", "missing")
# Docker writes nanoseconds; Python reads at most microseconds.
_FRACTION = re.compile(r"(\.\d{6})\d+")


class LauncherError(Exception):
    """The launcher could not be asked, or refused; the reason for the user."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def _timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    text = _FRACTION.sub(r"\1", value.strip()).replace("Z", "+00:00")
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo else None


@dataclass(frozen=True)
class ModuleState:
    """A store module as the launcher reports it.

    Attributes:
        state: ``running`` (every container runs), ``starting`` (one is
            restarting), ``stopped`` (one is not running) or ``missing`` (the
            stack has no container for the module).
        started_at: When the module's latest running container started.
        never_started: Whether none of its containers has ever run, as
            after ``docker compose create``.
    """

    state: str
    started_at: Optional[datetime] = None
    never_started: bool = False

    @classmethod
    def from_answer(cls, body: Any) -> "ModuleState":
        body = body if isinstance(body, dict) else {}
        state = body.get("state")
        if state not in MODULE_STATES:
            raise LauncherError(
                "The store launcher gave an answer Flyover cannot read."
            )
        return cls(
            state=state,
            started_at=_timestamp(body.get("startedAt")),
            never_started=body.get("neverStarted") is True,
        )


class StoreLauncher:
    """Asks the launcher at ``base_url`` about a store module."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def state(self, module: str) -> ModuleState:
        return self._call("GET", f"/modules/{module}")

    def start(self, module: str) -> ModuleState:
        """Start the module's containers that are not running."""
        return self._call("POST", f"/modules/{module}/start")

    def stop(self, module: str) -> ModuleState:
        """Stop the module's running containers."""
        return self._call("POST", f"/modules/{module}/stop")

    def _call(self, method: str, path: str) -> ModuleState:
        try:
            response = requests.request(
                method, f"{self.base_url}{path}", timeout=LAUNCHER_TIMEOUT
            )
        except requests.RequestException as error:
            logger.info(
                "The store launcher could not be reached (%s)", type(error).__name__
            )
            raise LauncherError("Flyover cannot reach its store launcher.") from None
        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code != 200:
            reason = body.get("error") if isinstance(body, dict) else ""
            raise LauncherError(
                "The store launcher refused: "
                + (reason or f"HTTP {response.status_code}.")
            )
        return ModuleState.from_answer(body)
