"""
Getting the store Flyover works on running, and saying where it stands.

Flyover starts the local store only once a user has chosen to work on it: by
opening a project whose map names it (an endpoint on localhost, or none), or
by continuing without a project, in the standard RDF flow. Until someone
chooses, Flyover waits. A store on another server is not Flyover's to start.
Flyover never stops a store: the store may serve others than Flyover too.

The states the status bar shows (see ``StoreLifecycle.status``):

- ``waiting``: nobody has chosen a project, or the standard RDF flow, yet;
- ``starting``: Flyover started the store a moment ago, and it does not
  answer yet;
- ``reachable`` and ``unreachable``: the store answers, or it does not, and
  why not;
- ``none``: the work uses no database. Nothing does yet; a later workflow
  (a mapped CSV export, say) may keep everything in memory.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Tuple

from services.store_launcher import LauncherError, StoreLauncher
from services.store_selection import StoreAddress, StoreSelection
from utils.rdf_store_url import normalise_rdf_store_base_url

logger = logging.getLogger(__name__)

WAITING = "waiting"
STARTING = "starting"
REACHABLE = "reachable"
UNREACHABLE = "unreachable"
NO_DATABASE = "none"

# How long a store Flyover started may take before its silence counts as a
# fault: GraphDB opens a large repository slowly.
STARTING_GRACE = timedelta(minutes=5)

# Also when the container predates the launcher's label.
STORE_MISSING = (
    "Flyover cannot start the RDF store: the stack has no container for it "
    "with the launcher's label. Run `docker compose create` to create or "
    "update it."
)
STORE_STOPPED = (
    "The RDF store is not running. Start it with Docker, or choose it again "
    "under Switch project."
)
LAUNCHER_UNREACHABLE = (
    "Flyover cannot reach its store launcher, so it cannot start the RDF store."
)


def _server(store: StoreAddress) -> str:
    return normalise_rdf_store_base_url(store.base_url, store.repository).rstrip("/")


class StoreLifecycle:
    """Starts the stores Flyover may start, and tells where a store stands.

    Args:
        selection: The store selection; its configured store is the local
            one, whose server the launcher can start.
        launcher: The store launcher, or None where the stack runs none:
            then every store counts as running already.
        module: The launcher's name for the local store's module.
        now: The clock, for tests.
    """

    def __init__(
        self,
        selection: StoreSelection,
        launcher: Optional[StoreLauncher] = None,
        module: str = "rdf-store",
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._selection = selection
        self._launcher = launcher
        self._module = module
        self._now = now

    def manages(self, store: StoreAddress) -> bool:
        """Whether Flyover may start ``store``: one on the local store's
        server (any repository), where the stack runs a launcher."""
        return self._launcher is not None and _server(store) == _server(
            self._selection.configured
        )

    def ensure_started(self, store: StoreAddress) -> None:
        """Ask the launcher to start ``store`` when it is Flyover's to start.

        Called when a user chooses to work on a store. A store that does not
        start shows as unreachable, with the reason, at the next status check.
        """
        if not self.manages(store):
            return
        try:
            self._launcher.start(self._module)
        except LauncherError as error:
            logger.warning("Flyover could not start the RDF store: %s", error.message)

    def status(
        self,
        store: StoreAddress,
        chosen: bool,
        check: Callable[[], Tuple[bool, str]],
    ) -> Tuple[str, str]:
        """Where ``store`` stands: one of the states above, and the reason
        to show (empty unless the store is unreachable).

        Args:
            store: The store the app works on.
            chosen: Whether a user has chosen to work on it (a project, or
                the standard RDF flow); until then the app waits.
            check: Asks the store whether it answers: (reachable, reason).
        """
        if not chosen:
            return WAITING, ""
        reachable, reason = check()
        if reachable:
            return REACHABLE, ""
        if not self.manages(store):
            return UNREACHABLE, reason
        try:
            module = self._launcher.state(self._module)
        except LauncherError:
            return UNREACHABLE, LAUNCHER_UNREACHABLE
        if module.state == "starting":
            return STARTING, ""
        if module.state == "running":
            started = module.started_at
            if started is not None and self._now() - started < STARTING_GRACE:
                return STARTING, ""
            return UNREACHABLE, reason
        if module.state == "missing":
            return UNREACHABLE, STORE_MISSING
        return UNREACHABLE, STORE_STOPPED
