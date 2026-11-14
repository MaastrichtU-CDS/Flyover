"""
Which RDF store the app works on.

Flyover starts on the store its settings name (``FLYOVER_RDF_STORE_URL`` and
``FLYOVER_REPOSITORY_NAME``). An open project may name another one through
its map's top-level ``endpoint``; while it is open, every step (ingest,
describe, annotate, share and the suggestions) works on that store instead.

``StoreSelection`` holds the choice. ``SelectedStoreService`` stands in the
app context where the store service used to be: it hands every call to the
service of the selected store, and a request keeps the store it started on,
so a project that changes while a request is answered never splits that
request over two stores.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from flask import g, has_request_context

from services.rdf_store_service import RDFStoreService
from utils.rdf_store_url import (
    build_repository_endpoint,
    names_this_machine,
    normalise_rdf_store_base_url,
    repository_of_endpoint,
    without_credentials,
)

CONFIGURED = "configured"
PROJECT = "project"

# Why a map's localhost endpoint does not mean what it says inside Docker.
LOCALHOST_NOTE = (
    "The map's endpoint names this machine (localhost), which inside "
    "Flyover's container is the container itself, so Flyover uses its "
    "configured store with the map's repository."
)

# The name the store a request works on is kept under in flask.g.
_REQUEST_STORE = "flyover_store_address"


@dataclass(frozen=True)
class StoreAddress:
    """Where an RDF store is: its server's base URL and the repository.

    Attributes:
        base_url: The store server's base URL as the app reaches it,
            credentials included.
        repository: The repository's name.
        source: ``configured`` for the store the settings name, ``project``
            for one an open project's map names.
        note: Anything worth telling the user about how the address was found.
        public_base_url: The same server's base URL as its users reach it,
            for showing; empty when that is ``base_url``. Inside Docker the
            app reaches the configured store by its service name.
    """

    base_url: str
    repository: str
    source: str = CONFIGURED
    note: str = ""
    public_base_url: str = ""

    @property
    def key(self) -> Tuple[str, str]:
        """What makes two addresses the same store."""
        base = normalise_rdf_store_base_url(self.base_url, self.repository)
        return base.rstrip("/"), self.repository

    @property
    def repository_url(self) -> str:
        """The repository's query endpoint, credentials included."""
        return build_repository_endpoint(self.base_url, self.repository)

    def same_store(self, other: "StoreAddress") -> bool:
        return self.key == other.key

    def describe(self) -> Dict[str, str]:
        """What the browser may be told: the address its users know the
        store by, never the credentials."""
        shown = build_repository_endpoint(
            self.public_base_url or self.base_url, self.repository
        )
        return {
            "kind": "rdf-store",
            "repository": self.repository,
            "url": without_credentials(shown),
            "source": self.source,
            "note": self.note,
        }


def store_for_endpoint(
    endpoint: Optional[str], configured: StoreAddress
) -> StoreAddress:
    """The store a map's top-level ``endpoint`` names.

    An empty or missing endpoint means the configured store. A localhost or
    loopback endpoint means the configured store too, with the repository
    the map names: such endpoints are written for the annotation tool run on
    the host, and inside Flyover's container localhost is the container.

    Raises:
        ValueError: when the endpoint cannot be used; the message says why.
    """
    if endpoint is None or not str(endpoint).strip():
        return configured
    base_url, repository = repository_of_endpoint(str(endpoint))
    note, public_base_url = "", ""
    if names_this_machine(base_url):
        # Shown as its users know it: the configured store's public address,
        # else the localhost address the map wrote.
        public_base_url = configured.public_base_url or base_url
        base_url, note = configured.base_url, LOCALHOST_NOTE
    found = StoreAddress(base_url, repository, PROJECT, note, public_base_url)
    if found.same_store(configured):
        return StoreAddress(
            configured.base_url, repository, CONFIGURED, note, public_base_url
        )
    return found


class StoreSelection:
    """The store the app works on: the configured one or an open project's.

    It keeps one store service per store, so every caller that asks for the
    same store shares one service.
    """

    def __init__(
        self,
        configured: StoreAddress,
        service_factory: Callable[[str, str], Any] = RDFStoreService,
    ):
        self.configured = configured
        self._current = configured
        self._service_factory = service_factory
        self._services: Dict[Tuple[str, str], Any] = {}

    @property
    def current(self) -> StoreAddress:
        return self._current

    def use(self, address: StoreAddress) -> None:
        """Work on ``address`` from the next request on."""
        self._current = address

    def use_configured(self) -> None:
        """Work on the configured store from the next request on."""
        self._current = self.configured

    def service(self, address: Optional[StoreAddress] = None) -> Any:
        """The store service of ``address``, or of the current store."""
        address = address or self._current
        service = self._services.get(address.key)
        if service is None:
            service = self._service_factory(address.base_url, address.repository)
            self._services[address.key] = service
        return service


class SelectedStoreService:
    """The store service of whichever store is selected.

    Attribute look-ups go to the service of the store the current request
    works on: the store selected when the request first used one. Outside a
    request (a background job, a test) they go to the store selected at the
    time of the look-up; a job that must stay on one store takes
    ``pinned()`` once and keeps it.
    """

    def __init__(self, selection: StoreSelection):
        self._selection = selection

    @property
    def selection(self) -> StoreSelection:
        return self._selection

    def pinned(self) -> Any:
        """The concrete store service this request (or this moment) works on."""
        if has_request_context():
            address = g.get(_REQUEST_STORE)
            if address is None:
                address = self._selection.current
                setattr(g, _REQUEST_STORE, address)
            return self._selection.service(address)
        return self._selection.service()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.pinned(), name)

    def __repr__(self) -> str:
        url = without_credentials(self._selection.current.repository_url)
        return f"<SelectedStoreService on {url!r}>"


def pin_store(service: Any) -> Any:
    """The concrete store service ``service`` works on right now.

    A background job takes this once, so it keeps writing to the store it
    started on even when the project changes while it runs. Any other
    service (a test double, a plain store service) is returned as it is.
    """
    pinned = getattr(service, "pinned", None)
    return pinned() if callable(pinned) else service
