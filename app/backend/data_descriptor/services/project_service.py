"""
The open project, as the server keeps it.

A project is a JSON-LD semantic map the user opened on purpose: from the
pop-up Flyover shows when it opens, from the status bar's "Switch project",
or by accepting the offer an upload in a step makes. The browser keeps the
project's working copy; the server keeps what it needs to work on it:

- the map, as the session's map (``jsonld_mapping``), so the steps that read
  the session's map use the project's;
- the RDF store the map's top-level ``endpoint`` names (the configured store
  when it names none), so every step works on the project's data.

Whenever the project changes, the server forgets what it worked out for the
previous map: the annotation status, pasted LLM answers, suggestion jobs and
the describe answers the forms posted. When the store changes as well, it
also forgets what it knew of the previous store's databases. The data in the
stores is never touched.

gunicorn replaces its worker every thousand or so requests, and a new worker
starts from the state the app had when it was built. The open project is
therefore also written to a file outside the folders Flask serves, and every
request first catches up with that file (``sync``).
"""

import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlsplit

import requests

from loaders import JSONLDMapping
from services.store_selection import StoreAddress, StoreSelection, store_for_endpoint
from utils.rdf_store_url import without_credentials
from validation.mapping_validator import MappingValidator

logger = logging.getLogger(__name__)

STATE_FILE_VERSION = 1

# Session state worked out against one map. It goes when the map changes.
MAP_TIED_STATE = (
    "global_semantic_map",
    "annotation_status",
    "annotation_json_path",
    "descriptive_info",
    "DescriptiveInfoDetails",
    "StatusToDisplay",
    "suggestion_jobs",
    "suggestion_ingested",
)

# What a browser may name a project by (the token it compares on its next
# visit); anything else is replaced by a token of the server's own.
_TOKEN = re.compile(r"^[A-Za-z0-9_-]{1,100}$")
_TEXT_LIMIT = 500

ENDPOINT_SUGGESTION = (
    "Write it as http(s)://<host>/repositories/<name>, or leave it empty to "
    "use Flyover's configured store."
)


class ProjectError(Exception):
    """A map that cannot be opened as a project, with the reasons for the UI."""

    def __init__(self, message: str, validation_errors: Optional[List[dict]] = None):
        super().__init__(message)
        self.message = message
        self.validation_errors = validation_errors or []


def _text(value: Any) -> str:
    return value.strip()[:_TEXT_LIMIT] if isinstance(value, str) else ""


def _token(value: Any) -> str:
    """The browser's token for the project, or a new one when it sent none."""
    if isinstance(value, str) and _TOKEN.match(value):
        return value
    return uuid.uuid4().hex


def project_details(mapping: Dict[str, Any]) -> Dict[str, str]:
    """What a map says about itself: its own name and version, and the
    schema (the general part every site of a project shares) it follows."""
    schema = mapping.get("schema")
    schema = schema if isinstance(schema, dict) else {}
    return {
        "name": _text(mapping.get("name")),
        "version": _text(mapping.get("version")),
        "description": _text(mapping.get("description")),
        "schemaId": _text(schema.get("@id")),
        "schemaName": _text(schema.get("name")),
        "schemaVersion": _text(schema.get("version")),
    }


@dataclass(frozen=True)
class OpenProject:
    """The project the server works on."""

    token: str
    mapping: Dict[str, Any] = field(repr=False)
    file_name: str
    source_url: str
    opened_at: str
    store: StoreAddress
    parsed: Any = field(default=None, compare=False, repr=False)

    def describe(self) -> Dict[str, Any]:
        return {
            "token": self.token,
            **project_details(self.mapping),
            "fileName": self.file_name,
            "sourceUrl": self.source_url,
            "openedAt": self.opened_at,
        }


class ProjectService:
    """Opens, replaces and closes the project the server works on."""

    def __init__(self, session_cache: Any, selection: StoreSelection, state_file: str):
        self._session_cache = session_cache
        self._selection = selection
        self._state_file = state_file
        self._project: Optional[OpenProject] = None
        # (mtime, size) of the state file as this process last read or wrote
        # it; None while there is none.
        self._seen: Optional[Tuple[int, int]] = None

    @property
    def project(self) -> Optional[OpenProject]:
        return self._project

    def describe(self) -> Dict[str, Any]:
        """The open project (None without one) and the store the app is on.

        ``configuredStore`` is the store working without a project uses, so
        the browser can say so while a project's own store is in use.
        """
        return {
            "project": self._project.describe() if self._project else None,
            "store": self._selection.current.describe(),
            "configuredStore": self._selection.configured.describe(),
        }

    def check(self, mapping: Any, file_name: str = "") -> Dict[str, Any]:
        """What opening ``mapping`` as a project would mean, changing nothing.

        The browser asks before it offers to switch: whether the map can be
        opened, which project it belongs to and which store it would use.
        """
        try:
            store, _ = self._examine(mapping)
        except ProjectError as error:
            return {
                "valid": False,
                "error": error.message,
                "validation_errors": error.validation_errors,
            }
        return {
            "valid": True,
            "project": {**project_details(mapping), "fileName": _text(file_name)},
            "store": store.describe(),
        }

    def open(
        self,
        mapping: Any,
        file_name: str = "",
        source_url: str = "",
        token: str = "",
    ) -> Dict[str, Any]:
        """Work on ``mapping`` as the project from now on.

        Opening replaces any open project, also one with the same schema (a
        site's filled-in map replacing the empty template it started from).

        Raises:
            ProjectError: the map cannot be opened; nothing changed.
        """
        store, parsed = self._examine(mapping)
        project = OpenProject(
            token=_token(token),
            mapping=mapping,
            file_name=_text(file_name),
            source_url=_text(source_url),
            opened_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            store=store,
            parsed=parsed,
        )
        self._apply(project)
        self._write(project)
        return self.describe()

    def close(self) -> Dict[str, Any]:
        """Work without a project: the configured store, no project's map.

        Without an open project there is nothing to close: the session keeps
        what the steps worked out without one (a map an upload put there).
        """
        if self._project is not None:
            self._apply(None)
        self._remove_file()
        return self.describe()

    def sync(self) -> None:
        """Catch up with the state file when another process changed it.

        A worker that gunicorn started after the project changed begins
        from the state the app had when it was built; this brings it to the
        project the previous worker wrote. It costs one ``stat`` when
        nothing changed.
        """
        seen = self._signature()
        if seen == self._seen:
            return
        self._seen = seen
        project = None
        if seen is not None:
            try:
                project = self._read()
            except Exception:
                logger.warning(
                    "The open project in %s could not be restored; working "
                    "without a project",
                    self._state_file,
                    exc_info=True,
                )
        if project != self._project:
            self._apply(project)

    # ------------------------------------------------------------------

    def _examine(self, mapping: Any) -> Tuple[StoreAddress, Any]:
        """The store ``mapping`` names and the map parsed, or ProjectError."""
        if not isinstance(mapping, dict):
            raise ProjectError("A project's map must be a JSON object.")
        validator = MappingValidator()
        result = validator.validate(mapping)
        if not result.is_valid:
            raise ProjectError(
                "The map is not a valid Flyover semantic map.",
                validator.format_errors_for_ui(result),
            )
        try:
            parsed = JSONLDMapping.from_dict(mapping)
        except Exception as error:
            raise ProjectError(f"The map could not be read: {error}") from None
        endpoint = mapping.get("endpoint")
        try:
            store = store_for_endpoint(endpoint, self._selection.configured)
        except ValueError as reason:
            shown = without_credentials(str(endpoint))
            raise ProjectError(
                "Flyover cannot use the store the map's endpoint names.",
                [
                    {
                        "path": "endpoint",
                        "severity": "error",
                        "message": f"Flyover cannot use the endpoint {shown}: {reason}.",
                        "suggestion": ENDPOINT_SUGGESTION,
                        "value": shown,
                    }
                ],
            ) from None
        return store, parsed

    def _apply(self, project: Optional[OpenProject]) -> None:
        """Make ``project`` (None: no project) the one the server works on."""
        cache = self._session_cache
        store = project.store if project else self._selection.configured
        for name in MAP_TIED_STATE:
            setattr(cache, name, None)
        if not store.same_store(self._selection.current):
            cache.databases = None
            cache.existing_graph = False
        cache.jsonld_mapping = project.parsed if project else None
        cache.repo = store.repository
        self._selection.use(store)
        self._project = project

    # -- the state file ---------------------------------------------------

    def _signature(self) -> Optional[Tuple[int, int]]:
        try:
            status = os.stat(self._state_file)
        except FileNotFoundError:
            return None
        return status.st_mtime_ns, status.st_size

    def _write(self, project: OpenProject) -> None:
        payload = {
            "version": STATE_FILE_VERSION,
            "token": project.token,
            "fileName": project.file_name,
            "sourceUrl": project.source_url,
            "openedAt": project.opened_at,
            "mapping": project.mapping,
        }
        os.makedirs(os.path.dirname(self._state_file) or ".", exist_ok=True)
        partial = f"{self._state_file}.partial"
        # The map may name its store with credentials: readable by the app only.
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(partial, self._state_file)
        self._seen = self._signature()

    def _read(self) -> OpenProject:
        with open(self._state_file, encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("version") != STATE_FILE_VERSION:
            raise ValueError(f"unknown state file version {payload.get('version')!r}")
        store, parsed = self._examine(payload.get("mapping"))
        return OpenProject(
            token=_token(payload.get("token")),
            mapping=payload["mapping"],
            file_name=_text(payload.get("fileName")),
            source_url=_text(payload.get("sourceUrl")),
            opened_at=_text(payload.get("openedAt")),
            store=store,
            parsed=parsed,
        )

    def _remove_file(self) -> None:
        try:
            os.remove(self._state_file)
        except FileNotFoundError:
            pass
        self._seen = None


# ---------------------------------------------------------------------------
# A project's map from a web address
# ---------------------------------------------------------------------------

# A map of a few hundred variables is well under a megabyte.
MAX_FETCHED_MAP_BYTES = 20 * 1024 * 1024
# Seconds to connect, and to wait between bytes once connected.
FETCH_TIMEOUT = (5, 30)
FETCH_ACCEPT = "application/ld+json, application/json;q=0.9, */*;q=0.1"
NOT_JSON_HINT = (
    "If the file is on GitHub or a similar site, use the address of its " "raw version."
)


class MapFetchError(Exception):
    """A map that could not be fetched, with the reason for the user."""

    def __init__(self, message: str, *, upstream: bool = True):
        super().__init__(message)
        self.message = message
        # False when the address itself is unusable, True when fetching failed.
        self.upstream = upstream


def fetch_map(url: str) -> Tuple[Dict[str, Any], str]:
    """The JSON-LD map at ``url``, and a file name to show for it.

    The server fetches the map, not the browser: a browser may only read
    another site's file when that site allows it, which institutional
    servers often do not. Only http(s) is fetched, with time and size
    limits; what comes back still has to pass the project check.

    Raises:
        MapFetchError: the address is unusable, could not be fetched, or
            did not return a JSON object.
    """
    url = (url or "").strip()
    parts = urlsplit(url)
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        raise MapFetchError(
            "Give a web address that starts with http:// or https://.",
            upstream=False,
        )
    try:
        with requests.get(
            url, timeout=FETCH_TIMEOUT, stream=True, headers={"Accept": FETCH_ACCEPT}
        ) as response:
            if response.status_code != 200:
                raise MapFetchError(
                    f"The address answered with HTTP {response.status_code}."
                )
            declared = response.headers.get("Content-Length", "")
            if declared.isdigit() and int(declared) > MAX_FETCHED_MAP_BYTES:
                raise _too_large()
            received = bytearray()
            for chunk in response.iter_content(64 * 1024):
                received.extend(chunk)
                if len(received) > MAX_FETCHED_MAP_BYTES:
                    raise _too_large()
    except requests.Timeout:
        raise MapFetchError("The address did not answer in time.") from None
    except requests.RequestException as error:
        # The error repeats the URL, which may carry credentials.
        logger.info("Fetching a map failed (%s)", type(error).__name__)
        raise MapFetchError("Flyover could not reach that address.") from None
    try:
        mapping = json.loads(bytes(received).decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError):
        raise MapFetchError(
            f"What that address returned is not JSON. {NOT_JSON_HINT}"
        ) from None
    if not isinstance(mapping, dict):
        raise MapFetchError("What that address returned is not a JSON-LD map.")
    file_name = os.path.basename(unquote(parts.path)) or "map.jsonld"
    return mapping, file_name


def _too_large() -> MapFetchError:
    limit = MAX_FETCHED_MAP_BYTES // (1024 * 1024)
    return MapFetchError(f"The file at that address is larger than {limit} MB.")
