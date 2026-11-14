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

Every choice is for a workflow: a project for the one its map's
``targetStandard`` names, or working without one for the one the user
entered. Only the RDF workflow is built, so every choice is RDF's for now;
the server keeps the workflow with the choice, so the browser can tell
whether the choice it finds is its own workflow's.

Whenever the project changes, the server forgets what it worked out for the
previous map: the annotation status, pasted LLM answers, suggestion jobs and
the describe answers the forms posted. When the store changes as well, it
also forgets what it knew of the previous store's databases. The data in the
stores is never touched.

gunicorn replaces its worker every thousand or so requests, and a new worker
starts from the state the app had when it was built. The open project is
therefore also written to a file outside the folders Flask serves, and every
request first catches up with that file (``sync``). The file names the run
that wrote it: when Flyover itself restarts, the choice of the run before is
forgotten, so no store starts until a user chooses again (the browser still
remembers the project, and offers it first).
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
from utils.raw_file_address import (
    ZenodoRecord,
    raw_file_address,
    shown_address,
    zenodo_record,
)
from utils.rdf_store_url import without_credentials
from validation.mapping_validator import MappingValidator

logger = logging.getLogger(__name__)

STATE_FILE_VERSION = 1

# This process's run of Flyover. gunicorn (with --preload) builds the app
# before it forks its worker, so the worker it starts after every thousand or
# so requests shares the run; a restart of Flyover starts a new one.
RUN_ID = uuid.uuid4().hex

# What a user chose to work on: a project, or the workflow's standard flow
# (the standard RDF flow) without one. Until someone chooses, there is no
# route and Flyover waits.
PROJECT_ROUTE = "project"
STANDARD_ROUTE = "standard"

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

# The standard a map's data is transformed to, which names the workflow that
# uses it (the map's top-level ``targetStandard``; the map schema allows only
# these). A map without one is an RDF map, as every map was before.
TARGET_STANDARDS = ("rdf", "omop", "csv")
DEFAULT_TARGET_STANDARD = "rdf"
_STANDARD_NAMES = {
    "rdf": "RDF",
    "omop": "the OMOP common data model",
    "csv": "CSV files",
}

# The workflows, one per standard: a workflow's id is its standard's. A
# choice is for one that is built; one that names none is RDF's, as every
# choice was before.
WORKFLOW_LABELS = {"rdf": "RDF", "omop": "OMOP", "csv": "CSV"}
BUILT_WORKFLOWS = ("rdf",)
DEFAULT_WORKFLOW = DEFAULT_TARGET_STANDARD


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


def target_standard(mapping: Dict[str, Any]) -> Tuple[str, bool]:
    """The standard ``mapping`` targets, and whether it says so itself.

    A map that names none, or one Flyover does not know, counts as RDF; the
    map schema refuses an unknown one before a map gets here.
    """
    declared = mapping.get("targetStandard")
    if isinstance(declared, str) and declared in TARGET_STANDARDS:
        return declared, True
    return DEFAULT_TARGET_STANDARD, False


def chosen_workflow(value: Any) -> str:
    """The workflow a choice is for: ``value``, or RDF when it names none.

    Raises:
        ProjectError: Flyover has no such workflow, or has not built it yet.
    """
    if value is None or value == "":
        return DEFAULT_WORKFLOW
    if not isinstance(value, str) or value not in WORKFLOW_LABELS:
        raise ProjectError(f"Flyover has no workflow called {str(value)[:40]!r}.")
    if value not in BUILT_WORKFLOWS:
        raise ProjectError(
            f"The {WORKFLOW_LABELS[value]} workflow is under development, so "
            "Flyover cannot work in it yet."
        )
    return value


def project_details(mapping: Dict[str, Any]) -> Dict[str, Any]:
    """What a map says about itself: its own name and version, the schema
    (the general part every site of a project shares) it follows, and the
    standard it targets (``targetStandardDeclared``: whether it names it)."""
    schema = mapping.get("schema")
    schema = schema if isinstance(schema, dict) else {}
    standard, declared = target_standard(mapping)
    return {
        "name": _text(mapping.get("name")),
        "version": _text(mapping.get("version")),
        "description": _text(mapping.get("description")),
        "schemaId": _text(schema.get("@id")),
        "schemaName": _text(schema.get("name")),
        "schemaVersion": _text(schema.get("version")),
        "targetStandard": standard,
        "targetStandardDeclared": declared,
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

    def __init__(
        self,
        session_cache: Any,
        selection: StoreSelection,
        state_file: str,
        run_id: Optional[str] = None,
    ):
        self._session_cache = session_cache
        self._selection = selection
        self._state_file = state_file
        self._run_id = run_id or RUN_ID
        self._project: Optional[OpenProject] = None
        # The workflow's standard flow, once chosen without a project.
        self._standard = False
        # The workflow the choice is for; None while nobody has chosen.
        self._workflow: Optional[str] = None
        # (mtime, size) of the state file as this process last read or wrote
        # it; None while there is none.
        self._seen: Optional[Tuple[int, int]] = None

    @property
    def project(self) -> Optional[OpenProject]:
        return self._project

    @property
    def route(self) -> Optional[str]:
        """What a user chose to work on: ``project``, ``standard`` (the
        workflow's standard flow, without a project), or None while nobody
        has chosen. Flyover starts no store before someone chooses."""
        if self._project is not None:
            return PROJECT_ROUTE
        return STANDARD_ROUTE if self._standard else None

    def describe(self) -> Dict[str, Any]:
        """The open project (None without one), the route chosen, the
        workflow it is for and the store the app is on.

        ``configuredStore`` is the store working without a project uses, so
        the browser can say so while a project's own store is in use.
        """
        return {
            "project": self._project.describe() if self._project else None,
            "route": self.route,
            "workflow": self._workflow,
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
        workflow: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Work on ``mapping`` as the project of ``workflow`` (RDF when it
        names none) from now on.

        Opening replaces any open project, also one with the same schema (a
        site's filled-in map replacing the empty template it started from).
        Only a map that targets the workflow's standard opens (the browser
        offers to switch before it gets this far).

        Raises:
            ProjectError: the workflow is not built, or the map cannot be
                opened in it; nothing changed.
        """
        workflow = chosen_workflow(workflow)
        store, parsed = self._examine(mapping)
        standard, _ = target_standard(mapping)
        if standard != workflow:
            raise ProjectError(
                f"This JSON-LD targets {_STANDARD_NAMES[standard]} "
                f'(its targetStandard is "{standard}"), so the '
                f"{WORKFLOW_LABELS[workflow]} workflow cannot open it as a project."
            )
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
        self._standard = False
        self._workflow = workflow
        self._write(project, workflow)
        return self.describe()

    def close(self, workflow: Optional[str] = None) -> Dict[str, Any]:
        """Work without a project in ``workflow`` (RDF when it names none),
        in its standard flow: the configured store, no project's map.

        It is also how a user chooses the standard flow on entering the
        workflow.
        Without an open project there is nothing to close: the session keeps
        what the steps worked out without one (a map an upload put there).

        Raises:
            ProjectError: the workflow is not built; nothing changed.
        """
        workflow = chosen_workflow(workflow)
        if self._project is not None:
            self._apply(None)
        self._standard = True
        self._workflow = workflow
        self._write_payload(
            {
                "version": STATE_FILE_VERSION,
                "route": STANDARD_ROUTE,
                "workflow": workflow,
            }
        )
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
        project, standard, workflow = None, False, None
        if seen is not None:
            try:
                project, standard, workflow = self._read()
            except Exception:
                logger.warning(
                    "The open project in %s could not be restored; waiting "
                    "for a user to choose again",
                    self._state_file,
                    exc_info=True,
                )
        if project != self._project:
            self._apply(project)
        self._standard = standard
        self._workflow = workflow

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

    def _write(self, project: OpenProject, workflow: str) -> None:
        self._write_payload(
            {
                "version": STATE_FILE_VERSION,
                "workflow": workflow,
                "token": project.token,
                "fileName": project.file_name,
                "sourceUrl": project.source_url,
                "openedAt": project.opened_at,
                "mapping": project.mapping,
            }
        )

    def _write_payload(self, payload: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self._state_file) or ".", exist_ok=True)
        partial = f"{self._state_file}.partial"
        # The map may name its store with credentials: readable by the app only.
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump({**payload, "run": self._run_id}, handle)
        os.replace(partial, self._state_file)
        self._seen = self._signature()

    def _read(self) -> Tuple[Optional[OpenProject], bool, Optional[str]]:
        """What the file keeps: the project (None without one), whether the
        standard flow was chosen without one, and the workflow the choice is
        for (a file that names none is RDF's). (None, False, None) for a
        choice an earlier run of Flyover made, which a user has to make
        again."""
        with open(self._state_file, encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("version") != STATE_FILE_VERSION:
            raise ValueError(f"unknown state file version {payload.get('version')!r}")
        if payload.get("run") != self._run_id:
            logger.info(
                "The choice in %s is from an earlier run of Flyover", self._state_file
            )
            return None, False, None
        try:
            workflow = chosen_workflow(payload.get("workflow"))
        except ProjectError as error:
            raise ValueError(error.message) from None
        if "mapping" not in payload and payload.get("route") == STANDARD_ROUTE:
            return None, True, workflow
        store, parsed = self._examine(payload.get("mapping"))
        project = OpenProject(
            token=_token(payload.get("token")),
            mapping=payload["mapping"],
            file_name=_text(payload.get("fileName")),
            source_url=_text(payload.get("sourceUrl")),
            opened_at=_text(payload.get("openedAt")),
            store=store,
            parsed=parsed,
        )
        return project, False, workflow


# ---------------------------------------------------------------------------
# A project's map from a web address
# ---------------------------------------------------------------------------

# A map of a few hundred variables is well under a megabyte.
MAX_FETCHED_MAP_BYTES = 20 * 1024 * 1024
# Seconds to connect, and to wait between bytes once connected.
FETCH_TIMEOUT = (5, 30)
FETCH_ACCEPT = "application/ld+json, application/json;q=0.9, */*;q=0.1"
# Zenodo's API answers application/ld+json with the record's schema.org
# description, which lists no files.
ZENODO_ACCEPT = "application/json"
NOT_JSON_HINT = (
    "Give the address of the map file itself, of its page on a code hosting "
    "site such as Codeberg, GitHub or GitLab, or of its Zenodo record."
)
PRIVATE_HINT = (
    "If the repository is private, download the map from the site and open "
    "the file instead."
)
RESTRICTED_HINT = (
    "If the record's files are restricted, download the map from Zenodo and "
    "open the file instead."
)
# The file a page or a record leads to, as the messages below name it.
RAW_FILE = "the raw file behind that page"
ZENODO_FILE = "the file in that Zenodo record"
# What fetching that file met, worded about it.
FILE_FAILURES = {
    "status": "{The_file}, {address}, answered with HTTP {status}.",
    "timeout": "{The_file}, {address}, did not answer in time.",
    "unreachable": "Flyover could not reach {the_file}, {address}.",
    "too-large": "{The_file}, {address}, is larger than {limit} MB.",
    "not-json": "{The_file}, {address}, is not JSON.",
    "not-map": "{The_file}, {address}, is not a JSON-LD map.",
}
# Answers a visitor without access gets: not found, refused, or a sign-in
# page instead of the file.
NO_ACCESS_ANSWERS = (401, 403, 404)
# The files that may hold a map, by preference: JSON-LD, then plain JSON.
MAP_FILE_KINDS = ((".jsonld", "JSON-LD"), (".json", "JSON"))


class MapFetchError(Exception):
    """A map that could not be fetched, with the reason for the user."""

    def __init__(
        self,
        message: str,
        *,
        upstream: bool = True,
        reason: str = "",
        status: Optional[int] = None,
    ):
        super().__init__(message)
        self.message = message
        # False when the address itself is unusable, True when fetching failed.
        self.upstream = upstream
        # What went wrong (status, timeout, unreachable, too-large, not-json
        # or not-map), and the HTTP status for "status".
        self.reason = reason
        self.status = status


class MapFileChoice(Exception):
    """An address that leads to several maps, of which the user picks one.

    ``choices`` lists them by name, as ``{"name", "url", "size"}`` (size in
    bytes, or None); fetching a choice's url gives that map.
    """

    def __init__(self, message: str, choices: List[Dict[str, Any]]):
        super().__init__(message)
        self.message = message
        self.choices = choices


def fetch_map(url: str) -> Tuple[Dict[str, Any], str]:
    """The JSON-LD map at ``url``, and a file name to show for it.

    The server fetches the map, not the browser: a browser may only read
    another site's file when that site allows it, which institutional
    servers often do not. Only http(s) is fetched, with time and size
    limits; what comes back still has to pass the project check.

    A link to a file's page on a code hosting site (Bitbucket, Forgejo,
    Gitea, GitHub, GitLab, SourceHut; see utils.raw_file_address) is read
    through the raw file behind the page. Should that fail, the address as
    given is read too, in case it held the map after all; when it does not
    either, the error is the raw file's.

    A Zenodo record's page or DOI is looked up in Zenodo's API, and the
    record's JSON-LD file read (plain JSON when it has none). A record that
    holds several asks which one (MapFileChoice).

    Raises:
        MapFetchError: the address is unusable, could not be fetched, or
            did not return a JSON object.
        MapFileChoice: the address names a Zenodo record with several maps.
    """
    url = (url or "").strip()
    parts = urlsplit(url)
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        raise MapFetchError(
            "Give a web address that starts with http:// or https://.",
            upstream=False,
        )
    record = zenodo_record(url)
    if record is not None:
        return _map_from_zenodo(record)
    file_name = os.path.basename(unquote(parts.path)) or "map.jsonld"
    raw = raw_file_address(url)
    if raw is None:
        return _fetch_json(url), file_name
    try:
        return _fetch_json(raw), file_name
    except MapFetchError as error:
        raw_error = error
    # A host that did not answer for the raw file will not for the page.
    if raw_error.reason not in ("timeout", "unreachable"):
        try:
            return _fetch_json(url), file_name
        except MapFetchError:
            pass
    raise _about_the_file(raw_error, raw, RAW_FILE, PRIVATE_HINT)


def _map_from_zenodo(record: ZenodoRecord) -> Tuple[Dict[str, Any], str]:
    """The map in a Zenodo record, and its file name."""
    files, shared = _zenodo_files(record)
    for suffix, kind in MAP_FILE_KINDS:
        maps = [f for f in files if f["name"].lower().endswith(suffix)]
        if maps:
            break
    else:
        if not shared:
            raise MapFetchError(
                f"Zenodo record {record.record_id} does not share its files "
                "openly: download the map from Zenodo and open the file instead."
            )
        raise MapFetchError(f"Zenodo record {record.record_id} holds no JSON-LD file.")
    if len(maps) > 1:
        raise MapFileChoice(
            f"This Zenodo record holds {len(maps)} {kind} files.",
            sorted(maps, key=lambda f: f["name"].lower()),
        )
    [only] = maps
    try:
        mapping = _fetch_json(only["url"])
    except MapFetchError as error:
        raise _about_the_file(
            error, only["url"], ZENODO_FILE, RESTRICTED_HINT
        ) from None
    return mapping, os.path.basename(only["name"])


def _zenodo_files(record: ZenodoRecord) -> Tuple[List[Dict[str, Any]], bool]:
    """The files of a Zenodo record, as ``{"name", "url", "size"}``, and
    whether the record shares its files openly.

    A concept record (every version) answers as its latest version, whose
    files these are. Zenodo's own form lists the files; InvenioRDM's, which
    Zenodo runs on, keeps them under ``entries``.
    """
    try:
        body = _fetch_json(record.api_address, accept=ZENODO_ACCEPT)
    except MapFetchError as error:
        raise _about_zenodo(error, record) from None
    version = str(body.get("id") or "")
    if not version.isdigit():
        version = record.record_id
    listed = body.get("files")
    if isinstance(listed, dict):
        listed = list((listed.get("entries") or {}).values())
    files = []
    for entry in listed if isinstance(listed, list) else []:
        name = entry.get("key") if isinstance(entry, dict) else None
        if not isinstance(name, str) or not name:
            continue
        size = entry.get("size")
        files.append(
            {
                "name": name,
                "url": record.file_address(version, name),
                "size": size if isinstance(size, int) else None,
            }
        )
    metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
    access = body.get("access") if isinstance(body.get("access"), dict) else {}
    shared = (
        metadata.get("access_right", "open") == "open"
        and access.get("files", "public") == "public"
    )
    return files, shared


def _about_zenodo(error: MapFetchError, record: ZenodoRecord) -> MapFetchError:
    """``error``, met asking Zenodo's API for ``record``, worded about it."""
    number = record.record_id
    if error.status == 404:
        message = f"Zenodo has no record {number}."
    elif error.status == 410:
        message = f"Zenodo record {number} has been removed."
    elif error.reason == "status":
        message = f"Zenodo answered with HTTP {error.status} for record {number}."
    elif error.reason == "timeout":
        message = "Zenodo did not answer in time."
    elif error.reason == "unreachable":
        message = "Flyover could not reach Zenodo."
    else:
        message = f"Zenodo's answer for record {number} could not be read."
    return MapFetchError(message, reason=error.reason, status=error.status)


def _fetch_json(url: str, accept: str = FETCH_ACCEPT) -> Dict[str, Any]:
    """The JSON object at ``url``.

    Raises:
        MapFetchError: with the reason, worded about "that address".
    """
    try:
        with requests.get(
            url, timeout=FETCH_TIMEOUT, stream=True, headers={"Accept": accept}
        ) as response:
            if response.status_code != 200:
                raise MapFetchError(
                    f"The address answered with HTTP {response.status_code}.",
                    reason="status",
                    status=response.status_code,
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
        raise MapFetchError(
            "The address did not answer in time.", reason="timeout"
        ) from None
    except requests.RequestException as error:
        # The error repeats the URL, which may carry credentials.
        logger.info("Fetching a map failed (%s)", type(error).__name__)
        raise MapFetchError(
            "Flyover could not reach that address.", reason="unreachable"
        ) from None
    try:
        mapping = json.loads(bytes(received).decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError):
        raise MapFetchError(
            f"What that address returned is not JSON. {NOT_JSON_HINT}",
            reason="not-json",
        ) from None
    if not isinstance(mapping, dict):
        raise MapFetchError(
            "What that address returned is not a JSON-LD map.", reason="not-map"
        )
    return mapping


def _about_the_file(
    error: MapFetchError, address: str, the_file: str, hint: str
) -> MapFetchError:
    """``error``, met fetching the file at ``address`` that a page or record
    led to, worded about that file; ``hint`` follows when access was
    refused."""
    message = FILE_FAILURES[error.reason].format(
        The_file=the_file[0].upper() + the_file[1:],
        the_file=the_file,
        address=shown_address(address),
        status=error.status,
        limit=MAX_FETCHED_MAP_BYTES // (1024 * 1024),
    )
    if error.status in NO_ACCESS_ANSWERS or error.reason == "not-json":
        message = f"{message} {hint}"
    return MapFetchError(message, reason=error.reason, status=error.status)


def _too_large() -> MapFetchError:
    limit = MAX_FETCHED_MAP_BYTES // (1024 * 1024)
    return MapFetchError(
        f"The file at that address is larger than {limit} MB.", reason="too-large"
    )
