"""
Project controller: the project the server works on.

The browser opens a project when the user picks a JSON-LD map in the
pop-up that asks for it on entering a workflow, switches through the
status bar, or accepts the offer an upload in a step makes; it closes the
project when the user continues without one. Each choice names the workflow
it is for (only RDF is built). Routes are the ``/api/v1/project`` surface:

- ``GET /api/v1/project``: the open project (or none), the route chosen (a
  project, the workflow's standard flow, or none yet), the workflow it is
  for, the store in use and the configured store (the one working without a
  project uses);
- ``PUT /api/v1/project``: open a map as the project of a workflow,
  replacing any other;
- ``DELETE /api/v1/project``: work without a project, in a workflow's
  standard flow (also how a user chooses it on entering the workflow);
- ``POST /api/v1/project/check``: what opening a map would mean, changing
  nothing, so the browser can ask the user before it switches;
- ``GET /api/v1/project/store-status``: where the store in use stands
  (waiting for a choice, starting, stopped, reachable or not), for the
  status bar;
- ``POST /api/v1/project/store-start``: start the store in use at a user's
  request (the status bar's Start, for a store someone stopped);
- ``POST /api/v1/project/store-stop``: stop the store in use at a user's
  request, as they leave the workflow;
- ``POST /api/v1/project/fetch-map``: a map from a web address, fetched by
  the server, for the pop-up to check and open like an uploaded file; or,
  for a Zenodo record holding several, the maps to choose from.
"""

import logging

from flask import Blueprint, current_app, jsonify, request

from services.project_service import MapFetchError, MapFileChoice, ProjectError
from services.store_launcher import LauncherError
from utils.mapping_request import json_object_body

logger = logging.getLogger(__name__)

project_bp = Blueprint("project", __name__)

NO_MAP = "No map was sent: the body needs a 'mapping' object."
NOT_OUR_STORE = "Flyover does not run this store, so it leaves it as it is."


def _context() -> dict:
    return current_app.config.get("APP_CONTEXT", {})


def _project_service():
    return _context()["project_service"]


def _start_the_chosen_store() -> None:
    """A user chose what to work on: start its store if Flyover may."""
    _context()["store_lifecycle"].ensure_started(_context()["store_selection"].current)


def _mapping_from_body():
    body = json_object_body()
    mapping = body.get("mapping")
    return body, mapping if isinstance(mapping, dict) else None


@project_bp.route("/api/v1/project", methods=["GET"])
def get_project():
    """The open project, or none, and the store the app works on."""
    return jsonify(_project_service().describe())


@project_bp.route("/api/v1/project", methods=["PUT"])
def open_project():
    """Open the posted map as the project.

    The body holds the map, what the browser knows of it and the workflow
    it opens in (RDF when it names none):
    ``{"mapping": {...}, "fileName": "...", "sourceUrl": "...", "token": "...",
    "workflow": "rdf"}``. The token is the browser's name for this version of
    the project; it comes back with every answer, so the browser can tell on
    a later visit whether the server still works on the same project.
    """
    body, mapping = _mapping_from_body()
    if mapping is None:
        return jsonify({"error": NO_MAP}), 400
    try:
        described = _project_service().open(
            mapping,
            file_name=body.get("fileName"),
            source_url=body.get("sourceUrl"),
            token=body.get("token"),
            workflow=body.get("workflow"),
        )
    except ProjectError as error:
        return (
            jsonify(
                {"error": error.message, "validation_errors": error.validation_errors}
            ),
            400,
        )
    logger.info("Opened project %r", described["project"]["name"])
    _start_the_chosen_store()
    return jsonify(described)


@project_bp.route("/api/v1/project", methods=["DELETE"])
def close_project():
    """Work without a project, in the standard flow of the workflow the
    query names (``?workflow=rdf``; RDF when it names none): the configured
    store, no project's map. Starts that store if Flyover may."""
    try:
        described = _project_service().close(workflow=request.args.get("workflow"))
    except ProjectError as error:
        return jsonify({"error": error.message}), 400
    _start_the_chosen_store()
    return jsonify(described)


@project_bp.route("/api/v1/project/check", methods=["POST"])
def check_project_map():
    """Whether the posted map can be opened as a project, and what as.

    Answers ``{"valid": true, "project": {...}, "store": {...}}``, or
    ``{"valid": false, "error": "...", "validation_errors": [...]}`` for a
    map that cannot be opened. Nothing changes on the server.
    """
    body, mapping = _mapping_from_body()
    if mapping is None:
        return jsonify({"error": NO_MAP}), 400
    return jsonify(_project_service().check(mapping, file_name=body.get("fileName")))


@project_bp.route("/api/v1/project/store-status", methods=["GET"])
def store_status():
    """Where the store the app works on stands, and if it does not answer, why.

    Answers ``{"state": "...", "reachable": bool, "reason": "...",
    "managed": bool, "store": {...}}``, where ``managed`` says whether
    Flyover may start and stop the store. The state is ``waiting`` (nobody
    has chosen a project or a workflow's standard flow yet, so no store was
    started), ``starting``, ``stopped`` (Flyover's to start, but someone
    stopped it), ``reachable`` or ``unreachable`` (see
    services/store_lifecycle.py; a later ``none`` will mean the work uses no
    database). An unreachable store is an answer, not an error. A chosen
    store whose container never ran is started.
    """
    return _store_status_answer()


@project_bp.route("/api/v1/project/store-start", methods=["POST"])
def start_store():
    """Start the store the app works on, at a user's request.

    For the status bar's Start: a store someone stopped stays stopped until
    a user asks. Only a store Flyover may start is started (the local one,
    where the stack runs a launcher). Answers as the store status does.
    Before anyone has chosen what to work on, there is nothing to start.
    """
    if _project_service().route is None:
        return (
            jsonify({"error": "Choose a project, or continue without one, first."}),
            409,
        )
    _start_the_chosen_store()
    return _store_status_answer()


@project_bp.route("/api/v1/project/store-stop", methods=["POST"])
def stop_store():
    """Stop the store the app works on, at a user's request.

    For leaving the workflow, when the user chooses to stop the store rather
    than keep it running; Flyover never stops one of its own accord. Only a
    store Flyover runs (the local one, where the stack runs a launcher) can
    be stopped: another is a 409, and a launcher that cannot stop it a 502,
    with the reason. Answers as the store status does.
    """
    lifecycle = _context()["store_lifecycle"]
    store = _context()["store_selection"].current
    if not lifecycle.manages(store):
        return jsonify({"error": NOT_OUR_STORE}), 409
    try:
        lifecycle.stop(store)
    except LauncherError as error:
        logger.warning("Flyover could not stop the store: %s", error.message)
        return jsonify({"error": error.message}), 502
    return _store_status_answer()


def _store_status_answer():
    service = _project_service()
    store = service.describe()["store"]

    def check():
        try:
            return _context()["rdf_store_service"].check_reachable()
        except Exception:
            logger.warning("Checking the store failed", exc_info=True)
            return False, "Flyover could not check the store."

    lifecycle = _context()["store_lifecycle"]
    current = _context()["store_selection"].current
    state, reason = lifecycle.status(current, service.route is not None, check)
    return jsonify(
        {
            "state": state,
            "reachable": state == "reachable",
            "reason": reason,
            "managed": bool(lifecycle.manages(current)),
            "store": store,
        }
    )


@project_bp.route("/api/v1/project/fetch-map", methods=["POST"])
def fetch_project_map():
    """Fetch the map at the posted address: ``{"url": "https://..."}``.

    Answers ``{"mapping": {...}, "fileName": "...", "sourceUrl": "..."}``.
    Nothing is opened: the browser checks and opens the map as it would an
    uploaded file. An address that leads to several maps (a Zenodo record)
    answers ``{"message": "...", "choices": [{"name", "url", "size"}, ...],
    "sourceUrl": "..."}`` instead: the browser asks which one and fetches its
    url. An unusable address is a 400; an address that could not be fetched,
    or did not return a map, a 502.
    """
    url = json_object_body().get("url")
    if not isinstance(url, str) or not url.strip():
        return jsonify({"error": "No address was sent: the body needs a 'url'."}), 400
    try:
        mapping, file_name = _context()["fetch_map"](url)
    except MapFileChoice as choice:
        return jsonify(
            {
                "message": choice.message,
                "choices": choice.choices,
                "sourceUrl": url.strip(),
            }
        )
    except MapFetchError as error:
        return jsonify({"error": error.message}), (502 if error.upstream else 400)
    return jsonify(
        {"mapping": mapping, "fileName": file_name, "sourceUrl": url.strip()}
    )
