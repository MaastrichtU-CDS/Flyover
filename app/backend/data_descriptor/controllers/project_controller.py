"""
Project controller: the project the server works on.

The browser opens a project when the user picks a JSON-LD map in the
start-up pop-up, switches through the status bar, or accepts the offer an
upload in a step makes; it closes the project when the user continues
without one. Routes are the ``/api/v1/project`` surface:

- ``GET /api/v1/project``: the open project (or none) and the store in use;
- ``PUT /api/v1/project``: open a map as the project, replacing any other;
- ``DELETE /api/v1/project``: work without a project again;
- ``POST /api/v1/project/check``: what opening a map would mean, changing
  nothing, so the browser can ask the user before it switches;
- ``GET /api/v1/project/store-status``: whether the store in use answers,
  for the status bar's dot;
- ``POST /api/v1/project/fetch-map``: a map from a web address, fetched by
  the server, for the pop-up to check and open like an uploaded file.
"""

import logging

from flask import Blueprint, current_app, jsonify

from services.project_service import MapFetchError, ProjectError
from utils.mapping_request import json_object_body

logger = logging.getLogger(__name__)

project_bp = Blueprint("project", __name__)

NO_MAP = "No map was sent: the body needs a 'mapping' object."


def _context() -> dict:
    return current_app.config.get("APP_CONTEXT", {})


def _project_service():
    return _context()["project_service"]


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

    The body holds the map and what the browser knows of it:
    ``{"mapping": {...}, "fileName": "...", "sourceUrl": "...", "token": "..."}``.
    The token is the browser's name for this version of the project; it
    comes back with every answer, so the browser can tell on a later visit
    whether the server still works on the same project.
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
        )
    except ProjectError as error:
        return (
            jsonify(
                {"error": error.message, "validation_errors": error.validation_errors}
            ),
            400,
        )
    logger.info("Opened project %r", described["project"]["name"])
    return jsonify(described)


@project_bp.route("/api/v1/project", methods=["DELETE"])
def close_project():
    """Work without a project: the configured store, no project's map."""
    return jsonify(_project_service().close())


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
    """Whether the store the app works on answers, and if not, why.

    Answers ``{"reachable": bool, "reason": "...", "store": {...}}``; an
    unreachable store is an answer, not an error.
    """
    store = _project_service().describe()["store"]
    try:
        reachable, reason = _context()["rdf_store_service"].check_reachable()
    except Exception:
        logger.warning("Checking the store failed", exc_info=True)
        reachable, reason = False, "Flyover could not check the store."
    return jsonify({"reachable": reachable, "reason": reason, "store": store})


@project_bp.route("/api/v1/project/fetch-map", methods=["POST"])
def fetch_project_map():
    """Fetch the map at the posted address: ``{"url": "https://..."}``.

    Answers ``{"mapping": {...}, "fileName": "...", "sourceUrl": "..."}``.
    Nothing is opened: the browser checks and opens the map as it would an
    uploaded file. An unusable address is a 400; an address that could not
    be fetched, or did not return a map, a 502.
    """
    url = json_object_body().get("url")
    if not isinstance(url, str) or not url.strip():
        return jsonify({"error": "No address was sent: the body needs a 'url'."}), 400
    try:
        mapping, file_name = _context()["fetch_map"](url)
    except MapFetchError as error:
        return jsonify({"error": error.message}), (502 if error.upstream else 400)
    return jsonify(
        {"mapping": mapping, "fileName": file_name, "sourceUrl": url.strip()}
    )
