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
  nothing, so the browser can ask the user before it switches.
"""

import logging

from flask import Blueprint, current_app, jsonify

from services.project_service import ProjectError
from utils.mapping_request import json_object_body

logger = logging.getLogger(__name__)

project_bp = Blueprint("project", __name__)

NO_MAP = "No map was sent: the body needs a 'mapping' object."


def _project_service():
    return current_app.config.get("APP_CONTEXT", {})["project_service"]


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
