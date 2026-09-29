"""
Suggestions controller for mapping suggestion endpoints.

Serves the polling API the describe pages use to start suggestion jobs,
fetch arriving suggestions, and reprioritise the queue. Routes are the
``/api/v1/suggestions/*`` surface. The ``/ingest`` and ``/prompt`` routes
belong to issues 2/3 and are deliberately absent here;
``SuggestionService.ingest()`` is reserved with the plan's signature but
raises ``NotImplementedError`` until those issues land.

Adapted from the LLM branch's ``llm_controller.py`` with provider-specific
status replaced by the tier-aware ``/status`` shape.
"""

import logging

from flask import Blueprint, jsonify, request

from utils.mapping_request import parse_and_validate_mapping

logger = logging.getLogger(__name__)

suggestions_bp = Blueprint("suggestions", __name__)

_VALID_PHASES = ("variables", "values")


def get_app_context() -> dict:
    """Get application context (session_cache, suggestion_service, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


def _maybe_adopt_mapping(session_cache, mapping) -> None:
    """Adopt a body mapping into the session — only when the session has none.

    Never overwrites an existing session mapping: the browser may hold a
    stale or partial copy, and the session's mapping is the one the rest
    of the app (annotation, export) reads.
    """
    if mapping is None:
        return
    if getattr(session_cache, "jsonld_mapping", None) is not None:
        return
    session_cache.jsonld_mapping = mapping


@suggestions_bp.route("/api/v1/suggestions/status", methods=["GET"])
def suggestions_status():
    """Report active/inactive tiers, compute mode, and rules version."""
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    if service is None or not service.config.enabled:
        return jsonify(
            {
                "enabled": False,
                "compute": service.config.compute if service else "host",
                "tiers": {
                    1: {
                        "state": "inactive",
                        "reason": "disabled by FLYOVER_SUGGESTION_TIERS",
                    },
                    2: {
                        "state": "inactive",
                        "reason": "disabled by FLYOVER_SUGGESTION_TIERS",
                    },
                    3: {
                        "state": "inactive",
                        "reason": "disabled by FLYOVER_SUGGESTION_TIERS",
                    },
                },
                "threshold": service.config.threshold if service else 0.8,
            }
        )
    return jsonify(
        {
            "enabled": True,
            **service.status(),
        }
    )


@suggestions_bp.route("/api/v1/suggestions/<phase>/start", methods=["POST"])
def start_suggestions(phase: str):
    """Start (idempotently) the suggestion job for one phase."""
    if phase not in _VALID_PHASES:
        return jsonify({"error": f"unknown phase '{phase}'"}), 400
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    if service is None:
        return jsonify({"status": "disabled"}), 200

    body = request.get_json(silent=True) or {}

    # The describe pages work on the semantic map in the browser's
    # IndexedDB: the describe-landing upload never reaches the session, so
    # the session may hold an older map (or one another browser sent).
    # Both phases therefore run on the body mapping when one is sent, for
    # this job only (the service keeps it job-local); the fingerprint
    # covers the mapping, so a different map starts a fresh job. A request
    # body never overwrites the session's own mapping: the variables phase
    # only adopts it when the session has none (e.g. after a container
    # restart), after MappingValidator passes.
    mapping = parse_and_validate_mapping(body.get("mapping"))
    if phase == "variables":
        _maybe_adopt_mapping(session_cache, mapping)

    result = service.start(
        phase,
        session_cache,
        rdf_store_service,
        force=bool(body.get("force")),
        mapping=mapping,
    )
    return jsonify(result)


@suggestions_bp.route("/api/v1/suggestions/<phase>", methods=["GET"])
def get_suggestions(phase: str):
    """Return the current snapshot of the suggestion job for one phase."""
    if phase not in _VALID_PHASES:
        return jsonify({"error": f"unknown phase '{phase}'"}), 400
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    session_cache = ctx.get("session_cache")
    if service is None:
        return jsonify(
            {
                "enabled": False,
                "status": "idle",
                "progress": {"done": 0, "total": 0},
                "error": None,
                "records": {},
            }
        )
    state = service.get_state(session_cache, phase)
    state["enabled"] = service.config.enabled
    return jsonify(state)


@suggestions_bp.route("/api/v1/suggestions/<phase>/priority", methods=["POST"])
def prioritise_suggestions(phase: str):
    """Bump the visible items' priority. Tier 1 is synchronous (no-op)."""
    if phase not in _VALID_PHASES:
        return jsonify({"error": f"unknown phase '{phase}'"}), 400
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    session_cache = ctx.get("session_cache")
    if service is None:
        return jsonify({"status": "no_job"})

    body = request.get_json(silent=True) or {}
    items = body.get("items") or body.get("columns") or []
    if not items:
        return jsonify({"error": "items are required"}), 400
    result = service.bump_priority(session_cache, phase, items)
    return jsonify(result)
