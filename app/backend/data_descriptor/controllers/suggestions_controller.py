"""
Suggestions controller for mapping suggestion endpoints.

Serves the polling API the describe pages use to start suggestion jobs,
fetch arriving suggestions, and reprioritise the queue. Routes are the
``/api/v1/suggestions/*`` surface; the ``/ingest`` and ``/prompt`` routes
are reserved for issues 2/3 and added later, but the blueprint and
``SuggestionService.ingest()`` signature are reserved now.

Adapted from the LLM branch's ``llm_controller.py`` with provider-specific
status replaced by the tier-aware ``/status`` shape.
"""

import logging

from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

suggestions_bp = Blueprint("suggestions", __name__)

_VALID_PHASES = ("variables", "values")


def get_app_context() -> dict:
    """Get application context (session_cache, suggestion_service, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@suggestions_bp.route("/api/v1/suggestions/status", methods=["GET"])
def suggestions_status():
    """Report active/inactive tiers, compute mode, and rules version."""
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    if service is None or not service.config.enabled:
        return jsonify({
            "enabled": False,
            "compute": service.config.compute if service else "host",
            "tiers": {
                1: {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"},
                2: {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"},
                3: {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"},
            },
            "threshold": service.config.threshold if service else 0.8,
        })
    return jsonify({
        "enabled": True,
        **service.status(),
    })


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

    # The mapping may only survive in the browser's IndexedDB (e.g. after a
    # container restart) and the values phase needs the UPDATED mapping
    # (reflecting the user's variable selections from the describe-variables
    # page) to know which variable each column maps to. Always accept the
    # mapping from the body when provided — the service's fingerprint check
    # prevents redundant reruns.
    mapping_data = body.get("mapping")
    if mapping_data:
        from loaders import JSONLDMapping
        try:
            session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_data)
        except Exception:
            logger.warning("Failed to parse mapping from request body", exc_info=True)

    result = service.start(
        phase,
        session_cache,
        rdf_store_service,
        force=bool(body.get("force")),
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
        return jsonify({
            "enabled": False,
            "status": "idle",
            "progress": {"done": 0, "total": 0},
            "error": None,
            "records": {},
        })
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
