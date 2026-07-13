"""
LLM controller for mapping suggestion endpoints.

Serves the polling API the describe pages use to start suggestion jobs,
fetch progressively arriving suggestions, and reprioritise the queue
toward what the user is currently looking at.
"""

import logging

from flask import Blueprint, jsonify, request

from loaders import JSONLDMapping
from validation import MappingValidator

logger = logging.getLogger(__name__)

llm_bp = Blueprint("llm", __name__)


def get_app_context() -> dict:
    """Get application context (session_cache, llm_service, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


def _phase_state(phase: str):
    """Build the polling response for one suggestion phase."""
    ctx = get_app_context()
    llm_service = ctx.get("llm_service")
    session_cache = ctx.get("session_cache")

    state = llm_service.get_state(session_cache, phase)
    state["enabled"] = llm_service.config.enabled
    return jsonify(state)


def _maybe_adopt_mapping(session_cache, mapping_data: dict | None) -> tuple[bool, dict]:
    """Adopt a client-supplied JSON-LD mapping when the backend has none.

    Covers the case where the mapping only exists in the browser's
    IndexedDB (e.g. after a backend restart).

    Returns:
        (ok, error_response) — error_response is meaningful when not ok.
    """
    if session_cache.jsonld_mapping is not None or not mapping_data:
        return True, {}

    validator = MappingValidator()
    result = validator.validate(mapping_data)
    if not result.is_valid:
        return False, {
            "error": "Semantic map validation failed",
            "validation_errors": validator.format_errors_for_ui(result),
        }

    session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_data)
    return True, {}


@llm_bp.route("/api/v1/llm/status", methods=["GET"])
def llm_status():
    """Report the configured provider and whether its backend is reachable."""
    ctx = get_app_context()
    llm_service = ctx.get("llm_service")

    config = llm_service.config
    if not config.enabled:
        return jsonify(
            {
                "enabled": False,
                "provider": None,
                "model": None,
                "remote": False,
                "backend": None,
            }
        )

    reachable = llm_service.client.is_reachable()
    return jsonify(
        {
            "enabled": True,
            "provider": config.provider,
            "model": config.model,
            "remote": config.remote,
            "backend": "ready" if reachable else "unreachable",
        }
    )


@llm_bp.route("/api/v1/llm/suggestions/variables/start", methods=["POST"])
def start_variable_suggestions():
    """Start (idempotently) the column-to-variable suggestion job."""
    ctx = get_app_context()
    llm_service = ctx.get("llm_service")
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    body = request.get_json(silent=True) or {}
    ok, error = _maybe_adopt_mapping(session_cache, body.get("mapping"))
    if not ok:
        return jsonify(error), 400

    result = llm_service.start_variable_job(
        session_cache, rdf_store_service, force=bool(body.get("force"))
    )
    return jsonify(result)


@llm_bp.route("/api/v1/llm/suggestions/variables", methods=["GET"])
def get_variable_suggestions():
    """Return the current snapshot of the variables suggestion job."""
    return _phase_state("variables")


@llm_bp.route("/api/v1/llm/suggestions/variables/priority", methods=["POST"])
def prioritise_variable_suggestions():
    """Move the given columns' chunks to the front of the queue."""
    ctx = get_app_context()
    llm_service = ctx.get("llm_service")
    session_cache = ctx.get("session_cache")

    body = request.get_json(silent=True) or {}
    database = body.get("database")
    columns = body.get("columns")
    if not database or not isinstance(columns, list) or not columns:
        return jsonify({"error": "database and columns are required"}), 400

    result = llm_service.bump_priority(
        session_cache, "variables", database, columns, retry=bool(body.get("retry"))
    )
    return jsonify(result)


@llm_bp.route("/api/v1/llm/suggestions/values/start", methods=["POST"])
def start_value_suggestions():
    """Start (idempotently) the categorical value suggestion job."""
    ctx = get_app_context()
    llm_service = ctx.get("llm_service")
    session_cache = ctx.get("session_cache")

    body = request.get_json(silent=True) or {}
    result = llm_service.start_value_job(session_cache, force=bool(body.get("force")))
    return jsonify(result)


@llm_bp.route("/api/v1/llm/suggestions/values", methods=["GET"])
def get_value_suggestions():
    """Return the current snapshot of the values suggestion job."""
    return _phase_state("values")


@llm_bp.route("/api/v1/llm/suggestions/values/priority", methods=["POST"])
def prioritise_value_suggestions():
    """Move one categorical variable's chunks to the front of the queue."""
    ctx = get_app_context()
    llm_service = ctx.get("llm_service")
    session_cache = ctx.get("session_cache")

    body = request.get_json(silent=True) or {}
    database = body.get("database")
    column = body.get("column")
    if not database or not column:
        return jsonify({"error": "database and column are required"}), 400

    result = llm_service.bump_priority(
        session_cache, "values", database, [column], retry=bool(body.get("retry"))
    )
    return jsonify(result)
