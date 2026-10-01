"""
Suggestions controller for mapping suggestion endpoints.

Serves the polling API the describe pages use to start suggestion jobs,
fetch arriving suggestions, and reprioritise the queue, plus the LLM
prompt-export round trip (``/prompt`` composes a prompt for an external
LLM, ``/<phase>/ingest`` merges the pasted answer as suggestions). Routes
are the ``/api/v1/suggestions/*`` surface.

Adapted from the LLM branch's ``llm_controller.py`` with provider-specific
status replaced by the tier-aware ``/status`` shape.
"""

import logging

from flask import Blueprint, jsonify, request

from services.suggestions import SuggestionRequestError
from services.suggestions.prompt_export import chunk_size_from_env
from utils.mapping_request import parse_and_validate_mapping

logger = logging.getLogger(__name__)

suggestions_bp = Blueprint("suggestions", __name__)

_VALID_PHASES = ("variables", "values")


def get_app_context() -> dict:
    """Get application context (session_cache, suggestion_service, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


def _request_error(exc: SuggestionRequestError):
    return jsonify({"error": exc.message, "kind": exc.kind}), 400


def _json_body() -> dict:
    """The request body when it is a JSON object, else ``{}``.

    A body that parses to a list, a string or a number is a caller
    mistake, not a server error: it is treated as empty so the route's
    own required-field checks answer with a readable 400 instead of an
    unhandled ``AttributeError`` (a 500).
    """
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else {}


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
                # The copy-prompt / paste-answer round trip has no model
                # and no flag: it is available whenever the service is.
                # The chunk default matches the enabled branch.
                "prompt_export": (
                    {"state": "active", "chunk": chunk_size_from_env()}
                    if service is not None
                    else {"state": "inactive", "reason": "no suggestion service"}
                ),
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
    # With every tier disabled the only job that can exist is one a pasted
    # LLM answer created: the page must render it, so the snapshot reports
    # the suggestion UI enabled whenever there is a job to show.
    state["enabled"] = service.config.enabled or state.get("status") != "idle"
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


@suggestions_bp.route("/api/v1/suggestions/prompt", methods=["GET", "POST"])
def suggestions_prompt():
    """Compose the copy-prompt payload for one database and phase.

    ``phase`` and ``database`` come from the query string or the JSON
    body; a POST body may also carry the browser's semantic map (the map
    the describe pages work on), used for this response only. Option:
    ``chunk`` (items per prompt).
    """
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    if service is None:
        return (
            jsonify({"error": "suggestions are not available", "kind": "no_service"}),
            503,
        )

    body = _json_body() if request.method == "POST" else {}
    params = {
        **request.args.to_dict(),
        **{k: v for k, v in body.items() if k != "mapping"},
    }
    phase = params.get("phase")
    database = params.get("database")
    if phase not in _VALID_PHASES:
        return (
            jsonify({"error": f"unknown phase '{phase}'", "kind": "unknown_phase"}),
            400,
        )
    if not database:
        return (
            jsonify({"error": "database is required", "kind": "unknown_database"}),
            400,
        )

    mapping_data = (
        body.get("mapping") if isinstance(body.get("mapping"), dict) else None
    )
    mapping = parse_and_validate_mapping(mapping_data)
    if mapping is None:
        mapping_data = None
    chunk = params.get("chunk")
    try:
        chunk = int(chunk) if chunk not in (None, "") else None
    except (TypeError, ValueError):
        chunk = None
    try:
        result = service.build_prompt(
            phase,
            session_cache,
            rdf_store_service,
            database=database,
            mapping=mapping,
            mapping_data=mapping_data,
            chunk=chunk,
        )
    except SuggestionRequestError as exc:
        return _request_error(exc)
    return jsonify(result)


@suggestions_bp.route("/api/v1/suggestions/<phase>/ingest", methods=["POST"])
def ingest_suggestions(phase: str):
    """Merge a pasted LLM answer into the phase's job as suggestions.

    Body: ``{"database": ..., "answer": "<pasted text>" | "records": [...],
    "source": "pasted_llm", "mapping": {...}}``. The answer is parsed and
    validated server-side; malformed input is a 400 with a readable
    message. Nothing is written to the JSON-LD.
    """
    if phase not in _VALID_PHASES:
        return (
            jsonify({"error": f"unknown phase '{phase}'", "kind": "unknown_phase"}),
            400,
        )
    ctx = get_app_context()
    service = ctx.get("suggestion_service")
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    if service is None:
        return (
            jsonify({"error": "suggestions are not available", "kind": "no_service"}),
            503,
        )

    body = _json_body()

    database = body.get("database")
    if not database:
        return (
            jsonify({"error": "database is required", "kind": "unknown_database"}),
            400,
        )
    answer = body.get("answer")
    records = body.get("records")
    if answer is None and not isinstance(records, list):
        return (
            jsonify(
                {
                    "error": "paste the LLM answer as 'answer' (text) or send 'records'",
                    "kind": "bad_answer",
                }
            ),
            400,
        )
    mapping = parse_and_validate_mapping(body.get("mapping"))
    try:
        result = service.ingest(
            phase,
            session_cache,
            rdf_store_service,
            database=database,
            answer=answer,
            records=records if answer is None else None,
            mapping=mapping,
            source=body.get("source") or "pasted_llm",
        )
    except SuggestionRequestError as exc:
        return _request_error(exc)
    return jsonify(result)
