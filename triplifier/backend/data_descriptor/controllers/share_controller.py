"""
Share controller for share-related endpoints.

This module handles HTTP requests for sharing data, including
semantic map downloads, ontology downloads, and share page rendering.
"""

import logging

from flask import (
    Blueprint,
    jsonify,
    redirect,
    request,
    send_from_directory,
)

from services import ShareService

logger = logging.getLogger(__name__)

share_bp = Blueprint("share", __name__)


def get_app_context() -> dict:
    """Get application context (session_cache, rdf_store_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@share_bp.route("/share_landing")
def share_landing():
    return redirect("/share")


@share_bp.route("/downloadSemanticMap", methods=["GET"])
def download_semantic_map():
    """Download the semantic map in JSON-LD or legacy JSON format."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    formulate_local_map = ctx.get("formulate_local_map")
    return ShareService.download_semantic_map(session_cache, formulate_local_map)


@share_bp.route("/downloadOntology", methods=["GET"])
def download_ontology():
    """Download ontology files from the RDF store."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    rdf_store_url = ctx.get("rdf_store_url", "http://localhost:7200")
    named_graph = "http://ontology.local/"
    return ShareService.download_ontology(
        session_cache, rdf_store_service, rdf_store_url, named_graph
    )


@share_bp.route("/favicon.ico")
def favicon():
    """Serve the favicon."""
    ctx = get_app_context()
    root_dir = ctx.get("root_dir", "")
    child_dir = ctx.get("child_dir", ".")

    return send_from_directory(
        f"{root_dir}{child_dir}/static",
        "favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@share_bp.route("/share_mock")
def share_mock():
    return redirect("/share/mock")


@share_bp.route("/share_publish")
def share_publish():
    return redirect("/share/publish")


@share_bp.route("/api/generate-mock-data", methods=["POST"])
def generate_mock_data():
    """Generate mock data from a JSON-LD semantic map."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400

        jsonld_map = data.get("jsonld_map")
        num_rows = data.get("num_rows", 100)
        random_seed = data.get("random_seed")
        database_id = data.get("database_id")
        table_id = data.get("table_id")

        if not jsonld_map:
            return jsonify({"success": False, "error": "No semantic map provided"}), 400

        if table_id and not database_id:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "table_id requires database_id to be specified",
                    }
                ),
                400,
            )

        result = ShareService.generate_mock_data_from_semantic_map(
            jsonld_map,
            num_rows=num_rows,
            random_seed=random_seed,
            database_id=database_id,
            table_id=table_id,
        )

        if result["success"]:
            response = jsonify(result)
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response, 200
        else:
            response = jsonify(result)
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response, 500

    except Exception as e:
        response = jsonify({"success": False, "error": f"Server error: {str(e)}"})
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 500
