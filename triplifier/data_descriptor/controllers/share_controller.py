"""
Share controller for share-related endpoints.

This module handles HTTP requests for sharing data, including
semantic map downloads, ontology downloads, and share page rendering.
"""

import json
import logging
import os
import zipfile

from flask import (
    Blueprint,
    Response,
    abort,
    after_this_request,
    render_template,
    send_from_directory,
    jsonify,
    request,
)

from services import ShareService

logger = logging.getLogger(__name__)

share_bp = Blueprint("share", __name__)


def get_app_context():
    """Get application context (session_cache, graphdb_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@share_bp.route("/describe_downloads")
def describe_downloads():
    """
    Render the Share landing page.

    Returns:
        Rendered share_landing.html template.
    """
    return render_template(
        "share_landing.html",
        graphdb_location="http://localhost:7200/",
    )


@share_bp.route("/downloadSemanticMap", methods=["GET"])
def download_semantic_map():
    """
    Download the semantic map in JSON-LD or legacy JSON format.

    Returns:
        Response with semantic map file.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    formulate_local_map = ctx.get("formulate_local_map")

    return ShareService.download_semantic_map(session_cache, formulate_local_map)


@share_bp.route("/downloadOntology", methods=["GET"])
def download_ontology():
    """
    Download ontology files from GraphDB.

    Returns:
        Response with ontology file(s).
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")
    graphdb_url = ctx.get("graphdb_url", "http://localhost:7200")
    named_graph = "http://ontology.local/"

    return ShareService.download_ontology(
        session_cache, graphdb_service, graphdb_url, named_graph
    )


@share_bp.route("/favicon.ico")
def favicon():
    """
    Serve the favicon.

    Returns:
        Favicon file.
    """
    ctx = get_app_context()
    root_dir = ctx.get("root_dir", "")
    child_dir = ctx.get("child_dir", ".")

    return send_from_directory(
        f"{root_dir}{child_dir}/assets",
        "favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@share_bp.route("/data_descriptor/assets/<path:filename>")
def custom_static(filename):
    """
    Serve static files from assets directory.

    Args:
        filename: Path to the static file.

    Returns:
        Static file.
    """
    ctx = get_app_context()
    root_dir = ctx.get("root_dir", "")
    child_dir = ctx.get("child_dir", ".")

    return send_from_directory(f"{root_dir}{child_dir}/assets", filename)


@share_bp.route("/share_landing")
def share_landing():
    """
    Render the Share landing page.

    Returns:
        Rendered share_landing.html template.
    """
    return render_template(
        "share_landing.html", graphdb_location="http://localhost:7200/"
    )


@share_bp.route("/share_mock")
def share_mock():
    """
    Render the mock data generation page.

    Returns:
        Rendered share_mock.html template.
    """
    return render_template("share_mock.html", graphdb_location="http://localhost:7200/")


@share_bp.route("/share_publish")
def share_publish():
    """
    Render the publishing options page.

    Returns:
        Rendered share_publish.html template.
    """
    return render_template(
        "share_publish.html", graphdb_location="http://localhost:7200/"
    )


@share_bp.route("/api/generate-mock-data", methods=["POST"])
def generate_mock_data():
    """
    Generate mock data from a JSON-LD semantic map.

    Expects JSON payload with:
    - jsonld_map: The semantic map data
    - num_rows: Number of rows to generate (default: 100)
    - random_seed: Optional random seed for reproducibility
    - database_id: Optional specific database to generate for
    - table_id: Optional specific table to generate for

    Returns:
        JSON response with generated mock data or error
    """
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

        # Generate mock data using the service
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
