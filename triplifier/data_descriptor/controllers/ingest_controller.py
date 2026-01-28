"""
Ingest controller for data ingestion endpoints.

This module handles HTTP requests related to data ingestion,
including file uploads, semantic map uploads, and data processing.
"""

import json
import logging
import os

from flask import Blueprint, jsonify, redirect, render_template, request, url_for, flash
from markupsafe import Markup
from werkzeug.utils import secure_filename

from services import IngestService
from validation import MappingValidator
from loaders import JSONLDMapping

logger = logging.getLogger(__name__)

ingest_bp = Blueprint("ingest", __name__)


def get_app_context():
    """Get application context (session_cache, graphdb_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@ingest_bp.route("/")
def landing():
    """
    Render the landing page.

    Returns:
        Rendered index.html template.
    """
    return render_template("index.html")


@ingest_bp.route("/ingest")
def index():
    """
    Render the ingest page.

    Checks for existing data and renders appropriate view.

    Returns:
        Rendered ingest.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")

    try:
        if graphdb_service and graphdb_service.check_data_exists():
            session_cache.existing_graph = True
            return render_template("ingest.html", graph_exists=True)
    except Exception as e:
        flash(f"Failed to check if data graph exists: {e}")

    return render_template("ingest.html")


@ingest_bp.route("/upload-semantic-map", methods=["POST"])
def upload_semantic_map():
    """
    Handle semantic map file upload.

    Returns:
        JSON response with validation result.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")

    semantic_map_file = request.files.get("semanticMapFile")

    if not semantic_map_file or not semantic_map_file.filename:
        return jsonify({"error": "No semantic map file provided"}), 400

    if not IngestService.allowed_file(semantic_map_file.filename, {"jsonld"}):
        return jsonify({"error": "Please upload a valid .jsonld file"}), 400

    try:
        file_content = semantic_map_file.read().decode("utf-8")

        try:
            mapping_data = json.loads(file_content)
        except json.JSONDecodeError as e:
            return (
                jsonify(
                    {
                        "error": f"Invalid JSON-LD syntax at line {e.lineno}, column {e.colno}: {e.msg}",
                        "validation_errors": [
                            {
                                "path": f"(line {e.lineno}, column {e.colno})",
                                "severity": "error",
                                "message": f"JSON-LD syntax error: {e.msg}",
                                "suggestion": "Check for missing commas, brackets, or quotes",
                            }
                        ],
                    }
                ),
                400,
            )

        validator = MappingValidator()
        result = validator.validate(mapping_data)

        if not result.is_valid:
            errors = validator.format_errors_for_ui(result)
            return (
                jsonify(
                    {
                        "error": "Mapping file validation failed",
                        "validation_errors": errors,
                    }
                ),
                400,
            )

        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_data)

        return jsonify(
            {
                "success": True,
                "message": "JSON-LD semantic mapping uploaded successfully",
                "statistics": result.statistics,
            }
        )

    except Exception as e:
        logger.error(f"Error processing semantic map: {e}")
        return jsonify({"error": f"Unexpected error: {e}"}), 400


@ingest_bp.route("/submit-indexeddb-semantic-map", methods=["POST"])
def submit_indexeddb_semantic_map():
    """
    Handle semantic map submission from IndexedDB.

    Returns:
        JSON response with redirect URL.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")

    try:
        mapping_data = request.get_json()

        if not mapping_data:
            return jsonify({"error": "No semantic map data provided"}), 400

        validator = MappingValidator()
        result = validator.validate(mapping_data)

        if not result.is_valid:
            errors = validator.format_errors_for_ui(result)
            return (
                jsonify(
                    {
                        "error": "Semantic map validation failed",
                        "validation_errors": errors,
                    }
                ),
                400,
            )

        session_cache.jsonld_mapping = JSONLDMapping.from_dict(mapping_data)

        return jsonify(
            {
                "success": True,
                "message": "Semantic map loaded from browser storage",
                "redirect_url": "/annotation-review",
                "statistics": result.statistics,
            }
        )

    except json.JSONDecodeError as e:
        return jsonify({"error": f"Invalid JSON data: {e.msg}"}), 400
    except Exception as e:
        logger.error(f"Error processing IndexedDB semantic map: {e}")
        return jsonify({"error": f"Unexpected error: {e}"}), 400


@ingest_bp.route("/upload", methods=["POST"])
def upload_file():
    """
    Handle file upload for CSV or PostgreSQL data.

    Returns:
        Redirect to appropriate page based on success.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    run_triplifier = ctx.get("run_triplifier")
    upload_func = ctx.get("upload_func")
    start_background = ctx.get("start_background")

    file_type = request.form.get("fileType")
    csv_files = request.files.getlist("csvFile")
    pk_fk_data = request.form.get("pkFkData")
    cross_graph_data = request.form.get("crossGraphLinkData")

    # Store relationship data
    if pk_fk_data:
        session_cache.pk_fk_data = IngestService.parse_pk_fk_data(pk_fk_data)
    if cross_graph_data:
        session_cache.cross_graph_link_data = IngestService.parse_cross_graph_data(
            cross_graph_data
        )

    upload = True

    if file_type == "CSV" and csv_files:
        # Validate CSV files
        is_valid, error = IngestService.validate_csv_files(csv_files)
        if not is_valid:
            flash(error)
            return render_template("ingest.html", error=True)

        # Parse CSV files
        separator = request.form.get("csv_separator_sign", ",")
        decimal = request.form.get("csv_decimal_sign", ".")

        dataframes, table_names, error = IngestService.parse_csv_files(
            csv_files, separator, decimal
        )

        if error:
            flash(error)
            return render_template("ingest.html", error=True)

        session_cache.csvData = dataframes
        session_cache.csvTableNames = table_names

        success, message = run_triplifier("triplifierCSV.properties")

    elif file_type == "Postgres":
        # Handle PostgreSQL connection
        handle_postgres = ctx.get("handle_postgres")
        if handle_postgres:
            handle_postgres(
                request.form.get("username"),
                request.form.get("password"),
                request.form.get("POSTGRES_URL"),
                request.form.get("POSTGRES_DB"),
                request.form.get("table"),
            )
        success, message = run_triplifier("triplifierSQL.properties")

    elif file_type != "Postgres" and not any(f.filename for f in csv_files):
        success = True
        upload = False
        message = Markup(
            "You have opted to not submit any new data. "
            "You can now proceed to describe your data."
        )
    else:
        success = False
        message = "An unexpected error occurred. Please try again."

    if success:
        session_cache.StatusToDisplay = message

        if upload and upload_func:
            logger.info("Initiating upload to GraphDB")
            upload_success, upload_messages = upload_func(
                file_type, session_cache.output_files
            )

            for msg in upload_messages:
                logger.info(f"Upload: {msg}")

            # Start background processing
            if file_type == "CSV" and start_background:
                start_background(session_cache)

        return redirect(url_for("ingest.data_submission"))
    else:
        flash(f"Error: {message}")
        return render_template(
            "ingest.html",
            error=True,
            graph_exists=session_cache.existing_graph,
        )


@ingest_bp.route("/data-submission")
def data_submission():
    """
    Render data submission confirmation page.

    Returns:
        Rendered describe_landing.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")

    return render_template(
        "describe_landing.html",
        message=Markup(session_cache.StatusToDisplay or ""),
    )


@ingest_bp.route("/api/graphdb-databases", methods=["GET"])
def get_graphdb_databases():
    """
    Get list of databases from GraphDB.

    Returns:
        JSON response with database list.
    """
    ctx = get_app_context()
    graphdb_service = ctx.get("graphdb_service")
    session_cache = ctx.get("session_cache")

    try:
        databases = graphdb_service.get_databases() if graphdb_service else []
        session_cache.databases = databases

        if databases:
            return jsonify({"success": True, "databases": databases})
        else:
            return jsonify(
                {
                    "success": False,
                    "databases": [],
                    "message": "No databases found",
                }
            )
    except Exception as e:
        logger.error(f"Error fetching databases: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@ingest_bp.route("/api/check-graph-exists", methods=["GET"])
def api_check_graph_exists():
    """
    Check if graph data exists.

    Returns:
        JSON response with existence status.
    """
    ctx = get_app_context()
    graphdb_service = ctx.get("graphdb_service")

    try:
        exists = graphdb_service.check_data_exists() if graphdb_service else False
        return jsonify({"exists": exists})
    except Exception as e:
        return jsonify({"exists": False, "error": str(e)})


@ingest_bp.route("/get-existing-graph-structure", methods=["GET"])
def get_existing_graph_structure():
    """
    Get structure of existing graph data.

    Returns:
        JSON response with tables and columns.
    """
    ctx = get_app_context()
    graphdb_service = ctx.get("graphdb_service")

    try:
        if graphdb_service:
            return jsonify(graphdb_service.get_graph_structure())
        return {"tables": [], "tableColumns": {}}
    except Exception as e:
        logger.error(f"Error getting graph structure: {e}")
        return {"tables": [], "tableColumns": {}}
