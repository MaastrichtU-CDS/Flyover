"""
Ingest controller for data ingestion endpoints.

This module handles HTTP requests related to data ingestion,
including file uploads, semantic map uploads, and data processing.
"""

import json
import logging

from flask import Blueprint, jsonify, redirect, request

from services import IngestService
from validation import MappingValidator
from loaders import JSONLDMapping

logger = logging.getLogger(__name__)

ingest_bp = Blueprint("ingest", __name__)


def get_app_context() -> dict:
    """Get application context (session_cache, rdf_store_url, etc.)."""
    from flask import current_app
    from services import RDFStoreService

    ctx = current_app.config.get("APP_CONTEXT", {})

    if not ctx.get("rdf_store_service") and "rdf_store_url" in current_app.config:
        ctx["rdf_store_service"] = RDFStoreService(
            current_app.config["rdf_store_url"], current_app.config["repo"]
        )

    return ctx


@ingest_bp.route("/")
def landing():
    return redirect("/app/")


@ingest_bp.route("/ingest")
def index():
    return redirect("/app/ingest")


@ingest_bp.route("/upload-semantic-map", methods=["POST"])
def upload_semantic_map():
    """Handle semantic map file upload."""
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
    """Handle semantic map submission from IndexedDB."""
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
                "redirect_url": "/app/annotate/review",
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
    """Handle file upload for CSV or PostgreSQL data."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    run_triplifier = ctx.get("run_triplifier")
    upload_func = ctx.get("upload_func")
    start_background = ctx.get("start_background")

    file_type = request.form.get("fileType")
    csv_files = request.files.getlist("csvFile")
    pk_fk_data = request.form.get("pkFkData")
    cross_graph_data = request.form.get("crossGraphLinkData")

    if pk_fk_data:
        session_cache.pk_fk_data = IngestService.parse_pk_fk_data(pk_fk_data)
    if cross_graph_data:
        session_cache.cross_graph_link_data = IngestService.parse_cross_graph_data(
            cross_graph_data
        )

    upload = True

    if file_type == "CSV" and csv_files:
        is_valid, error = IngestService.validate_csv_files(csv_files)
        if not is_valid:
            return redirect(f"/app/ingest?error={error}")

        separator = request.form.get("csv_separator_sign", ",")
        decimal = request.form.get("csv_decimal_sign", ".")

        dataframes, table_names, error = IngestService.parse_csv_files(
            csv_files, separator, decimal
        )

        if error:
            return redirect(f"/app/ingest?error={error}")

        session_cache.csvData = dataframes
        session_cache.csvTableNames = table_names

        success, message = run_triplifier("triplifierCSV.properties")

    elif file_type == "Postgres":
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
        message = (
            "You have opted to not submit any new data. "
            "You can now proceed to describe your data."
        )
    else:
        success = False
        message = "An unexpected error occurred. Please try again."

    if success:
        session_cache.StatusToDisplay = message

        if upload and upload_func:
            logger.info("Initiating upload to RDF store")
            upload_success, upload_messages = upload_func(
                file_type, session_cache.output_files
            )

            for msg in upload_messages:
                logger.info(f"Upload: {msg}")

            if file_type == "CSV" and start_background:
                start_background(session_cache)

        return redirect("/app/describe")
    else:
        return redirect(f"/app/ingest?error=Error: {message}")


@ingest_bp.route("/data-submission")
def data_submission():
    return redirect("/app/describe")


@ingest_bp.route("/api/rdf-store-databases", methods=["GET"])
def get_rdf_store_databases():
    """Get list of databases from the RDF store."""
    ctx = get_app_context()
    rdf_store_service = ctx.get("rdf_store_service")
    session_cache = ctx.get("session_cache")

    try:
        databases = rdf_store_service.get_databases() if rdf_store_service else []
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
    """Check if graph data exists."""
    ctx = get_app_context()
    rdf_store_service = ctx.get("rdf_store_service")

    try:
        exists = rdf_store_service.check_data_exists() if rdf_store_service else False
        return jsonify({"exists": exists})
    except Exception as e:
        return jsonify({"exists": False, "error": str(e)})


@ingest_bp.route("/get-existing-graph-structure", methods=["GET"])
def get_existing_graph_structure():
    """Get structure of existing graph data."""
    ctx = get_app_context()
    rdf_store_service = ctx.get("rdf_store_service")

    try:
        if rdf_store_service:
            return jsonify(rdf_store_service.get_graph_structure())
        return {"tables": [], "tableColumns": {}}
    except Exception as e:
        logger.error(f"Error getting graph structure: {e}")
        return {"tables": [], "tableColumns": {}}
