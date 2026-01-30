"""
Annotate controller for annotation endpoints.

This module handles HTTP requests related to semantic annotation,
including annotation execution and verification.
"""

import json
import logging
import os

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from services import AnnotateService
from validation import MappingValidator
from loaders import JSONLDMapping

logger = logging.getLogger(__name__)

annotate_bp = Blueprint("annotate", __name__)


def get_app_context():
    """Get application context (session_cache, graphdb_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


def safe_remove_file(filepath):
    """
    Safely remove a file, logging any errors but not raising exceptions.

    Args:
        filepath: Path to the file to remove.
    """
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except OSError as e:
        logger.warning(f"Failed to remove file {filepath}: {e}")


@annotate_bp.route("/annotation_landing")
def annotation_landing():
    """
    Render the annotation landing page.

    Returns:
        Rendered annotation_landing.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")

    try:
        data_exists = graphdb_service.check_data_exists() if graphdb_service else False
        session_cache.existing_graph = data_exists

        return render_template(
            "annotation_landing.html",
            data_exists=data_exists,
            message=None,
        )
    except Exception as e:
        flash(f"Error accessing annotation step: {e}")
        return redirect(url_for("ingest.landing"))


@annotate_bp.route("/upload-annotation-json", methods=["POST"])
def upload_annotation_json():
    """
    Handle JSON-LD file upload for annotation.

    Returns:
        JSON response with validation result.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")
    name_matcher = ctx.get("name_matcher")
    upload_folder = ctx.get("upload_folder")

    try:
        if "annotationJsonFile" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files["annotationJsonFile"]
        if file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        if not file.filename.lower().endswith(".jsonld"):
            return jsonify({"error": "File must be a .jsonld file"}), 400

        # Save file
        filename = secure_filename(file.filename)
        filepath = os.path.join(upload_folder, filename)
        file.save(filepath)

        try:
            with open(filepath, "r") as f:
                json_data = json.load(f)

            # Ensure databases are available
            databases = graphdb_service.get_databases() if graphdb_service else []
            session_cache.databases = databases

            if not databases:
                safe_remove_file(filepath)
                return (
                    jsonify(
                        {
                            "error": "No data found. Please complete Ingest step first.",
                            "graphdb_databases": [],
                            "jsonld_databases": [],
                        }
                    ),
                    400,
                )

            # Extract tables from JSON-LD
            jsonld_tables = []
            if json_data.get("databases"):
                for db_key, db_data in json_data["databases"].items():
                    if isinstance(db_data, dict) and db_data.get("tables"):
                        for table_key, table_data in db_data["tables"].items():
                            table_name = (
                                table_data.get("sourceFile", table_key)
                                if isinstance(table_data, dict)
                                else table_key
                            )
                            jsonld_tables.append(table_name)
            elif json_data.get("database_name"):
                jsonld_tables.append(json_data["database_name"])

            if not jsonld_tables:
                safe_remove_file(filepath)
                return (
                    jsonify(
                        {
                            "error": "No table definitions found in semantic map.",
                            "graphdb_databases": databases,
                            "jsonld_databases": [],
                        }
                    ),
                    400,
                )

            # Find matching tables
            matching = []
            non_matching = []

            for jsonld_table in jsonld_tables:
                matched = None
                for db in databases:
                    if name_matcher(jsonld_table, db):
                        matched = db
                        break
                if matched:
                    matching.append({"jsonld": jsonld_table, "graphdb": matched})
                else:
                    non_matching.append(jsonld_table)

            if not matching:
                safe_remove_file(filepath)
                return (
                    jsonify(
                        {
                            "error": "No data sources match GraphDB data.",
                            "graphdb_databases": databases,
                            "jsonld_databases": jsonld_tables,
                            "matching_databases": [],
                            "non_matching_jsonld": non_matching,
                        }
                    ),
                    400,
                )

            # Store in session
            session_cache.global_semantic_map = json_data
            session_cache.annotation_json_path = filepath

            # Validate and store as jsonld_mapping
            validator = MappingValidator()
            result = validator.validate(json_data)
            if result.is_valid:
                session_cache.jsonld_mapping = JSONLDMapping.from_dict(json_data)

            return jsonify(
                {
                    "success": True,
                    "message": "JSON-LD file validated successfully",
                    "filename": filename,
                    "graphdb_databases": databases,
                    "jsonld_databases": jsonld_tables,
                    "matching_databases": matching,
                    "non_matching_jsonld": non_matching,
                }
            )

        except json.JSONDecodeError:
            safe_remove_file(filepath)
            return jsonify({"error": "Invalid JSON file format"}), 400

    except Exception as e:
        return jsonify({"error": f"Upload failed: {e}"}), 500


@annotate_bp.route("/annotation-review")
def annotation_review():
    """
    Render the annotation review page.

    Returns:
        Rendered annotation_review.html template.
    """
    return render_template("annotation_review.html")


@annotate_bp.route("/start-annotation", methods=["POST"])
def start_annotation():
    """
    Start the annotation process.

    Returns:
        JSON response with annotation status.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")
    graphdb_url = ctx.get("graphdb_url")
    get_semantic_map = ctx.get("get_semantic_map")
    formulate_local_map = ctx.get("formulate_local_map")
    name_matcher = ctx.get("name_matcher")
    get_table_names = ctx.get("get_table_names")
    has_semantic_map = ctx.get("has_semantic_map")

    try:
        if not has_semantic_map(session_cache):
            return jsonify({"success": False, "error": "No semantic map available"})

        # Ensure databases are available
        databases = graphdb_service.get_databases() if graphdb_service else []
        session_cache.databases = databases

        if not databases:
            return jsonify({"success": False, "error": "No databases available"})

        endpoint = f"{graphdb_url}/repositories/{session_cache.repo}/statements"
        map_table_names = get_table_names(session_cache)

        # Prepare annotation data
        annotation_data = AnnotateService.prepare_annotation_data(
            databases,
            session_cache,
            map_table_names,
            name_matcher,
            get_semantic_map,
            formulate_local_map,
        )

        if not annotation_data:
            return jsonify(
                {
                    "success": False,
                    "error": "No variables with local definitions found",
                }
            )

        # Execute annotations
        session_cache.annotation_status = {}
        total_annotated = 0

        for database, data in annotation_data.items():
            logger.info(f"Processing annotation for database: {database}")

            success, error = AnnotateService.execute_annotation(
                endpoint,
                database,
                data["prefixes"],
                data["variables"],
            )

            for var_name in data["variables"]:
                status_key = f"{database}.{var_name}"
                if success:
                    session_cache.annotation_status[status_key] = {
                        "success": True,
                        "message": "Annotation completed",
                        "database": database,
                    }
                    total_annotated += 1
                else:
                    session_cache.annotation_status[status_key] = {
                        "success": False,
                        "error": error,
                        "database": database,
                    }

        if total_annotated == 0:
            return jsonify(
                {
                    "success": False,
                    "error": "No variables were successfully annotated",
                }
            )

        return jsonify(
            {
                "success": True,
                "message": f"Annotated {total_annotated} variables across {len(annotation_data)} databases",
            }
        )

    except Exception as e:
        logger.error(f"Annotation error: {e}")
        return jsonify({"success": False, "error": str(e)})


@annotate_bp.route("/annotation-verify")
def annotation_verify():
    """
    Render the annotation verification page.

    Returns:
        Rendered annotation_verify.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")
    get_semantic_map = ctx.get("get_semantic_map")
    formulate_local_map = ctx.get("formulate_local_map")
    name_matcher = ctx.get("name_matcher")
    get_table_names = ctx.get("get_table_names")
    has_semantic_map = ctx.get("has_semantic_map")

    if not has_semantic_map(session_cache):
        flash("No semantic map available.")
        return redirect(url_for("download.describe_downloads"))

    databases = graphdb_service.get_databases() if graphdb_service else []
    session_cache.databases = databases

    if not databases:
        flash("No databases available.")
        return redirect(url_for("download.describe_downloads"))

    map_table_names = get_table_names(session_cache)

    annotated, unannotated, variable_data = AnnotateService.get_verification_data(
        databases,
        session_cache,
        map_table_names,
        name_matcher,
        get_semantic_map,
        formulate_local_map,
    )

    annotation_status = session_cache.annotation_status or {}

    success_message = None
    if annotation_status and all(s.get("success") for s in annotation_status.values()):
        success_message = (
            "Data processing complete. Semantic interoperability achieved."
        )

    return render_template(
        "annotation_verify.html",
        annotated_variables=annotated,
        unannotated_variables=unannotated,
        annotation_status=annotation_status,
        variable_data=variable_data,
        success_message=success_message,
    )


@annotate_bp.route("/verify-annotation-ask", methods=["POST"])
def verify_annotation_ask():
    """
    Verify annotation using ASK query.

    Returns:
        JSON response with verification result.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")
    get_semantic_map = ctx.get("get_semantic_map")
    formulate_local_map = ctx.get("formulate_local_map")

    try:
        data = request.get_json()
        variable_name = data.get("variable")

        if not variable_name:
            return jsonify({"success": False, "error": "No variable specified"})

        success, is_valid, query, error = AnnotateService.verify_single_annotation(
            variable_name,
            session_cache,
            graphdb_service,
            get_semantic_map,
            formulate_local_map,
        )

        if not success:
            return jsonify({"success": False, "error": error})

        return jsonify(
            {
                "success": True,
                "valid": is_valid,
                "query": query,
            }
        )

    except Exception as e:
        logger.error(f"Verification error: {e}")
        return jsonify({"success": False, "error": str(e)})
