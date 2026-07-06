"""
Annotate controller for annotation endpoints.

This module handles HTTP requests related to semantic annotation,
including annotation execution and verification.
"""

import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, jsonify, redirect, request
from werkzeug.utils import secure_filename

from services import AnnotateService
from validation import MappingValidator
from loaders import JSONLDMapping
from utils.rdf_store_url import build_repository_endpoint

logger = logging.getLogger(__name__)

annotate_bp = Blueprint("annotate", __name__)


def get_app_context() -> dict:
    """Get application context (session_cache, rdf_store_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


def safe_remove_file(filepath: str) -> None:
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except OSError as e:
        logger.warning(f"Failed to remove file {filepath}: {e}")


@annotate_bp.route("/annotation_landing")
def annotation_landing():
    return redirect("/annotate")


@annotate_bp.route("/upload-annotation-json", methods=["POST"])
def upload_annotation_json():
    """Handle JSON-LD file upload for annotation."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
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

        filename = secure_filename(file.filename)
        filepath = os.path.join(upload_folder, filename)
        file.save(filepath)

        try:
            with open(filepath, "r") as f:
                json_data = json.load(f)

            databases = rdf_store_service.get_databases() if rdf_store_service else []
            session_cache.databases = databases

            if not databases:
                safe_remove_file(filepath)
                return (
                    jsonify(
                        {
                            "error": "No data found. Please complete Ingest step first.",
                            "rdf_store_databases": [],
                            "jsonld_databases": [],
                        }
                    ),
                    400,
                )

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
                            "rdf_store_databases": databases,
                            "jsonld_databases": [],
                        }
                    ),
                    400,
                )

            matching = []
            non_matching = []

            for jsonld_table in jsonld_tables:
                matched = None
                for db in databases:
                    if name_matcher(jsonld_table, db):
                        matched = db
                        break
                if matched:
                    matching.append({"jsonld": jsonld_table, "rdf_store": matched})
                else:
                    non_matching.append(jsonld_table)

            if not matching:
                safe_remove_file(filepath)
                return (
                    jsonify(
                        {
                            "error": "No data sources match RDF store data.",
                            "rdf_store_databases": databases,
                            "jsonld_databases": jsonld_tables,
                            "matching_databases": [],
                            "non_matching_jsonld": non_matching,
                        }
                    ),
                    400,
                )

            # Clean the mapping of orphan columns right at the start
            # This ensures faulty mappings don't persist anywhere
            removed_columns = []
            try:
                if rdf_store_service is not None:
                    columns_by_database = (
                        rdf_store_service.get_column_info_by_database() or {}
                    )
                    if columns_by_database:
                        # Get loaded databases - use the databases list we already fetched
                        loaded_databases = databases

                        from .ingest_controller import (
                            _collect_orphan_column_issues,
                            _remove_orphan_columns_from_mapping,
                        )

                        orphan_warnings, _, orphan_columns = (
                            _collect_orphan_column_issues(
                                json_data, columns_by_database, loaded_databases
                            )
                        )

                        # Clean the mapping immediately, before validation
                        if orphan_columns:
                            json_data = _remove_orphan_columns_from_mapping(
                                json_data, orphan_columns
                            )
                            # Record which local column mappings were removed so the
                            # user can be warned about them in the UI.
                            for warning in orphan_warnings:
                                values = warning.get("values")
                                if values:
                                    removed_columns.extend(values)
            except Exception as e:
                logger.warning(f"Orphan column check skipped: {e}")

            # Use the cleaned mapping for both session cache entries
            session_cache.global_semantic_map = json_data
            session_cache.annotation_json_path = filepath

            # Now validate the cleaned mapping
            validator = MappingValidator()
            result = validator.validate(json_data)
            if result.is_valid:
                session_cache.jsonld_mapping = JSONLDMapping.from_dict(json_data)

            return jsonify(
                {
                    "success": True,
                    "message": "JSON-LD file validated successfully",
                    "filename": filename,
                    "rdf_store_databases": databases,
                    "jsonld_databases": jsonld_tables,
                    "matching_databases": matching,
                    "non_matching_jsonld": non_matching,
                    "removed_columns": removed_columns,
                    "cleaned_mapping": json_data,
                }
            )

        except json.JSONDecodeError:
            safe_remove_file(filepath)
            return jsonify({"error": "Invalid JSON file format"}), 400

    except Exception as e:
        return jsonify({"error": f"Upload failed: {e}"}), 500


@annotate_bp.route("/annotation-review")
def annotation_review():
    return redirect("/annotate/review")


@annotate_bp.route("/start-annotation", methods=["POST"])
def start_annotation():
    """Start the annotation process."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    rdf_store_url = ctx.get("rdf_store_url")
    get_semantic_map = ctx.get("get_semantic_map")
    formulate_local_map = ctx.get("formulate_local_map")
    name_matcher = ctx.get("name_matcher")
    get_table_names = ctx.get("get_table_names")
    has_semantic_map = ctx.get("has_semantic_map")

    try:
        if not has_semantic_map(session_cache):
            return jsonify({"success": False, "error": "No semantic map available"})

        databases = rdf_store_service.get_databases() if rdf_store_service else []
        session_cache.databases = databases

        if not databases:
            return jsonify({"success": False, "error": "No databases available"})

        endpoint = build_repository_endpoint(
            rdf_store_url, session_cache.repo, "/statements"
        )
        map_table_names = get_table_names(session_cache)

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

        session_cache.annotation_status = {}
        total_annotated = 0

        def _annotate_database(database, data):
            """Run annotation for a single database and return (database, results, error)."""
            logger.info(f"Processing annotation for database: {database}")
            # Give each database its own temp directory so that concurrent
            # annotation runs never race on directory creation or overwrite
            # each other's generated query files (add_annotation writes into
            # <temp_dir>/generated_queries using non-atomic os.mkdir calls).
            temp_dir = os.path.join(
                tempfile.gettempdir(),
                "annotation_temp",
                secure_filename(database) or "db",
            )
            annotation_results, error = AnnotateService.execute_annotation(
                endpoint,
                database,
                data["prefixes"],
                data["variables"],
                temp_dir=temp_dir,
            )
            return database, data, annotation_results, error

        # Annotation runs are I/O-bound (they fire HTTP requests at the RDF
        # store and wait on the network), so the worker count is not tied to
        # CPU cores. Fire all databases in parallel, capped to a sane upper
        # bound to avoid overwhelming the store with too many connections.
        max_workers = min(len(annotation_data), 32)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_annotate_database, database, data): database
                for database, data in annotation_data.items()
            }
            for future in as_completed(futures):
                database, data, annotation_results, error = future.result()

                for var_name in data["variables"]:
                    status_key = f"{database}.{var_name}"
                    if annotation_results is None:
                        session_cache.annotation_status[status_key] = {
                            "success": False,
                            "error": error,
                            "database": database,
                        }
                        continue

                    result = annotation_results.get(var_name, {})
                    is_success = result.get("success", False)

                    session_cache.annotation_status[status_key] = {
                        "success": is_success,
                        "message": (
                            "Annotation completed successfully"
                            if is_success
                            else "Annotation failed"
                        ),
                        "database": database,
                        "details": result,
                    }

                    if not is_success and error:
                        session_cache.annotation_status[status_key]["error"] = error

                    total_annotated += int(is_success)

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
    return redirect("/annotate/verify")


@annotate_bp.route("/api/v1/annotation-verify-state", methods=["GET"])
def api_annotation_verify_state():
    """Return the data the SPA's AnnotationVerifyView needs."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    get_semantic_map = ctx.get("get_semantic_map")
    formulate_local_map = ctx.get("formulate_local_map")
    name_matcher = ctx.get("name_matcher")
    get_table_names = ctx.get("get_table_names")
    has_semantic_map = ctx.get("has_semantic_map")

    if not has_semantic_map(session_cache):
        return jsonify({"error": "No semantic map available"}), 400

    databases = rdf_store_service.get_databases() if rdf_store_service else []
    session_cache.databases = databases

    if not databases:
        return jsonify({"error": "No databases available"}), 400

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

    return jsonify(
        {
            "annotated_variables": annotated,
            "unannotated_variables": unannotated,
            "variable_data": variable_data,
            "annotation_status": annotation_status,
            "success_message": success_message,
        }
    )


@annotate_bp.route("/verify-annotation-ask", methods=["POST"])
def verify_annotation_ask():
    """Verify annotation using ASK query."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
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
            rdf_store_service,
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
