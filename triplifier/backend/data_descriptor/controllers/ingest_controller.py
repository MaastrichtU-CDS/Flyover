"""
Ingest controller for data ingestion endpoints.

This module handles HTTP requests related to data ingestion,
including file uploads, semantic map uploads, and data processing.
"""

import json
import logging

from flask import Blueprint, jsonify, redirect, request

from services import IngestService, RDFStoreService
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

        # Clean the mapping of orphan columns right at the start
        # This ensures faulty mappings don't persist anywhere
        try:
            ctx = get_app_context()
            rdf_store_service = ctx.get("rdf_store_service")
            if rdf_store_service is not None:
                columns_by_database = (
                    rdf_store_service.get_column_info_by_database() or {}
                )

                # Always try to populate loaded_databases from session cache or RDF store
                loaded_databases = []
                if session_cache and hasattr(session_cache, "databases"):
                    loaded_databases = session_cache.databases or []
                else:
                    # Populate session_cache.databases if not already set
                    try:
                        loaded_databases = rdf_store_service.get_databases() or []
                        if session_cache:
                            session_cache.databases = loaded_databases
                    except Exception as e:
                        logger.warning(f"Failed to fetch databases from RDF store: {e}")
                        loaded_databases = []

                if columns_by_database:
                    _, _, orphan_columns = _collect_orphan_column_issues(
                        mapping_data, columns_by_database, loaded_databases
                    )
                    # Clean the mapping immediately, before validation
                    if orphan_columns:
                        mapping_data = _remove_orphan_columns_from_mapping(
                            mapping_data, orphan_columns
                        )
        except Exception as e:
            logger.warning(f"Orphan column check skipped: {e}")

        # Now validate the cleaned mapping
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
                "cleaned_mapping": mapping_data,
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
                "redirect_url": "/annotate/review",
                "statistics": result.statistics,
                "cleaned_mapping": mapping_data,
            }
        )

    except json.JSONDecodeError as e:
        return jsonify({"error": f"Invalid JSON data: {e.msg}"}), 400
    except Exception as e:
        logger.error(f"Error processing IndexedDB semantic map: {e}")
        return jsonify({"error": f"Unexpected error: {e}"}), 400


def _collect_orphan_column_issues(
    mapping_data, columns_by_database, loaded_databases=None
):
    """Find localColumn references in the mapping that aren't present in the CSV.

    Only validates databases that are currently loaded (in loaded_databases).
    For unloaded databases, validation is skipped entirely.
    For loaded databases, returns warnings (not errors) for column mismatches.

    Args:
        mapping_data: The JSON-LD mapping data
        columns_by_database: Dict mapping database names to lists of column names
        loaded_databases: List of currently loaded database names (optional)

    Returns a tuple of (warnings, databases_checked, orphan_columns):
        - warnings: List of UI-formatted warning dicts for column mismatches
        - databases_checked: List of database names that were validated
        - orphan_columns: List of (db_key, table_key, col_key) tuples for columns to remove
    """
    if not columns_by_database:
        return [], [], []

    # Build a set of all available columns across all databases
    available_columns = set()
    for cols in columns_by_database.values():
        for c in cols or []:
            available_columns.add(c)

    warnings = []
    databases_checked = []
    orphan_columns = []
    orphan_columns_by_db = {}
    databases = (mapping_data or {}).get("databases") or {}
    if not isinstance(databases, dict):
        return [], [], []

    for db_key, db_data in databases.items():
        if not isinstance(db_data, dict):
            continue

        # Get the database name - use 'name' field or fall back to the key
        db_name = db_data.get("name", db_key)
        if not db_name:
            continue

        tables = db_data.get("tables") or {}
        if not isinstance(tables, dict):
            tables = {}

        # The RDF store groups columns by the CSV file name, which in the
        # mapping corresponds to a table's `sourceFile` (e.g.
        # "synthetic_dutch_150"), not the database `name`
        # ("Centre B Electronic Health Records") or the database key
        # ("centre_b_ehr"). So when deciding whether a mapping database is
        # loaded — and which columns are available — we must also match on the
        # table sourceFiles, mirroring the frontend matching logic.
        source_files = []
        for table_data in tables.values():
            if isinstance(table_data, dict):
                source_file = table_data.get("sourceFile")
                if source_file:
                    source_files.append(source_file)

        # Candidate names that may correspond to a loaded database.
        candidate_names = [n for n in [db_name, db_key, *source_files] if n]

        # Check if this database is loaded
        is_loaded = False
        if loaded_databases:
            for loaded_db in loaded_databases:
                if any(
                    RDFStoreService.graph_database_find_name_match(name, str(loaded_db))
                    for name in candidate_names
                ):
                    is_loaded = True
                    break
        elif columns_by_database:
            # If no loaded_databases specified but we have columns_by_database,
            # check if this database has columns in the store
            for existing_db in columns_by_database.keys():
                if any(
                    RDFStoreService.graph_database_find_name_match(
                        name, str(existing_db)
                    )
                    for name in candidate_names
                ):
                    is_loaded = True
                    break

        # Skip unloaded databases entirely (no validation, no warnings)
        if not is_loaded:
            continue

        # This database is loaded - validate its columns
        databases_checked.append(db_name)
        if not tables:
            continue

        for table_key, table_data in tables.items():
            if not isinstance(table_data, dict):
                continue
            columns = table_data.get("columns") or {}
            if not isinstance(columns, dict):
                continue

            # Resolve the columns available for this specific table. Match on
            # the table sourceFile first (the RDF store key), then fall back to
            # the database name/key for older mappings without a sourceFile.
            table_source_file = table_data.get("sourceFile")
            table_match_names = [n for n in [table_source_file, db_name, db_key] if n]
            table_available_columns = set()
            for loaded_db_name, cols in columns_by_database.items():
                if any(
                    RDFStoreService.graph_database_find_name_match(
                        name, str(loaded_db_name)
                    )
                    for name in table_match_names
                ):
                    for c in cols or []:
                        table_available_columns.add(c)

            for col_key, col_data in columns.items():
                if not isinstance(col_data, dict):
                    continue
                local_column = col_data.get("localColumn")
                if not local_column:
                    continue
                # Only check against columns available for this specific table
                if local_column in table_available_columns:
                    continue
                # Track orphan column by database
                if db_name not in orphan_columns_by_db:
                    orphan_columns_by_db[db_name] = []
                orphan_columns_by_db[db_name].append(local_column)
                # Track this column for removal
                orphan_columns.append(
                    (db_key, table_key, col_key, col_data.get("mapsTo"))
                )

    # Generate a single compact warning for all orphan columns
    if orphan_columns_by_db:
        # Build database info for the message
        db_info = []
        all_orphan_columns = []
        for db_name, cols in orphan_columns_by_db.items():
            db_info.append(f"{db_name}: {', '.join(cols)}")
            all_orphan_columns.extend(cols)

        db_list = "; ".join(db_info)
        db_count = len(orphan_columns_by_db)
        warnings.append(
            {
                "severity": "warning",
                "message": f"{len(all_orphan_columns)} column mapping(s) in {db_count} database(s) ({db_list}) were faulty.<br>You can still proceed, but these mappings will be removed.",
                "values": all_orphan_columns,
            }
        )

    return warnings, databases_checked, orphan_columns


def _remove_orphan_columns_from_mapping(mapping_data, orphan_columns):
    """Remove orphan column entries from the mapping data.

    Removes only the specific column entries (variables) that have orphan localColumn
    references (columns that don't exist in the loaded CSV data). This ensures the
    faulty mappings (including the mapsTo section for those columns) do not persist
    in IndexedDB.

    Also removes empty tables and databases that result from column removal to prevent
    validation errors about empty dictionaries.

    Args:
        mapping_data: The JSON-LD mapping data
        orphan_columns: List of (db_key, table_key, col_key, maps_to) tuples for columns to remove

    Returns:
        The modified mapping_data with orphan column entries removed
    """
    if not orphan_columns:
        return mapping_data

    databases = mapping_data.get("databases") or {}
    if not isinstance(databases, dict):
        return mapping_data

    import copy
    cleaned_mapping = copy.deepcopy(mapping_data)

    # First pass: remove all the specified columns
    columns_removed = set()  # Track (db_key, table_key) for tables that had columns removed
    
    for db_key, table_key, col_key, maps_to in orphan_columns:
        db_data = cleaned_mapping.get("databases", {}).get(db_key)
        if not isinstance(db_data, dict):
            continue

        table_data = db_data.get("tables", {}).get(table_key)
        if not isinstance(table_data, dict):
            continue

        columns = table_data.get("columns")
        if not isinstance(columns, dict):
            continue

        if col_key in columns:
            del columns[col_key]
            columns_removed.add((db_key, table_key))

    # Second pass: remove empty tables
    empty_databases = set()
    for db_key, table_key in columns_removed:
        db_data = cleaned_mapping.get("databases", {}).get(db_key)
        if not isinstance(db_data, dict):
            continue

        tables = db_data.get("tables", {})
        if not isinstance(tables, dict):
            continue

        if table_key in tables and not tables[table_key].get("columns"):
            del tables[table_key]
            if not tables:  # Database is now empty
                empty_databases.add(db_key)

    # Remove empty databases
    databases_cleaned = cleaned_mapping.get("databases", {})
    for db_key in empty_databases:
        if db_key in databases_cleaned:
            del databases_cleaned[db_key]

    return cleaned_mapping


@ingest_bp.route("/api/v1/validate-mapping", methods=["POST"])
def validate_mapping():
    """Validate a JSON-LD mapping against the schema and the loaded CSV columns.

    Pure validation — does not mutate the session cache. Returns schema errors
    (blocking) plus column warnings for loaded databases (non-blocking).
    Unloaded databases in the mapping are skipped silently.

    When column warnings are found, returns a cleaned version of the mapping
    with the problematic columns removed.
    """
    try:
        mapping_data = request.get_json(silent=True)
    except Exception as e:
        return jsonify({"error": f"Invalid JSON body: {e}"}), 400

    if not mapping_data or not isinstance(mapping_data, dict):
        return jsonify({"error": "No mapping data provided"}), 400

    # Accept either the mapping object directly or wrapped as {"mapping": ...}
    if "mapping" in mapping_data and isinstance(mapping_data["mapping"], dict):
        mapping_data = mapping_data["mapping"]

    validator = MappingValidator()
    result = validator.validate(mapping_data)
    schema_errors = validator.format_errors_for_ui(result)

    csv_checked = False
    validation_warnings = []
    databases_checked = []
    cleaned_mapping = None

    try:
        ctx = get_app_context()
        rdf_store_service = ctx.get("rdf_store_service")
        session_cache = ctx.get("session_cache")
        if rdf_store_service is not None:
            columns_by_database = rdf_store_service.get_column_info_by_database() or {}

            # Always try to populate loaded_databases from session cache or RDF store
            loaded_databases = []
            if session_cache and hasattr(session_cache, "databases"):
                loaded_databases = session_cache.databases or []
            else:
                # Populate session_cache.databases if not already set
                try:
                    loaded_databases = rdf_store_service.get_databases() or []
                    if session_cache:
                        session_cache.databases = loaded_databases
                except Exception as e:
                    logger.warning(f"Failed to fetch databases from RDF store: {e}")
                    loaded_databases = []

            if columns_by_database:
                csv_checked = True

                validation_warnings, databases_checked, orphan_columns = (
                    _collect_orphan_column_issues(
                        mapping_data, columns_by_database, loaded_databases
                    )
                )

                # If there are orphan columns, clean the mapping
                if orphan_columns:
                    cleaned_mapping = _remove_orphan_columns_from_mapping(
                        mapping_data, orphan_columns
                    )
                # If there are warnings but no orphan_columns (shouldn't happen, but safe fallback)
                elif validation_warnings:
                    # This shouldn't occur, but if it does, ensure we have a cleaned mapping
                    cleaned_mapping = mapping_data
    except Exception as e:
        # CSV cross-check is best-effort — never let it fail the schema check.
        logger.warning(f"CSV cross-check skipped: {e}")

    # Schema errors are blocking, column warnings are non-blocking
    # valid is True if there are no schema errors, regardless of warnings
    is_valid = result.is_valid

    return jsonify(
        {
            "valid": is_valid,
            "validation_errors": schema_errors,
            "validation_warnings": validation_warnings,
            "statistics": result.statistics,
            "csv_checked": csv_checked,
            "databases_checked": databases_checked,
            "cleaned_mapping": cleaned_mapping,
        }
    )


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
            return redirect(f"/ingest?error={error}")

        separator = request.form.get("csv_separator_sign", ",")
        decimal = request.form.get("csv_decimal_sign", ".")

        dataframes, table_names, error = IngestService.parse_csv_files(
            csv_files, separator, decimal
        )

        if error:
            return redirect(f"/ingest?error={error}")

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

        return redirect("/describe")
    else:
        return redirect(f"/ingest?error=Error: {message}")


@ingest_bp.route("/data-submission")
def data_submission():
    return redirect("/describe")


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
