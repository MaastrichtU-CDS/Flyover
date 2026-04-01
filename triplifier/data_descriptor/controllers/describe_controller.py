"""
Describe controller for variable description endpoints.

This module handles HTTP requests related to describing
data variables, including type specification and categorisation.
"""

import json
import logging
from io import StringIO

import polars as pl
from flask import Blueprint, redirect, render_template, request, url_for

from services import DescribeService

logger = logging.getLogger(__name__)

describe_bp = Blueprint("describe", __name__)


def get_app_context():
    """Get application context (session_cache, rdf_store_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@describe_bp.route("/describe_landing")
def describe_landing():
    """
    Render the describe landing page.

    Checks for data existence and shows appropriate view.

    Returns:
        Rendered describe_landing.html template.
    """
    from markupsafe import Markup

    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    try:
        if rdf_store_service and rdf_store_service.check_data_exists():
            session_cache.existing_graph = True
            message = "Data uploaded successfully. You can now describe your variables."
            return render_template("describe_landing.html", message=Markup(message))
        else:
            message = """
            <div class="alert alert-warning" role="alert">
                <i class="fas fa-exclamation-triangle"></i>
                <strong>No Data Found</strong><br>
                Please complete the Ingest step first.
                <br><br>
                <a href="/ingest" class="btn btn-primary">Go to Ingest Step</a>
            </div>
            """
            return render_template("describe_landing.html", message=Markup(message))

    except Exception as e:
        message = f"""
        <div class="alert alert-danger" role="alert">
            <i class="fas fa-exclamation-triangle"></i>
            <strong>Error</strong><br>
            {e}
            <br><br>
            <a href="/ingest" class="btn btn-primary">Go to Ingest Step</a>
        </div>
        """
        return render_template("describe_landing.html", message=Markup(message))


@describe_bp.route("/describe_variables", methods=["GET", "POST"])
def describe_variables():
    """
    Render the variable description page.

    Returns:
        Rendered describe_variables.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    # Get column info by database
    columns_by_database = rdf_store_service.get_column_info_by_database()
    session_cache.databases = list(columns_by_database.keys())

    return render_template(
        "describe_variables.html",
        column_info=columns_by_database,
    )


@describe_bp.route("/units", methods=["POST"])
def retrieve_descriptive_info():
    """
    Process variable descriptions from form submission.

    Returns:
        Redirect to appropriate next page.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    session_cache.descriptive_info = {}
    session_cache.DescriptiveInfoDetails = {}

    for database in session_cache.databases:
        # Parse form data for this database
        descriptive_info = DescribeService.parse_form_data_for_database(
            request.form, database, session_cache.databases
        )
        session_cache.descriptive_info[database] = descriptive_info
        session_cache.DescriptiveInfoDetails[database] = []

        # Process each variable
        for local_var, var_info in descriptive_info.items():
            data_type = var_info.get("type", "").replace("Variable type: ", "")
            global_var = var_info.get("description", "").replace(
                "Variable description: ", ""
            )

            # Format display name
            if not global_var or global_var.strip() == "":
                display_name = f'Missing Description (or "{local_var}")'
            else:
                display_name = f'{global_var} (or "{local_var}")'

            if data_type == "categorical":
                # Get categories from the RDF store, scoped to this database
                cat_result = rdf_store_service.get_categories(local_var, database)
                if cat_result:
                    df = pl.read_csv(
                        StringIO(cat_result),
                        separator=",",
                        infer_schema_length=0,
                        null_values=[],
                        try_parse_dates=False,
                    )
                    session_cache.DescriptiveInfoDetails[database].append(
                        {display_name: df.to_dicts()}
                    )
            elif data_type == "continuous":
                session_cache.DescriptiveInfoDetails[database].append(display_name)
            else:
                # Insert equivalencies for other types
                rdf_store_service.insert_equivalencies(
                    local_var, database, descriptive_info[local_var]
                )

        # Remove empty databases
        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]

    # Redirect based on whether there are variables to detail
    if session_cache.DescriptiveInfoDetails:
        return redirect(url_for("describe.describe_variable_details"))
    else:
        return redirect(url_for("share.share_landing"))


@describe_bp.route("/describe_variable_details")
def describe_variable_details():
    """
    Render the variable details page.

    Ensures all mapped variables with relevant datatypes are included,
    regardless of whether the user visited describe_variables first.
    When a JSON-LD mapping exists, variables with categorical or continuous
    datatypes from the mapping are automatically included.

    Returns:
        Rendered describe_variable_details.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    name_matcher = ctx.get("name_matcher")

    # Ensure databases are available
    if not session_cache.databases:
        session_cache.databases = rdf_store_service.get_databases()

    # Populate DescriptiveInfoDetails from JSON-LD mapping.
    # Always run when JSON-LD exists, even if some variables were already
    # populated via the form, to include variables from pages the user
    # did not visit (e.g. due to pagination).
    if session_cache.jsonld_mapping:
        _populate_details_from_jsonld(session_cache, rdf_store_service, name_matcher)

    preselected_values = {}

    if session_cache.jsonld_mapping and session_cache.DescriptiveInfoDetails:
        preselected_values = DescribeService.get_preselected_values(
            session_cache.jsonld_mapping,
            session_cache.DescriptiveInfoDetails,
            session_cache.databases,
            name_matcher,
        )

    return render_template(
        "describe_variable_details.html",
        descriptive_info=json.dumps(session_cache.descriptive_info or {}),
        descriptive_info_details=json.dumps(session_cache.DescriptiveInfoDetails or {}),
        preselected_values=preselected_values,
    )


def _variable_exists_in_details(details_list, local_column):
    """
    Check if a variable already exists in the DescriptiveInfoDetails list.

    Args:
        details_list: List of variable detail entries.
        local_column: The local column name to check for.

    Returns:
        True if the variable already exists, False otherwise.
    """
    for item in details_list:
        if isinstance(item, str) and local_column in item:
            return True
        if isinstance(item, dict):
            for key in item:
                if local_column in key:
                    return True
    return False


def _populate_details_from_jsonld(session_cache, rdf_store_service, name_matcher):
    """
    Populate DescriptiveInfoDetails from JSON-LD mapping.

    Ensures variables with categorical or continuous datatypes from the
    JSON-LD mapping are included even if the user skipped describe_variables.

    Args:
        session_cache: The session cache object.
        rdf_store_service: RDF store service instance.
        name_matcher: Function to match database names.
    """
    mapping = session_cache.jsonld_mapping
    if not mapping or not session_cache.databases:
        return

    if not session_cache.descriptive_info:
        session_cache.descriptive_info = {}
    if not session_cache.DescriptiveInfoDetails:
        session_cache.DescriptiveInfoDetails = {}

    map_db_name = mapping.get_first_database_name()
    if map_db_name is None:
        logger.warning(
            "JSON-LD mapping has no database name, skipping details population"
        )
        return

    for database in session_cache.databases:
        if not name_matcher(map_db_name, database):
            continue

        if database not in session_cache.DescriptiveInfoDetails:
            session_cache.DescriptiveInfoDetails[database] = []
        if database not in session_cache.descriptive_info:
            session_cache.descriptive_info[database] = {}

        # Get all variables from the mapping
        for var_key in mapping.get_all_variable_keys():
            var_info = mapping.get_variable(var_key)
            if not var_info:
                continue

            data_type = getattr(var_info, "data_type", None)
            local_column = mapping.get_local_column(var_key)

            if not local_column or not data_type:
                continue

            display_name = f'{var_key.replace("_", " ").title()} (or "{local_column}")'

            if _variable_exists_in_details(
                session_cache.DescriptiveInfoDetails[database], local_column
            ):
                continue

            # Ensure descriptive_info is populated for this variable
            if local_column not in session_cache.descriptive_info[database]:
                session_cache.descriptive_info[database][local_column] = {
                    "type": f"Variable type: {data_type}",
                    "description": f"Variable description: {var_key.replace('_', ' ').title()}",
                    "comments": "Variable comment: No comment provided",
                }

            if data_type == "categorical":
                cat_result = rdf_store_service.get_categories(local_column, database)
                if cat_result:
                    try:
                        df = pl.read_csv(
                            StringIO(cat_result),
                            separator=",",
                            infer_schema_length=0,
                            null_values=[],
                            try_parse_dates=False,
                        )
                        session_cache.DescriptiveInfoDetails[database].append(
                            {display_name: df.to_dicts()}
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse categories for {local_column}: {e}"
                        )
            elif data_type == "continuous":
                session_cache.DescriptiveInfoDetails[database].append(display_name)

        # Remove empty databases
        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]


@describe_bp.route("/end", methods=["GET", "POST"])
def retrieve_detailed_descriptive_info():
    """
    Process detailed variable info (units, categories).

    Returns:
        Redirect to download page.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    for database in session_cache.databases:
        # Update descriptive info with detailed data
        if database not in session_cache.descriptive_info:
            session_cache.descriptive_info[database] = {}

        updated_info = DescribeService.process_detailed_form_data(
            request.form,
            database,
            session_cache.databases,
            session_cache.descriptive_info[database],
        )
        session_cache.descriptive_info[database] = updated_info

        # Insert equivalencies for each variable
        for variable in set(updated_info.keys()):
            rdf_store_service.insert_equivalencies(
                variable, database, updated_info.get(variable, {})
            )

    # Redirect based on whether a JSON-LD mapping is already loaded
    # If the user used a JSON-LD for describing, skip annotation_landing
    # and go directly to annotation_review
    if session_cache.jsonld_mapping:
        return redirect(url_for("annotate.annotation_review"))
    else:
        return redirect(url_for("annotate.annotation_landing"))
