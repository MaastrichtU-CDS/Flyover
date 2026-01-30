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
    """Get application context (session_cache, graphdb_url, etc.)."""
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
    graphdb_service = ctx.get("graphdb_service")

    try:
        if graphdb_service and graphdb_service.check_data_exists():
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
    graphdb_service = ctx.get("graphdb_service")

    # Get column info by database
    columns_by_database = graphdb_service.get_column_info_by_database()
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
    graphdb_service = ctx.get("graphdb_service")

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
                # Get categories from GraphDB
                cat_result = graphdb_service.get_categories(local_var)
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
                graphdb_service.insert_equivalencies(
                    local_var, database, descriptive_info[local_var]
                )

        # Remove empty databases
        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]

    # Redirect based on whether there are variables to detail
    if session_cache.DescriptiveInfoDetails:
        return redirect(url_for("describe.describe_variable_details"))
    else:
        return redirect(url_for("download.describe_downloads"))


@describe_bp.route("/describe_variable_details")
def describe_variable_details():
    """
    Render the variable details page.

    Returns:
        Rendered describe_variable_details.html template.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    name_matcher = ctx.get("name_matcher")

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


@describe_bp.route("/end", methods=["GET", "POST"])
def retrieve_detailed_descriptive_info():
    """
    Process detailed variable info (units, categories).

    Returns:
        Redirect to download page.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")

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
            graphdb_service.insert_equivalencies(
                variable, database, updated_info.get(variable, {})
            )

    return redirect(url_for("download.describe_downloads"))
