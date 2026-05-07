"""
Describe controller for variable description endpoints.

This module handles HTTP requests related to describing
data variables, including type specification and categorisation.
"""

import json
import logging
from io import StringIO

import polars as pl
from flask import Blueprint, jsonify, redirect, request

from typing import Any

from services import DescribeService

logger = logging.getLogger(__name__)

describe_bp = Blueprint("describe", __name__)


def get_app_context() -> dict:
    """Get application context (session_cache, rdf_store_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@describe_bp.route("/describe_landing")
def describe_landing():
    return redirect("/app/describe")


@describe_bp.route("/describe_variables", methods=["GET"])
def describe_variables_get():
    return redirect("/app/describe/variables")


@describe_bp.route("/api/v1/describe-variables-state", methods=["GET"])
def api_describe_variables_state():
    """Return column info per database (the JSON version of the data the
    Jinja describe_variables page used to receive via render_template)."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    columns_by_database = (
        rdf_store_service.get_column_info_by_database() if rdf_store_service else {}
    )
    session_cache.databases = list(columns_by_database.keys())
    return jsonify({"column_info": columns_by_database})


@describe_bp.route("/api/v1/describe-variable-details-state", methods=["GET"])
def api_describe_variable_details_state():
    """Return descriptive info, descriptive details, and preselected values."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    name_matcher = ctx.get("name_matcher")

    if not session_cache.databases:
        session_cache.databases = rdf_store_service.get_databases()

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

    return jsonify(
        {
            "descriptive_info": session_cache.descriptive_info or {},
            "descriptive_info_details": session_cache.DescriptiveInfoDetails or {},
            "preselected_values": preselected_values,
        }
    )


@describe_bp.route("/units", methods=["POST"])
def retrieve_descriptive_info():
    """Process variable descriptions from form submission."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    session_cache.descriptive_info = {}
    session_cache.DescriptiveInfoDetails = {}

    for database in session_cache.databases:
        if not database:
            continue
        descriptive_info = DescribeService.parse_form_data_for_database(
            request.form, database, session_cache.databases
        )
        session_cache.descriptive_info[database] = descriptive_info
        session_cache.DescriptiveInfoDetails[database] = []

        for local_var, var_info in descriptive_info.items():
            data_type = var_info.get("type", "").replace("Variable type: ", "")
            global_var = var_info.get("description", "").replace(
                "Variable description: ", ""
            )

            if not global_var or global_var.strip() == "":
                display_name = f'Missing Description (or "{local_var}")'
            else:
                display_name = f'{global_var} (or "{local_var}")'

            if data_type == "categorical":
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
                rdf_store_service.insert_equivalencies(
                    local_var, database, descriptive_info[local_var]
                )

        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]

    if session_cache.DescriptiveInfoDetails:
        return redirect("/app/describe/variable-details")
    else:
        return redirect("/app/share")


@describe_bp.route("/describe_variable_details")
def describe_variable_details():
    return redirect("/app/describe/variable-details")


def _variable_exists_in_details(details_list: list, local_column: str) -> bool:
    for item in details_list:
        if isinstance(item, str) and local_column in item:
            return True
        if isinstance(item, dict):
            for key in item:
                if local_column in key:
                    return True
    return False


def _populate_details_from_jsonld(
    session_cache: Any, rdf_store_service: Any, name_matcher: Any
) -> None:
    """Populate DescriptiveInfoDetails from JSON-LD mapping."""
    mapping = session_cache.jsonld_mapping
    if not mapping or not session_cache.databases:
        return

    if not session_cache.descriptive_info:
        session_cache.descriptive_info = {}
    if not session_cache.DescriptiveInfoDetails:
        session_cache.DescriptiveInfoDetails = {}

    map_db_name = mapping.get_first_database_name()
    if not map_db_name:
        logger.warning(
            "JSON-LD mapping has no database name, skipping details population"
        )
        return

    for database in session_cache.databases:
        if not database:
            continue
        if not name_matcher(map_db_name, database):
            continue

        if database not in session_cache.DescriptiveInfoDetails:
            session_cache.DescriptiveInfoDetails[database] = []
        if database not in session_cache.descriptive_info:
            session_cache.descriptive_info[database] = {}

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

        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]


@describe_bp.route("/end", methods=["GET", "POST"])
def retrieve_detailed_descriptive_info():
    """Process detailed variable info (units, categories)."""
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")

    for database in session_cache.databases:
        if database not in session_cache.descriptive_info:
            session_cache.descriptive_info[database] = {}

        updated_info = DescribeService.process_detailed_form_data(
            request.form,
            database,
            session_cache.databases,
            session_cache.descriptive_info[database],
        )
        session_cache.descriptive_info[database] = updated_info

        for variable in set(updated_info.keys()):
            rdf_store_service.insert_equivalencies(
                variable, database, updated_info.get(variable, {})
            )

    return redirect("/app/annotate/review")
