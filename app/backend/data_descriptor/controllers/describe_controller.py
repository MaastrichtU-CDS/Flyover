"""
Describe controller for variable description endpoints.

This module handles HTTP requests related to describing
data variables, including type specification and categorisation.
"""

import copy
import json
import logging
from io import StringIO

import polars as pl
from flask import Blueprint, jsonify, redirect, request

from typing import Any

from services import DescribeService
from utils.mapping_request import parse_and_validate_mapping

logger = logging.getLogger(__name__)

describe_bp = Blueprint("describe", __name__)


def get_app_context() -> dict:
    """Get application context (session_cache, rdf_store_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@describe_bp.route("/describe_landing")
def describe_landing():
    return redirect("/describe")


@describe_bp.route("/describe_variables", methods=["GET"])
def describe_variables_get():
    return redirect("/describe/variables")


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


@describe_bp.route("/api/v1/describe-variable-details-state", methods=["GET", "POST"])
def api_describe_variable_details_state():
    """Return descriptive info, descriptive details, and preselected values.

    The describe pages work on the semantic map in the browser's IndexedDB,
    which may differ from the session's adopted map (the describe-landing
    upload never reaches the session, and another browser's map may have been
    the one that was adopted). A POST body may therefore carry the browser's
    map: it is used for THIS response only — for the details population and
    the preselected values — so the page's variables, dropdown options and
    pre-filled values all derive from the one map the user is looking at.
    It is never written to the session. Without a body mapping the session's
    own mapping is used (the previous behaviour).
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    rdf_store_service = ctx.get("rdf_store_service")
    name_matcher = ctx.get("name_matcher")

    if not session_cache.databases:
        session_cache.databases = rdf_store_service.get_databases()

    if session_cache.descriptive_info is None:
        session_cache.descriptive_info = {}
    if session_cache.DescriptiveInfoDetails is None:
        session_cache.DescriptiveInfoDetails = {}

    body = request.get_json(silent=True) or {}
    body_mapping = parse_and_validate_mapping(body.get("mapping"))
    effective_mapping = body_mapping or session_cache.jsonld_mapping

    # A browser-supplied map renders this response only: work on copies so
    # one browser's map can never leak into the shared session state. The
    # session's dicts are populated from the session's own mapping alone.
    if body_mapping is not None:
        descriptive_info = copy.deepcopy(session_cache.descriptive_info)
        details = copy.deepcopy(session_cache.DescriptiveInfoDetails)
    else:
        descriptive_info = session_cache.descriptive_info
        details = session_cache.DescriptiveInfoDetails

    if effective_mapping:
        _populate_details_from_jsonld(
            effective_mapping,
            descriptive_info,
            details,
            session_cache.databases,
            rdf_store_service,
            name_matcher,
        )

    preselected_values = {}
    if effective_mapping and details:
        preselected_values = DescribeService.get_preselected_values(
            effective_mapping,
            details,
            session_cache.databases,
            name_matcher,
        )

    return jsonify(
        {
            "descriptive_info": descriptive_info,
            "descriptive_info_details": details,
            "preselected_values": preselected_values,
            "category_options": _category_options_from_mapping(
                effective_mapping, details
            ),
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

    # `databases` is normally populated by the describe-variables-state call,
    # but guard against it being unset (e.g. if that earlier request failed)
    # so we don't crash with "'NoneType' object is not iterable".
    if not session_cache.databases:
        session_cache.databases = rdf_store_service.get_databases()

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
        return redirect("/describe/variable-details")
    else:
        return redirect("/share")


@describe_bp.route("/describe_variable_details")
def describe_variable_details():
    return redirect("/describe/variable-details")


def _variable_exists_in_details(details_list: list, local_column: str) -> bool:
    for item in details_list:
        if isinstance(item, str) and local_column in item:
            return True
        if isinstance(item, dict):
            for key in item:
                if local_column in key:
                    return True
    return False


def _category_options_from_mapping(mapping: Any, details: dict) -> dict:
    """Value-mapping options per variable display name, from the mapping the
    response is rendered on.

    A browser without a semantic map of its own (a fresh or incognito window
    viewing the session's describe state) cannot collect a variable's value
    mappings client-side. The response carries them, so the variables, the
    preselected values and the dropdown options all come from the same map.
    """
    if not mapping:
        return {}
    options = {}
    for variables in details.values():
        for variable in variables:
            if not isinstance(variable, dict):
                continue
            for var_name in variable:
                if var_name in options:
                    continue
                global_var = var_name.split(" (or")[0].lower().replace(" ", "_")
                var_info = mapping.get_variable(global_var)
                terms = getattr(var_info, "value_mappings", None) or {}
                options[var_name] = [
                    term[0].upper() + term[1:].replace("_", " ") for term in terms
                ]
    return options


def _populate_details_from_jsonld(
    mapping: Any,
    descriptive_info: dict,
    details: dict,
    databases: list,
    rdf_store_service: Any,
    name_matcher: Any,
) -> None:
    """Populate the details dicts from a JSON-LD mapping (mutates them).

    ``descriptive_info`` and ``details`` are the caller's dicts: the session's
    own when no body mapping was supplied (so the population persists, as it
    always has), or request-local copies when the browser's map was sent.
    """
    if not mapping or not databases:
        return

    map_db_name = mapping.get_first_database_name()
    if not map_db_name:
        logger.warning(
            "JSON-LD mapping has no database name, skipping details population"
        )
        return

    # The JSON-LD may reference columns the user's data doesn't actually
    # contain. Without filtering, those phantom variables would surface on
    # the details page even though the user never saw or chose them on
    # /describe/variables.
    columns_by_database = rdf_store_service.get_column_info_by_database() or {}

    for database in databases:
        if not database:
            continue
        if not name_matcher(map_db_name, database):
            continue

        if database not in details:
            details[database] = []
        if database not in descriptive_info:
            descriptive_info[database] = {}

        actual_columns = set(columns_by_database.get(database, []))

        for var_key in mapping.get_all_variable_keys():
            var_info = mapping.get_variable(var_key)
            if not var_info:
                continue

            data_type = getattr(var_info, "data_type", None)
            local_column = mapping.get_local_column(var_key)

            if not local_column or not data_type:
                continue

            if actual_columns and local_column not in actual_columns:
                continue

            display_name = f'{var_key.replace("_", " ").title()} (or "{local_column}")'

            if _variable_exists_in_details(details[database], local_column):
                continue

            if local_column not in descriptive_info[database]:
                descriptive_info[database][local_column] = {
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
                        details[database].append({display_name: df.to_dicts()})
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse categories for {local_column}: {e}"
                        )
            elif data_type == "continuous":
                details[database].append(display_name)

        if not details[database]:
            del details[database]


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

    return redirect("/annotate/review")
