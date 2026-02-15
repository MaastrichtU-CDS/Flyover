"""
Download controller for export and download endpoints.

This module handles HTTP requests for downloading semantic maps,
ontologies, and other export operations.
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
)

logger = logging.getLogger(__name__)

download_bp = Blueprint("download", __name__)


def get_app_context():
    """Get application context (session_cache, graphdb_url, etc.)."""
    from flask import current_app

    return current_app.config.get("APP_CONTEXT", {})


@download_bp.route("/describe_downloads")
def describe_downloads():
    """
    Render the downloads page.

    Returns:
        Rendered describe_downloads.html template.
    """
    return render_template(
        "describe_downloads.html",
        graphdb_location="http://localhost:7200/",
    )


@download_bp.route("/downloadSemanticMap", methods=["GET"])
def download_semantic_map():
    """
    Download the semantic map in JSON-LD or legacy JSON format.

    Returns:
        Response with semantic map file.
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    formulate_local_map = ctx.get("formulate_local_map")

    try:
        # Prefer JSON-LD format
        if session_cache.jsonld_mapping is not None:
            mapping_dict = session_cache.jsonld_mapping.to_dict()
            filename = "semantic_mapping.jsonld"

            return Response(
                json.dumps(mapping_dict, indent=2, ensure_ascii=False),
                mimetype="application/ld+json",
                headers={"Content-Disposition": f"attachment;filename={filename}"},
            )

        # Fall back to legacy format
        if len(session_cache.databases) > 1:
            return _download_multiple_semantic_maps(session_cache, formulate_local_map)
        else:
            return _download_single_semantic_map(session_cache, formulate_local_map)

    except Exception as e:
        abort(500, description=f"Error processing semantic map: {e}")


def _download_multiple_semantic_maps(session_cache, formulate_local_map):
    """Create zip file with multiple semantic maps."""
    zip_filename = "local_semantic_maps.zip"

    for database in session_cache.databases:
        filename = f"local_semantic_map_{database}.json"
        with zipfile.ZipFile(zip_filename, "a") as zipf:
            modified_map = formulate_local_map(database)
            zipf.writestr(filename, json.dumps(modified_map, indent=4))

    @after_this_request
    def remove_file(response):
        try:
            os.remove(zip_filename)
        except Exception as error:
            logger.error(f"Error removing zip file: {error}")
        return response

    with open(zip_filename, "rb") as f:
        return Response(
            f.read(),
            mimetype="application/zip",
            headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
        )


def _download_single_semantic_map(session_cache, formulate_local_map):
    """Download single semantic map."""
    database = session_cache.databases[0]
    filename = f"local_semantic_map_{database}.json"

    try:
        modified_map = formulate_local_map(database)
        return Response(
            json.dumps(modified_map, indent=4),
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment;filename={filename}"},
        )
    except Exception as e:
        abort(500, description=f"Error processing semantic map: {e}")


@download_bp.route("/downloadOntology", methods=["GET"])
def download_ontology():
    """
    Download ontology files from GraphDB.

    Returns:
        Response with ontology file(s).
    """
    ctx = get_app_context()
    session_cache = ctx.get("session_cache")
    graphdb_service = ctx.get("graphdb_service")
    named_graph = "http://ontology.local/"

    try:
        # Determine databases to process
        databases = []
        if session_cache.databases and len(session_cache.databases) > 1:
            databases = session_cache.databases
        else:
            databases = graphdb_service.repository.get_ontology_graphs()

        # Multiple databases -> zip file
        if len(databases) > 1:
            return _download_multiple_ontologies(
                databases, graphdb_service, named_graph
            )
        else:
            return _download_single_ontology(databases, graphdb_service, named_graph)

    except Exception as e:
        abort(500, description=f"Error downloading ontology: {e}")


def _download_multiple_ontologies(databases, graphdb_service, named_graph):
    """Create zip file with multiple ontologies."""
    zip_filename = "local_ontologies.zip"
    files_added = 0

    with zipfile.ZipFile(zip_filename, "w") as zipf:
        for database in databases:
            table_graph = f"{named_graph}{database}/"
            ontology_filename = f"local_ontology_{database}.nt"

            content, status = graphdb_service.repository.download_ontology(table_graph)

            if status == 200 and content and content.strip():
                zipf.writestr(ontology_filename, content)
                files_added += 1
            else:
                logger.warning(f"No ontology data for graph: {table_graph}")

    if files_added == 0:
        if os.path.exists(zip_filename):
            os.remove(zip_filename)
        abort(404, description="No ontology data found.")

    @after_this_request
    def remove_file(response):
        try:
            os.remove(zip_filename)
        except Exception as error:
            logger.error(f"Error removing zip file: {error}")
        return response

    with open(zip_filename, "rb") as f:
        return Response(
            f.read(),
            mimetype="application/zip",
            headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
        )


def _download_single_ontology(databases, graphdb_service, named_graph):
    """Download single ontology."""
    if len(databases) == 1:
        database = databases[0]
        ontology_graph = f"{named_graph}{database}/"
        filename = f"local_ontology_{database}.nt"
    else:
        ontology_graph = named_graph
        filename = "local_ontology.nt"

    content, status = graphdb_service.repository.download_ontology(ontology_graph)

    if status == 200:
        return Response(
            content,
            mimetype="application/n-triples",
            headers={"Content-Disposition": f"attachment;filename={filename}"},
        )
    else:
        abort(500, description=f"Failed to download ontology. Status: {status}")


@download_bp.route("/favicon.ico")
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


@download_bp.route("/data_descriptor/assets/<path:filename>")
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
