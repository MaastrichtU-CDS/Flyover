"""
Application factory for the Flyover backend.

``create_app()`` builds the Flask app the way production runs it: the
controller blueprints, the Vue SPA served at the root, the refusal of
state-changing requests from other sites, response compression
(``utils/response_compression.py``), and the ``APP_CONTEXT`` the controllers read
their services and helpers from.

The gunicorn entry point (``data_descriptor_main.py``) applies gevent's
monkey patching first and then calls ``create_app()`` with the settings it
reads from the environment. Tests call it with their own settings and
replace the services and helpers through ``context``, so they drive the same
wiring production uses.
"""

import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlsplit

import gevent
from flask import Flask, abort, request, send_from_directory
from werkzeug.security import safe_join

from controllers import (
    annotate_bp,
    describe_bp,
    ingest_bp,
    share_bp,
    suggestions_bp,
)
from services.describe_service import DescribeService
from services.ingest_service import IngestService
from services.rdf_store_service import RDFStoreService
from services.suggestions import SuggestionService
from utils.response_compression import FlyoverCompress
from utils.rdf_store_url import normalise_rdf_store_base_url
from utils.session_helpers import get_semantic_map_for_annotation

# The Vite build output is copied into data_descriptor/spa/ by the Dockerfile.
SPA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spa")

STATE_CHANGING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


@dataclass(frozen=True)
class AppSettings:
    """Where the app finds the RDF store and keeps its working files.

    Attributes:
        rdf_store_url: Base URL of the RDF store.
        repo: RDF store repository name.
        root_dir: Directory the triplifier output and uploads are read from.
        child_dir: The package directory below ``root_dir``.
        upload_folder: Where uploaded annotation maps are saved.
    """

    rdf_store_url: str
    repo: str
    root_dir: str
    child_dir: str
    upload_folder: str

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None) -> "AppSettings":
        """Read the settings the Docker image passes in, or local defaults."""
        env = os.environ if environ is None else environ
        rdf_store_url = env.get("FLYOVER_RDF_STORE_URL") or env.get(
            "FLYOVER_GRAPHDB_URL"
        )
        repo = env.get("FLYOVER_REPOSITORY_NAME")
        if rdf_store_url and repo:
            # Assume it is running in Docker
            child_dir = "data_descriptor"
            return cls(
                rdf_store_url=normalise_rdf_store_base_url(rdf_store_url, repo),
                repo=repo,
                root_dir="/app/",
                child_dir=child_dir,
                upload_folder=os.path.join(child_dir, "static", "files"),
            )
        # Assume it is not running in Docker
        child_dir = "."
        return cls(
            rdf_store_url="http://localhost:7200",
            repo="userRepo",
            root_dir="",
            child_dir=child_dir,
            upload_folder=os.path.join(child_dir, "static", "files"),
        )


class SessionCache:
    """
    Session cache for storing application state.

    This class holds session-specific data for the Flyover application,
    including RDF store configuration, semantic mappings, and processing status.
    There is one instance per app, shared by every request (and every browser).

    Attributes:
        repo: RDF store repository name
        csvData: List of parsed CSV dataframes
        csvTableNames: List of table names derived from CSV filenames
        global_semantic_map: DEPRECATED - Legacy JSON semantic map (for backward compatibility)
        jsonld_mapping: JSONLDMapping object for JSON-LD format semantic maps
        existing_graph: Boolean indicating if data graph exists
        databases: List of database names from the RDF store
        descriptive_info: Variable description metadata by database
        DescriptiveInfoDetails: Detailed variable info (categories, units) by database
        StatusToDisplay: Message to display on status pages
        pk_fk_data: Primary key/foreign key relationship data
        pk_fk_status: PK/FK processing status ("processing", "success", "failed")
        cross_graph_link_data: Cross-graph linking relationship data
        cross_graph_link_status: Cross-graph processing status
        annotation_status: Annotation results by variable
        annotation_json_path: Path to uploaded annotation JSON file
        output_files: List of output files from triplification

    Removed attributes (no longer used):
        table, url, username, password, db_name, conn: PostgreSQL connection
            details - removed as PostgreSQL connections are no longer managed
            via the session cache.
        file_path, col_cursor, uploaded_file: File handling attributes -
            removed as file handling is now managed in the frontend.
    """

    def __init__(self, repo: str):
        # RDF store configuration
        self.repo = repo

        # CSV data storage
        self.csvData = None
        self.csvTableNames = None

        # Semantic mapping storage
        # DEPRECATED: global_semantic_map is deprecated in favor of jsonld_mapping.
        # Kept for backward compatibility with legacy JSON uploads.
        # Scheduled for removal in a future version.
        self.global_semantic_map = None
        self.jsonld_mapping = None  # Store JSONLDMapping object for JSON-LD format

        # Application state
        self.existing_graph = False
        self.databases = None
        self.descriptive_info = None
        self.DescriptiveInfoDetails = None
        self.StatusToDisplay = None

        # Relationship processing
        self.pk_fk_data = None
        self.pk_fk_status = None  # "processing", "success", "failed"
        self.cross_graph_link_data = None
        self.cross_graph_link_status = None

        # Annotation state
        self.annotation_status = None  # Store annotation results
        self.annotation_json_path = None  # Store path to the uploaded JSON file

        # Output files from triplification
        self.output_files = None


def build_app_context(
    settings: AppSettings, overrides: Optional[Mapping[str, Any]] = None
) -> Dict[str, Any]:
    """Build the ``APP_CONTEXT`` the controllers read.

    ``overrides`` replaces entries of the default wiring. The session cache
    and the two services are resolved first, so the default helpers act on
    the ones that end up in the context. An unknown entry is an error: it
    would otherwise sit unused next to the default it was meant to replace.
    """
    overrides = dict(overrides or {})
    session_cache = (
        overrides["session_cache"]
        if "session_cache" in overrides
        else SessionCache(settings.repo)
    )
    rdf_store_service = (
        overrides["rdf_store_service"]
        if "rdf_store_service" in overrides
        else RDFStoreService(settings.rdf_store_url, settings.repo)
    )
    suggestion_service = (
        overrides["suggestion_service"]
        if "suggestion_service" in overrides
        else SuggestionService()
    )
    rdf_store_url = settings.rdf_store_url
    repo = settings.repo
    root_dir = settings.root_dir
    child_dir = settings.child_dir

    def run_triplifier(properties_file: str) -> tuple:
        """Run triplifier and cache output files in session_cache."""
        result = IngestService().run_triplifier(
            properties_file,
            root_dir,
            child_dir,
            csv_data_list=getattr(session_cache, "csvData", None),
            csv_table_names=getattr(session_cache, "csvTableNames", None),
        )
        session_cache.output_files = result[2]
        return result[:2]

    context: Dict[str, Any] = {
        "session_cache": session_cache,
        "rdf_store_service": rdf_store_service,
        "rdf_store_url": rdf_store_url,
        "upload_folder": settings.upload_folder,
        "root_dir": root_dir,
        "child_dir": child_dir,
        "run_triplifier": run_triplifier,
        "suggestion_service": suggestion_service,
        "upload_func": lambda file_type, output_files: (
            IngestService().upload_multiple_graphs(
                root_dir, rdf_store_url, repo, output_files, data_background=False
            )
            if file_type in ("CSV", "Excel")
            else IngestService().upload_ontology_then_data(
                root_dir, rdf_store_url, repo, data_background=False
            )
        ),
        "start_background": lambda sc: [
            gevent.spawn(
                IngestService.background_pk_fk_processing, sc, rdf_store_service
            ),
            gevent.spawn(
                IngestService.background_cross_graph_processing, sc, rdf_store_service
            ),
        ],
        "handle_postgres": lambda username, password, postgres_url, postgres_db, table: (
            IngestService().handle_postgres_connection(
                username,
                password,
                postgres_url,
                postgres_db,
                table,
                root_dir,
                child_dir,
            )
        ),
        "has_semantic_map": lambda sc: DescribeService.has_semantic_map(sc),
        "formulate_local_map": lambda db: DescribeService.formulate_local_semantic_map(
            db
        ),
        "get_table_names": lambda sc: IngestService.get_table_names_from_mapping(sc),
        "name_matcher": lambda map_db, target_db: (
            RDFStoreService.graph_database_find_name_match(map_db, target_db)
        ),
        "get_semantic_map": lambda sc, database_key=None: (
            get_semantic_map_for_annotation(sc, database_key)
        ),
    }

    unknown = set(overrides) - set(context)
    if unknown:
        raise ValueError(f"Unknown APP_CONTEXT entries: {sorted(unknown)}")
    context.update(overrides)
    return context


def _refuse_requests_from_other_sites() -> None:
    """Refuse a state-changing request that a page on another site sent.

    Flyover's forms (``/upload``, ``/units``, ``/end``) carry no CSRF token,
    so without this any page the user visits could submit them to the Flyover
    on their machine. Browsers name the sending page's origin in ``Origin``
    (older ones only in ``Referer``); a request naming another host or port
    is refused with a 403. Requests with neither header (scripts, curl) do not
    come from a page and are let through. A reverse proxy in front of Flyover
    must pass the original ``Host`` header on.
    """
    if request.method not in STATE_CHANGING_METHODS:
        return
    source = request.headers.get("Origin") or request.headers.get("Referer")
    if not source:
        return
    if urlsplit(source).netloc.lower() != request.host.lower():
        abort(403)


def _register_spa(app: Flask, spa_dir: str) -> None:
    """Serve the built Vue SPA at the application root.

    In non-Docker dev the directory may not exist yet, in which case
    requests return 404.
    """

    @app.route("/")
    @app.route("/<path:subpath>")
    def serve_spa(subpath: str = ""):
        if not os.path.isdir(spa_dir):
            abort(404)
        # safe_join refuses a path that leaves the SPA folder before anything
        # touches the file system, so the answer never shows whether a file
        # outside it exists.
        asset = safe_join(spa_dir, subpath) if subpath else None
        if asset is not None and os.path.isfile(asset):
            return send_from_directory(spa_dir, subpath)
        return send_from_directory(spa_dir, "index.html")


def create_app(
    settings: Optional[AppSettings] = None,
    *,
    context: Optional[Mapping[str, Any]] = None,
    spa_dir: str = SPA_DIR,
) -> Flask:
    """Build the Flask app.

    Args:
        settings: Store location and working directories; read from the
            environment when omitted.
        context: ``APP_CONTEXT`` entries that replace the default wiring
            (tests inject their own services and helpers here).
        spa_dir: Directory holding the built Vue SPA.

    Returns:
        The configured Flask application.
    """
    settings = settings or AppSettings.from_env()

    app = Flask(__name__)
    app.config["DEBUG"] = False
    app.secret_key = "secret_key"
    app.config["UPLOAD_FOLDER"] = settings.upload_folder
    os.makedirs(settings.upload_folder, exist_ok=True)
    app.before_request(_refuse_requests_from_other_sites)
    FlyoverCompress(app)

    for blueprint in (ingest_bp, describe_bp, annotate_bp, share_bp, suggestions_bp):
        app.register_blueprint(blueprint)
    _register_spa(app, spa_dir)

    app.config["rdf_store_url"] = settings.rdf_store_url
    app.config["repo"] = settings.repo
    app.config["APP_CONTEXT"] = build_app_context(settings, context)
    return app
