# Apply gevent monkey patching as early as possible
import gevent
from gevent import monkey

monkey.patch_all()

import os
import sys
import logging


from flask import Flask, send_from_directory, abort

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def setup_logging() -> None:
    """Setup centralised logging with timestamp format"""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
        handlers=[logging.StreamHandler()],
    )


# Initialize logging immediately
setup_logging()
logger = logging.getLogger(__name__)

from services.ingest_service import (
    IngestService,
)
from services.describe_service import DescribeService
from services.llm import LLMConfig, LLMSuggestionService, OllamaProvider
from services.rdf_store_service import RDFStoreService
from utils.session_helpers import (
    get_semantic_map_for_annotation,
)

app = Flask(__name__)

if (
    os.getenv("FLYOVER_RDF_STORE_URL") or os.getenv("FLYOVER_GRAPHDB_URL")
) and os.getenv("FLYOVER_REPOSITORY_NAME"):
    # Assume it is running in Docker
    rdf_store_url = os.getenv("FLYOVER_RDF_STORE_URL") or os.getenv(
        "FLYOVER_GRAPHDB_URL"
    )
    repo = os.getenv("FLYOVER_REPOSITORY_NAME")
    app.config["DEBUG"] = False
    root_dir = "/app/"
    child_dir = "flyover"
else:
    # Assume it is not running in Docker
    rdf_store_url = "http://localhost:7200"
    repo = "userRepo"
    app.config["DEBUG"] = False
    root_dir = ""
    child_dir = "."

# Initialize RDF store service for use in route handlers
rdf_store_service = RDFStoreService(rdf_store_url, repo)

app.secret_key = "secret_key"
app.config["UPLOAD_FOLDER"] = os.path.join(child_dir, "static", "files")
if not os.path.exists(app.config["UPLOAD_FOLDER"]):
    os.makedirs(app.config["UPLOAD_FOLDER"])


class Cache:
    """
    Session cache for storing application state.

    This class holds session-specific data for the Flyover application,
    including RDF store configuration, semantic mappings, and processing status.

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

    def __init__(self):
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

        # LLM mapping suggestion jobs, keyed by phase ("variables"/"values")
        self.llm_jobs = None


session_cache = Cache()

# LLM mapping suggestion services (feature-flagged via FLYOVER_LLM_ENABLED /
# FLYOVER_OLLAMA_HOST; disabled means zero background work and no LLM UI)
llm_config = LLMConfig.from_env()
llm_provider = OllamaProvider(
    llm_config.base_url,
    llm_config.model,
    fallback_models=llm_config.fallback_models,
    read_timeout=llm_config.request_timeout,
)
llm_service = LLMSuggestionService(llm_config, llm_provider)

def _warm_up_llm_model() -> None:
    """Warm the configured LLM provider up in the background at boot."""
    try:
        model = llm_provider.ensure_ready()
        logger.info("LLM provider ready (%s): %s", llm_config.provider, model)
    except Exception as exc:
        logger.warning("LLM warm-up failed (feature stays available): %s", exc)


def _maybe_start_llm_variable_job(sc) -> None:
    """Start the variable suggestion job, swallowing every failure.

    LLM trouble must never break the ingest/describe flow, so this wrapper
    is what other controllers call.
    """
    try:
        result = llm_service.start_variable_job(sc, rdf_store_service)
        logger.info("LLM variable suggestion job: %s", result)
    except Exception as exc:
        logger.warning("LLM variable suggestion job failed to start: %s", exc)


def _maybe_start_llm_value_job(sc) -> None:
    """Start the value suggestion job, swallowing every failure."""
    try:
        result = llm_service.start_value_job(sc)
        logger.info("LLM value suggestion job: %s", result)
    except Exception as exc:
        logger.warning("LLM value suggestion job failed to start: %s", exc)


if llm_config.enabled:
    gevent.spawn(_warm_up_llm_model)


# Register controller blueprints
from controllers import ingest_bp, describe_bp, annotate_bp, share_bp, llm_bp

app.register_blueprint(ingest_bp)
app.register_blueprint(describe_bp)
app.register_blueprint(annotate_bp)
app.register_blueprint(share_bp)
app.register_blueprint(llm_bp)


# Serve the built Vue SPA at the application root. The Vite build output is
# copied into flyover/spa/ by the Dockerfile; in non-Docker dev the
# directory may not exist yet, in which case requests return 404.
SPA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spa")

# SPA routes that the Vue Router owns. Listed explicitly so the catch-all
# only matches paths the SPA actually claims — keeping unknown URLs as real
# 404s rather than silently rendering the index.
_SPA_ROUTES = (
    "ingest",
    "describe",
    "describe/variables",
    "describe/variable-details",
    "annotate",
    "annotate/review",
    "annotate/verify",
    "share",
    "share/mock",
    "share/publish",
)


@app.route("/")
def serve_spa_root():
    if not os.path.isdir(SPA_DIR):
        abort(404)
    return send_from_directory(SPA_DIR, "index.html")


@app.route("/assets/<path:subpath>")
def serve_spa_assets(subpath: str):
    if not os.path.isdir(SPA_DIR):
        abort(404)
    assets_dir = os.path.join(SPA_DIR, "assets")
    return send_from_directory(assets_dir, subpath)


for _route in _SPA_ROUTES:
    app.add_url_rule(
        f"/{_route}",
        endpoint=f"spa_{_route.replace('/', '_')}",
        view_func=lambda: (
            send_from_directory(SPA_DIR, "index.html")
            if os.path.isdir(SPA_DIR)
            else abort(404)
        ),
    )


# Set up application config
app.config["rdf_store_url"] = rdf_store_url
app.config["repo"] = repo


def _run_triplifier_and_cache(properties_file: str) -> tuple:
    """Run triplifier and cache output files in session_cache."""
    result = IngestService().run_triplifier(
        properties_file,
        root_dir,
        child_dir,
        csv_data_list=(
            session_cache.csvData if hasattr(session_cache, "csvData") else None
        ),
        csv_table_names=(
            session_cache.csvTableNames
            if hasattr(session_cache, "csvTableNames")
            else None
        ),
    )
    session_cache.output_files = result[2]
    return result[:2]


# Set up the application context with required functions and services
app.config["APP_CONTEXT"] = {
    "session_cache": session_cache,
    "rdf_store_service": rdf_store_service,
    "rdf_store_url": rdf_store_url,
    "upload_folder": app.config["UPLOAD_FOLDER"],
    "root_dir": root_dir,
    "child_dir": child_dir,
    "run_triplifier": _run_triplifier_and_cache,
    "upload_func": lambda file_type, output_files: (
        IngestService().upload_multiple_graphs(
            root_dir, rdf_store_url, repo, output_files, data_background=False
        )
        if file_type == "CSV"
        else IngestService().upload_ontology_then_data(
            root_dir, rdf_store_url, repo, data_background=False
        )
    ),
    "start_background": lambda sc: [
        gevent.spawn(IngestService.background_pk_fk_processing, sc, rdf_store_service),
        gevent.spawn(
            IngestService.background_cross_graph_processing, sc, rdf_store_service
        ),
    ],
    "handle_postgres": lambda username, password, postgres_url, postgres_db, table: (
        IngestService().handle_postgres_connection(
            username, password, postgres_url, postgres_db, table, root_dir, child_dir
        )
    ),
    "has_semantic_map": lambda sc: DescribeService.has_semantic_map(sc),
    "formulate_local_map": lambda db: DescribeService.formulate_local_semantic_map(db),
    "get_table_names": lambda sc: IngestService.get_table_names_from_mapping(sc),
    "name_matcher": lambda map_db, target_db: RDFStoreService.graph_database_find_name_match(
        map_db, target_db
    ),
    "get_semantic_map": lambda sc, database_key=None: get_semantic_map_for_annotation(
        sc, database_key
    ),
    "llm_service": llm_service,
    "llm_config": llm_config,
    "maybe_start_llm_variable_job": _maybe_start_llm_variable_job,
    "maybe_start_llm_value_job": _maybe_start_llm_value_job,
}

if __name__ == "__main__":
    # Use 0.0.0.0 in Docker (safe within container network), 127.0.0.1 for local dev
    is_docker = (
        os.getenv("FLYOVER_RDF_STORE_URL") or os.getenv("FLYOVER_GRAPHDB_URL")
    ) is not None
    default_host = "0.0.0.0" if is_docker else "127.0.0.1"

    host = os.getenv("FLASK_HOST", default_host)
    port = int(os.getenv("FLASK_PORT", "5000"))

    app.run(host=host, port=port)
