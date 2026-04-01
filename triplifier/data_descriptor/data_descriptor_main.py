# Apply gevent monkey patching as early as possible
import gevent
from gevent import monkey

monkey.patch_all()

import os
import sys
import logging


from flask import Flask

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def setup_logging():
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
    child_dir = "data_descriptor"
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


session_cache = Cache()


# Register controller blueprints
from controllers import ingest_bp, describe_bp, annotate_bp, share_bp

app.register_blueprint(ingest_bp)
app.register_blueprint(describe_bp)
app.register_blueprint(annotate_bp)
app.register_blueprint(share_bp)


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
