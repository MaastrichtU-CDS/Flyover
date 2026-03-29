# Apply gevent monkey patching as early as possible
import gevent
from gevent import monkey

monkey.patch_all()

import copy
import json
import os
import re
import zipfile
import time
import sys
import logging
import requests

import polars as pl

from io import StringIO
from markupsafe import Markup
from psycopg2 import connect
from werkzeug.utils import secure_filename

from flask import (
    abort,
    after_this_request,
    Flask,
    redirect,
    render_template,
    request,
    flash,
    Response,
    url_for,
    send_from_directory,
    jsonify,
)

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
from services.graphdb_service import GraphDBService
from utils.data_preprocessing import (
    preprocess_dataframe,
    read_csv_with_encoding_detection,
    sanitise_table_name,
)
from utils.session_helpers import (
    check_any_data_graph_exists,
    graph_database_ensure_backend_initialisation,
    graph_database_find_matching,
    process_variable_for_annotation,
    get_semantic_map_for_annotation,
    has_semantic_map,
    COLUMN_INFO_QUERY,
    DATABASE_NAME_PATTERN,
)

from annotation_helper.src.miscellaneous import add_annotation
from validation import MappingValidator
from loaders import JSONLDMapping

app = Flask(__name__)

if os.getenv("FLYOVER_GRAPHDB_URL") and os.getenv("FLYOVER_REPOSITORY_NAME"):
    # Assume it is running in Docker
    graphdb_url = os.getenv("FLYOVER_GRAPHDB_URL")
    repo = os.getenv("FLYOVER_REPOSITORY_NAME")
    app.config["DEBUG"] = False
    root_dir = "/app/"
    child_dir = "data_descriptor"
else:
    # Assume it is not running in Docker
    graphdb_url = "http://localhost:7200"
    repo = "userRepo"
    app.config["DEBUG"] = False
    root_dir = ""
    child_dir = "."

# Initialize GraphDB service for use in route handlers
graphdb_service = GraphDBService(graphdb_url, repo)

app.secret_key = "secret_key"
app.config["UPLOAD_FOLDER"] = os.path.join(child_dir, "static", "files")
if not os.path.exists(app.config["UPLOAD_FOLDER"]):
    os.makedirs(app.config["UPLOAD_FOLDER"])


class Cache:
    """
    Session cache for storing application state.

    This class holds session-specific data for the Flyover application,
    including database connections, semantic mappings, and processing status.

    Attributes:
        repo: GraphDB repository name
        table: PostgreSQL table name (for SQL data sources)
        url: PostgreSQL URL
        username: PostgreSQL username
        password: PostgreSQL password
        db_name: PostgreSQL database name
        conn: PostgreSQL connection object
        csvData: List of parsed CSV dataframes
        csvTableNames: List of table names derived from CSV filenames
        global_semantic_map: DEPRECATED - Legacy JSON semantic map (for backward compatibility)
        jsonld_mapping: JSONLDMapping object for JSON-LD format semantic maps
        existing_graph: Boolean indicating if data graph exists
        databases: List of database names from GraphDB
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
    """

    def __init__(self):
        # GraphDB configuration
        self.repo = repo

        # PostgreSQL connection details
        self.table = None
        self.url = None
        self.username = None
        self.password = None
        self.db_name = None
        self.conn = None

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





























def allowed_file(filename, allowed_extensions):
    """
    This function checks if the uploaded file has an allowed extension.

    Parameters:
    filename (str): The name of the file to be checked.
    allowed_extensions (set): A set of strings representing the allowed file extensions.

    Returns:
    bool: True if the file has an allowed extension, False otherwise.
    """
    return "." in filename and filename.rsplit(".", 1)[1].lower() in allowed_extensions



def formulate_local_semantic_map(database):
    """
    This function modifies the global semantic map by updating local definitions
    and setting unmapped variables to null. It also includes datatype information.

    Parameters:
    database (str): The name of the database for which the local semantic map is to be formulated.

    Returns:
    dict: A dictionary representing the modified semantic map with proper null handling.
    """
    # Create a deep copy of the global semantic map
    modified_semantic_map = copy.deepcopy(session_cache.global_semantic_map)

    # Update the 'database_name' field in the semantic map
    if isinstance(modified_semantic_map.get("database_name"), str):
        modified_semantic_map["database_name"] = database
    else:
        modified_semantic_map.update({"database_name": database})

    # Reset all local_definitions to null and datatypes to empty string
    # This ensures that unmapped fields are properly cleared
    for variable_name, variable_info in modified_semantic_map["variable_info"].items():
        modified_semantic_map["variable_info"][variable_name]["local_definition"] = None

        # Reset all local_terms in value_mapping to null
        if (
            "value_mapping" in variable_info
            and "terms" in variable_info["value_mapping"]
        ):
            for term_key in variable_info["value_mapping"]["terms"]:
                modified_semantic_map["variable_info"][variable_name]["value_mapping"][
                    "terms"
                ][term_key]["local_term"] = None

    # Process only the variables that are filled in the UI
    # Process local definitions and update the existing semantic map
    used_global_variables = {}  # Track usage for duplicate handling

    # Check if descriptive_info exists and has data for this database
    # If not, return the modified semantic map with all local_definitions as null
    if (
        session_cache.descriptive_info is None
        or database not in session_cache.descriptive_info
        or session_cache.descriptive_info[database] is None
    ):
        logger.info(
            f"No descriptive info available for database '{database}'. "
            "Returning semantic map without local mappings."
        )
        return modified_semantic_map

    for local_variable, local_value in session_cache.descriptive_info[database].items():
        # Skip if no description is provided (empty field in UI)
        if "description" not in local_value or not local_value["description"]:
            continue

        global_variable = (
            local_value["description"]
            .split("Variable description: ")[1]
            .lower()
            .replace(" ", "_")
        )

        if (
            global_variable
            and global_variable in session_cache.global_semantic_map["variable_info"]
        ):
            # Handle duplicate global variables by creating new entries with suffix
            if global_variable in used_global_variables:
                suffix = used_global_variables[global_variable] + 1
                new_global_variable = f"{global_variable}_{suffix}"
                used_global_variables[global_variable] = suffix

                # Create new entry based on original
                modified_semantic_map["variable_info"][new_global_variable] = (
                    copy.deepcopy(
                        session_cache.global_semantic_map["variable_info"][
                            global_variable
                        ]
                    )
                )

                # Reset the new entry's local fields to null initially
                modified_semantic_map["variable_info"][new_global_variable][
                    "local_definition"
                ] = None
                modified_semantic_map["variable_info"][new_global_variable][
                    "data_type"
                ] = None
            else:
                new_global_variable = global_variable
                used_global_variables[global_variable] = 0

            # Update local definition (only if the field was filled in UI)
            modified_semantic_map["variable_info"][new_global_variable][
                "local_definition"
            ] = local_variable

            # Extract and add datatype information from UI
            datatype_value = (
                local_value["type"]
                .split("Variable type: ")[1]
                .lower()
                .replace(" ", "_")
            )
            # Only set datatype if it's not empty
            if datatype_value and datatype_value.strip():
                modified_semantic_map["variable_info"][new_global_variable][
                    "data_type"
                ] = datatype_value
            else:
                # Try to extract from request data or set default
                modified_semantic_map["variable_info"][new_global_variable][
                    "data_type"
                ] = None

            # Process value mapping if it exists
            if (
                "value_mapping"
                in modified_semantic_map["variable_info"][new_global_variable]
            ):
                original_terms = modified_semantic_map["variable_info"][
                    new_global_variable
                ]["value_mapping"]["terms"]
                used_global_terms = {}  # Track usage for duplicate term handling

                # Reset all local_terms to null first (already done above, but being explicit here)
                # Reset local_term for all terms first
                for term_key in original_terms:
                    original_terms[term_key]["local_term"] = None

                # Update local terms based on UI input (only for filled categories)
                for category, value in local_value.items():
                    if category.startswith("Category: ") and value and value.strip():
                        global_term = (
                            value.split(": ")[1]
                            .split(", comment")[0]
                            .lower()
                            .replace(" ", "_")
                        )
                        local_term_value = category.split(": ")[1]

                        if global_term in original_terms:
                            # Handle duplicate terms
                            if global_term in used_global_terms:
                                suffix = used_global_terms[global_term] + 1
                                new_global_term = f"{global_term}_{suffix}"
                                used_global_terms[global_term] = suffix

                                # Create a new term entry
                                original_terms[new_global_term] = copy.deepcopy(
                                    original_terms[global_term]
                                )
                                original_terms[new_global_term][
                                    "local_term"
                                ] = local_term_value
                            else:
                                original_terms[global_term][
                                    "local_term"
                                ] = local_term_value
                                used_global_terms[global_term] = 0

    return modified_semantic_map




def insert_equivalencies(descriptive_info, variable, database):
    """
    This function inserts equivalencies into a GraphDB repository.

    Parameters:
    descriptive_info (dict): A dictionary containing descriptive information about the variables.
                             The keys are the variable names, and the values are dictionaries containing
                             the type, description, comments, and categories of the variables.
    variable (str): The name of the variable for which the equivalency is to be inserted.

    Returns:
    str: The result of the query execution as a string if the execution is successful.

    The function performs the following steps:
    1. Constructs a SPARQL INSERT query that inserts an owl:equivalentClass triple into the ontology graph.
       The subject of the triple is the URI of the variable, and the object is the first value in the
       'values' field of the variable in the descriptive_info dictionary.
    2. Executes the query on the GraphDB repository using the execute_query function.
    3. Returns the result of the query execution.

    The SPARQL query works as follows:
    1. It selects the URI of the variable in the ontology graph.
    2. It inserts an owl:equivalentClass triple into the ontology graph.
       The subject of the triple is the selected URI, and the object is the first value in the
       'values' field of the variable in the descriptive_info dictionary.
    """

    # Skip if variable missing or empty
    if variable not in descriptive_info or not descriptive_info[variable]:
        return None

    var_info = descriptive_info[variable]

    # Get the three main fields
    type_value = var_info.get("type", "")
    description_value = var_info.get("description", "")
    comments_value = var_info.get("comments", "")

    # Check if any of these fields has meaningful content
    has_type = type_value not in ["", "Variable type: ", "Variable type: None"]
    has_description = description_value not in [
        "",
        "Variable description: ",
        "Variable description: None",
    ]
    has_comments = comments_value not in ["", "Variable comment: No comment provided"]

    # Skip if none of the fields has meaningful content
    if not (has_type or has_description or has_comments):
        return None

    # Construct the named graph URI for this specific database's ontology
    ontology_graph = f"http://ontology.local/{database}/"

    query = f"""
                PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
                PREFIX db: <http://{session_cache.repo}.local/rdf/ontology/>
                PREFIX roo: <http://www.cancerdata.org/roo/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>

                INSERT
                {{
                    GRAPH <{ontology_graph}>
                    {{ ?s owl:equivalentClass "{list(var_info.values())}". }}
                }}
                WHERE
                {{
                    ?s dbo:column '{variable}'.
                }}
            """
    return graphdb_service.execute_query(query, "update", "/statements")


# Register controller blueprints
from controllers import ingest_bp, describe_bp, annotate_bp, share_bp

app.register_blueprint(ingest_bp)
app.register_blueprint(describe_bp)
app.register_blueprint(annotate_bp)
app.register_blueprint(share_bp)

# Add compatibility endpoint for custom_static (templates expect this name)
@app.route("/data_descriptor/assets/<path:filename>")
def custom_static(filename):
    """
    Compatibility endpoint for custom_static.
    This route maintains backward compatibility with templates that expect 'custom_static' endpoint.
    It delegates to the share blueprint's custom_static implementation.
    """
    return share_bp.view_functions["custom_static"](filename)

# Set up application config
app.config["graphdb_url"] = graphdb_url
app.config["repo"] = repo

# Set up application context with required functions and services
app.config["APP_CONTEXT"] = {
    "session_cache": session_cache,
    "graphdb_service": graphdb_service,
    "name_matcher": None,  # Will be initialized later if needed
    "upload_folder": app.config["UPLOAD_FOLDER"],
    "root_dir": root_dir,
    "child_dir": child_dir,
    "run_triplifier": lambda properties_file: (
        lambda result: (
            setattr(session_cache, 'output_files', result[2]),
            result[:2]
        )[1]
    )(
        IngestService().run_triplifier(
            properties_file, root_dir, child_dir,
            csv_data_list=session_cache.csvData if hasattr(session_cache, 'csvData') else None,
            csv_table_names=session_cache.csvTableNames if hasattr(session_cache, 'csvTableNames') else None
        )
    ),
    "upload_func": lambda file_type, output_files: IngestService().upload_multiple_graphs(
        root_dir, graphdb_url, repo, output_files, data_background=False
    ) if file_type == "CSV" else IngestService().upload_ontology_then_data(
        root_dir, graphdb_url, repo, data_background=False
    ),
    "start_background": lambda sc: [
        gevent.spawn(IngestService.background_pk_fk_processing, sc, graphdb_service),
        gevent.spawn(IngestService.background_cross_graph_processing, sc, graphdb_service)
    ],
    "handle_postgres": lambda username, password, postgres_url, postgres_db, table: (
        IngestService().handle_postgres_connection(
            username, password, postgres_url, postgres_db, table, root_dir, child_dir
        )
    ),
    "has_semantic_map": lambda sc: DescribeService.has_semantic_map(sc),
    "get_semantic_map": lambda sc, db_key=None: (
        __import__('utils.session_helpers').get_semantic_map_for_annotation(sc, db_key)
    ),
    "formulate_local_map": lambda db: DescribeService.formulate_local_semantic_map(db),
    "get_table_names": lambda sc: IngestService.get_table_names_from_mapping(sc),
    "name_matcher": lambda map_db, target_db: GraphDBService.graph_database_find_name_match(map_db, target_db),
    "get_semantic_map_for_annotation": lambda sc, db_key=None: (
        __import__('utils.session_helpers').get_semantic_map_for_annotation(sc, db_key)
    ),
}

if __name__ == "__main__":
    # Use 0.0.0.0 in Docker (safe within container network), 127.0.0.1 for local dev
    is_docker = os.getenv("FLYOVER_GRAPHDB_URL") is not None
    default_host = "0.0.0.0" if is_docker else "127.0.0.1"

    host = os.getenv("FLASK_HOST", default_host)
    port = int(os.getenv("FLASK_PORT", "5000"))

    app.run(host=host, port=port)
