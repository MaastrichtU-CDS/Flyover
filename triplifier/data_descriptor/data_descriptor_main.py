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

from utils.data_preprocessing import (
    preprocess_dataframe,
    sanitise_table_name,
)
from utils.data_ingest import upload_ontology_then_data, upload_multiple_graphs
from utils.session_helpers import (
    check_any_data_graph_exists,
    graph_database_ensure_backend_initialisation,
    graph_database_find_name_match,
    graph_database_find_matching,
    process_variable_for_annotation,
    get_semantic_map_for_annotation,
    has_semantic_map,
    COLUMN_INFO_QUERY,
    DATABASE_NAME_PATTERN,
    get_table_names_from_mapping,
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





    def extract_variable_from_key(key, database):
        prefix = f"{database}_"
        if "_category_" in key:
            base = key.split("_category_")[0]
        elif "_notation_missing_or_unspecified" in key:
            base = key.split("_notation_missing_or_unspecified")[0]
        else:
            base = key
        if base.startswith(prefix):
            return base[len(prefix) :]
        return None

    # Iterate over each database in the session cache
    for database in session_cache.databases:
        keys = []
        for key in request.form:
            if not key.startswith(f"{database}_"):
                continue
            matching_dbs = [
                db for db in session_cache.databases if key.startswith(f"{db}_")
            ]
            if matching_dbs and max(matching_dbs, key=len) == database:
                keys.append(key)

        variables = []
        for key in keys:
            var = extract_variable_from_key(key, database)
            if var:
                variables.append(var)

        # Iterate over each unique variable
        for variable in set(variables):
            # Retrieve all keys from the request form that contain the variable name and do not start with 'comment_'
            keys = [
                key
                for key in request.form
                if variable in key
                and not key.startswith("comment_")
                and not key.startswith("count_")
            ]

            for key in keys:

                if "_notation_missing_or_unspecified" in key:
                    session_cache.descriptive_info[database][variable][
                        f"Category: {request.form.get(key)}"
                    ] = (
                        f"Category {request.form.get(key)}: missing_or_unspecified"
                        or "No missing value notation provided"
                    )

                elif "_category_" in key and not key.startswith("count_"):
                    # Retrieve the category and the associated value and comment from the request form and
                    # store them in the session cache
                    category = key.split('_category_"')[1].split('"')[0]
                    count_form = f'count_{database}_{variable}_category_"{category}"'
                    session_cache.descriptive_info[database][variable][
                        f"Category: {category}"
                    ] = (
                        f"Category {category}: {request.form.get(key)}, comment: "
                        f'{request.form.get(f"comment_{key}") or "No comment provided"},  '
                        f'count: {request.form.get(count_form) or "No count available"}'
                    )
                # Handle units
                elif "count_" not in key:
                    session_cache.descriptive_info[database][variable]["units"] = (
                        request.form.get(key) or "No units specified"
                    )

            # Call the 'insert_equivalencies' function to insert equivalencies into the GraphDB repository
            insert_equivalencies(
                session_cache.descriptive_info[database], variable, database
            )

    # Redirect the user to the 'download_page' URL
    return redirect(url_for("describe_downloads"))


            def remove_file(response):
                try:
                    os.remove(_filename)
                except Exception as error:
                    app.logger.error(
                        "Error removing or closing downloaded file handle", error
                    )
                return response

            # Open the zip file in binary mode and return it as a response
            with open(_filename, "rb") as f:
                return Response(
                    f.read(),
                    mimetype="application/zip",
                    headers={"Content-Disposition": f"attachment;filename={_filename}"},
                )
        else:
            # If there is only one database
            database = session_cache.databases[0]
            filename = f"local_semantic_map_{database}.json"

            try:
                # Generate a modified version of the global semantic map by adding local definitions to it
                modified_semantic_map = formulate_local_semantic_map(database)

                # Return the modified semantic map as a JSON response
                return Response(
                    json.dumps(modified_semantic_map, indent=4),
                    mimetype="application/json",
                    headers={"Content-Disposition": f"attachment;filename={filename}"},
                )
            except Exception as e:
                abort(
                    500,
                    description=f"An error occurred while processing the semantic map, error: {str(e)}",
                )

    except Exception as e:
        abort(
            500,
            description=f"An error occurred while processing the semantic map, error: {str(e)}",
        )


            def remove_file(response):
                try:
                    os.remove(_filename)
                except Exception as error:
                    app.logger.error(
                        "Error removing or closing downloaded file handle", error
                    )
                return response

            # Open the zip file in binary mode and return it as a response
            with open(_filename, "rb") as f:
                return Response(
                    f.read(),
                    mimetype="application/zip",
                    headers={"Content-Disposition": f"attachment;filename={_filename}"},
                )

        else:
            # Single ontology: download directly
            if len(databases_to_process) == 1:
                # Use the single database name
                database = databases_to_process[0]
                ontology_graph = f"{named_graph}{database}/"
                filename = f"local_ontology_{database}.nt"
            else:
                # Fallback to default graph
                ontology_graph = named_graph
                if filename is None:
                    filename = "local_ontology.nt"

            response = requests.get(
                f"{graphdb_url}/repositories/{session_cache.repo}/rdf-graphs/service",
                params={"graph": ontology_graph},
                headers={"Accept": "application/n-triples"},
            )

            if response.status_code == 200:
                return Response(
                    response.text,
                    mimetype="application/n-triples",
                    headers={"Content-Disposition": f"attachment;filename={filename}"},
                )
            else:
                abort(
                    500,
                    description=f"Failed to download ontology. Status code: {response.status_code}",
                )

    except Exception as e:
        abort(
            500,
            description=f"An error occurred while downloading the ontology, error: {str(e)}",
        )


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


def execute_query(repo, query, query_type=None, endpoint_appendices=None):
    """
    This function executes a SPARQL query on a specified GraphDB repository.

    Parameters:
    repo (str): The name of the GraphDB repository on which the query is to be executed.
    query (str): The SPARQL query to be executed.
    query_type (str, optional): The type of the SPARQL query. Defaults to "query".
    endpoint_appendices (str, optional): Additional endpoint parameters. Defaults to "".

    Returns:
    str: The result of the query execution as a string if the execution is successful.
    flask.render_template: A Flask function that renders the 'ingest.html' template
    if an error occurs during the query execution.

    Raises:
    Exception: If an error occurs during the query execution,
    an exception is raised, and its error message is flashed to the user.

    The function performs the following steps:
    1. Checks if query_type and endpoint_appendices are None. If they are, set them to their default values.
    2. Constructs the endpoint URL using the provided repository name and endpoint_appendices.
    3. Executes the SPARQL query on the constructed endpoint URL.
    4. If the query execution is successful, it returns the result as a string.
    5. If an error occurs during the query execution,
    flashes an error message to the user and renders the 'ingest.html' template.
    """
    if query_type is None:
        query_type = "query"

    if endpoint_appendices is None:
        endpoint_appendices = ""
    try:
        # Construct the endpoint URL
        endpoint = f"{graphdb_url}/repositories/" + repo + endpoint_appendices
        # Execute the query
        response = requests.post(
            endpoint,
            data={query_type: query},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        # Return the result of the query execution
        return response.text
    except Exception as e:
        # If an error occurs, flash the error message to the user and render the 'ingest.html' template
        flash(f"Unexpected error when connecting to GraphDB, error: {e}.")
        return render_template("ingest.html")


def retrieve_categories(repo, column_name):
    """
    This function executes a SPARQL query on a specified GraphDB repository
    to retrieve the categories of a given column.

    Parameters:
    repo (str): The name of the GraphDB repository on which the query is to be executed.
    column_name (str): The name of the column for which the categories are to be retrieved.

    Returns:
    str: The result of the query execution as a string if the execution is successful.

    The function performs the following steps:
    1. Constructs a SPARQL query that selects the value and count of each category in the specified column.
    2. Executes the query on the specified GraphDB repository using the execute_query function.
    3. Returns the result of the query execution.

    The SPARQL query works as follows:
    1. It selects the value and count of each category in the specified column.
    2. It groups the results by the value of the category.
    """
    query_categories = f"""
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        PREFIX db: <http://{repo}.local/rdf/ontology/>
        PREFIX roo: <http://www.cancerdata.org/roo/>
        SELECT ?value (COUNT(?value) as ?count)
        WHERE
        {{
           ?a a ?v.
           ?v dbo:column '{column_name}'.
           ?a dbo:has_cell ?cell.
           ?cell dbo:has_value ?value
        }}
        GROUP BY (?value)
    """
    return execute_query(repo, query_categories)


def retrieve_global_names():
    """
    This function retrieves the names of global variables from the session cache.

    The function first checks if a semantic map is available (jsonld_mapping or global_semantic_map).
    If not, it returns a list of default global variable names.
    If a semantic map is available, it attempts to retrieve the variable keys,
    capitalise them, replace underscores with spaces, and return them as a list.
    If an error occurs during this process,
    it flashes an error message to the user and renders the 'ingest.html' template.

    Returns:
        list: A list of strings representing the names of the global variables.
        flask.render_template: A Flask function that renders a template.
        In this case, it renders the 'ingest.html' template if an error occurs.
    """
    default_names = [
        "Research subject identifier",
        "Biological sex",
        "Age at inclusion",
        "Other",
    ]

    # Prefer jsonld_mapping when available
    if session_cache.jsonld_mapping is not None:
        try:
            return [
                name.capitalize().replace("_", " ")
                for name in session_cache.jsonld_mapping.get_all_variable_keys()
            ] + ["Other"]
        except Exception as e:
            flash(f"Failed to read the JSON-LD mapping. Error: {e}")
            return render_template("ingest.html", error=True)

    # Fall back to global_semantic_map
    if not isinstance(session_cache.global_semantic_map, dict):
        return default_names
    else:
        try:
            return [
                name.capitalize().replace("_", " ")
                for name in session_cache.global_semantic_map["variable_info"].keys()
            ] + ["Other"]
        except Exception as e:
            flash(f"Failed to read the global semantic map. Error: {e}")
            return render_template("ingest.html", error=True)


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


def handle_postgres_data(username, password, postgres_url, postgres_db, table):
    """
    This function handles the PostgreSQL data. It caches the provided information,
    establishes a connection to the PostgreSQL database, and writes the connection details to a properties file.

    Parameters:
    username (str): The username for the PostgreSQL database.
    password (str): The password for the PostgreSQL database.
    postgres_url (str): The URL of the PostgreSQL database.
    postgres_db (str): The name of the PostgreSQL database.
    table (str): The name of the table in the PostgreSQL database.

    Returns:
    flask.Response: A Flask response object containing the rendered 'ingest.html' template if
                    the connection to the PostgreSQL database fails.
    None: If the connection to the PostgreSQL database is successful.
    """
    # Cache information
    (
        session_cache.username,
        session_cache.password,
        session_cache.url,
        session_cache.db_name,
        session_cache.table,
    ) = (username, password, postgres_url, postgres_db, table)

    try:
        # Establish PostgreSQL connection
        session_cache.conn = connect(
            dbname=session_cache.db_name,
            user=session_cache.username,
            host=session_cache.url,
            password=session_cache.password,
        )
        print("Connection:", session_cache.conn)
    except Exception as err:
        print("connect() ERROR:", err)
        session_cache.conn = None
        flash(
            "Attempting to connect to PostgreSQL datasource unsuccessful. Please check your details!"
        )
        return render_template("ingest.html", error=True)

    # Write connection details to the properties file
    with open(f"{root_dir}{child_dir}/triplifierSQL.properties", "w") as f:
        f.write(
            f"jdbc.url = jdbc:postgresql://{session_cache.url}/{session_cache.db_name}\n"
            f"jdbc.user = {session_cache.username}\n"
            f"jdbc.password = {session_cache.password}\n"
            f"jdbc.driver = org.postgresql.Driver\n\n"
            # f"repo.type = rdf4j\n"
            # f"repo.url = {graphdb_url}\n"
            # f"repo.id = {repo}"
        )


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
    return execute_query(session_cache.repo, query, "update", "/statements")


def get_column_class_uri(table_name, column_name):
    """Retrieve column class URI"""
    query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?uri WHERE {{
        ?uri dbo:column '{column_name}' .
        FILTER(CONTAINS(LCASE(STR(?uri)), LCASE('{table_name}')))
    }}
    LIMIT 1
    """

    try:
        query_result = execute_query(session_cache.repo, query)

        if not query_result or query_result.strip() == "":
            print(f"No results found for column {table_name}.{column_name}")
            return None

        column_info = pl.read_csv(
            StringIO(query_result),
            infer_schema_length=0,
            null_values=[],
            try_parse_dates=False,
        )

        if column_info.is_empty():
            print(f"Empty result set for column {table_name}.{column_name}")
            return None

        if "uri" not in column_info.columns:
            print("Query result format error: no 'uri' column found")
            return None

        return column_info.get_column("uri")[0]

    except Exception as e:
        print(f"Error fetching column URI for {table_name}.{column_name}: {e}")
        return None


def insert_fk_relation(
    fk_predicate,
    column_class_uri,
    target_class_uri,
    relationships_graph="http://relationships.local/",
):
    """Insert PK/FK relationship into the relationships graph"""
    insert_query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT {{
        GRAPH <{relationships_graph}> {{
            ?sources <{fk_predicate}> ?targets .
        }}
    }} WHERE {{
        ?sources rdf:type <{column_class_uri}> ;
                 dbo:has_cell ?sourceCell .
        ?sourceCell dbo:has_value ?columnValue .

        ?targets rdf:type <{target_class_uri}> ;
                 dbo:has_cell ?targetCell .
        ?targetCell dbo:has_value ?columnValue .
    }}
    """

    return execute_query(session_cache.repo, insert_query, "update", "/statements")


def process_pk_fk_relationships():
    """Process all PK/FK relationships after successful triplification"""
    if not session_cache.pk_fk_data:
        return True

    try:
        print("Starting PK/FK relationship processing...")
        session_cache.pk_fk_status = "processing"

        # Create mapping of files with their PK/FK info
        file_map = {rel["fileName"]: rel for rel in session_cache.pk_fk_data}

        # Process each relationship
        for rel in session_cache.pk_fk_data:
            if not all(
                [
                    rel.get("foreignKey"),
                    rel.get("foreignKeyTable"),
                    rel.get("foreignKeyColumn"),
                ]
            ):
                continue

            # Find the target table's PK info
            target_file = rel["foreignKeyTable"]
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get("primaryKey"):
                continue

            # Generate FK configuration - sanitise table name before handling
            fk_config = {
                "foreignKeyTable": sanitise_table_name(rel["fileName"]),
                "foreignKeyColumn": rel["foreignKey"],
                "primaryKeyTable": sanitise_table_name(target_file),
                "primaryKeyColumn": target_rel["primaryKey"],
            }

            source_uri = get_column_class_uri(
                fk_config["foreignKeyTable"], fk_config["foreignKeyColumn"]
            )

            target_uri = get_column_class_uri(
                fk_config["primaryKeyTable"], fk_config["primaryKeyColumn"]
            )

            if not source_uri or not target_uri:
                print(f"Could not find URIs for FK relationship: {fk_config}")
                continue

            fk_predicate = "http://um-cds/ontologies/databaseontology/fk_refers_to"

            # Insert the relationship
            insert_fk_relation(fk_predicate, source_uri, target_uri)
            print(
                f"Created FK relationship: "
                f"{fk_config['foreignKeyTable']}.{fk_config['foreignKeyColumn']} -> "
                f"{fk_config['primaryKeyTable']}.{fk_config['primaryKeyColumn']}"
            )

        session_cache.pk_fk_status = "success"
        print("PK/FK relationship processing completed successfully.")
        return True

    except Exception as e:
        print(f"Error processing PK/FK relationships: {e}")
        session_cache.pk_fk_status = "failed"
        return False


def background_pk_fk_processing():
    """Background function to process PK/FK relationships"""
    try:
        # Small delay to ensure GraphDB has processed the uploaded data
        time.sleep(3)
        process_pk_fk_relationships()
    except Exception as e:
        print(f"Background PK/FK processing error: {e}")
        session_cache.pk_fk_status = "failed"


def get_existing_column_class_uri(table_name, column_name):
    """Retrieve existing column class URI from the existing graph"""
    query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?uri WHERE {{
            ?uri dbo:column '{column_name}' .
            FILTER(CONTAINS(LCASE(STR(?uri)), LCASE('{table_name}')))
    }}
    LIMIT 1
    """

    try:
        query_result = execute_query(session_cache.repo, query)

        if not query_result or query_result.strip() == "":
            print(f"No existing results found for column {table_name}.{column_name}")
            return None

        column_info = pl.read_csv(
            StringIO(query_result),
            infer_schema_length=0,
            null_values=[],
            try_parse_dates=False,
        )

        if column_info.is_empty() or "uri" not in column_info.columns:
            return None

        return column_info.get_column("uri")[0]

    except Exception as e:
        print(f"Error fetching existing column URI for {table_name}.{column_name}: {e}")
        return None


def insert_cross_graph_relation(
    predicate,
    new_column_uri,
    existing_column_uri,
    relationships_graph="http://relationships.local/",
):
    """Insert cross-graph relationship into the relationships graph"""
    insert_query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT {{
        GRAPH <{relationships_graph}> {{
            ?newSources <{predicate}> ?existingSources .
        }}
    }} WHERE {{
        ?newSources rdf:type <{new_column_uri}> ;
                    dbo:has_cell ?newCell .
        ?newCell dbo:has_value ?columnValue .

        ?existingSources rdf:type <{existing_column_uri}> ;
                         dbo:has_cell ?existingCell .
        ?existingCell dbo:has_value ?columnValue .
    }}
    """

    return execute_query(session_cache.repo, insert_query, "update", "/statements")


def process_cross_graph_relationships():
    """Process cross-graph relationships after successful triplification"""
    if not session_cache.cross_graph_link_data:
        return True

    try:
        print("Starting cross-graph relationship processing...")
        session_cache.cross_graph_link_status = "processing"

        link_data = session_cache.cross_graph_link_data

        # Get URIs for new and existing columns
        new_column_uri = get_column_class_uri(
            sanitise_table_name(link_data["newTableName"]), link_data["newColumnName"]
        )

        existing_column_uri = get_existing_column_class_uri(
            sanitise_table_name(link_data["existingTableName"]),
            link_data["existingColumnName"],
        )

        if not new_column_uri or not existing_column_uri:
            print(f"Could not find URIs for cross-graph relationship: {link_data}")
            session_cache.cross_graph_link_status = "failed"
            return False

        predicate = "http://um-cds/ontologies/databaseontology/fk_refers_to"

        # Insert the relationship
        insert_cross_graph_relation(predicate, new_column_uri, existing_column_uri)

        print(
            f"Created cross-graph relationship: "
            f"{link_data['newTableName']}.{link_data['newColumnName']} -> "
            f"{link_data['existingTableName']}.{link_data['existingColumnName']}"
        )

        session_cache.cross_graph_link_status = "success"
        print("Cross-graph relationship processing completed successfully.")
        return True

    except Exception as e:
        print(f"Error processing cross-graph relationships: {e}")
        session_cache.cross_graph_link_status = "failed"
        return False


def background_cross_graph_processing():
    """Background function to process cross-graph relationships"""
    try:
        # Small delay to ensure GraphDB has processed the uploaded data
        time.sleep(5)  # Slightly longer delay than PK/FK to ensure it runs after
        process_cross_graph_relationships()
    except Exception as e:
        print(f"Background cross-graph processing error: {e}")
        session_cache.cross_graph_link_status = "failed"


def run_triplifier(properties_file=None):
    """
    This function runs the Python Triplifier and checks if it ran successfully.
    Wrapper function that calls the actual implementation in python_triplifier_integration.py
    """
    try:
        from utils.python_triplifier_integration import (
            run_triplifier as run_triplifier_impl,
        )

        if properties_file == "triplifierCSV.properties":
            # Use Python Triplifier for CSV processing
            # DataFrames are loaded directly into SQLite, no need to save CSV files
            success, message, output_files = run_triplifier_impl(
                properties_file=properties_file,
                root_dir=root_dir,
                child_dir=child_dir,
                csv_data_list=session_cache.csvData,
                csv_table_names=session_cache.csvTableNames,
            )

            # Store output files in session cache for later use
            session_cache.output_files = output_files

        elif properties_file == "triplifierSQL.properties":
            # Use Python Triplifier for PostgreSQL processing
            success, message, output_files = run_triplifier_impl(
                properties_file=properties_file, root_dir=root_dir, child_dir=child_dir
            )
            session_cache.output_files = output_files
        else:
            return False, f"Unknown properties file: {properties_file}"

        if success:
            # Note: Background PK/FK and cross-graph processing are now started
            # after upload completes (in upload_file function) to ensure data
            # is available in GraphDB before relationships are processed

            return True, Markup(
                "The data you have submitted was triplified successfully and "
                "is now available in GraphDB."
                "<br>"
                "You can now proceed to describe your data, "
                "but please note that this requires in-depth knowledge of the data."
                "<br><br>"
                "<i>In case you do not yet wish to describe your data, "
                "or you would like to add more data, "
                "please return to the ingest page.</i>"
                "<br>"
                "<i>You can always return to Flyover to "
                "describe the data that is present in GraphDB.</i>"
            )
        else:
            return False, message

    except Exception as e:
        logger.error(f"Unexpected error attempting to run the Python Triplifier: {e}")
        import traceback

        traceback.print_exc()
        return False, f"Unexpected error attempting to run the Triplifier, error: {e}"


# Register controller blueprints
from controllers import ingest_bp, describe_bp, annotate_bp, download_bp

app.register_blueprint(ingest_bp)
app.register_blueprint(describe_bp)
app.register_blueprint(annotate_bp)
app.register_blueprint(download_bp)

# Set up application context with required functions and services
app.config["APP_CONTEXT"] = {
    "session_cache": session_cache,
    "graphdb_service": None,  # Will be initialized later if needed
    "name_matcher": None,  # Will be initialized later if needed
    "upload_folder": app.config["UPLOAD_FOLDER"],
    "run_triplifier": run_triplifier,
    "upload_func": upload_multiple_graphs,
    "start_background": lambda sc: [
        gevent.spawn(background_pk_fk_processing),
        gevent.spawn(background_cross_graph_processing)
    ],
    "handle_postgres": handle_postgres_data,
}

if __name__ == "__main__":
    # Use 0.0.0.0 in Docker (safe within container network), 127.0.0.1 for local dev
    is_docker = os.getenv("FLYOVER_GRAPHDB_URL") is not None
    default_host = "0.0.0.0" if is_docker else "127.0.0.1"

    host = os.getenv("FLASK_HOST", default_host)
    port = int(os.getenv("FLASK_PORT", "5000"))

    app.run(host=host, port=port)
