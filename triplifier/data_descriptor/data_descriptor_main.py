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
    def __init__(self):
        self.repo = repo
        self.file_path = None
        self.table = None
        self.url = None
        self.username = None
        self.password = None
        self.db_name = None
        self.conn = None
        self.col_cursor = None
        self.csvData = None
        self.csvTableNames = None
        self.uploaded_file = None
        # DEPRECATED: global_semantic_map is deprecated in favor of jsonld_mapping.
        # Kept for backward compatibility with legacy JSON uploads.
        # Scheduled for removal in a future version.
        self.global_semantic_map = None
        self.jsonld_mapping = None  # Store JSONLDMapping object for JSON-LD format
        self.existing_graph = False
        self.databases = None
        self.descriptive_info = None
        self.DescriptiveInfoDetails = None
        self.StatusToDisplay = None
        self.pk_fk_data = None
        self.pk_fk_status = None  # "processing", "success", "failed"
        self.cross_graph_link_data = None
        self.cross_graph_link_status = None
        self.annotation_status = None  # Store annotation results
        self.annotation_json_path = None  # Store path to the uploaded JSON file
        self.output_files = None  # Store output files list for CSV uploads


session_cache = Cache()


@app.route("/")
def landing():
    """
    Render the landing page that provides an overview of the three-step workflow.
    This serves as the main entry point describing the Ingest, Describe, Annotate process.
    """
    return render_template("index.html")


@app.route("/ingest")
def index():
    """
    This function is responsible for rendering the ingest.html page.
    It is mapped to the root URL ("/") of the Flask application.

    The function first checks if a data graph already exists in the GraphDB repository.
    If it does, the ingest.html page is rendered with a flag indicating that the graph exists.
    If the graph does not exist or if an error occurs during the check,
    the ingest.html page is rendered without the flag.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'ingest.html' template.

    Raises:
        Exception: If an error occurs while checking if the data graph exists,
        an exception is raised, and its error message is flashed to the user.
    """
    # Check whether a data graph already exists
    try:
        if check_any_data_graph_exists(session_cache.repo, graphdb_url):
            # If the data graph exists, render the ingest.html page with a flag indicating that the graph exists
            session_cache.existing_graph = True
            return render_template(
                "ingest.html", graph_exists=session_cache.existing_graph
            )
    except Exception as e:
        # If an error occurs, flash the error message to the user
        flash(f"Failed to check if the a data graph already exists, error: {e}")

    # If the data graph does not exist or if an error occurs, render the ingest.html page without the flag
    return render_template("ingest.html")


@app.route("/upload-semantic-map", methods=["POST"])
def upload_semantic_map():
    """
    Handle the upload of a JSON-LD semantic map file from the data_submission page.
    This function validates and stores the mapping in the session cache.

    Returns:
        flask.Response: JSON response indicating success or error with validation details
    """
    semantic_map_file = request.files.get("semanticMapFile")

    if not semantic_map_file or not semantic_map_file.filename:
        return jsonify({"error": "No semantic map file provided"}), 400

    if not allowed_file(semantic_map_file.filename, {"jsonld"}):
        return (
            jsonify(
                {"error": "Please upload a valid .jsonld file for the semantic map"}
            ),
            400,
        )

    try:
        file_content = semantic_map_file.read().decode("utf-8")

        try:
            mapping_data = json.loads(file_content)
        except json.JSONDecodeError as e:
            return (
                jsonify(
                    {
                        "error": f"Invalid JSON-LD syntax at line {e.lineno}, column {e.colno}: {e.msg}",
                        "validation_errors": [
                            {
                                "path": f"(line {e.lineno}, column {e.colno})",
                                "severity": "error",
                                "message": f"JSON-LD syntax error: {e.msg}",
                                "suggestion": "Check for missing commas, brackets, or quotes",
                            }
                        ],
                    }
                ),
                400,
            )

        validator = MappingValidator()
        result = validator.validate(mapping_data)

        if not result.is_valid:
            errors = validator.format_errors_for_ui(result)
            return (
                jsonify(
                    {
                        "error": "Mapping file validation failed",
                        "validation_errors": errors,
                    }
                ),
                400,
            )

        jsonld_mapping = JSONLDMapping.from_dict(mapping_data)
        session_cache.jsonld_mapping = jsonld_mapping

        return jsonify(
            {
                "success": True,
                "message": "JSON-LD semantic mapping uploaded and validated successfully",
                "statistics": result.statistics,
            }
        )

    except Exception as e:
        logger.error(f"Error processing semantic map upload: {str(e)}")
        return (
            jsonify(
                {"error": f"Unexpected error processing the semantic map: {str(e)}"}
            ),
            400,
        )


@app.route("/submit-indexeddb-semantic-map", methods=["POST"])
def submit_indexeddb_semantic_map():
    """
    Handle submission of semantic map data from IndexedDB (frontend).
    This endpoint accepts JSON-LD data directly from the request body,
    validates it, and stores it in the session cache.

    Returns:
        flask.Response: JSON response indicating success or error with redirect URL
    """
    try:
        # Get JSON data from request body
        mapping_data = request.get_json()

        if not mapping_data:
            return jsonify({"error": "No semantic map data provided"}), 400

        # Validate the mapping data
        validator = MappingValidator()
        result = validator.validate(mapping_data)

        if not result.is_valid:
            errors = validator.format_errors_for_ui(result)
            return (
                jsonify(
                    {
                        "error": "Semantic map validation failed",
                        "validation_errors": errors,
                    }
                ),
                400,
            )

        # Store the validated mapping in session cache
        jsonld_mapping = JSONLDMapping.from_dict(mapping_data)
        session_cache.jsonld_mapping = jsonld_mapping

        return jsonify(
            {
                "success": True,
                "message": "Semantic map loaded successfully from browser storage",
                "redirect_url": "/annotation-review",
                "statistics": result.statistics,
            }
        )

    except json.JSONDecodeError as e:
        return (
            jsonify(
                {
                    "error": f"Invalid JSON data: {e.msg}",
                }
            ),
            400,
        )
    except Exception as e:
        logger.error(f"Error processing IndexedDB semantic map: {str(e)}")
        return (
            jsonify(
                {"error": f"Unexpected error processing the semantic map: {str(e)}"}
            ),
            400,
        )


@app.route("/api/graphdb-databases", methods=["GET"])
def get_graphdb_databases():
    """
    Fetch the list of databases available in GraphDB.
    This endpoint is used by the frontend to get actual database names
    from the RDF store for validation and display purposes.

    Returns:
        flask.Response: JSON response with list of databases or error
    """
    try:
        # Fetch databases from GraphDB
        if graph_database_ensure_backend_initialisation(session_cache, execute_query):
            return jsonify({"success": True, "databases": session_cache.databases})
        else:
            return jsonify(
                {
                    "success": False,
                    "databases": [],
                    "message": "No databases found in GraphDB",
                }
            )
    except Exception as e:
        logger.error(f"Error fetching GraphDB databases: {str(e)}")
        return (
            jsonify(
                {"success": False, "error": f"Failed to fetch databases: {str(e)}"}
            ),
            500,
        )


@app.route("/upload", methods=["POST"])
def upload_file():
    """
    This function handles the file upload process.
    It accepts CSV files and handles data from a PostgreSQL database.

    The function works as follows:
    1. It retrieves the file type and CSV files from the form data.
    2. If the file type is 'CSV' and CSV files are provided and their file extensions are allowed,
    it uploads and saves the files, stores the file paths in the session cache, and runs the triplifier.
    3. If the file type is 'Postgres', it handles the PostgreSQL data using the provided username, password, URL,
     database name, and table name, and runs the triplifier.
    4. It returns a response indicating whether the triplifier run was successful.

    Returns:
        flask.Response: A Flask response object containing the rendered 'describe_landing.html' template
         if the triplifier run was successful, or the 'ingest.html' template if it was not.
    """
    upload = True
    file_type = request.form.get("fileType")
    csv_files = request.files.getlist("csvFile")
    pk_fk_data = request.form.get("pkFkData")
    cross_graph_link_data = request.form.get("crossGraphLinkData")

    # Store PK/FK data in session cache
    if pk_fk_data:
        session_cache.pk_fk_data = json.loads(pk_fk_data)

    # Store cross-graph linking data in the session cache
    if cross_graph_link_data:
        session_cache.cross_graph_link_data = json.loads(cross_graph_link_data)

    if file_type == "CSV" and csv_files:
        # Check if any CSV file has a filename
        if not any(csv_file.filename for csv_file in csv_files):
            flash(
                "If opting to submit a CSV data source, please upload it as a '.csv' file."
            )
            return render_template("ingest.html", error=True)

        for csv_file in csv_files:
            if allowed_file(csv_file.filename, {"csv"}) is False:
                flash(
                    "If opting to submit a CSV data source, please upload it as a '.csv' file."
                )
                return render_template("ingest.html", error=True)

        try:
            separator_sign = str(request.form.get("csv_separator_sign"))
            if len(separator_sign) == 0:
                separator_sign = ","

            decimal_sign = str(request.form.get("csv_decimal_sign"))
            if len(decimal_sign) == 0:
                decimal_sign = "."

            session_cache.csvData = []
            for csv_file in csv_files:
                # Use polars to read CSV with minimal inference
                df = pl.read_csv(
                    csv_file,
                    separator=separator_sign,
                    infer_schema_length=0,  # Treat everything as strings
                    null_values=[],  # Don't infer nulls
                    try_parse_dates=False,  # Don't auto-parse dates
                )
                # TODO improve this approach,
                #  if data conversion is done in the end,
                #  we can use data descriptions to infer datatype and set them properly
                # Handle decimal conversion: normalize user-specified decimal separator to standard "."
                # Note: This replaces the decimal_sign character in ALL string columns. For CSV data
                # where the user specifies a decimal separator (e.g., "," for European formats),
                # this ensures consistent numeric representation. Free text fields with the same
                # character will also be affected, but this is acceptable since:
                # 1. The user explicitly specifies this is their decimal separator
                # 2. The original file is preserved - this is an internal representation
                # 3. Downstream processing (triplification) expects standard "." decimals
                if decimal_sign != ".":
                    df = df.with_columns(
                        [
                            pl.col(c).str.replace_all(decimal_sign, ".")
                            for c in df.columns
                        ]
                    )
                session_cache.csvData.append(preprocess_dataframe(df))

        except Exception as e:
            flash(f"Unexpected error attempting to cache the CSV data, error: {e}")
            return render_template("ingest.html", error=True)

        # Store table names derived from CSV filenames (no need to save CSV files)
        session_cache.csvTableNames = [
            os.path.splitext(secure_filename(csv_file.filename))[0]
            for csv_file in csv_files
        ]
        success, message = run_triplifier("triplifierCSV.properties")

    elif file_type == "Postgres":
        handle_postgres_data(
            request.form.get("username"),
            request.form.get("password"),
            request.form.get("POSTGRES_URL"),
            request.form.get("POSTGRES_DB"),
            request.form.get("table"),
        )
        success, message = run_triplifier("triplifierSQL.properties")

    elif file_type != "Postgres" and not any(
        csv_file.filename for csv_file in csv_files
    ):
        success = True
        upload = False
        message = Markup(
            "You have opted to not submit any new data, "
            "you can now proceed to describe your data."
            "<br>"
            "<i>In case you do wish to submit data, please return to the ingest page.</i>"
        )

    else:
        success = False
        message = "An unexpected error occurred. Please try again."

    if success:
        session_cache.StatusToDisplay = message

        if upload:
            logger.info("🚀 Initiating upload to GraphDB")

            # Use different upload strategy based on file type
            if file_type == "CSV" and session_cache.output_files:
                # Upload multiple graphs for CSV files
                upload_success, upload_messages = upload_multiple_graphs(
                    root_dir,
                    graphdb_url,
                    repo,
                    session_cache.output_files,
                    data_background=False,
                )
            else:
                # Use traditional single-graph upload for PostgreSQL
                upload_success, upload_messages = upload_ontology_then_data(
                    root_dir, graphdb_url, repo, data_background=False
                )

            for msg in upload_messages:
                logger.info(f"📝 {msg}")

            # START BACKGROUND PK/FK AND CROSS-GRAPH PROCESSING AFTER UPLOAD
            # This ensures data is in GraphDB before relationships are processed
            if file_type == "CSV":
                if session_cache.pk_fk_data:
                    logger.info(
                        "Upload complete. Starting background PK/FK processing..."
                    )
                    gevent.spawn(background_pk_fk_processing)

                if session_cache.cross_graph_link_data:
                    logger.info(
                        "Upload complete. Starting background cross-graph processing..."
                    )
                    gevent.spawn(background_cross_graph_processing)

        # Redirect to the new route after processing the POST request
        return redirect(url_for("data_submission"))
    else:
        flash(f"Attempting to proceed resulted in an error: {message}")
        return render_template(
            "ingest.html", error=True, graph_exists=session_cache.existing_graph
        )


@app.route("/data-submission")
def data_submission():
    """
    This function is mapped to the "/data-submission" URL and is invoked when a GET request is made to this URL.
    It retrieves a status message from the session cache and
    renders the 'describe_landing.html' template with the message.

    The function performs the following steps:
    1. Retrieves the status message from the 'StatusToDisplay' object in the session cache.
    2. The status message is marked as safe for inclusion in HTML/XML output
    using the Markup function from the 'markupsafe' module.
    3. Renders the 'describe_landing.html' template with the status message.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'describe_landing.html' template with the status message.
    """
    # Render the 'describe_landing.html' template with the 'title', 'message', and 'route'
    return render_template(
        "describe_landing.html", message=Markup(session_cache.StatusToDisplay)
    )


@app.route("/describe_landing")
def describe_landing():
    """
    This function provides access to the describe landing page when users navigate directly
    or when they have completed the 'ingest' step.
    It checks if data exists in the repository and shows appropriate messaging based on data availability.

    Returns:
        flask.render_template: Renders the describe_landing.html template with appropriate status
    """
    try:
        # Check if the graph exists first
        if check_any_data_graph_exists(session_cache.repo, graphdb_url):
            session_cache.existing_graph = True
            message = "Data has been uploaded successfully. You can now proceed to describe your data variables."
            return render_template("describe_landing.html", message=Markup(message))
        else:
            # No data found - render page with a warning message instead of redirecting
            message = """
            <div class="alert alert-warning" role="alert">
                <i class="fas fa-exclamation-triangle"></i>
                <strong>No Data Found</strong><br>
                You cannot proceed with the describe step as no data has been uploaded yet.
                Please go back to the Ingest step to upload your data first.
                <br><br>
                <a href="/ingest" class="btn btn-primary">
                    <i class="fas fa-arrow-left"></i> Go to Ingest Step
                </a>
            </div>
            """
            return render_template("describe_landing.html", message=Markup(message))

    except Exception as e:
        # On error, show a warning message instead of redirecting
        message = f"""
        <div class="alert alert-danger" role="alert">
            <i class="fas fa-exclamation-triangle"></i>
            <strong>Error Accessing Data</strong><br>
            An error occurred while checking for uploaded data: {e}<br>
            Please ensure you have completed the Ingest step before proceeding.
            <br><br>
            <a href="/ingest" class="btn btn-primary">
                <i class="fas fa-arrow-left"></i> Go to Ingest Step
            </a>
        </div>
        """
        return render_template("describe_landing.html", message=Markup(message))


@app.route("/describe_variables", methods=["GET", "POST"])
def describe_variables():
    column_info = pl.read_csv(
        StringIO(execute_query(session_cache.repo, COLUMN_INFO_QUERY)),
        infer_schema_length=0,
        null_values=[],
        try_parse_dates=False,
    )
    column_info = column_info.with_columns(
        pl.col("uri").str.extract(DATABASE_NAME_PATTERN, 1).alias("database")
    )
    column_info = column_info.drop("uri")

    unique_values = column_info.get_column("database").unique().to_list()
    session_cache.databases = unique_values

    columns_by_database = {}
    for db in unique_values:
        db_columns = column_info.filter(pl.col("database") == db)
        columns_by_database[db] = db_columns.get_column("column").to_list()

    return render_template(
        "describe_variables.html",
        column_info=columns_by_database,
    )


@app.route("/units", methods=["POST"])
def retrieve_descriptive_info():
    """
    This function is responsible for retrieving descriptive information about the variables in the databases.
    It is mapped to the "/describe_variable_details" URL and is invoked when a POST request is made to this URL.

    The function performs the following steps:
    1. Iterates over each database in the session cache.
    2. For each database, it iterates over each local variable name in the form data.
    3. If the local variable name does not start with "ncit_comment_" and
       does not contain the name of any other database, it processes the local variable.
    4. It retrieves the data type, global variable name, and comment for the local variable from the form data.
    5. It stores this information in the session cache.
    6. If the data type of the local variable is 'Categorical',
       it retrieves the categories for the local variable and stores them in the session cache.
    7. If the data type of the local variable is 'Continuous',
    it adds the local variable to a list of variables to further specify.
    8. Finally, it renders the 'describe_variable_details.html' template with the list of variables to further specify.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'describe_variable_details.html' template with the list of variables to further specify,
        or proceeds to 'describe_downloads' in case there are no variables to specify.
    """
    session_cache.descriptive_info = {}
    session_cache.DescriptiveInfoDetails = {}

    for database in session_cache.databases:
        session_cache.DescriptiveInfoDetails[database] = []
        session_cache.descriptive_info[database] = {}

        for local_variable_name in request.form:
            if not re.search("^ncit_comment_", local_variable_name):
                matching_dbs = [db for db in session_cache.databases if local_variable_name.startswith(f"{db}_")]
                if not matching_dbs or max(matching_dbs, key=len) != database:
                    continue
                local_variable_name = local_variable_name.replace(f"{database}_", "")
                form_local_variable_name = f"{database}_{local_variable_name}"

                data_type = request.form.get(form_local_variable_name)
                global_variable_name = request.form.get(
                    "ncit_comment_" + form_local_variable_name
                )
                comment = request.form.get("comment_" + form_local_variable_name)

                # Store the data type, global variable name, and comment for the local variable in the session cache
                session_cache.descriptive_info[database][local_variable_name] = {
                    "type": f"Variable type: {data_type}",
                    "description": f"Variable description: {global_variable_name}",
                    "comments": f'Variable comment: {comment if comment else "No comment provided"}',
                }

                # If the data type of the local variable is 'categorical',
                # retrieve the categories for the local variable and store them in the session cache
                if data_type == "categorical":
                    cat = retrieve_categories(session_cache.repo, local_variable_name)
                    df = pl.read_csv(
                        StringIO(cat),
                        separator=",",
                        infer_schema_length=0,
                        null_values=[],
                        try_parse_dates=False,
                    )
                    # Check if the description is missing and format display name accordingly
                    if not global_variable_name or global_variable_name.strip() == "":
                        display_name = (
                            f'Missing Description (or "{local_variable_name}")'
                        )
                    else:
                        display_name = (
                            f'{global_variable_name} (or "{local_variable_name}")'
                        )
                    session_cache.DescriptiveInfoDetails[database].append(
                        {display_name: df.to_dicts()}
                    )
                # If the data type of the local variable is 'continuous',
                # add the local variable to a list of variables to further specify
                elif data_type == "continuous":
                    # Check if the description is missing and format display name accordingly
                    if not global_variable_name or global_variable_name.strip() == "":
                        display_name = (
                            f'Missing Description (or "{local_variable_name}")'
                        )
                    else:
                        display_name = (
                            f'{global_variable_name} (or "{local_variable_name}")'
                        )
                    session_cache.DescriptiveInfoDetails[database].append(display_name)
                else:
                    insert_equivalencies(
                        session_cache.descriptive_info[database],
                        local_variable_name,
                        database,
                    )

        # Remove databases that do not have any descriptive information
        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]

    # Render the 'describe_variable_details.html' template with the list of variables to further specify
    if session_cache.DescriptiveInfoDetails:
        return redirect(url_for("describe_variable_details"))
    else:
        # Redirect to the new route after processing the POST request
        return redirect(url_for("describe_downloads"))


@app.route("/describe_variable_details")
def describe_variable_details():
    """
    This function is responsible for rendering the 'describe_variable_details.html' page.
    It passes descriptive_info and DescriptiveInfoDetails to the frontend for IndexedDB storage.

    Returns:
        flask.render_template: A Flask function that renders the 'variable-details.html' template.
    """
    preselected_values = {}

    map_database_name = None
    if session_cache.jsonld_mapping:
        map_database_name = session_cache.jsonld_mapping.get_first_database_name()

    if session_cache.jsonld_mapping and session_cache.DescriptiveInfoDetails:
        for database, variables in session_cache.DescriptiveInfoDetails.items():
            if not graph_database_find_name_match(map_database_name, database):
                continue

            for variable in variables:
                if isinstance(variable, dict):
                    for var_name, categories in variable.items():
                        global_var = var_name.split(" (or")[0].lower().replace(" ", "_")
                        var_info = session_cache.jsonld_mapping.get_variable(global_var)
                        if var_info:
                            local_column = (
                                session_cache.jsonld_mapping.get_local_column(
                                    global_var
                                )
                            )
                            for category in categories:
                                category_value = category.get("value")
                                for (
                                    term,
                                    _target_class,
                                ) in var_info.value_mappings.items():
                                    local_term = (
                                        session_cache.jsonld_mapping.get_local_term(
                                            global_var, term
                                        )
                                    )
                                    if str(local_term) == str(category_value):
                                        key = f"{database}_{local_column or ''}_category_\"{category.get('value')}\""
                                        preselected_values[key] = term.title().replace(
                                            "_", " "
                                        )
                                        break

    return render_template(
        "describe_variable_details.html",
        descriptive_info=json.dumps(session_cache.descriptive_info or {}),
        descriptive_info_details=json.dumps(session_cache.DescriptiveInfoDetails or {}),
        preselected_values=preselected_values,
    )


@app.route("/end", methods=["GET", "POST"])
def retrieve_detailed_descriptive_info():
    """
    This function is responsible for retrieving detailed descriptive information about the variables in the databases.
    It is mapped to the "/end" URL and is invoked when a POST request is made to this URL.

    The function performs the following steps:
    1. Iterates over each database in the session cache.
    2. For each database, it retrieves all keys from the request form that start with the database name.
    3. It then identifies the variables associated with these keys.
    4. For each unique variable, it retrieves all keys from the request form that contain the variable name and
     do not start with 'comment_'.
    5. If there is only one key, it retrieves the value associated with this key from the request form and
    stores it in the 'units' field of the variable in the session cache.
    6. If there are multiple keys, it iterates over each key. If the key contains '_category_',
    it retrieves the category and the associated value, comment, and count from the request form and
    stores them in the session cache.
    7. It then calls the 'insert_equivalencies' function to insert equivalencies into the GraphDB repository.
    8. Finally, it redirects the user to the 'download_page' URL.

    Returns:
        flask.redirect: A Flask function that redirects the user to another URL.
        In this case, it redirects the user to the 'download_page' URL.
    """
    # Iterate over each database in the session cache
    for database in session_cache.databases:
        # Retrieve all keys from the request form that start with the database name
        keys = [key for key in request.form if key.startswith(database)]
        # Identify the variables associated with these keys
        variables = [
            (
                key.split("_category_")[0].split(f"{database}_")[1]
                if "_category_" in key
                else (
                    key.split("_notation_missing_or_unspecified")[0].split(
                        f"{database}_"
                    )[1]
                    if "_notation_missing_or_unspecified" in key
                    else key.split(f"{database}_")[1]
                )
            )
            for key in keys
        ]

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


@app.route("/describe_downloads")
def describe_downloads():
    """
    This function is responsible for rendering the 'describe_downloads.html' page.
    Supports both jsonld_mapping (preferred) and global_semantic_map (fallback).

    Returns:
        flask.render_template: A Flask function that renders the 'describe_downloads.html' template.
    """
    return render_template(
        "describe_downloads.html", graphdb_location="http://localhost:7200/"
    )


@app.route("/downloadSemanticMap", methods=["GET"])
def download_semantic_map():
    """
    Download the semantic map in JSON-LD format (preferred) or legacy JSON format (fallback).

    If jsonld_mapping is available, downloads a single JSON-LD file that supports
    multi-database natively. This deprecates the previous multi-JSON zip download approach.

    For backward compatibility, if only global_semantic_map is available, falls back
    to the legacy format using formulate_local_semantic_map.

    Returns:
        flask.Response: A Flask response object containing the semantic map as JSON-LD or JSON.
    """
    try:
        # Prefer JSON-LD format if jsonld_mapping is available
        if session_cache.jsonld_mapping is not None:
            # JSON-LD supports multi-database natively, so we output a single file
            mapping_dict = session_cache.jsonld_mapping.to_dict()
            filename = "semantic_mapping.jsonld"

            return Response(
                json.dumps(mapping_dict, indent=2, ensure_ascii=False),
                mimetype="application/ld+json",
                headers={"Content-Disposition": f"attachment;filename={filename}"},
            )

        # Fall back to legacy format if only global_semantic_map is available
        # NOTE: Multi-JSON zip download is deprecated in favor of JSON-LD multi-database support
        if len(session_cache.databases) > 1:
            _filename = "local_semantic_maps.zip"
            # Loop through each database
            for database in session_cache.databases:
                filename = f"local_semantic_map_{database}.json"

                # Open the zip file in the 'append' mode
                with zipfile.ZipFile(_filename, "a") as zipf:
                    # Generate a modified version of the global semantic map by adding local definitions to it
                    modified_semantic_map = formulate_local_semantic_map(database)

                    # Write the modified semantic map to the zip file
                    zipf.writestr(filename, json.dumps(modified_semantic_map, indent=4))

            # Define a function to remove the zip file after the request has been handled
            @after_this_request
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


@app.route("/downloadOntology", methods=["GET"])
def download_ontology(named_graph="http://ontology.local/", filename=None):
    """
    This function downloads ontology files from GraphDB and returns them as a response.
    For multiple ontologies, it creates a zip file containing all ontologies.

    Parameters:
    named_graph (str): The base URL of the graph from which the ontology is to be downloaded.
    Defaults to "http://ontology.local/".
    filename (str): The name of the file to be downloaded. Defaults to 'local_ontology_{database_name}.nt'.

    Returns:
        flask.Response: A Flask response object containing the ontology as a string (single ontology)
                        or a zip file (multiple ontologies) if the download is successful,
                        or an error message if the download fails.
    """
    try:
        # Check if we have multiple databases (either from session or need to query GraphDB)
        databases_to_process = []

        # First, try to get databases from session cache
        if session_cache.databases and len(session_cache.databases) > 1:
            databases_to_process = session_cache.databases
        else:
            # Query GraphDB to find all ontology graphs
            query_graphs = """
                SELECT DISTINCT ?g WHERE {
                    GRAPH ?g {
                        ?s ?p ?o .
                    }
                    FILTER(STRSTARTS(STR(?g), "http://ontology.local/"))
                }
            """
            result = execute_query(session_cache.repo, query_graphs)

            if result and result.strip():
                graphs_df = pl.read_csv(
                    StringIO(result),
                    infer_schema_length=0,
                    null_values=[],
                    try_parse_dates=False,
                )

                if not graphs_df.is_empty() and "g" in graphs_df.columns:
                    # Extract database names from graph URIs
                    for graph_uri in graphs_df.get_column("g").to_list():
                        # Extract database name from URI like "http://ontology.local/database_name/"
                        db_name = graph_uri.replace(named_graph, "").rstrip("/")
                        if db_name:
                            databases_to_process.append(db_name)

        # If we have multiple databases, create a zip file
        if len(databases_to_process) > 1:
            _filename = "local_ontologies.zip"

            # Create a new zip file and loop through each database
            files_added = 0
            with zipfile.ZipFile(_filename, "w") as zipf:
                for database in databases_to_process:
                    # Construct the graph URI for this database
                    table_graph = f"{named_graph}{database}/"
                    ontology_filename = f"local_ontology_{database}.nt"

                    # Fetch the ontology for this database
                    response = requests.get(
                        f"{graphdb_url}/repositories/{session_cache.repo}/rdf-graphs/service",
                        params={"graph": table_graph},
                        headers={"Accept": "application/n-triples"},
                    )

                    if response.status_code == 200 and response.text.strip():
                        zipf.writestr(ontology_filename, response.text)
                        files_added += 1
                    else:
                        logger.warning(
                            f"No ontology data found for graph: {table_graph}"
                        )

            # Check if the zip file was created successfully and contains data
            if files_added == 0 or not os.path.exists(_filename):
                # No files were added or zip file doesn't exist
                if os.path.exists(_filename):
                    os.remove(_filename)
                abort(
                    404,
                    description="No ontology data found for any of the specified graphs.",
                )

            # Define a function to remove the zip file after the request has been handled
            @after_this_request
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


@app.route("/annotation_landing")
def annotation_landing():
    """
    This function provides access to the annotation landing page when users navigate directly
    or when they want to start the annotation process.
    It checks if data exists and provides the appropriate options, including JSON upload.

    Returns:
        flask.render_template: Renders the annotation_landing.html template with appropriate status
    """
    try:
        # Check if the graph exists
        data_exists = check_any_data_graph_exists(session_cache.repo, graphdb_url)
        session_cache.existing_graph = data_exists

        return render_template(
            "annotation_landing.html", data_exists=data_exists, message=None
        )
    except Exception as e:
        flash(f"Error accessing annotation step: {e}")
        return redirect(url_for("landing"))


@app.route("/upload-annotation-json", methods=["POST"])
def upload_annotation_json():
    """
    Handle the upload of a JSON-LD file for direct annotation.
    This allows users to upload JSON-LD files with data descriptions for annotation.
    Validates that databases in the JSON-LD match databases available in GraphDB.

    Returns:
        flask.jsonify: JSON response indicating success or failure
    """
    try:
        if "annotationJsonFile" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files["annotationJsonFile"]

        if file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        if not file.filename.lower().endswith(".jsonld"):
            return jsonify({"error": "File must be a JSON-LD (.jsonld) file"}), 400

        # Save the uploaded file
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        # Try to parse the JSON to validate it
        try:
            with open(filepath, "r") as f:
                json_data = json.load(f)

            # Ensure databases are initialised from RDF-store
            if not graph_database_ensure_backend_initialisation(
                session_cache, execute_query
            ):
                os.remove(filepath)  # Clean up file
                return (
                    jsonify(
                        {
                            "error": "No data found in the data store. Please complete the Ingest step first.",
                            "graphdb_databases": [],
                            "jsonld_databases": [],
                        }
                    ),
                    400,
                )

            # Extract tables from JSON-LD structure (tables are the actual data sources)
            jsonld_tables = []
            if json_data.get("databases"):
                for _db_key, db_data in json_data["databases"].items():
                    if isinstance(db_data, dict) and db_data.get("tables"):
                        for table_key, table_data in db_data["tables"].items():
                            # Use sourceFile if available, otherwise use the table key
                            table_name = (
                                table_data.get("sourceFile", table_key)
                                if isinstance(table_data, dict)
                                else table_key
                            )
                            jsonld_tables.append(table_name)
            elif json_data.get("database_name"):
                # Fallback for flat structure
                jsonld_tables.append(json_data["database_name"])

            if not jsonld_tables:
                os.remove(filepath)  # Clean up file
                return (
                    jsonify(
                        {
                            "error": "The uploaded semantic map does not contain any table definitions.<br>"
                            "Please ensure your JSON-LD file has tables defined in the 'databases' section.",
                            "graphdb_databases": session_cache.databases,
                            "jsonld_databases": [],
                        }
                    ),
                    400,
                )

            # Find matching tables between JSON-LD and GraphDB
            matching_databases = []
            non_matching_jsonld = []

            for jsonld_table in jsonld_tables:
                matched = graph_database_find_matching(
                    jsonld_table, session_cache.databases
                )
                if matched:
                    matching_databases.append(
                        {"jsonld": jsonld_table, "graphdb": matched}
                    )
                else:
                    non_matching_jsonld.append(jsonld_table)

            # Check if we have at least one matching table
            if not matching_databases:
                os.remove(filepath)  # Clean up file
                return (
                    jsonify(
                        {
                            "error": "None of the data sources in the semantic map match data in GraphDB.",
                            "graphdb_databases": session_cache.databases,
                            "jsonld_databases": jsonld_tables,
                            "matching_databases": [],
                            "non_matching_jsonld": non_matching_jsonld,
                        }
                    ),
                    400,
                )

            # Store the JSON data for use in annotation
            session_cache.global_semantic_map = json_data
            session_cache.annotation_json_path = filepath

            # Also store as jsonld_mapping for consistency
            validator = MappingValidator()
            result = validator.validate(json_data)
            if result.is_valid:
                jsonld_mapping = JSONLDMapping.from_dict(json_data)
                session_cache.jsonld_mapping = jsonld_mapping

            return jsonify(
                {
                    "success": True,
                    "message": "JSON-LD file uploaded and validated successfully",
                    "filename": filename,
                    "graphdb_databases": session_cache.databases,
                    "jsonld_databases": jsonld_tables,
                    "matching_databases": matching_databases,
                    "non_matching_jsonld": non_matching_jsonld,
                }
            )

        except json.JSONDecodeError:
            os.remove(filepath)  # Clean up invalid file
            return jsonify({"error": "Invalid JSON file format"}), 400

    except Exception as e:
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500


@app.route("/annotation-review")
def annotation_review():
    """
    Display the annotation review page where users can inspect their semantic map
    before running the annotation process. The page loads data from IndexedDB
    on the client side and renders it dynamically.
    """
    # Just render the template - data will be loaded from IndexedDB on the client side
    return render_template("annotation_review.html")


@app.route("/start-annotation", methods=["POST"])
def start_annotation():
    """
    Start the annotation process using semantic maps per database.
    Supports both jsonld_mapping (preferred) and global_semantic_map (fallback).
    """
    try:
        # Check if any semantic map is available
        if not has_semantic_map(session_cache):
            return jsonify({"success": False, "error": "No semantic map available"})

        # Ensure databases are initialised from the RDF-store if not already populated
        if not graph_database_ensure_backend_initialisation(
            session_cache, execute_query
        ):
            return jsonify(
                {"success": False, "error": "No databases available for annotation"}
            )

        # Get endpoint information
        endpoint = f"{graphdb_url}/repositories/{session_cache.repo}/statements"

        # Create a temporary directory for the annotation process
        temp_dir = "/tmp/annotation_temp"
        os.makedirs(temp_dir, exist_ok=True)

        # Initialize annotation status
        session_cache.annotation_status = {}

        total_annotated_vars = 0

        # Get table names from the mapping (handles both JSON-LD and legacy formats)
        map_table_names = get_table_names_from_mapping(session_cache)

        # Process each database separately using its semantic map
        for database in session_cache.databases:
            # Check if this database matches any table name from the semantic map
            matches_any_table = False
            for table_name in map_table_names:
                if graph_database_find_name_match(table_name, database):
                    matches_any_table = True
                    break

            # If no table names specified (empty list), treat as global template
            if not map_table_names:
                matches_any_table = True

            if not matches_any_table:
                logger.info(
                    f"Skipping database {database} - does not match any table in semantic map:  {map_table_names}"
                )
                continue

            logger.info(f"Processing annotation for database: {database}")

            database_key = None
            if session_cache.jsonld_mapping:
                database_key = (
                    session_cache.jsonld_mapping.find_database_key_for_graphdb(database)
                )
                logger.info(
                    f"Found matching JSON-LD database key: {database_key} for GraphDB database: {database}"
                )

            # Get the semantic map for this database (uses jsonld_mapping if available)
            semantic_map, _, is_jsonld = get_semantic_map_for_annotation(
                session_cache, database_key=database_key
            )

            # Get variable info and prefixes based on source
            if is_jsonld:
                variable_info = semantic_map.get("variable_info", {})
                prefixes = semantic_map.get("prefixes", "")
            else:
                local_semantic_map = formulate_local_semantic_map(database)
                variable_info = local_semantic_map.get("variable_info", {})
                prefixes = local_semantic_map.get(
                    "prefixes",
                    "PREFIX db: <http://data.local/> PREFIX dbo: <http://um-cds/ontologies/databaseontology/> "
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                    "PREFIX owl: <http://www.w3.org/2002/07/owl#> "
                    "PREFIX roo: <http://www.cancerdata.org/roo/> "
                    "PREFIX ncit: <http://ncicb.nci. nih.gov/xml/owl/EVS/Thesaurus. owl#>",
                )

            # Filter only variables that have local definitions
            annotated_variables = {}
            for var_name, var_data in variable_info.items():
                # For JSON-LD, local_definition is already in var_data
                # For legacy, use the shared helper to process variable
                if is_jsonld:
                    has_local_def = var_data.get("local_definition") is not None
                    var_copy = copy.deepcopy(var_data)
                else:
                    var_copy, has_local_def = process_variable_for_annotation(
                        var_name, var_data, session_cache.global_semantic_map
                    )

                # Only add variables with local definitions
                if has_local_def:
                    annotated_variables[var_name] = var_copy

            if not annotated_variables:
                logger.info(
                    f"No variables with local definitions found for database {database}"
                )
                continue

            logger.info(
                f"Starting annotation process for {len(annotated_variables)} variables in {database}"
            )

            for var_name, var_data in annotated_variables.items():
                logger.info(
                    f"Variable {var_name}: predicate={var_data.get('predicate')}, class={var_data.get('class')}, local_definition={var_data.get('local_definition')}"
                )

            try:
                # Use add_annotation function from the annotation helper for this database
                add_annotation(
                    endpoint=endpoint,
                    database=database,
                    prefixes=prefixes,
                    annotation_data=annotated_variables,
                    path=temp_dir,
                    remove_has_column=False,
                    save_query=True,
                )

                # For now, we'll assume success for variables with local definitions
                # In the future the add_annotation function should return status
                for var_name, _var_data in annotated_variables.items():
                    session_cache.annotation_status[f"{database}. {var_name}"] = {
                        "success": True,
                        "message": "Annotation completed successfully",
                        "database": database,
                    }

                total_annotated_vars += len(annotated_variables)
                logger.info(
                    f"Annotation process completed successfully for database {database}"
                )

            except Exception as annotation_error:
                logger.error(
                    f"Error during annotation execution for database {database}:  {str(annotation_error)}"
                )

                # Mark all variables as failed for this database
                for var_name in annotated_variables.keys():
                    session_cache.annotation_status[f"{database}.{var_name}"] = {
                        "success": False,
                        "error": str(annotation_error),
                        "database": database,
                    }

        if total_annotated_vars == 0:
            return jsonify(
                {
                    "success": False,
                    "error": "No variables with local definitions found across all databases",
                }
            )

        return jsonify(
            {
                "success": True,
                "message": f"Annotation process completed for {total_annotated_vars} "
                f"variables across {len(session_cache.databases)} databases",
            }
        )

    except Exception as e:
        logger.error(f"Error during annotation setup: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/annotation-verify")
def annotation_verify():
    """
    Display the annotation verification page where users can test their annotations.
    Supports both jsonld_mapping (preferred) and global_semantic_map (fallback).
    """
    # Check if any semantic map is available
    if not has_semantic_map(session_cache):
        flash("No semantic map available.")
        return redirect(url_for("describe_downloads"))

    # Ensure databases are initialised from the RDF-store if not already populated
    if not graph_database_ensure_backend_initialisation(session_cache, execute_query):
        flash("No databases available.")
        return redirect(url_for("describe_downloads"))

    map_table_names = get_table_names_from_mapping(session_cache)

    annotated_variables = []
    unannotated_variables = []
    variable_data = {}

    for database in session_cache.databases:
        matches_any_table = False
        for table_name in map_table_names:
            if graph_database_find_name_match(table_name, database):
                matches_any_table = True
                break

        if not map_table_names:
            matches_any_table = True

        if not matches_any_table:
            continue

        database_key = None
        if session_cache.jsonld_mapping:
            database_key = session_cache.jsonld_mapping.find_database_key_for_graphdb(
                database
            )

        # Get the semantic map for this database (uses jsonld_mapping if available)
        semantic_map, _, is_jsonld = get_semantic_map_for_annotation(
            session_cache, database_key=database_key
        )

        # Get variable info and prefixes based on source
        if is_jsonld:
            variable_info = semantic_map.get("variable_info", {})
            prefixes = semantic_map.get("prefixes", "")
        else:
            local_semantic_map = formulate_local_semantic_map(database)
            variable_info = local_semantic_map.get("variable_info", {})
            prefixes = local_semantic_map.get("prefixes", "")

        for var_name, var_data in variable_info.items():
            full_var_name = f"{database}.{var_name}"

            # For JSON-LD, local_definition is already in var_data
            # For legacy, use the shared helper to process variable
            if is_jsonld:
                has_local_def = var_data.get("local_definition") is not None
                var_copy = copy.deepcopy(var_data)
            else:
                var_copy, has_local_def = process_variable_for_annotation(
                    var_name, var_data, session_cache.global_semantic_map
                )

            if has_local_def:
                annotated_variables.append(full_var_name)
                variable_data[full_var_name] = {
                    **var_copy,
                    "database": database,
                    "prefixes": prefixes,
                }
            else:
                unannotated_variables.append(full_var_name)

    # Get annotation status
    annotation_status = session_cache.annotation_status or {}

    # Set success message if annotation was successful
    success_message = None
    if annotation_status and all(
        status.get("success") for status in annotation_status.values()
    ):
        success_message = (
            "The data processing is now complete and "
            "semantic interoperability has been achieved for the variables outlined above."
            "You can now close this page and proceed to the next steps in your workflow."
        )

    return render_template(
        "annotation_verify.html",
        annotated_variables=annotated_variables,
        unannotated_variables=unannotated_variables,
        annotation_status=annotation_status,
        variable_data=variable_data,
        success_message=success_message,
    )


@app.route("/verify-annotation-ask", methods=["POST"])
def verify_annotation_ask():
    """
    Verify annotation using an ASK query for a specific variable.
    This endpoint is used for live validation on the annotation verify page.
    Supports both jsonld_mapping (preferred) and global_semantic_map (fallback).
    """
    try:
        data = request.get_json()
        variable_name = data.get("variable")

        if not variable_name:
            return jsonify({"success": False, "error": "No variable specified"})

        # Parse database and variable from the full variable name (database.variable)
        if "." in variable_name:
            database, var_name = variable_name.split(".", 1)
        else:
            return jsonify(
                {
                    "success": False,
                    "error": "Invalid variable format. Expected: database.variable",
                }
            )

        database_key = None
        if session_cache.jsonld_mapping:
            database_key = session_cache.jsonld_mapping.find_database_key_for_graphdb(
                database
            )

        # Get the semantic map for this database (uses jsonld_mapping if available)
        semantic_map, _, is_jsonld = get_semantic_map_for_annotation(
            session_cache, database_key=database_key
        )

        if semantic_map is None:
            logger.warning(f"No semantic map available for database '{database}'")
            return jsonify({"success": False, "error": "No semantic map available"})

        logger.debug(
            f"verify_annotation_ask: is_jsonld={is_jsonld}, database={database}, var_name={var_name}"
        )

        # Get variable info based on source
        if is_jsonld:
            variable_info = semantic_map.get("variable_info", {})
        else:
            local_semantic_map = formulate_local_semantic_map(database)
            variable_info = local_semantic_map.get("variable_info", {})

        var_data = variable_info.get(var_name)

        if not var_data:
            logger.warning(
                f"Variable '{var_name}' not found in variable_info. Available keys: {list(variable_info.keys())}"
            )
            return jsonify({"success": False, "error": "Variable not found"})

        # Create a deep copy of the variable data to avoid reference issues
        var_copy = copy.deepcopy(var_data)

        # Check if this variable has a local definition
        local_definition = var_copy.get("local_definition")

        # For legacy format: If no local definition from the formulated map, check original JSON
        if (
            not local_definition
            and not is_jsonld
            and isinstance(session_cache.global_semantic_map, dict)
        ):
            original_var_info = session_cache.global_semantic_map.get(
                "variable_info", {}
            ).get(var_name, {})
            if original_var_info.get("local_definition"):
                local_definition = original_var_info["local_definition"]
                var_copy["local_definition"] = local_definition

                # Also copy value mappings if they exist in the original JSON
                # Only copy if the current var_copy doesn't have local_term values already
                if "value_mapping" in original_var_info:
                    # Check if the formulated map already has local_term values (from the 'describe' step)
                    current_value_mapping = var_copy.get("value_mapping", {})
                    has_local_terms = False
                    if current_value_mapping.get("terms"):
                        for term_data in current_value_mapping["terms"].values():
                            if term_data.get("local_term") is not None:
                                has_local_terms = True
                                break

                    # Only overwrite if no local_terms exist yet
                    if not has_local_terms:
                        var_copy["value_mapping"] = original_var_info["value_mapping"]

        var_class = var_copy.get("class")

        if not local_definition:
            logger.warning(
                f"Variable '{var_name}' has no local_definition. var_copy keys: {list(var_copy.keys())}"
            )
            return jsonify(
                {"success": False, "error": "Variable has no local definition"}
            )

        if not var_class:
            logger.warning(
                f"Variable '{var_name}' has no class mapping. var_copy: {var_copy}"
            )
            return jsonify({"success": False, "error": "Variable has no class mapping"})

        if isinstance(local_definition, list):
            local_definition = local_definition[0] if local_definition else ""
        if isinstance(local_definition, str):
            local_definition = local_definition.strip("[]'\"")

        # Build prefixes string with required prefixes
        base_prefixes = {
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "dbo": "http://um-cds/ontologies/databaseontology/",
            "db": "http://data.local/rdf/ontology/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "roo": "http://www.cancerdata.org/roo/",
            "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
            "sio": "http://semanticscience.org/resource/",
        }

        if is_jsonld and session_cache.jsonld_mapping:
            for prefix_key, prefix_uri in session_cache.jsonld_mapping.prefixes.items():
                if prefix_key not in base_prefixes:
                    base_prefixes[prefix_key] = prefix_uri

        prefixes = "\n".join(f"PREFIX {k}: <{v}>" for k, v in base_prefixes.items())

        # Build the ASK query according to the specification
        ask_query_parts = []

        # Add the main equivalentClass statement
        ask_query_parts.append(
            f"db:{database}.{local_definition} owl:equivalentClass {var_class} ."
        )

        # Check for value mappings and add target_class subClassOf statements
        value_mapping = var_copy.get("value_mapping", {})
        if value_mapping and value_mapping.get("terms"):
            for _term, term_info in value_mapping["terms"].items():
                if term_info.get("local_term") and term_info.get("target_class"):
                    ask_query_parts.append(
                        f"{term_info['target_class']} rdfs:subClassOf {var_class} ."
                    )

        ask_query = f"""
            {prefixes}
            ASK {{
              {' '.join(ask_query_parts)}
            }}
            """

        logger.info(f"Executing ASK query for {variable_name}: {ask_query}")

        # Execute the ASK query
        response = requests.get(
            f"{graphdb_url}/repositories/{session_cache.repo}",
            params={"query": ask_query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=30,
        )

        if response.status_code == 200:
            result = response.json()
            is_valid = result.get("boolean", False)

            return jsonify(
                {
                    "success": True,
                    "valid": is_valid,
                    "query": ask_query,  # For debugging purposes
                }
            )
        else:
            logger.error(
                f"ASK query failed with status {response.status_code}: {response.text}"
            )
            return jsonify(
                {
                    "success": False,
                    "error": f"Query failed with status {response.status_code}",
                }
            )

    except Exception as e:
        logger.error(f"Error verifying annotation with ASK query: {str(e)}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/favicon.ico")
def favicon():
    """
    Serve the favicon.ico file from the 'assets' directory.
    This route handles browser requests for the favicon.
    """
    return send_from_directory(
        f"{root_dir}{child_dir}/assets",
        "favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@app.route("/data_descriptor/assets/<path:filename>")
def custom_static(filename):
    """
    Serve static files from the custom assets directory.

    This route is used to serve static files such as CSS, JavaScript, and images
    from a custom directory within the project structure. The files are served
    from the 'assets' directory located within the 'data_descriptor' directory.

    Args:
        filename (str): The path to the static file relative to the 'assets' directory.

    Returns:
        flask.Response: The response containing the static file.

    Example:
        To serve a CSS file located at 'triplifier/data_descriptor/assets/css/bootstrap.min.css',
        you would access it via the URL '/data_descriptor/assets/css/bootstrap.min.css'.
    """
    return send_from_directory(f"{root_dir}{child_dir}/assets", filename)


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


@app.route("/api/check-graph-exists", methods=["GET"])
def api_check_graph_exists():
    """
    API endpoint to check if graph data exists in the repository.

    Returns:
        flask.jsonify: JSON response with the 'exists' boolean and optional error message
    """
    try:
        if not session_cache.repo:
            return jsonify({"exists": False, "error": "No repository configured"})

        # Check if any data graph exists (supports both single and multi-graph patterns)
        exists = check_any_data_graph_exists(session_cache.repo, graphdb_url)

        return jsonify({"exists": exists})
    except Exception as e:
        return jsonify({"exists": False, "error": str(e)})


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


@app.route("/get-existing-graph-structure", methods=["GET"])
def get_existing_graph_structure():
    """
    Get the structure of existing graph data for linking purposes
    """
    try:
        # Query to get existing tables and their columns
        structure_query = """
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        SELECT ?uri ?column
        WHERE {
                ?uri dbo:column ?column .
            }
        """

        result = execute_query(session_cache.repo, structure_query)

        if not result or result.strip() == "":
            return {"tables": [], "tableColumns": {}}

        # Parse the result using polars
        structure_info = pl.read_csv(
            StringIO(result),
            infer_schema_length=0,
            null_values=[],
            try_parse_dates=False,
        )

        if structure_info.is_empty():
            return {"tables": [], "tableColumns": {}}

        # Extract table names from URIs and organise by table
        structure_info = structure_info.with_columns(
            pl.col("uri")
            .str.extract(r".*/(.*?)\.", 1)
            .fill_null("unknown")
            .alias("table")
        )

        # Get unique tables
        tables = structure_info.get_column("table").unique().to_list()

        # Create table-column mapping
        table_columns = {}
        for table in tables:
            columns = (
                structure_info.filter(pl.col("table") == table)
                .get_column("column")
                .to_list()
            )
            table_columns[table] = columns

        return jsonify({"tables": tables, "tableColumns": table_columns})

    except Exception as e:
        print(f"Error getting existing graph structure: {e}")
        return {"tables": [], "tableColumns": {}}


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


if __name__ == "__main__":
    # Use 0.0.0.0 in Docker (safe within container network), 127.0.0.1 for local dev
    is_docker = os.getenv("FLYOVER_GRAPHDB_URL") is not None
    default_host = "0.0.0.0" if is_docker else "127.0.0.1"

    host = os.getenv("FLASK_HOST", default_host)
    port = int(os.getenv("FLASK_PORT", "5000"))

    app.run(host=host, port=port)
