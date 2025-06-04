import copy
import json
import os
import re
import zipfile
import threading
import time

import requests
import subprocess

import pandas as pd

from io import StringIO
from markupsafe import Markup
from psycopg2 import connect
from werkzeug.utils import secure_filename

from flask import (abort, after_this_request, Flask, redirect, render_template, request, flash, Response, url_for,
                   send_from_directory, jsonify)


app = Flask(__name__)

if os.getenv('FLYOVER_GRAPHDB_URL') and os.getenv('FLYOVER_REPOSITORY_NAME'):
    # Assume it is running in Docker
    graphdb_url = os.getenv('FLYOVER_GRAPHDB_URL')
    repo = os.getenv('FLYOVER_REPOSITORY_NAME')
    app.config["DEBUG"] = False
    root_dir = '/app/'
    child_dir = 'data_descriptor'
else:
    # Assume it is not running in Docker
    graphdb_url = 'http://localhost:7200'
    repo = 'userRepo'
    app.config["DEBUG"] = True
    root_dir = ''
    child_dir = '.'

app.secret_key = "secret_key"
app.config['UPLOAD_FOLDER'] = os.path.join(child_dir, 'static', 'files')
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])


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
        self.csvPath = None
        self.uploaded_file = None
        self.global_schema = None
        self.existing_graph = False
        self.databases = None
        self.descriptive_info = None
        self.DescriptiveInfoDetails = None
        self.StatusToDisplay = None
        self.pk_fk_data = None
        self.pk_fk_status = None  # "processing", "success", "failed"
        self.cross_graph_link_data = None
        self.cross_graph_link_status = None


session_cache = Cache()


@app.route('/')
def index():
    """
    This function is responsible for rendering the index.html page.
    It is mapped to the root URL ("/") of the Flask application.

    The function first checks if a data graph already exists in the GraphDB repository.
    If it does, the index.html page is rendered with a flag indicating that the graph exists.
    If the graph does not exist or if an error occurs during the check,
    the index.html page is rendered without the flag.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'index.html' template.

    Raises:
        Exception: If an error occurs while checking if the data graph exists,
        an exception is raised, and its error message is flashed to the user.
    """
    # Check whether a data graph already exists
    try:
        if check_graph_exists(session_cache.repo, "http://data.local/"):
            # If the data graph exists, render the index.html page with a flag indicating that the graph exists
            session_cache.existing_graph = True
            return render_template('index.html', graph_exists=session_cache.existing_graph)
    except Exception as e:
        # If an error occurs, flash the error message to the user
        flash(f"Failed to check if the a data graph already exists, error: {e}")

    # If the data graph does not exist or if an error occurs, render the index.html page without the flag
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    This function handles the file upload process.
    It accepts JSON and CSV files, and also handles data from a PostgreSQL database.

    The function works as follows:
    1. It retrieves the file type, JSON file, and CSV files from the form data.
    2. If a JSON file is provided and its file extension is allowed, it uploads and saves the file,
     and stores the file path in the session cache.
    3. If the file type is 'CSV' and CSV files are provided and their file extensions are allowed,
    it uploads and saves the files, stores the file paths in the session cache, and runs the triplifier.
    4. If the file type is 'Postgres', it handles the PostgreSQL data using the provided username, password, URL,
     database name, and table name, and runs the triplifier.
    5. It returns a response indicating whether the triplifier run was successful.

    Returns:
        flask.Response: A Flask response object containing the rendered 'triples.html' template
         if the triplifier run was successful, or the 'index.html' template if it was not.
    """
    upload = True
    file_type = request.form.get('fileType')
    json_file = request.files.get('jsonFile') or request.files.get('jsonFile2')
    csv_files = request.files.getlist('csvFile')
    pk_fk_data = request.form.get('pkFkData')
    cross_graph_link_data = request.form.get('crossGraphLinkData')

    # Store PK/FK data in session cache
    if pk_fk_data:
        session_cache.pk_fk_data = json.loads(pk_fk_data)

    # Store cross-graph linking data in session cache
    if cross_graph_link_data:
        session_cache.cross_graph_link_data = json.loads(cross_graph_link_data)

    if json_file:
        if not allowed_file(json_file.filename, {'json'}):
            flash("If opting to submit a global schema, please upload it as a '.json' file.")
            return render_template('index.html', error=True)

        try:
            session_cache.global_schema = json.loads(json_file.read().decode('utf-8'))

            if not isinstance(session_cache.global_schema.get('variable_info'), dict):
                flash("If opting to submit a global schema, please ensure it has a 'variable_info' field. "
                      "Please refer to the documentation for more information.")
                return render_template('index.html', error=True)

        except Exception as e:
            flash(f"Unexpected error attempting to cache the global schema file, error: {e}")
            return render_template('index.html', error=True)

    if file_type == 'CSV' and csv_files:
        # Check if any CSV file has a filename
        if not any(csv_file.filename for csv_file in csv_files):
            flash("If opting to submit a CSV data source, please upload it as a '.csv' file.")
            return render_template('index.html', error=True)

        for csv_file in csv_files:
            if allowed_file(csv_file.filename, {'csv'}) is False:
                flash("If opting to submit a CSV data source, please upload it as a '.csv' file.")
                return render_template('index.html', error=True)

        try:
            separator_sign = str(request.form.get('csv_separator_sign'))
            if len(separator_sign) == 0:
                separator_sign = ','

            decimal_sign = str(request.form.get('csv_decimal_sign'))
            if len(decimal_sign) == 0:
                decimal_sign = '.'

            session_cache.csvData = []
            for csv_file in csv_files:
                session_cache.csvData.append(pd.read_csv(csv_file, sep=separator_sign, decimal=decimal_sign))

        except Exception as e:
            flash(f"Unexpected error attempting to cache the CSV data, error: {e}")
            return render_template('index.html', error=True)

        session_cache.csvPath = [os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(csv_file.filename)) for
                                 csv_file in csv_files]
        try:
            success, message = run_triplifier('triplifierCSV.properties')
        finally:
            for path in session_cache.csvPath:
                if os.path.exists(path):
                    os.remove(path)

    elif file_type == 'Postgres':
        handle_postgres_data(request.form.get('username'), request.form.get('password'),
                             request.form.get('POSTGRES_URL'), request.form.get('POSTGRES_DB'),
                             request.form.get('table'))
        success, message = run_triplifier('triplifierSQL.properties')

    elif json_file and file_type != 'Postgres' and not any(csv_file.filename for csv_file in csv_files):
        success = True
        upload = False
        message = Markup("You have opted to not submit any new data, "
                         "you can now proceed to describe your data using the global schema that you have provided."
                         "<br>"
                         "<i>In case you do wish to submit data, please return to the welcome page.</i>")

    elif not json_file and file_type != 'Postgres' and not any(csv_file.filename for csv_file in csv_files):
        success = True
        upload = False
        message = Markup("You have opted to not submit any new data, "
                         "you can now proceed to describe your data."
                         "<br>"
                         "<i>In case you do wish to submit data, please return to the welcome page.</i>")

    else:
        success = False
        message = "An unexpected error occurred. Please try again."

    if success:
        session_cache.StatusToDisplay = message

        if upload:
            # Upload files to GraphDB
            subprocess.run(
                ["curl", "-X", "POST", "-H", "Content-Type: application/rdf+xml", "--data-binary",
                 f"@{root_dir}ontology.owl",
                 f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://ontology.local/"])
            subprocess.run(
                ["curl", "-X", "POST", "-H", "Content-Type: application/x-turtle", "--data-binary",
                 f"@{root_dir}output.ttl",
                 f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://data.local/"])

        # Redirect to the new route after processing the POST request
        return redirect(url_for('data_submission'))
    else:
        flash(f"Attempting to proceed resulted in an error: {message}")
        return render_template('index.html', error=True, graph_exists=session_cache.existing_graph)


@app.route('/data-submission')
def data_submission():
    """
    This function is mapped to the "/data-submission" URL and is invoked when a GET request is made to this URL.
    It retrieves a status message from the session cache and renders the 'triples.html' template with the message.

    The function performs the following steps:
    1. Retrieves the status message from the 'StatusToDisplay' object in the session cache.
    2. The status message is marked as safe for inclusion in HTML/XML output
    using the Markup function from the 'markupsafe' module.
    3. Renders the 'triples.html' template with the status message.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'triples.html' template with the status message.
    """
    # Render the 'triples.html' template with the 'title', 'message', and 'route'
    return render_template('triples.html', message=Markup(session_cache.StatusToDisplay))


@app.route("/repo", methods=['GET', 'POST'])
def retrieve_columns():
    """
    This function is mapped to the "/repo" URL and is invoked when a POST request is made to this URL.
    It retrieves column information from a GraphDB repository and
    prepares it for rendering in the 'categories.html' template.

    The function performs the following steps:
    1. Executes a SPARQL query to fetch the URI and column name of each column in the GraphDB repository.
    2. Reads the query results into a pandas DataFrame.
    3. Extracts the database name from the URI and adds it as a new column in the DataFrame.
    4. Drops the 'uri' column from the DataFrame.
    5. Gets the unique values in the 'database' column and stores them in the session cache.
    6. Creates a dictionary of dataframes, where the key is the unique database name and
    the value is the corresponding dataframe.
    7. Gets the global variable names for the description drop-down menu.
    8. Renders the 'categories.html' template with the dictionary of dataframes and the global variable names.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'categories.html' template with the dictionary of dataframes and the global variable names.
    """
    # SPARQL query to fetch the URI and column name of each column in the GraphDB repository
    column_query = """
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        SELECT ?uri ?column 
        WHERE {
        ?uri dbo:column ?column .
        }
    """
    # Execute the query and read the results into a pandas DataFrame
    column_info = pd.read_csv(StringIO(execute_query(session_cache.repo, column_query)))
    # Extract the database name from the URI and add it as a new column in the DataFrame
    column_info['database'] = column_info['uri'].str.extract(r'.*/(.*?)\.', expand=False)

    # Drop the 'uri' column from the DataFrame
    column_info = column_info.drop(columns=['uri'])

    # Get unique values in 'database' column and store them in the session cache
    unique_values = column_info['database'].unique()
    session_cache.databases = unique_values

    # Create a dictionary of dataframes, where the key is the database name, and the value is a corresponding dataframe
    dataframes = {value: column_info[column_info['database'] == value] for value in unique_values}

    # Get the global variable names for the description drop-down menu
    global_names = retrieve_global_names()

    # Create dictionaries to store preselected values
    preselected_descriptions = {}
    preselected_datatypes = {}

    # If a global schema exists and contains variable_info
    if isinstance(session_cache.global_schema, dict) and 'variable_info' in session_cache.global_schema:
        for var_name, var_info in session_cache.global_schema['variable_info'].items():
            for db in unique_values:  # For each database
                # Match by local_definition if available
                local_def = var_info.get('local_definition', var_name)
                key = f"{db}_{local_def}"
                preselected_descriptions[key] = var_name.capitalize().replace('_', ' ')
                if 'data_type' in var_info:
                    datatype = var_info['data_type'].lower()
                    words = datatype.split()
                    preselected_datatypes[key] = ' '.join(word.capitalize() for word in words)

                # Match by column name variations
                variations = [
                    var_name,
                    var_name.lower(),
                    var_name.replace('_', ''),
                    var_name.replace('_', ' '),
                ]
                for variation in variations:
                    key_variation = f"{db}_{variation}"
                    if key_variation not in preselected_descriptions and 'data_type' in var_info:
                        datatype = var_info['data_type'].lower()
                        words = datatype.split()
                        preselected_datatypes[key_variation] = ' '.join(word.capitalize() for word in words)

    # Render the 'categories.html' template with all the necessary data
    return render_template('categories.html',
                           dataframes=dataframes,
                           global_variable_names=global_names,
                           preselected_descriptions=preselected_descriptions,
                           preselected_datatypes=preselected_datatypes)


@app.route("/units", methods=['POST'])
def retrieve_descriptive_info():
    """
    This function is responsible for retrieving descriptive information about the variables in the databases.
    It is mapped to the "/units" URL and is invoked when a POST request is made to this URL.

    The function performs the following steps:
    1. Iterates over each database in the session cache.
    2. For each database, it iterates over each local variable name in the form data.
    3. If the local variable name does not start with "ncit_comment_" and
       does not contain the name of any other database, it processes the local variable.
    4. It retrieves the data type, global variable name, and comment for the local variable from the form data.
    5. It stores this information in the session cache.
    6. If the data type of the local variable is 'Categorical Nominal' or 'Categorical Ordinal',
       it retrieves the categories for the local variable and stores them in the session cache.
    7. If the data type of the local variable is 'Continuous',
    it adds the local variable to a list of variables to further specify.
    8. Finally, it renders the 'units.html' template with the list of variables to further specify.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case,
        it renders the 'units.html' template with the list of variables to further specify,
        or proceeds to 'download.html' in case there are no variables to specify.
    """
    session_cache.descriptive_info = {}
    session_cache.DescriptiveInfoDetails = {}

    for database in session_cache.databases:
        session_cache.DescriptiveInfoDetails[database] = []
        session_cache.descriptive_info[database] = {}

        # TODO improve database name handling; e.g. using database_2025_a and database_2025 combined will cause issues
        for local_variable_name in request.form:
            if (not re.search("^ncit_comment_", local_variable_name) and
                    not any(db in local_variable_name for db in session_cache.databases if db != database)):
                local_variable_name = local_variable_name.replace(f'{database}_', '')
                form_local_variable_name = f'{database}_{local_variable_name}'

                data_type = request.form.get(form_local_variable_name)
                global_variable_name = request.form.get('ncit_comment_' + form_local_variable_name)
                comment = request.form.get('comment_' + form_local_variable_name)

                # Store the data type, global variable name, and comment for the local variable in the session cache
                session_cache.descriptive_info[database][local_variable_name] = {
                    'type': f'Variable type: {data_type}',
                    'description': f'Variable description: {global_variable_name}',
                    'comments': f'Variable comment: {comment if comment else "No comment provided"}'
                }

                # If the data type of the local variable is 'Categorical Nominal' or 'Categorical Ordinal',
                # retrieve the categories for the local variable and store them in the session cache
                if data_type in ['Categorical Nominal', 'Categorical Ordinal']:
                    cat = retrieve_categories(session_cache.repo, local_variable_name)
                    df = pd.read_csv(StringIO(cat), sep=",", na_filter=False)
                    session_cache.DescriptiveInfoDetails[database].append(
                        {f'{global_variable_name} (or "{local_variable_name}")': df.to_dict('records')})
                # If the data type of the local variable is 'Continuous',
                # add the local variable to a list of variables to further specify
                elif data_type == 'Continuous':
                    session_cache.DescriptiveInfoDetails[database].append(
                        f'{global_variable_name} (or "{local_variable_name}")')
                else:
                    insert_equivalencies(session_cache.descriptive_info[database], local_variable_name)

        # Remove databases that do not have any descriptive information
        if not session_cache.DescriptiveInfoDetails[database]:
            del session_cache.DescriptiveInfoDetails[database]

    # Render the 'units.html' template with the list of variables to further specify
    if session_cache.DescriptiveInfoDetails:
        return redirect(url_for('variable_details'))
    else:
        # Redirect to the new route after processing the POST request
        return redirect(url_for('download_page'))


@app.route("/variable-details")
def variable_details():
    """
    This function is responsible for rendering the 'units.html' page.

    Returns:
        flask.render_template: A Flask function that renders the 'variable-details.html' template.
    """
    dataframes = {}
    preselected_values = {}

    # Iterate over the items in the dictionary
    for database, variables in session_cache.DescriptiveInfoDetails.items():
        # Initialize an empty list to hold the rows of the dataframe
        rows = []

        # Iterate over the variables
        for variable in variables:
            # Check if the variable is a string (continuous variable)
            if isinstance(variable, str):
                # Add a row to the dataframe with the column name as the variable name and the value as pd.NA
                rows.append({'column': variable, 'value': None})
            # Check if the variable is a dictionary (categorical variable)
            elif isinstance(variable, dict):
                # Iterate over the items in the variable dictionary
                for var_name, categories in variable.items():
                    # If this variable exists in the global schema
                    if isinstance(session_cache.global_schema, dict):
                        # Get global variable name (removing the local name in parentheses)
                        global_var = var_name.split(' (or')[0].lower().replace(' ', '_')

                        var_info = session_cache.global_schema['variable_info'].get(global_var, {})
                        value_mapping = var_info.get('value_mapping', {}).get('terms', {})

                        # Iterate over the categories
                        for category in categories:
                            # Find matching term in value_mapping
                            matching_term = None
                            category_value = category.get('value')

                            for term, term_info in value_mapping.items():
                                local_term = term_info.get('local_term')
                                # Convert both to strings for comparison
                                if str(local_term) == str(category_value):
                                    matching_term = term.title().replace('_', ' ')
                                    break

                            # Add preselected value to dictionary
                            if matching_term:
                                key = f"{database}_{var_info.get('local_definition', '')}_category_\"{category.get('value')}\""
                                preselected_values[key] = matching_term

                            # Add a row to the dataframe
                            rows.append({'column': var_name, 'value': category})
                    else:
                        # Iterate over the categories
                        for category in categories:
                            # Add a row to the dataframe for each category with
                            # the column name as the variable name and the value as the category
                            rows.append({'column': var_name, 'value': category})

        # Convert the list of rows to a dataframe
        df = pd.DataFrame(rows)

        # Add the dataframe to the result dictionary with the database name as the key
        dataframes[database] = df

    if isinstance(session_cache.global_schema, dict):
        variable_info = session_cache.global_schema.get('variable_info')
    else:
        variable_info = {}

    return render_template('units.html',
                           dataframes=dataframes,
                           global_variable_info=variable_info,
                           preselected_values=preselected_values)


@app.route("/end", methods=['GET', 'POST'])
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
    it retrieves the category and the associated value, comment and count from the request form and
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
            key.split('_category_')[0].split(f'{database}_')[1] if '_category_' in key else
            (key.split('_notation_missing_or_unspecified')[0].split(f'{database}_')[
                 1] if '_notation_missing_or_unspecified' in key else
             key.split(f'{database}_')[1])
            for key in keys]

        # Iterate over each unique variable
        for variable in set(variables):
            # Retrieve all keys from the request form that contain the variable name and do not start with 'comment_'
            keys = [key for key in request.form if variable in key and not key.startswith('comment_')
                    and not key.startswith('count_')]

            for key in keys:

                if '_notation_missing_or_unspecified' in key:
                    session_cache.descriptive_info[database][variable][f'Category: {request.form.get(key)}'] = (
                            f'Category {request.form.get(key)}: missing_or_unspecified' or
                            "No missing value notation provided")

                elif '_category_' in key and not key.startswith('count_'):
                    # Retrieve the category and the associated value and comment from the request form and
                    # store them in the session cache
                    category = key.split('_category_"')[1].split(f'"')[0]
                    count_form = f'count_{database}_{variable}_category_"{category}"'
                    session_cache.descriptive_info[database][variable][f'Category: {category}'] = \
                        (f'Category {category}: {request.form.get(key)}, comment: '
                         f'{request.form.get(f"comment_{key}") or "No comment provided"},  '
                         f'count: {request.form.get(count_form) or "No count available"}')
                # Handle units
                elif 'count_' not in key:
                    session_cache.descriptive_info[database][variable]['units'] = request.form.get(
                        key) or 'No units specified'

            # Call the 'insert_equivalencies' function to insert equivalencies into the GraphDB repository
            insert_equivalencies(session_cache.descriptive_info[database], variable)

    # Redirect the user to the 'download_page' URL
    return redirect(url_for('download_page'))


@app.route('/download')
def download_page():
    """
    This function is responsible for rendering the 'download.html' page.

    Returns:
        flask.render_template: A Flask function that renders the 'download.html' template.
    """
    if isinstance(session_cache.global_schema, dict) and isinstance(session_cache.descriptive_info, dict):
        return render_template('download.html',
                               graphdb_location="http://localhost:7200/", show_schema=True)
    else:
        return render_template('download.html',
                               graphdb_location="http://localhost:7200/", show_schema=False)


@app.route('/downloadSchema', methods=['GET'])
def download_schema():
    """
    This function generates a modified version of the global schema by adding local definitions to it.
    The modified schema is then returned as a JSON response which can be downloaded as a file.

    Parameters:
    filename (str): The name of the file to be downloaded. Defaults to 'local_schema_{database_name}.json'.

    Returns:
        flask.Response: A Flask response object containing the modified schema as a JSON string.
                        If an error occurs during the processing of the schema,
                        an HTTP response with a status code of 500 (Internal Server Error)
                        is returned along with a message describing the error.

    The function performs the following steps:
    1. Checks if there are multiple databases.
    2. If there are multiple databases, it creates a zip file named 'local_schemas.zip'.
    3. It then loops through each database,
       generates a modified version of the global schema by adding local definitions to it,
       and writes the modified schema to the zip file.
    4. After the request has been handled, it removes the zip file.
    5. If there is only one database,
       it generates a modified version of the global schema by adding local definitions to it,
       and returns the modified schema as a JSON response.
    6. If an error occurs during the processing of the schema,
       it returns an HTTP response with a status code of 500 (Internal Server Error)
       along with a message describing the error.
    """
    try:
        # Check if there are multiple databases
        if len(session_cache.databases) > 1:
            _filename = 'local_schemas.zip'
            # Loop through each database
            for database in session_cache.databases:
                filename = f'local_schema_{database}.json'

                # Open the zip file in append mode
                with zipfile.ZipFile(_filename, 'a') as zipf:
                    # Generate a modified version of the global schema by adding local definitions to it
                    modified_schema = formulate_local_schema(database)

                    # Write the modified schema to the zip file
                    zipf.writestr(filename, json.dumps(modified_schema, indent=4))

            # Define a function to remove the zip file after the request has been handled
            @after_this_request
            def remove_file(response):
                try:
                    os.remove(_filename)
                except Exception as error:
                    app.logger.error("Error removing or closing downloaded file handle", error)
                return response

            # Open the zip file in binary mode and return it as a response
            with open(_filename, 'rb') as f:
                return Response(f.read(), mimetype='application/zip',
                                headers={'Content-Disposition': f'attachment;filename={_filename}'})
        else:
            # If there is only one database
            database = session_cache.databases[0]
            filename = f'local_schema_{database}.json'

            try:
                # Generate a modified version of the global schema by adding local definitions to it
                modified_schema = formulate_local_schema(database)

                # Return the modified schema as a JSON response
                return Response(json.dumps(modified_schema, indent=4),
                                mimetype='application/json',
                                headers={'Content-Disposition': f'attachment;filename={filename}'})
            except Exception as e:
                abort(500, description=f"An error occurred while processing the schema, error: {str(e)}")

    except Exception as e:
        abort(500, description=f"An error occurred while processing the schema, error: {str(e)}")


@app.route('/downloadOntology', methods=['GET'])
def download_ontology(named_graph="http://ontology.local/", filename=None):
    """
    This function downloads an ontology from a specified graph and returns it as a response which
    can be downloaded as a file.

    Parameters:
    named_graph (str): The URL of the graph from which the ontology is to be downloaded.
    Defaults to "http://ontology.local/".
    filename (str): The name of the file to be downloaded. Defaults to 'local_ontology_{database_name}.nt'.

    Returns:
        flask.Response: A Flask response object containing the ontology as a string if the download is successful,
                        or an error message if the download fails.
                        If an error occurs during the processing of the request,
                        an HTTP response with a status code of 500 (Internal Server Error)
                         is returned along with a message describing the error.
    """
    if session_cache.csvPath is not None and session_cache.existing_graph is False:
        if len(session_cache.csvPath) == 1:
            database_name = session_cache.csvPath[0][session_cache.csvPath[0].rfind(os.path.sep) + 1:
                                                     session_cache.csvPath[0].rfind('.')]
        else:
            database_name = 'for_multiple_databases'
    else:
        database_name = 'for_multiple_databases'

    if filename is None:
        filename = f'local_ontology_{database_name}.nt'

    try:
        response = requests.get(
            f"{graphdb_url}/repositories/{session_cache.repo}/rdf-graphs/service",
            params={"graph": named_graph},
            headers={"Accept": "application/n-triples"}
        )

        if response.status_code == 200:
            return Response(response.text,
                            mimetype='application/n-triples',
                            headers={'Content-Disposition': f'attachment;filename={filename}'})

    except Exception as e:
        abort(500, description=f"An error occurred while processing the ontology, error: {str(e)}")


@app.route('/data_descriptor/assets/<path:filename>')
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
    return send_from_directory(f'{root_dir}{child_dir}/assets', filename)


def allowed_file(filename, allowed_extensions):
    """
    This function checks if the uploaded file has an allowed extension.

    Parameters:
    filename (str): The name of the file to be checked.
    allowed_extensions (set): A set of strings representing the allowed file extensions.

    Returns:
    bool: True if the file has an allowed extension, False otherwise.
    """
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in allowed_extensions


def check_graph_exists(repo, graph_uri):
    """
    This function checks if a graph exists in a GraphDB repository.

    Parameters:
    repo (str): The name of the repository in GraphDB.
    graph_uri (str): The URI of the graph to check.

    Returns:
    bool: True if the graph exists, False otherwise.

    Raises:
    Exception: If the request to the GraphDB instance fails,
    an exception is raised with the status code of the failed request.
    """
    # Construct the SPARQL query
    query = f"ASK WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"

    # Send a GET request to the GraphDB instance
    response = requests.get(
        f"{graphdb_url}/repositories/{repo}",
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )

    # If the request is successful, return the result of the ASK query
    if response.status_code == 200:
        return response.json()['boolean']
    # If the request fails, raise an exception with the status code
    else:
        raise Exception(f"Query failed with status code {response.status_code}")


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
    flask.render_template: A Flask function that renders the 'index.html' template
    if an error occurs during the query execution.

    Raises:
    Exception: If an error occurs during the query execution,
    an exception is raised and its error message is flashed to the user.

    The function performs the following steps:
    1. Checks if query_type and endpoint_appendices are None. If they are, sets them to their default values.
    2. Constructs the endpoint URL using the provided repository name and endpoint_appendices.
    3. Executes the SPARQL query on the constructed endpoint URL.
    4. If the query execution is successful, returns the result as a string.
    5. If an error occurs during the query execution,
    flashes an error message to the user and renders the 'index.html' template.
    """
    if query_type is None:
        query_type = "query"

    if endpoint_appendices is None:
        endpoint_appendices = ""
    try:
        # Construct the endpoint URL
        endpoint = f"{graphdb_url}/repositories/" + repo + endpoint_appendices
        # Execute the query
        response = requests.post(endpoint,
                                 data={query_type: query},
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
        # Return the result of the query execution
        return response.text
    except Exception as e:
        # If an error occurs, flash the error message to the user and render the 'index.html' template
        flash(f'Unexpected error when connecting to GraphDB, error: {e}.')
        return render_template('index.html')


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

    The function first checks if the global schema in the session cache is a dictionary.
    If it is not, it returns a list of default global variable names.
    If it is a dictionary, it attempts to retrieve the keys from the 'variable_info' field of the global schema,
    capitalise them, replace underscores with spaces, and return them as a list.
    If an error occurs during this process,
    it flashes an error message to the user and renders the 'index.html' template.

    Returns:
        list: A list of strings representing the names of the global variables.
        flask.render_template: A Flask function that renders a template.
        In this case, it renders the 'index.html' template if an error occurs.
    """
    if not isinstance(session_cache.global_schema, dict):
        return ['Research subject identifier', 'Biological sex', 'Age at inclusion', 'Other']
    else:
        try:
            return [name.capitalize().replace('_', ' ') for name in
                    session_cache.global_schema['variable_info'].keys()] + ['Other']
        except Exception as e:
            flash(f"Failed to read the global schema. Error: {e}")
            return render_template('index.html', error=True)


def formulate_local_schema(database):
    """
    This function modifies the global schema by adding local definitions to it.
    The modified schema is then returned.

    Parameters:
    database (str): The name of the database for which the local schema is to be formulated.

    Returns:
    dict: A dictionary representing the modified schema.

    The function performs the following steps:
    1. Creates a deep copy of the global schema stored in the session cache.
    2. Updates the 'database_name' field in the schema with the provided database name.
    3. Updates the 'variable_info' field in the schema. If a variable does not have a 'local_definition' field,
       it adds one with an empty string as its value.
    4. Adds local definitions to the schema. For each local variable in the session cache,
       it retrieves the corresponding global variable name, converts it to lowercase, replaces spaces with underscores,
       and adds the local variable name as the 'local_definition' for the global variable in the schema.
    5. Adds local terms to the schema. For each local variable in the session cache,
       it retrieves the corresponding value mapping terms, converts it to lowercase, replaces spaces with underscores,
       and adds the local term name as the 'local_term' for the global term in the schema.
    6. Returns the modified schema.
    """
    # Create a deep copy of the global schema
    modified_schema = copy.deepcopy(session_cache.global_schema)

    # Update the 'database_name' field in the schema
    if isinstance(modified_schema.get('database_name'), str):
        modified_schema['database_name'] = database
    else:
        modified_schema.update({'database_name': database})

    # Update the 'variable_info' field in the schema
    modified_schema['variable_info'] = \
        {variable_name: variable_info if isinstance(variable_info.get('local_definition'), str) else {
            'local_definition': ""}
         for variable_name, variable_info in modified_schema['variable_info'].items()}

    modified_schema['variable_info'] = {}

    # Add local definitions to the schema
    for local_variable, local_value in session_cache.descriptive_info[database].items():
        global_variable = local_value['description'].split('Variable description: ')[1].lower().replace(' ', '_')
        if global_variable:
            # Add the global variable to a temporary schema if it is selected as a local variable
            _schema_info = {global_variable:
                                copy.deepcopy(session_cache.global_schema['variable_info'].get(global_variable, {}))}

            # Specify the local definition for the global variable
            _schema_info[global_variable]['local_definition'] = local_variable

            # Create a clean value_mapping section
            _value_map = {}

            # Update the 'value_mapping' field in the schema
            # For each category of the local variable,
            # find the corresponding term in the global variable and set its 'local_term' to the category
            for category, value in local_value.items():
                if category.startswith('Category: '):
                    global_term = value.split(': ')[1].split(', comment')[0].lower().replace(' ', '_')

                    if global_term:
                        # Add the global variable's value mapping to the schema if it is selected as a local value
                        __value_map = {global_term: copy.deepcopy(
                            session_cache.global_schema['variable_info'][global_variable]['value_mapping']['terms'].get(
                                global_term,
                                {
                                    "local_term": "",
                                    "target_class": "t.b.d."
                                }
                            )
                        )}

                        # Set the local term for the global term
                        __value_map[global_term]['local_term'] = category.split(': ')[1]

                        # Handling duplicate global terms by appending a suffix
                        if global_term in _value_map:
                            suffix = 1
                            new_global_term = f"{global_term}_{suffix}"
                            while new_global_term in _value_map:
                                suffix += 1
                                new_global_term = f"{global_term}_{suffix}"
                        else:
                            new_global_term = global_term

                        # Copy the specific value map to the general mapping, ensuring duplicates are handled correctly
                        __value_map[new_global_term] = __value_map.pop(global_term)
                        _value_map.update(copy.deepcopy(__value_map))
                        __value_map.clear()

            # Place the value mapping section in the schema
            if _value_map:
                _schema_info[global_variable]['value_mapping'] = copy.deepcopy(_value_map)

            # Handling duplicate global variables by appending a suffix
            if global_variable in modified_schema['variable_info']:
                suffix = 1
                new_global_variable = f"{global_variable}_{suffix}"
                while new_global_variable in modified_schema['variable_info']:
                    suffix += 1
                    new_global_variable = f"{global_variable}_{suffix}"

            else:
                new_global_variable = global_variable

            # Append the new global variable to the actual schema
            modified_schema['variable_info'][new_global_variable] = copy.deepcopy(_schema_info[global_variable])

    return modified_schema


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
    flask.Response: A Flask response object containing the rendered 'index.html' template if
                    the connection to the PostgreSQL database fails.
    None: If the connection to the PostgreSQL database is successful.
    """
    # Cache information
    session_cache.username, session_cache.password, session_cache.url, session_cache.db_name, session_cache.table = (
        username, password, postgres_url, postgres_db, table)

    try:
        # Establish PostgreSQL connection
        session_cache.conn = connect(dbname=session_cache.db_name, user=session_cache.username,
                                     host=session_cache.url,
                                     password=session_cache.password)
        print("Connection:", session_cache.conn)
    except Exception as err:
        print("connect() ERROR:", err)
        session_cache.conn = None
        flash('Attempting to connect to PostgreSQL datasource unsuccessful. Please check your details!')
        return render_template('index.html', error=True)

    # Write connection details to properties file
    with open(f"{root_dir}{child_dir}/triplifierSQL.properties", "w") as f:
        f.write(f"jdbc.url = jdbc:postgresql://{session_cache.url}/{session_cache.db_name}\n"
                f"jdbc.user = {session_cache.username}\n"
                f"jdbc.password = {session_cache.password}\n"
                f"jdbc.driver = org.postgresql.Driver\n\n"
                # f"repo.type = rdf4j\n"
                # f"repo.url = {graphdb_url}\n"
                # f"repo.id = {repo}"
        )


def insert_equivalencies(descriptive_info, variable):
    """
    This function inserts equivalencies into a GraphDB repository.

    Parameters:
    descriptive_info (dict): A dictionary containing descriptive information about the variables.
                             The keys are the variable names and the values are dictionaries containing
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
    type_value = var_info.get('type', '')
    description_value = var_info.get('description', '')
    comments_value = var_info.get('comments', '')

    # Check if any of these fields has meaningful content
    has_type = type_value not in ['', 'Variable type: ', 'Variable type: None']
    has_description = description_value not in ['', 'Variable description: ', 'Variable description: None']
    has_comments = comments_value not in ['', 'Variable comment: No comment provided']

    # Skip if none of the fields has meaningful content
    if not (has_type or has_description or has_comments):
        return None


    query = f"""
                PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
                PREFIX db: <http://{session_cache.repo}.local/rdf/ontology/>
                PREFIX roo: <http://www.cancerdata.org/roo/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>

                INSERT  
                {{
                    GRAPH <http://ontology.local/>
                    {{ ?s owl:equivalentClass "{list(var_info.values())}". }}
                }}
                WHERE 
                {{
                    ?s dbo:column '{variable}'.
                }}        
            """
    return execute_query(session_cache.repo, query, "update", "/statements")


def generate_fk_predicate(fk_config, base_uri):
    """Generate FK predicate URI using the same base as column class URIs"""
    return (
        f"{base_uri}{fk_config['foreignKeyTable']}_{fk_config['foreignKeyColumn']}_refersTo_"
        f"{fk_config['primaryKeyTable']}_{fk_config['primaryKeyColumn']}"
    )


def extract_base_uri(column_class_uri):
    """Extract base URI from column class URI"""
    # Example: http://data.local/rdf/ontology/patient_data_b.id
    # Returns: http://data.local/rdf/ontology/
    if column_class_uri:
        # Find the last slash and extract everything before it + slash
        last_slash = column_class_uri.rfind('/')
        if last_slash != -1:
            return column_class_uri[:last_slash + 1]
    return "http://data.local/rdf/ontology/"  # fallback


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

        column_info = pd.read_csv(StringIO(query_result))

        if column_info.empty:
            print(f"Empty result set for column {table_name}.{column_name}")
            return None

        if 'uri' not in column_info.columns:
            print(f"Query result format error: no 'uri' column found")
            return None

        return column_info['uri'].iloc[0]

    except Exception as e:
        print(f"Error fetching column URI for {table_name}.{column_name}: {e}")
        return None


def insert_fk_relation(fk_predicate, column_class_uri, target_class_uri):
    """Insert PK/FK relationship into the data graph"""
    insert_query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT {{
        ?sources <{fk_predicate}> ?targets .
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
        file_map = {rel['fileName']: rel for rel in session_cache.pk_fk_data}

        # Process each relationship
        for rel in session_cache.pk_fk_data:
            if not all([rel.get('foreignKey'), rel.get('foreignKeyTable'),
                        rel.get('foreignKeyColumn')]):
                continue

            # Find the target table's PK info
            target_file = rel['foreignKeyTable']
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get('primaryKey'):
                continue

            # Generate FK configuration
            fk_config = {
                'foreignKeyTable': rel['fileName'].replace('.csv', ''),
                'foreignKeyColumn': rel['foreignKey'],
                'primaryKeyTable': target_file.replace('.csv', ''),
                'primaryKeyColumn': target_rel['primaryKey']
            }

            source_uri = get_column_class_uri(
                fk_config['foreignKeyTable'],
                fk_config['foreignKeyColumn']
            )

            target_uri = get_column_class_uri(
                fk_config['primaryKeyTable'],
                fk_config['primaryKeyColumn']
            )

            if not source_uri or not target_uri:
                print(f"Could not find URIs for FK relationship: {fk_config}")
                continue

            # Extract base URI from the source column URI and generate predicate
            base_uri = extract_base_uri(source_uri)
            fk_predicate = generate_fk_predicate(fk_config, base_uri)

            # Insert the relationship
            insert_fk_relation(fk_predicate, source_uri, target_uri)
            print(
                f"Created FK relationship: {fk_config['foreignKeyTable']}.{fk_config['foreignKeyColumn']} -> {fk_config['primaryKeyTable']}.{fk_config['primaryKeyColumn']}")

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


@app.route('/get-existing-graph-structure', methods=['GET'])
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

        # Parse the result
        structure_info = pd.read_csv(StringIO(result))

        if structure_info.empty:
            return {"tables": [], "tableColumns": {}}

        # Extract table names from URIs and organize by table
        structure_info['table'] = structure_info['uri'].str.extract(r'.*/(.*?)\.', expand=False).fillna('unknown')

        # Get unique tables
        tables = structure_info['table'].unique().tolist()

        # Create table-column mapping
        table_columns = {}
        for table in tables:
            columns = structure_info[structure_info['table'] == table]['column'].tolist()
            table_columns[table] = columns

        return jsonify({
            "tables": tables,
            "tableColumns": table_columns
        })

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

        column_info = pd.read_csv(StringIO(query_result))

        if column_info.empty or 'uri' not in column_info.columns:
            return None

        return column_info['uri'].iloc[0]

    except Exception as e:
        print(f"Error fetching existing column URI for {table_name}.{column_name}: {e}")
        return None


def generate_cross_graph_predicate(new_table, new_column, existing_table, existing_column, base_uri):
    """Generate cross-graph predicate URI"""
    return f"{base_uri}{new_table}_{new_column}_refersTo_{existing_table}_{existing_column}"


def insert_cross_graph_relation(predicate, new_column_uri, existing_column_uri):
    """Insert cross-graph relationship into the data graph"""
    insert_query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT {{
        ?newSources <{predicate}> ?existingSources .
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
            link_data['newTableName'],
            link_data['newColumnName']
        )

        existing_column_uri = get_existing_column_class_uri(
            link_data['existingTableName'],
            link_data['existingColumnName']
        )

        if not new_column_uri or not existing_column_uri:
            print(f"Could not find URIs for cross-graph relationship: {link_data}")
            session_cache.cross_graph_link_status = "failed"
            return False

        # Generate predicate
        base_uri = extract_base_uri(new_column_uri)
        predicate = generate_cross_graph_predicate(
            link_data['newTableName'],
            link_data['newColumnName'],
            link_data['existingTableName'],
            link_data['existingColumnName'],
            base_uri
        )

        # Insert the relationship
        insert_cross_graph_relation(predicate, new_column_uri, existing_column_uri)

        print(
            f"Created cross-graph relationship: {link_data['newTableName']}.{link_data['newColumnName']} -> {link_data['existingTableName']}.{link_data['existingColumnName']}")

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
    This function runs the triplifier and checks if it ran successfully.

    Parameters:
    properties_file (str): The name of the properties file to be used by the triplifier.
                           It can be either 'triplifierCSV.properties' for CSV files or
                           'triplifierSQL.properties' for SQL files.
                           Defaults to None.

    Returns:
        tuple: A tuple containing a boolean indicating if the triplifier ran successfully,
        and a string containing the error message if it did not.
    """
    try:
        if properties_file == 'triplifierCSV.properties':
            if not os.access(app.config['UPLOAD_FOLDER'], os.W_OK):
                return False, "Unable to temporarily save the CSV file: no write access to the application folder."

            # Allow easier debugging outside Docker
            if len(root_dir) == 0 and child_dir == '.':
                # Read the properties file and replace the jdbc.url line
                with open('triplifierCSV.properties', "r") as f:
                    lines = f.readlines()

                modified_lines = [
                    line.replace(
                        "jdbc.url = jdbc:relique:csv:/app/data_descriptor/static/files?fileExtension=.csv",
                        "jdbc.url = jdbc:relique:csv:./static/files?fileExtension=.csv"
                    ) for line in lines
                ]

                # Write the modified content back to the properties file
                with open('triplifierCSV.properties', "w") as f:
                    f.writelines(modified_lines)

            for i, csv_data in enumerate(session_cache.csvData):
                csv_path = session_cache.csvPath[i]
                csv_data.to_csv(csv_path, index=False, sep=',', decimal='.', encoding='utf-8')

        process = subprocess.Popen(
            f"java -jar {root_dir}{child_dir}/javaTool/triplifier.jar -p {root_dir}{child_dir}/{properties_file}",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output, _ = process.communicate()
        print(output.decode())

        if properties_file == 'triplifierCSV.properties':
            # Allow easier debugging outside Docker
            if len(root_dir) == 0 and child_dir == '.':
                # Read the properties file and replace the jdbc.url line
                with open('triplifierCSV.properties', "r") as f:
                    lines = f.readlines()

                modified_lines = [
                    line.replace(
                        "jdbc.url = jdbc:relique:csv:./static/files?fileExtension=.csv",
                        "jdbc.url = jdbc:relique:csv:/app/data_descriptor/static/files?fileExtension=.csv"
                    ) for line in lines
                ]

                # Write the modified content back to the properties file
                with open('triplifierCSV.properties', "w") as f:
                    f.writelines(modified_lines)

        if process.returncode == 0:
            # START BACKGROUND PK/FK PROCESSING FOR CSV FILES
            if properties_file == 'triplifierCSV.properties' and session_cache.pk_fk_data:
                print("Triplifier successful. Starting background PK/FK processing...")
                threading.Thread(target=background_pk_fk_processing, daemon=True).start()

            # START BACKGROUND CROSS-GRAPH PROCESSING FOR CSV FILES - Add this block
            if properties_file == 'triplifierCSV.properties' and session_cache.cross_graph_link_data:
                print("Triplifier successful. Starting background cross-graph processing...")
                threading.Thread(target=background_cross_graph_processing, daemon=True).start()

            return True, Markup("The data you have submitted was triplified successfully and "
                                "is now available in GraphDB."
                                "<br>"                                
                                "You can now proceed to describe your data, "
                                "but please note that this requires in-depth knowledge of the data."
                                "<br><br>"
                                "<i>In case you do not yet wish to describe your data, "
                                "or you would like to add more data, "
                                "please return to the welcome page.</i>"
                                "<br>"
                                "<i>You can always return to Flyover to "
                                    "describe the data that is present in GraphDB.</i>")
        else:
            return False, output
    except OSError as e:
        return False, f'Unexpected error attempting to create the upload folder, error: {e}'
    except Exception as e:
        return False, f'Unexpected error attempting to run the Triplifier, error: {e}'


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
