import copy
import json
import os
import re
import zipfile

import requests
import subprocess

import pandas as pd

from flask import abort, after_this_request, Flask, redirect, render_template, request, flash, Response, url_for
from io import StringIO
from markupsafe import Markup
from psycopg2 import connect
from werkzeug.utils import secure_filename

graphdb_url = "http://rdf-store:7200"

app = Flask(__name__)
app.secret_key = "secret_key"
# enable debugging mode
app.config["DEBUG"] = False
app.config['UPLOAD_FOLDER'] = os.path.join('data_descriptor', 'static', 'files')
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])


class Cache:
    def __init__(self):
        self.repo = 'userRepo'
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
    1. It retrieves the file type, JSON file, and CSV file from the form data.
    2. If a JSON file is provided and its file extension is allowed, it uploads and saves the file,
     and stores the file path in the session cache.
    3. If the file type is 'CSV' and a CSV file is provided and its file extension is allowed,
    it uploads and saves the file, stores the file path in the session cache, and runs the triplifier.
    4. If the file type is 'Postgres', it handles the PostgreSQL data using the provided username, password, URL,
     database name, and table name, and runs the triplifier.
    5. It returns a response indicating whether the triplifier run was successful.

    Returns:
        flask.Response: A Flask response object containing the rendered 'triples.html' template
         if the triplifier run was successful, or the 'index.html' template if it was not.
    """
    file_type = request.form.get('fileType')
    json_file = request.files.get('jsonFile') or request.files.get('jsonFile2')
    csv_file = request.files.get('csvFile')

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

    if file_type == 'CSV' and csv_file:
        if allowed_file(csv_file.filename, {'csv'}) is False:
            flash("If opting to submit a CSV data source, please upload it as a '.csv' file.")
            return render_template('index.html', error=True)

        try:
            session_cache.csvData = pd.read_csv(csv_file)

        except Exception as e:
            flash(f"Unexpected error attempting to cache the CSV data, error: {e}")
            return render_template('index.html', error=True)

        session_cache.csvPath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(csv_file.filename))
        try:
            success, message = run_triplifier('triplifierCSV.properties')
        finally:
            if os.path.exists(session_cache.csvPath):
                os.remove(session_cache.csvPath)

    elif file_type == 'Postgres':
        handle_postgres_data(request.form.get('username'), request.form.get('password'),
                             request.form.get('POSTGRES_URL'), request.form.get('POSTGRES_DB'),
                             request.form.get('table'))
        success, message = run_triplifier('triplifierSQL.properties')

    elif json_file and file_type != 'Postgres' and not csv_file:
        success = True
        message = Markup("You have opted to not submit any new data, "
                         "you can now proceed to describe your data using the global schema that you have provided."
                         "<br>"
                         "<i>In case you do wish to submit data, please return to the welcome page.</i>")

    elif not json_file and file_type != 'Postgres' and not csv_file:
        success = True
        message = Markup("You have opted to not submit any new data, "
                         "you can now proceed to describe your data."
                         "<br>"
                         "<i>In case you do wish to submit data, please return to the welcome page.</i>")

    else:
        success = False
        message = "An unexpected error occurred. Please try again."

    if success:
        session_cache.StatusToDisplay = message
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

    # Render the 'categories.html' template with the dictionary of dataframes and the global variable names
    return render_template('categories.html',
                           dataframes=dataframes, global_variable_names=global_names)


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
                    df = pd.read_csv(StringIO(cat), sep=",")
                    session_cache.DescriptiveInfoDetails[database].append(
                        {f'{global_variable_name} (or "{local_variable_name}")': df.to_dict('records')})
                # If the data type of the local variable is 'Continuous',
                # add the local variable to a list of variables to further specify
                elif data_type == 'Continuous':
                    session_cache.DescriptiveInfoDetails[database].append(
                        f'{global_variable_name} (or "{local_variable_name}")')
                else:
                    insert_equivalencies(session_cache.descriptive_info[database], local_variable_name)

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

    return render_template('units.html', dataframes=dataframes,
                           global_variable_info=variable_info)


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
            key.split('_category_')[0].split(f'{database}_')[1] if '_category_' in key else key.split(f'{database}_')[1]
            for key in keys]

        # Iterate over each unique variable
        for variable in set(variables):
            # Retrieve all keys from the request form that contain the variable name and do not start with 'comment_'
            keys = [key for key in request.form if variable in key and not key.startswith('comment_')
                    and not key.startswith('count_')]
            # If there is only one key
            if len(keys) == 1:
                # Retrieve the value associated with this key from the request form and
                # store it in the 'units' field of the variable in the session cache
                session_cache.descriptive_info[database][variable]['units'] = request.form.get(
                    keys[0]) or 'No units specified'
            else:
                # If there are multiple keys, iterate over each key
                for key in keys:
                    # If the key contains '_category_'
                    if '_category_' in key and not key.startswith('count_'):
                        # Retrieve the category and the associated value and comment from the request form and
                        # store them in the session cache
                        category = key.split('_category_"')[1].split(f'"')[0]
                        count_form = f'count_{database}_{variable}_category_"{category}"'
                        session_cache.descriptive_info[database][variable][f'Category: {category}'] = \
                            (f'Category {category}: {request.form.get(key)}, comment: '
                             f'{request.form.get(f"comment_{key}") or "No comment provided"},  '
                             f'count: {request.form.get(count_form) or "No count available"}')

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
    if session_cache.csvPath is not None or session_cache.existing_graph is False:
        database_name = session_cache.csvPath[session_cache.csvPath.rfind(os.path.sep) + 1:
                                              session_cache.csvPath.rfind('.')]
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

    # Add local definitions to the schema
    for local_variable, local_value in session_cache.descriptive_info[database].items():
        global_variable = local_value['description'].split('Variable description: ')[1].lower().replace(' ', '_')
        if global_variable:
            modified_schema['variable_info'][global_variable]['local_definition'] = local_variable

        # Update the 'value_mapping' field in the schema
        # For each category of the local variable,
        # find the corresponding term in the global variable and set its 'local_term' to the category
        for category, value in local_value.items():
            if category.startswith('Category: '):
                key = value.split(': ')[1].split(', comment')[0].lower().replace(' ', '_')
                if 'value_mapping' in modified_schema['variable_info'][global_variable] and \
                        'terms' in modified_schema['variable_info'][global_variable]['value_mapping'] and \
                        key in modified_schema['variable_info'][global_variable]['value_mapping']['terms']:
                    modified_schema['variable_info'][global_variable]['value_mapping']['terms'][key]['local_term'] =\
                        category.split(': ')[1]

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
    with open("/app/data_descriptor/triplifierSQL.properties", "w") as f:
        f.write(f"jdbc.url = jdbc:postgresql://{session_cache.url}/{session_cache.db_name}\n"
                f"jdbc.user = {session_cache.username}\n"
                f"jdbc.password = {session_cache.password}\n"
                f"jdbc.driver = org.postgresql.Driver\n\n"
                f"repo.type = rdf4j\n"
                f"repo.url = {graphdb_url}\n"
                f"repo.id = userRepo")


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
    query = f"""
                PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
                PREFIX db: <http://{session_cache.repo}.local/rdf/ontology/>
                PREFIX roo: <http://www.cancerdata.org/roo/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>

                INSERT  
                {{
                    GRAPH <http://ontology.local/>
                    {{ ?s owl:equivalentClass "{list(descriptive_info[variable].values())}". }}
                }}
                WHERE 
                {{
                    ?s dbo:column '{variable}'.
                }}        
            """
    return execute_query(session_cache.repo, query, "update", "/statements")


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

            session_cache.csvData.to_csv(session_cache.csvPath, index=False)

        process = subprocess.Popen(
            f"java -jar /app/data_descriptor/javaTool/triplifier.jar -p /app/data_descriptor/{properties_file}",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        output, _ = process.communicate()
        print(output.decode())

        if process.returncode == 0:
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
    # app.run(port = 5001)
    app.run(host='0.0.0.0')