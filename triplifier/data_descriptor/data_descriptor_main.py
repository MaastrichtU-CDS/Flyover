import copy
import json
import os
import re
import requests
import subprocess

import pandas as pd

from flask import abort, Flask, render_template, request, flash, Response
from io import StringIO
from psycopg2 import connect
from werkzeug.utils import secure_filename

graphdb_url = "http://rdf-store:7200"

app = Flask(__name__)
app.secret_key = "secret_key"
# enable debugging mode
app.config["DEBUG"] = True
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


session_cache = Cache()


@app.route('/')
def index():
    """
    This function is responsible for rendering the index.html page. It is mapped to the root URL ("/") of the Flask application.

    The function first checks if a data graph already exists in the GraphDB repository. If it does, the index.html page is rendered with a flag indicating that the graph exists. If the graph does not exist or if an error occurs during the check, the index.html page is rendered without the flag.

    Returns:
        flask.render_template: A Flask function that renders a template. In this case, it renders the 'index.html' template.

    Raises:
        Exception: If an error occurs while checking if the data graph exists,
        an exception is raised and its error message is flashed to the user.
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
    Exception: If the request to the GraphDB instance fails, an exception is raised with the status code of the failed request.
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
        message = "Global schema was submitted successfully, please proceed to describe the data."

    elif not json_file and file_type != 'Postgres' and not csv_file:
        success = True
        message = "Note that no data or global schema was submitted, please proceed to describe the data with caution."

    else:
        success = False
        message = "An unexpected error occurred. Please try again."

    if success:
        return render_template('triples.html', variable=message)
    else:
        flash(f"Attempting to proceed resulted in an error: {message}")
        return render_template('index.html', error=True, graph_exists=session_cache.existing_graph)


def run_triplifier(properties_file=None):
    """
    This function runs the triplifier and checks if it ran successfully.

    Parameters:
    properties_file (str): The name of the properties file to be used by the triplifier.
                           It can be either 'triplifierCSV.properties' for CSV files or 'triplifierSQL.properties' for SQL files.
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
            return True, "Triplifier ran successfully!"
        else:
            return False, output
    except OSError as e:
        return False, f'Unexpected error attempting to create the upload folder, error: {e}'
    except Exception as e:
        return False, f'Unexpected error attempting to run the Triplifier, error: {e}'


def handle_postgres_data(username, password, postgres_url, postgres_db, table):
    """
    This function handles the PostgreSQL data. It caches the provided information, establishes a connection to the PostgreSQL database, and writes the connection details to a properties file.

    Parameters:
    username (str): The username for the PostgreSQL database.
    password (str): The password for the PostgreSQL database.
    postgres_url (str): The URL of the PostgreSQL database.
    postgres_db (str): The name of the PostgreSQL database.
    table (str): The name of the table in the PostgreSQL database.

    Returns:
    flask.Response: A Flask response object containing the rendered 'index.html' template if the connection to the PostgreSQL database fails.
    None: If the connection to the PostgreSQL database is successful.
    """
    # Cache information
    session_cache.username, session_cache.password, session_cache.url, session_cache.db_name, session_cache.table = username, password, postgres_url, postgres_db, table

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


# Get the uploaded files
@app.route("/repo", methods=['POST'])
def queryresult():
    queryColumn = """
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        select ?o where { 
        ?s dbo:column ?o .
    }
    """

    def queryresult(repo, query):
        try:
            endpoint = f"{graphdb_url}/repositories/" + repo
            annotationResponse = requests.post(endpoint,
                                               data="query=" + query,
                                               headers={
                                                   "Content-Type": "application/x-www-form-urlencoded",
                                                   # "Accept": "application/json"
                                               })
            output = annotationResponse.text
            return output

        except Exception as err:
            flash('Connection unsuccessful. Please check your details!')
            return render_template('index.html')

    columns = queryresult(session_cache.repo, queryColumn)
    local_column_names = pd.read_csv(StringIO(columns))
    local_column_names = local_column_names[local_column_names.columns[0]]

    # if a global schema was defined, read it
    if isinstance(session_cache.global_schema, dict) is False:
        global_names = ['Research subject identifier', 'Biological sex', 'Age at inclusion', 'Other']
        session_cache.global_schema = None
    else:
        try:
            # get the global variable names
            global_names = [global_name.capitalize().replace('_', ' ')
                            for global_name in session_cache.global_schema['variable_info'].keys()]

            # add an option to select 'Other'
            global_names.append('Other')
        except Exception as e:
            flash(f"Failed to read the global schema. Error: {e}")
            return render_template('index.html', error=True)

    return render_template('categories.html', local_variable_names=local_column_names,
                           global_variable_names=global_names)


@app.route("/units", methods=['POST'])
def units():
    conList = []
    session_cache.mydict = {}
    for local_variable_name in request.form:
        if not re.search("^ncit_comment_", local_variable_name):
            session_cache.mydict[local_variable_name] = {}
            data_type = request.form.get(local_variable_name)
            global_variable_name = request.form.get('ncit_comment_' + local_variable_name)
            comment = request.form.get('comment_' + local_variable_name)
            session_cache.mydict[local_variable_name]['type'] = data_type
            session_cache.mydict[local_variable_name]['description'] = global_variable_name
            session_cache.mydict[local_variable_name]['comments'] = comment
            if data_type == 'Categorical Nominal' or data_type == 'Categorical Ordinal':
                cat = getCategories(session_cache.repo, local_variable_name)
                TESTDATA = StringIO(cat)
                df = pd.read_csv(TESTDATA, sep=",")
                df = df.to_dict('records')
                session_cache.mydict[local_variable_name]['categories'] = df
                equivalencies(session_cache.mydict, local_variable_name)
            elif data_type == 'Continuous':
                conList.append(local_variable_name)
            else:
                equivalencies(session_cache.mydict, local_variable_name)

    return render_template('units.html', variable=conList)


def getCategories(repo, key):
    queryCategories = """
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        PREFIX db: <http://'%s'.local/rdf/ontology/>
        PREFIX roo: <http://www.cancerdata.org/roo/>
        SELECT ?value (COUNT(?value) as ?count)
        WHERE 
        {  
           ?a a ?v.
           ?v dbo:column '%s'.
           ?a dbo:has_cell ?cell.
           ?cell dbo:has_value ?value
        } groupby(?value)
        """ % (repo, key)

    endpoint = f"{graphdb_url}/repositories/" + repo
    annotationResponse = requests.post(endpoint,
                                       data="query=" + queryCategories,
                                       headers={
                                           "Content-Type": "application/x-www-form-urlencoded",
                                           # "Accept": "application/json"
                                       })
    output = annotationResponse.text
    return output


@app.route("/end", methods=['POST'])
def unitNames():
    # items = getColumns(file_path)
    for key in request.form:
        unitValue = request.form.get(key)
        if unitValue != "":
            session_cache.mydict[key]['units'] = unitValue
        equivalencies(session_cache.mydict, key)

    # try to fetch the global schema if it was read previously
    if isinstance(session_cache.global_schema, dict) and isinstance(session_cache.mydict, dict):
        return render_template('download.html',
                               graphdb_location="http://localhost:7200/", show_schema=True)
    else:
        return render_template('download.html',
                               graphdb_location="http://localhost:7200/", show_schema=False)


@app.route('/downloadSchema', methods=['GET'])
def download_schema(filename=None):
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
    """
    database_name = session_cache.csvPath[session_cache.csvPath.rfind(os.path.sep) + 1:
                                          session_cache.csvPath.rfind('.')]

    if filename is None:
        filename = f'local_schema_{database_name}.json'

    try:
        modified_schema = copy.deepcopy(session_cache.global_schema)

        if isinstance(modified_schema.get('database_name'), str):
            modified_schema['database_name'] = database_name
        else:
            modified_schema.update({'database_name': database_name})

        modified_schema['variable_info'] = \
            {variable_name: variable_info if isinstance(variable_info.get('local_definition'), str) else {
                'local_definition': ""}
             for variable_name, variable_info in modified_schema['variable_info'].items()}

        for local_variable, local_value in session_cache.mydict.items():
            global_variable = local_value['description'].lower().replace(' ', '_')
            if global_variable:
                modified_schema['variable_info'][global_variable]['local_definition'] = local_variable

        return Response(json.dumps(modified_schema, indent=4),
                        mimetype='application/json',
                        headers={'Content-Disposition': f'attachment;filename={filename}'})
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
    database_name = session_cache.csvPath[session_cache.csvPath.rfind(os.path.sep) + 1:
                                          session_cache.csvPath.rfind('.')]

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


def equivalencies(mydict, key):
    query = """
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        PREFIX db: <http://%s.local/rdf/ontology/>
        PREFIX roo: <http://www.cancerdata.org/roo/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>

        INSERT  
            {
            GRAPH <http://ontology.local/>
            { ?s owl:equivalentClass "%s". }}
        WHERE 
            {
            ?s dbo:column '%s'.
            }        
    """ % (session_cache.repo, list(mydict[key].values()), key)

    endpoint = f"{graphdb_url}/repositories/" + session_cache.repo + "/statements"
    annotationResponse = requests.post(endpoint,
                                       data="update=" + query,
                                       headers={
                                           "Content-Type": "application/x-www-form-urlencoded",
                                           # "Accept": "application/json"
                                       })
    output = annotationResponse.text
    print(output)


if (__name__ == "__main__"):
    # app.run(port = 5001)
    app.run(host='0.0.0.0')
