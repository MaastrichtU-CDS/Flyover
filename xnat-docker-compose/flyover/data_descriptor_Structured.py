from flask import Flask, render_template, request, flash
import requests
import re
import pandas as pd
from io import StringIO
import os
from psycopg2 import connect
import subprocess
import shutil

app = Flask(__name__)
app.secret_key = "secret_key"
# enable debugging mode
app.config["DEBUG"] = True
UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


class Cache:
    mydict = {}
    repo = 'userRepo'  # default added repository in RDF-store
    file_path = None
    table = None
    url = None
    username = None
    password = None
    db_name = None
    conn = None
    col_cursor = None
    csvPath = False
    uploaded_file = None


v = Cache()


# Root URL
@app.route('/')
def index():
    return render_template('index.html')


def allowed_log_file(filename):
    # We only want files with a .csv in the filename
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['csv']


# Get the uploaded files
@app.route("/csv", methods=['POST'])
def uploadFiles():
    # get the uploaded file
    if not request.files:
        flash('No file selected for uploading!')
        return render_template('index.html')
    v.uploaded_file = request.files['file']

    if v.uploaded_file.filename == '':
        flash('No file selected for uploading!')
        return render_template('index.html')

    if v.uploaded_file and allowed_log_file(v.uploaded_file.filename):
        folder = UPLOAD_FOLDER
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
        # save the file in the upload folder
        #v.file_path = os.path.join(UPLOAD_FOLDER, "data.csv")
        v.file_path = os.path.join(UPLOAD_FOLDER, v.uploaded_file.filename)
        # set the file path
        v.uploaded_file.save(v.file_path)
        v.csvPath = True

        try:
            # run triplifier on the uploaded file
            args1 = "java -jar javaTool/triplifier.jar -p triplifierCSV.properties"
            print(args1)
            command_run = subprocess.call(args1, shell=True)
        except Exception as err:
            print(err)
            message = "Triplifier run Unsucessful"
            flash(err)
        if command_run == 0:
            message = "Triplifier run successful!"
        else:
            message = "Triplifier run Unsuccessful!"
        return render_template('triples.html', variable=message)

    else:
        flash('The only allowed file type is CSV!')
        return render_template('index.html')


@app.route("/postgres", methods=['POST'])
def getCredentials():
    # get details for postgres connection
    v.username = request.form.get('username')
    v.password = request.form.get('password')
    v.url = request.form.get('POSTGRES_URL')
    v.db_name = request.form.get('POSTGRES_DB')
    v.table = request.form.get('table')

    try:
        # declare a new PostgreSQL connection object
        v.conn = connect(
            dbname=v.db_name,
            user=v.username,
            host=v.url,
            password=v.password
        )
        # print the connection if successful
        print("Connection:", v.conn)

    except Exception as err:
        print("connect() ERROR:", err)
        v.conn = None
        flash('Connection unsuccessful. Please check your details!')
        return render_template('index.html')

    try:
        # run triplifier on the SQL table
        f = open("triplifierSQL.properties", "w")
        f.write(
            "jdbc.url = jdbc:postgresql://" + v.url + "/" + v.db_name + "\njdbc.user = " + v.username + "\njdbc.password = " + v.password + "\njdbc.driver = org.postgresql.Driver\n\n"
                                                                                                                                            "repo.type = rdf4j\nrepo.url = http://rdf-store:7200\nrepo.id = userRepo")
        f.close()
        args2 = "java -jar javaTool/triplifier.jar -p triplifierSQL.properties"
        #args2 = "docker run --rm --hostname user_data.local --network custom_network -v $(pwd)/triplifierSQL.properties:/triplifier.properties registry.gitlab.com/um-cds/fair/tools/triplifier:1.1.0"
        print(args2)
        command_run = subprocess.call(args2, shell=True)
    except Exception as err:
        print(err)
        message = "Triplifier run Unsucessful"
        flash(err)
    if command_run == 0:
        message = "Triplifier run successful!"
    else:
        message = "Triplifier run Unsuccessful!"
    return render_template('triples.html', variable=message)


# Get the uploaded files
@app.route("/repo", methods=['POST'])
def queryresult():
    # query to get unique columns from the RDF repository
    queryColumn = """
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        select ?o where {
        ?s dbo:column ?o .
    }
    """

    def queryresult(repo, query):
        try:
            endpoint = "http://rdf-store:7200/repositories/" + repo
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

    columns = queryresult(v.repo, queryColumn)
    hnscc = pd.read_csv(StringIO(columns))
    hnscc = hnscc[hnscc.columns[0]]
    # return the unique columns and print it in the GUI
    return render_template('categories.html', variable=hnscc)


@app.route("/units", methods=['POST'])
def units():
    conList = []
    v.mydict = {}
    for key in request.form:
        if not re.search("^ncit_comment_", key):
            v.mydict[key] = {}
            value = request.form.get(key)
            ncit = request.form.get('ncit_comment_' + key)
            comment = request.form.get('comment_' + key)
            v.mydict[key]['type'] = value
            v.mydict[key]['description'] = ncit
            v.mydict[key]['comments'] = comment
            if value == 'Categorical Nominal' or value == 'Categorical Ordinal':
                # if column is categorical, get the unique categories
                cat = getCategories(v.repo, key)
                TESTDATA = StringIO(cat)
                df = pd.read_csv(TESTDATA, sep=",")
                df = df.to_dict('records')
                v.mydict[key]['categories'] = df
                equivalencies(v.mydict, key)
            elif value == 'Continuous':
                # if continuous, append it to a list to get the units of measurement
                conList.append(key)
            else:
                equivalencies(v.mydict, key)

    return render_template('units.html', variable=conList)


def getCategories(repo, key):
    # query to get unique categories from the RDF repository
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

    endpoint = "http://rdf-store:7200/repositories/" + repo
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
    #items = getColumns(file_path)
    for key in request.form:
        unitValue = request.form.get(key)
        if unitValue != "":
            v.mydict[key]['units'] = unitValue
        equivalencies(v.mydict, key)

    return render_template('success.html')


def equivalencies(mydict, key):
    # final query to add the information given by the user in GUI to the OWL file in RDF repository
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
    """ % (v.repo, list(mydict[key].values()), key)

    endpoint = "http://rdf-store:7200/repositories/" + v.repo + "/statements"
    annotationResponse = requests.post(endpoint,
                                       data="update=" + query,
                                       headers={
                                           "Content-Type": "application/x-www-form-urlencoded",
                                           # "Accept": "application/json"
                                       })
    output = annotationResponse.text
    print(output)


if (__name__ == "__main__"):
    #app.run(port = 5001)
    app.run(host='0.0.0.0')
