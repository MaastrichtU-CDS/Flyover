"""
Ingest blueprint for data upload and processing routes.

Contains routes for uploading CSV files, connecting to databases,
and initial data processing.
"""

import os
import json
import logging
import pandas as pd
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
from markupsafe import Markup

# Import modular components
from modules import (
    Cache,
    allowed_file,
    check_graph_exists,
    handle_postgres_data,
    preprocess_dataframe,
    upload_ontology_then_data,
    run_triplifier
)

ingest_bp = Blueprint('ingest', __name__, url_prefix='/ingest')
logger = logging.getLogger(__name__)


@ingest_bp.route('/')
def index():
    """
    Ingest page route handler for data upload and processing.
    
    This route renders the ingest.html page where users can upload CSV files or
    connect to PostgreSQL databases. The function checks if data already exists
    in the GraphDB repository and displays appropriate messaging.
    
    Workflow:
        1. Check if data graph exists in GraphDB using modular check_graph_exists()
        2. If graph exists, set session flag and display existing data message
        3. If no graph or error, display standard ingest page
        4. Flash error messages for any GraphDB connection issues
    
    Returns:
        flask.render_template: Rendered ingest.html template
            - With graph_exists=True if data already present
            - With standard content if no existing data
            
    Route:
        GET /ingest/ - Data ingestion and upload interface
        
    Template:
        ingest.html - File upload form and database connection interface
        
    Session Variables:
        session_cache.existing_graph (bool): Whether data already exists
    """
    # Get session cache from app context
    session_cache = current_app.session_cache
    graphdb_url = current_app.config['GRAPHDB_URL']
    
    try:
        if check_graph_exists(session_cache.repo, "http://data.local/", graphdb_url):
            session_cache.existing_graph = True
            logger.info("Existing data graph detected in repository")
            return render_template('ingest.html', graph_exists=True)
    except Exception as e:
        logger.error(f"GraphDB connection error during graph check: {e}")
        flash(f"Failed to check existing data: {e}")
    
    return render_template('ingest.html')


@ingest_bp.route('/upload-semantic-map', methods=['POST'])
def upload_semantic_map():
    """
    Handle semantic map file upload for data annotation.
    
    This route processes uploaded semantic map files that contain
    pre-defined column mappings and semantic annotations for datasets.
    
    Workflow:
        1. Validate uploaded file extension and security
        2. Save file to upload directory with secure filename
        3. Load and parse JSON semantic map content
        4. Store parsed data in session cache for later use
        5. Redirect to data submission page for next steps
    
    Returns:
        flask.redirect: Redirect to data-submission page on success
        flask.render_template: Re-render ingest page with error on failure
        
    Route:
        POST /ingest/upload-semantic-map - Semantic map file upload handler
        
    Request Files:
        semanticMapFile: JSON file containing semantic mappings
        
    Session Variables:
        session_cache.uploaded_semantic_map (dict): Parsed semantic map data
        
    Flash Messages:
        Success: Semantic map loaded successfully
        Error: File validation or parsing error messages
    """
    session_cache = current_app.session_cache
    
    if 'semanticMapFile' not in request.files:
        flash('No semantic map file selected')
        return render_template('ingest.html')
    
    file = request.files['semanticMapFile']
    if file.filename == '':
        flash('No semantic map file selected')
        return render_template('ingest.html')
    
    if not allowed_file(file.filename, {'json'}):
        flash('Invalid file type. Please upload a JSON file.')
        return render_template('ingest.html')
    
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Load semantic map from JSON
        with open(filepath, 'r') as f:
            semantic_map_data = json.load(f)
        
        session_cache.uploaded_semantic_map = semantic_map_data
        logger.info(f"Semantic map uploaded successfully: {filename}")
        flash('Semantic map loaded successfully!')
        
        return redirect(url_for('ingest.data_submission'))
        
    except json.JSONDecodeError:
        flash('Invalid JSON file. Please check the format.')
        return render_template('ingest.html')
    except Exception as e:
        logger.error(f"Error processing semantic map upload: {e}")
        flash(f'Error processing semantic map: {e}')
        return render_template('ingest.html')


@ingest_bp.route('/upload', methods=['POST'])
def upload():
    """
    Handle main data file upload processing with triplification.
    
    This route processes uploaded CSV files and PostgreSQL database connections,
    performing initial data validation, triplification, and upload to GraphDB.
    
    Workflow:
        1. Determine upload type (file upload vs database connection)
        2. Process and store data
        3. Run triplifier to convert data to RDF format
        4. Upload ontology and data to GraphDB
        5. Trigger background relationship processing
        6. Redirect to data-submission page with status
    
    Returns:
        flask.redirect: Redirect to data-submission page on success
        flask.render_template: Re-render ingest page with errors
        
    Route:
        POST /ingest/upload - Main data upload processing with triplification
        
    Request Data:
        upload_type (str): 'file' or 'database'
        pkFkData (str, optional): JSON string containing PK/FK relationship data
        crossGraphLinkData (str, optional): JSON string containing cross-graph linking data
        
        For file uploads:
            file: CSV file to process
            csv_separator_sign (str, optional): CSV separator character (default: ',')
            csv_decimal_sign (str, optional): CSV decimal character (default: '.')
            
        For database connections:
            host, port, database, username, password: Connection parameters
            
    Session Variables:
        session_cache.csvData (list): Preprocessed CSV dataframes
        session_cache.csvPath (list): Paths to saved CSV files
        session_cache.pk_fk_data (dict): PK/FK relationship data
        session_cache.cross_graph_link_data (dict): Cross-graph linking data
        session_cache.StatusToDisplay (str): Status message for display
        
    Flash Messages:
        Success: Triplification and upload completion status
        Error: Validation, processing, triplification, or upload errors
    """
    session_cache = current_app.session_cache
    upload_type = request.form.get('upload_type')
    pk_fk_data = request.form.get('pkFkData')
    cross_graph_link_data = request.form.get('crossGraphLinkData')
    upload = True

    # Store PK/FK data in session cache
    if pk_fk_data:
        try:
            session_cache.pk_fk_data = json.loads(pk_fk_data)
        except json.JSONDecodeError:
            flash('Invalid PK/FK data format')
            return render_template('ingest.html')

    # Store cross-graph linking data in session cache
    if cross_graph_link_data:
        try:
            session_cache.cross_graph_link_data = json.loads(cross_graph_link_data)
        except json.JSONDecodeError:
            flash('Invalid cross-graph linking data format')
            return render_template('ingest.html')
    
    try:
        if upload_type == 'file':
            success, message = _handle_file_upload_with_triplification(session_cache)
        elif upload_type == 'database':
            success, message = _handle_database_connection_with_triplification(session_cache)
        elif upload_type is None:
            # User opted to not submit new data
            success = True
            upload = False
            message = Markup("You have opted to not submit any new data, "
                             "you can now proceed to describe your data."
                             "<br>"
                             "<i>In case you do wish to submit data, please return to the ingest page.</i>")
        else:
            success = False
            message = "An unexpected error occurred. Please try again."

        if success:
            session_cache.StatusToDisplay = message

            if upload:
                logger.info("🚀 Initiating sequential upload to GraphDB")
                upload_success, upload_messages = upload_ontology_then_data(
                    current_app.config.get('ROOT_DIR', ''), 
                    current_app.config['GRAPHDB_URL'], 
                    session_cache.repo,
                    data_background=False
                )

                for msg in upload_messages:
                    logger.info(f"📝 {msg}")

            # Redirect to the data submission page after processing
            return redirect(url_for('ingest.data_submission'))
        else:
            flash(f"Attempting to proceed resulted in an error: {message}")
            return render_template('ingest.html', error=True, graph_exists=session_cache.existing_graph)
            
    except Exception as e:
        logger.error(f"Error in upload processing: {e}")
        flash(f'Upload processing error: {e}')
        return render_template('ingest.html')


@ingest_bp.route('/data-submission')
def data_submission():
    """
    Display data submission confirmation page with status.
    
    This route shows the status of the triplification and upload process,
    allowing users to proceed to the describe step.
    
    Returns:
        flask.render_template: Rendered describe_landing.html template with status message
        
    Route:
        GET /ingest/data-submission - Data submission status and confirmation page
        
    Template:
        describe_landing.html - Status display and next step navigation
        
    Session Variables:
        session_cache.StatusToDisplay (str): Status message from triplification process
    """
    session_cache = current_app.session_cache
    message = getattr(session_cache, 'StatusToDisplay', 'Data submission completed successfully.')
    return render_template('describe_landing.html', message=Markup(message))


def _handle_file_upload_with_triplification(session_cache: Cache):
    """
    Handle CSV file upload processing with triplification.
    
    Args:
        session_cache (Cache): Application session cache
        
    Returns:
        Tuple[bool, str]: (success_flag, status_message)
    """
    files = request.files.getlist('file') if 'file' in request.files else [request.files.get('file')]
    csv_files = [f for f in files if f and f.filename]
    
    if not csv_files:
        return False, "If opting to submit a CSV data source, please upload it as a '.csv' file."

    # Validate all files
    for csv_file in csv_files:
        if not allowed_file(csv_file.filename, {'csv'}):
            return False, "If opting to submit a CSV data source, please upload it as a '.csv' file."

    try:
        # Get CSV parsing parameters
        separator_sign = request.form.get('csv_separator_sign', ',')
        if not separator_sign:
            separator_sign = ','

        decimal_sign = request.form.get('csv_decimal_sign', '.')
        if not decimal_sign:
            decimal_sign = '.'

        # Process CSV files
        session_cache.csvData = []
        session_cache.csvPath = []
        
        for csv_file in csv_files:
            # Save the file
            filename = secure_filename(csv_file.filename)
            filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            csv_file.save(filepath)
            session_cache.csvPath.append(filepath)
            
            # Read and preprocess the CSV
            df = pd.read_csv(csv_file, sep=separator_sign, decimal=decimal_sign)
            cleaned_df, original_column_names = preprocess_dataframe(df)
            session_cache.csvData.append(cleaned_df)
            
            # Store additional metadata for the first file
            if len(session_cache.csvData) == 1:
                session_cache.cleaned_df = cleaned_df
                session_cache.original_column_names = original_column_names
                session_cache.file_path = filepath

        logger.info(f"CSV files processed successfully: {[f.filename for f in csv_files]}")
        
        # Run triplifier
        success, message = run_triplifier(
            'triplifierCSV.properties',
            session_cache,
            current_app.config['UPLOAD_FOLDER'],
            current_app.config.get('ROOT_DIR', ''),
            current_app.config.get('CHILD_DIR', '.')
        )
        
        # Clean up temporary files
        for path in session_cache.csvPath:
            if os.path.exists(path):
                os.remove(path)
        
        return success, message
        
    except Exception as e:
        # Clean up on error
        if hasattr(session_cache, 'csvPath'):
            for path in session_cache.csvPath:
                if os.path.exists(path):
                    os.remove(path)
        logger.error(f"File upload and triplification error: {e}")
        return False, f'Error processing file: {e}'


def _handle_database_connection_with_triplification(session_cache: Cache):
    """
    Handle PostgreSQL database connection and data processing with triplification.
    
    Args:
        session_cache (Cache): Application session cache
        
    Returns:
        Tuple[bool, str]: (success_flag, status_message)
    """
    try:
        # Extract database connection parameters
        host = request.form.get('host')
        port = request.form.get('port', '5432')
        database = request.form.get('database')
        username = request.form.get('username')
        password = request.form.get('password')
        table = request.form.get('table')
        
        if not all([host, database, username, password]):
            return False, 'All database connection fields are required'
        
        # Store connection parameters in session
        session_cache.pg_host = host
        session_cache.pg_port = port
        session_cache.pg_database = database
        session_cache.pg_username = username
        session_cache.pg_password = password
        
        # Test connection and retrieve data
        success = handle_postgres_data(session_cache)
        
        if not success:
            return False, 'Failed to connect to database. Please check your credentials.'
            
        logger.info(f"Database connection successful: {host}:{port}/{database}")
        
        # Run triplifier for SQL data
        success, message = run_triplifier(
            'triplifierSQL.properties',
            session_cache,
            current_app.config['UPLOAD_FOLDER'],
            current_app.config.get('ROOT_DIR', ''),
            current_app.config.get('CHILD_DIR', '.')
        )
        
        return success, message
            
    except Exception as e:
        logger.error(f"Database connection and triplification error: {e}")
        return False, f'Database processing error: {e}'