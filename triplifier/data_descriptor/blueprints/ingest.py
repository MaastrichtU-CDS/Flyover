"""
Ingest blueprint for data upload and processing routes.

Contains routes for uploading CSV files, connecting to databases,
and initial data processing.
"""

import os
import json
import logging
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename

# Import modular components
from modules import (
    Cache,
    allowed_file,
    check_graph_exists,
    handle_postgres_data,
    preprocess_dataframe
)
from modules import upload_ontology_then_data

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
    Handle main data file upload processing.
    
    This route processes uploaded CSV files and PostgreSQL database connections,
    performing initial data validation and preprocessing before storing in GraphDB.
    
    Workflow:
        1. Determine upload type (file upload vs database connection)
        2. For file uploads: validate, save, and preprocess CSV data
        3. For database connections: validate credentials and retrieve data
        4. Store processed data in session cache
        5. Redirect to appropriate next step based on semantic map availability
    
    Returns:
        flask.redirect: Redirect to data-submission or describe_landing
        flask.render_template: Re-render ingest page with errors
        
    Route:
        POST /ingest/upload - Main data upload processing handler
        
    Request Data:
        upload_type (str): 'file' or 'database'
        
        For file uploads:
            file: CSV file to process
            
        For database connections:
            host, port, database, username, password: Connection parameters
            
    Session Variables:
        session_cache.cleaned_df (DataFrame): Preprocessed data
        session_cache.original_column_names (dict): Column name mappings
        session_cache.pg_* (str): Database connection parameters
        
    Flash Messages:
        Success: File uploaded/database connected successfully
        Error: Validation, connection, or processing errors
    """
    session_cache = current_app.session_cache
    upload_type = request.form.get('upload_type')
    
    try:
        if upload_type == 'file':
            return _handle_file_upload(session_cache)
        elif upload_type == 'database':
            return _handle_database_connection(session_cache)
        else:
            flash('Invalid upload type specified')
            return render_template('ingest.html')
            
    except Exception as e:
        logger.error(f"Error in upload processing: {e}")
        flash(f'Upload processing error: {e}')
        return render_template('ingest.html')


@ingest_bp.route('/data-submission')
def data_submission():
    """
    Display data submission confirmation page.
    
    This route shows a summary of uploaded data and allows users to
    proceed with data submission or return to modify their upload.
    
    Returns:
        flask.render_template: Rendered data-submission.html template
        
    Route:
        GET /ingest/data-submission - Data submission confirmation page
        
    Template:
        data-submission.html - Data summary and submission options
    """
    return render_template('data-submission.html')


def _handle_file_upload(session_cache: Cache):
    """
    Handle CSV file upload processing.
    
    Args:
        session_cache (Cache): Application session cache
        
    Returns:
        flask.redirect or flask.render_template: Response based on processing result
    """
    if 'file' not in request.files:
        flash('No file selected')
        return render_template('ingest.html')

    file = request.files['file']
    if file.filename == '':
        flash('No file selected')
        return render_template('ingest.html')

    if not allowed_file(file.filename, current_app.config['ALLOWED_EXTENSIONS']):
        flash('Invalid file type. Please upload a CSV file.')
        return render_template('ingest.html')

    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Preprocess the uploaded file
        cleaned_df, original_column_names = preprocess_dataframe(filepath)
        
        session_cache.cleaned_df = cleaned_df
        session_cache.original_column_names = original_column_names
        session_cache.file_path = filepath
        
        logger.info(f"File uploaded and preprocessed successfully: {filename}")
        flash('File uploaded successfully!')
        
        # Redirect based on semantic map availability
        if hasattr(session_cache, 'uploaded_semantic_map'):
            return redirect(url_for('ingest.data_submission'))
        else:
            return redirect(url_for('describe.describe_landing'))
            
    except Exception as e:
        logger.error(f"File upload processing error: {e}")
        flash(f'Error processing file: {e}')
        return render_template('ingest.html')


def _handle_database_connection(session_cache: Cache):
    """
    Handle PostgreSQL database connection and data retrieval.
    
    Args:
        session_cache (Cache): Application session cache
        
    Returns:
        flask.redirect or flask.render_template: Response based on connection result
    """
    try:
        # Extract database connection parameters
        host = request.form.get('host')
        port = request.form.get('port', '5432')
        database = request.form.get('database')
        username = request.form.get('username')
        password = request.form.get('password')
        
        if not all([host, database, username, password]):
            flash('All database connection fields are required')
            return render_template('ingest.html')
        
        # Store connection parameters in session
        session_cache.pg_host = host
        session_cache.pg_port = port
        session_cache.pg_database = database
        session_cache.pg_username = username
        session_cache.pg_password = password
        
        # Test connection and retrieve data
        success = handle_postgres_data(session_cache)
        
        if success:
            logger.info(f"Database connection successful: {host}:{port}/{database}")
            flash('Database connected successfully!')
            return redirect(url_for('describe.describe_landing'))
        else:
            flash('Failed to connect to database. Please check your credentials.')
            return render_template('ingest.html')
            
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        flash(f'Database connection error: {e}')
        return render_template('ingest.html')