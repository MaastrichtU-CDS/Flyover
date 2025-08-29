"""
Describe blueprint for data description and semantic mapping routes.

Contains routes for describing variables, adding semantic mappings,
and generating ontologies.
"""

import json
import logging
import os
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app, Response
from werkzeug.utils import secure_filename

# Import modular components
from modules import (
    Cache,
    retrieve_categories,
    retrieve_global_names,
    formulate_local_semantic_map
)

describe_bp = Blueprint('describe', __name__, url_prefix='/describe')
logger = logging.getLogger(__name__)


@describe_bp.route('/landing')
def describe_landing():
    """
    Landing page for the data description workflow.
    
    This route displays the main describe landing page where users can
    begin the process of adding semantic descriptions to their data.
    
    Returns:
        flask.render_template: Rendered describe_landing.html template
        
    Route:
        GET /describe/landing - Data description workflow entry point
        
    Template:
        describe_landing.html - Description workflow overview and options
    """
    return render_template('describe_landing.html')


@describe_bp.route("/variables", methods=['GET', 'POST'])
def describe_variables():
    """
    Handle variable description and semantic mapping interface.
    
    This route displays the variable description page where users can
    add semantic mappings, units, and other metadata to their data columns.
    Supports both GET (display) and POST (form submission) methods.
    
    Workflow:
        1. For GET: Display current variable mappings and available categories
        2. For POST: Process submitted semantic mappings and update session
        3. Retrieve semantic categories and global names for dropdown options
        4. Handle form validation and error reporting
    
    Returns:
        flask.render_template: Rendered describe_variables.html template
        flask.redirect: Redirect to next step on successful form submission
        
    Route:
        GET/POST /describe/variables - Variable description interface
        
    Template:
        describe_variables.html - Variable mapping form and semantic options
        
    Session Variables:
        session_cache.column_mapping (dict): Current semantic mappings
        session_cache.categories (list): Available semantic categories
        session_cache.global_names (dict): Global naming conventions
    """
    session_cache = current_app.session_cache
    graphdb_url = current_app.config['GRAPHDB_URL']
    
    if request.method == 'POST':
        return _handle_variable_mapping_submission(session_cache)
    
    try:
        # Retrieve semantic categories and global names for the form
        categories = retrieve_categories(session_cache.repo, graphdb_url)
        global_names = retrieve_global_names(session_cache.repo, graphdb_url)
        
        session_cache.categories = categories
        session_cache.global_names = global_names
        
        # Get current column mappings or initialize empty
        column_mapping = getattr(session_cache, 'column_mapping', {})
        
        return render_template(
            'describe_variables.html',
            categories=categories,
            global_names=global_names,
            column_mapping=column_mapping,
            columns=session_cache.cleaned_df.columns.tolist() if hasattr(session_cache, 'cleaned_df') else []
        )
        
    except Exception as e:
        logger.error(f"Error in describe_variables: {e}")
        flash(f"Error loading variable description page: {e}")
        return redirect(url_for('describe.describe_landing'))


@describe_bp.route("/units", methods=['POST'])
def units():
    """
    Handle unit assignment for data columns.
    
    This route processes unit assignments submitted through AJAX requests,
    allowing users to specify measurement units for numerical columns.
    
    Workflow:
        1. Extract column name and unit from POST data
        2. Validate unit format and column existence
        3. Store unit mapping in session cache
        4. Return JSON response with success/error status
    
    Returns:
        flask.Response: JSON response with operation status
        
    Route:
        POST /describe/units - Unit assignment handler
        
    Request Data:
        column (str): Column name to assign unit to
        unit (str): Unit designation (e.g., 'kg', 'cm', 'seconds')
        
    Response:
        JSON: {"success": bool, "message": str}
        
    Session Variables:
        session_cache.column_units (dict): Column to unit mappings
    """
    session_cache = current_app.session_cache
    
    try:
        column = request.form.get('column')
        unit = request.form.get('unit')
        
        if not column or not unit:
            return Response(
                json.dumps({"success": False, "message": "Column and unit are required"}),
                mimetype='application/json',
                status=400
            )
        
        # Initialize units dictionary if not exists
        if not hasattr(session_cache, 'column_units'):
            session_cache.column_units = {}
        
        session_cache.column_units[column] = unit
        logger.info(f"Unit assigned: {column} -> {unit}")
        
        return Response(
            json.dumps({"success": True, "message": f"Unit '{unit}' assigned to column '{column}'"}),
            mimetype='application/json'
        )
        
    except Exception as e:
        logger.error(f"Error in units assignment: {e}")
        return Response(
            json.dumps({"success": False, "message": f"Error assigning unit: {e}"}),
            mimetype='application/json',
            status=500
        )


@describe_bp.route("/variable-details")
def describe_variable_details():
    """
    Display detailed variable description interface.
    
    This route shows a detailed form for describing individual variables
    with advanced semantic mapping options and relationship definitions.
    
    Returns:
        flask.render_template: Rendered describe_variable_details.html template
        
    Route:
        GET /describe/variable-details - Detailed variable description interface
        
    Template:
        describe_variable_details.html - Advanced variable mapping form
    """
    session_cache = current_app.session_cache
    
    # Get available categories and global names from session or retrieve fresh
    categories = getattr(session_cache, 'categories', [])
    global_names = getattr(session_cache, 'global_names', {})
    
    return render_template(
        'describe_variable_details.html',
        categories=categories,
        global_names=global_names
    )


@describe_bp.route("/end", methods=['GET', 'POST'])
def end():
    """
    Finalize the data description process.
    
    This route handles the completion of the data description workflow,
    generating final semantic mappings and preparing for the next phase.
    
    Workflow:
        1. For GET: Display description summary and finalization options
        2. For POST: Process final mappings and generate semantic map
        3. Store completed mappings in session for annotation phase
        4. Redirect to annotation workflow or download options
    
    Returns:
        flask.render_template: Rendered end.html template
        flask.redirect: Redirect to next workflow phase
        
    Route:
        GET/POST /describe/end - Description workflow finalization
        
    Template:
        end.html - Description summary and completion options
        
    Session Variables:
        session_cache.final_semantic_map (dict): Complete semantic mappings
        session_cache.description_complete (bool): Workflow completion flag
    """
    session_cache = current_app.session_cache
    
    if request.method == 'POST':
        return _handle_description_finalization(session_cache)
    
    try:
        # Generate local semantic map based on current mappings
        semantic_map = formulate_local_semantic_map(session_cache)
        session_cache.local_semantic_map = semantic_map
        
        return render_template(
            'end.html',
            semantic_map=semantic_map,
            column_mapping=getattr(session_cache, 'column_mapping', {}),
            column_units=getattr(session_cache, 'column_units', {})
        )
        
    except Exception as e:
        logger.error(f"Error in description finalization: {e}")
        flash(f"Error finalizing description: {e}")
        return redirect(url_for('describe.describe_landing'))


@describe_bp.route('/downloads')
def describe_downloads():
    """
    Display download options for generated semantic artifacts.
    
    This route shows available downloads including semantic maps,
    generated ontologies, and other description artifacts.
    
    Returns:
        flask.render_template: Rendered describe_downloads.html template
        
    Route:
        GET /describe/downloads - Description artifact download page
        
    Template:
        describe_downloads.html - Available downloads and descriptions
    """
    return render_template('describe_downloads.html')


@describe_bp.route('/download-semantic-map', methods=['GET'])
def download_semantic_map():
    """
    Download the generated semantic map as JSON file.
    
    This route generates and serves the semantic map file containing
    all column mappings and semantic descriptions for download.
    
    Returns:
        flask.Response: JSON file download response
        
    Route:
        GET /describe/download-semantic-map - Semantic map file download
        
    Content-Type:
        application/json
        
    Content-Disposition:
        attachment; filename="semantic_map.json"
    """
    session_cache = current_app.session_cache
    
    try:
        semantic_map = getattr(session_cache, 'local_semantic_map', {})
        
        if not semantic_map:
            flash('No semantic map available for download')
            return redirect(url_for('describe.describe_downloads'))
        
        # Generate JSON response for download
        json_data = json.dumps(semantic_map, indent=2)
        
        response = Response(
            json_data,
            mimetype='application/json',
            headers={'Content-Disposition': 'attachment; filename=semantic_map.json'}
        )
        
        logger.info("Semantic map downloaded")
        return response
        
    except Exception as e:
        logger.error(f"Error downloading semantic map: {e}")
        flash(f"Error downloading semantic map: {e}")
        return redirect(url_for('describe.describe_downloads'))


@describe_bp.route('/download-ontology', methods=['GET'])
def download_ontology():
    """
    Download the generated ontology file.
    
    This route generates and serves the ontology file based on
    the semantic mappings and data descriptions.
    
    Returns:
        flask.Response: Ontology file download response
        
    Route:
        GET /describe/download-ontology - Ontology file download
        
    Content-Type:
        application/rdf+xml or text/turtle (based on format)
        
    Content-Disposition:
        attachment; filename="ontology.owl" or "ontology.ttl"
    """
    session_cache = current_app.session_cache
    
    try:
        # TODO: Implement ontology generation based on semantic mappings
        # This would involve converting the semantic map to RDF/OWL format
        
        flash('Ontology generation not yet implemented')
        return redirect(url_for('describe.describe_downloads'))
        
    except Exception as e:
        logger.error(f"Error downloading ontology: {e}")
        flash(f"Error downloading ontology: {e}")
        return redirect(url_for('describe.describe_downloads'))


def _handle_variable_mapping_submission(session_cache: Cache):
    """
    Process variable mapping form submission.
    
    Args:
        session_cache (Cache): Application session cache
        
    Returns:
        flask.redirect: Redirect to next step or error page
    """
    try:
        # Extract form data for column mappings
        column_mapping = {}
        
        for key, value in request.form.items():
            if key.startswith('mapping_'):
                column_name = key.replace('mapping_', '')
                if value.strip():
                    column_mapping[column_name] = value.strip()
        
        session_cache.column_mapping = column_mapping
        logger.info(f"Variable mappings saved: {len(column_mapping)} columns mapped")
        
        flash('Variable mappings saved successfully!')
        return redirect(url_for('describe.describe_variable_details'))
        
    except Exception as e:
        logger.error(f"Error processing variable mapping submission: {e}")
        flash(f"Error saving variable mappings: {e}")
        return redirect(url_for('describe.describe_variables'))


def _handle_description_finalization(session_cache: Cache):
    """
    Process description workflow finalization.
    
    Args:
        session_cache (Cache): Application session cache
        
    Returns:
        flask.redirect: Redirect to next workflow phase
    """
    try:
        # Mark description as complete
        session_cache.description_complete = True
        
        # Generate final semantic map
        final_semantic_map = formulate_local_semantic_map(session_cache)
        session_cache.final_semantic_map = final_semantic_map
        
        logger.info("Description workflow finalized")
        flash('Data description completed successfully!')
        
        return redirect(url_for('annotate.annotation_landing'))
        
    except Exception as e:
        logger.error(f"Error finalizing description: {e}")
        flash(f"Error finalizing description: {e}")
        return redirect(url_for('describe.end'))