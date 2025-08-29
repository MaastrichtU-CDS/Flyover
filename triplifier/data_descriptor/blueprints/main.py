"""
Main blueprint for core application routes.

Contains landing page and static file serving routes.
"""

from flask import Blueprint, render_template, send_from_directory, current_app
import os

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def landing():
    """
    Landing page route handler for the Flyover application.
    
    Renders the main landing page that provides an overview of the three-step workflow:
    1. Ingest - Upload and process data
    2. Describe - Add metadata and semantic descriptions
    3. Annotate - Apply semantic annotations and relationships
    
    Returns:
        flask.render_template: Rendered index.html template
        
    Route:
        GET / - Main application landing page
        
    Template:
        index.html - Contains workflow overview and navigation
    """
    return render_template('index.html')


@main_bp.route('/favicon.ico')
def favicon():
    """
    Serve the favicon for the application.
    
    Returns:
        flask.send_from_directory: Favicon file from assets directory
        
    Route:
        GET /favicon.ico - Application favicon
    """
    return send_from_directory(
        os.path.join(current_app.root_path, 'assets'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


@main_bp.route('/data_descriptor/assets/<path:filename>')
def assets(filename):
    """
    Serve static assets for the application.
    
    This route serves CSS, JavaScript, images and other static files
    required by the application templates.
    
    Args:
        filename (str): Name of the asset file to serve
        
    Returns:
        flask.send_from_directory: Requested asset file
        
    Route:
        GET /data_descriptor/assets/<path:filename> - Static asset files
        
    Examples:
        /data_descriptor/assets/css/style.css
        /data_descriptor/assets/js/script.js
        /data_descriptor/assets/images/logo.png
    """
    assets_dir = os.path.join(current_app.root_path, 'assets')
    return send_from_directory(assets_dir, filename)