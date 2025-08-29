"""
Flask application factory for the Flyover data descriptor.

This module implements the Flask application factory pattern with
modular blueprints, configuration management, and proper initialization.
"""

import os
import sys
import logging
from flask import Flask

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import application components
from config import get_config
from modules import setup_logging, Cache
from blueprints import main_bp, ingest_bp, describe_bp, annotate_bp, api_bp


def create_app(config_name: str = None) -> Flask:
    """
    Create and configure the Flask application using the application factory pattern.
    
    Args:
        config_name (str, optional): Configuration environment name.
                                   If None, auto-detects from environment.
    
    Returns:
        Flask: Configured Flask application instance
    """
    # Create Flask application instance
    app = Flask(__name__)
    
    # Load configuration
    config = get_config(config_name)
    app.config.from_object(config)
    
    # Initialize configuration with app
    config.init_app(app)
    
    # Setup logging early
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Initialize session cache and attach to app
    session_cache = Cache(app.config['REPOSITORY_NAME'])
    app.session_cache = session_cache
    
    # Store config values in app for easy access
    app.graphdb_url = app.config['GRAPHDB_URL']
    app.repo = app.config['REPOSITORY_NAME']
    
    logger.info(f"Flyover application starting with config: {config.__class__.__name__}")
    logger.info(f"GraphDB URL: {app.config['GRAPHDB_URL']}")
    logger.info(f"Repository: {app.config['REPOSITORY_NAME']}")
    
    # Register blueprints
    register_blueprints(app)
    
    # Setup error handlers
    setup_error_handlers(app)
    
    # Setup context processors
    setup_context_processors(app)
    
    logger.info("Flask application factory initialization completed")
    
    return app


def register_blueprints(app: Flask) -> None:
    """
    Register all application blueprints.
    
    Args:
        app (Flask): Flask application instance
    """
    logger = logging.getLogger(__name__)
    
    # Register blueprints with appropriate URL prefixes
    app.register_blueprint(main_bp)
    app.register_blueprint(ingest_bp)  # URL prefix: /ingest
    app.register_blueprint(describe_bp)  # URL prefix: /describe  
    app.register_blueprint(annotate_bp)  # URL prefix: /annotate
    app.register_blueprint(api_bp)  # URL prefix: /api
    
    logger.info("All blueprints registered successfully")


def setup_error_handlers(app: Flask) -> None:
    """
    Setup application-wide error handlers.
    
    Args:
        app (Flask): Flask application instance
    """
    logger = logging.getLogger(__name__)
    
    @app.errorhandler(404)
    def page_not_found(error):
        """Handle 404 page not found errors."""
        logger.warning(f"404 error: {error}")
        return f"Page not found: {error}", 404
    
    @app.errorhandler(500)
    def internal_server_error(error):
        """Handle 500 internal server errors."""
        logger.error(f"500 error: {error}")
        return f"Internal server error: {error}", 500
    
    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        """Handle unexpected errors."""
        logger.error(f"Unexpected error: {error}", exc_info=True)
        return f"An unexpected error occurred: {error}", 500
    
    logger.info("Error handlers configured")


def setup_context_processors(app: Flask) -> None:
    """
    Setup template context processors.
    
    Args:
        app (Flask): Flask application instance
    """
    @app.context_processor
    def inject_global_variables():
        """Inject global variables into all templates."""
        return {
            'app_name': 'Flyover Data Descriptor',
            'version': '2.0.0',
            'graphdb_url': app.config.get('GRAPHDB_URL'),
            'repository': app.config.get('REPOSITORY_NAME')
        }


# Create application instance for development server
app = create_app()


if __name__ == "__main__":
    """
    Run the application in development mode.
    
    For production deployment, use a proper WSGI server like Gunicorn.
    """
    # Get configuration
    config = get_config()
    
    # Run with appropriate settings based on configuration
    if hasattr(config, 'DEBUG') and config.DEBUG:
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=True,
            use_reloader=True
        )
    else:
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False
        )