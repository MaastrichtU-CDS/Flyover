"""
Flask configuration module for the Flyover data descriptor application.

This module provides configuration classes for different environments and
centralizes all application configuration settings.
"""

import os
from typing import Dict, Any


class Config:
    """Base configuration class with common settings."""
    
    # Security
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'secret_key'
    
    # GraphDB Configuration
    GRAPHDB_URL = os.environ.get('FLYOVER_GRAPHDB_URL') or 'http://localhost:7200'
    REPOSITORY_NAME = os.environ.get('FLYOVER_REPOSITORY_NAME') or 'userRepo'
    
    # Application Paths
    ROOT_DIR = '/app/' if os.getenv('FLYOVER_GRAPHDB_URL') else ''
    CHILD_DIR = 'data_descriptor' if os.getenv('FLYOVER_GRAPHDB_URL') else '.'
    
    # Upload Configuration
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'csv', 'txt', 'json', 'owl', 'rdf', 'ttl', 'xml'}
    
    # Database Configuration (for PostgreSQL connections)
    DATABASE_TIMEOUT = 30
    DATABASE_CONNECTION_POOL_SIZE = 5
    
    @property
    def UPLOAD_FOLDER(self) -> str:
        """Get the upload folder path based on environment."""
        return os.path.join(self.CHILD_DIR, 'static', 'files')
    
    @classmethod
    def init_app(cls, app):
        """Initialize the Flask application with configuration."""
        # Ensure upload folder exists
        upload_folder = cls().UPLOAD_FOLDER
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
        
        # Set Flask config including all path configurations
        config_instance = cls()
        app.config['UPLOAD_FOLDER'] = upload_folder
        app.config['ROOT_DIR'] = config_instance.ROOT_DIR
        app.config['CHILD_DIR'] = config_instance.CHILD_DIR
        app.config['GRAPHDB_URL'] = config_instance.GRAPHDB_URL
        app.config['REPOSITORY_NAME'] = config_instance.REPOSITORY_NAME


class DevelopmentConfig(Config):
    """Development environment configuration."""
    
    DEBUG = True
    TESTING = False
    
    # Development-specific GraphDB settings
    GRAPHDB_URL = 'http://localhost:7200'
    REPOSITORY_NAME = 'userRepo'
    
    # Logging Configuration
    LOG_LEVEL = 'DEBUG'
    LOG_TO_STDOUT = True


class ProductionConfig(Config):
    """Production environment configuration."""
    
    DEBUG = False
    TESTING = False
    
    # Production-specific settings
    LOG_LEVEL = 'INFO'
    LOG_TO_STDOUT = False
    
    # Enhanced security in production
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'default-secret-key-change-in-production'


class TestingConfig(Config):
    """Testing environment configuration."""
    
    DEBUG = True
    TESTING = True
    
    # Test-specific settings
    GRAPHDB_URL = 'http://localhost:7200'
    REPOSITORY_NAME = 'testRepo'
    LOG_LEVEL = 'WARNING'


class DockerConfig(Config):
    """Docker environment configuration."""
    
    DEBUG = False
    TESTING = False
    
    # Docker-specific paths
    ROOT_DIR = '/app/'
    CHILD_DIR = 'data_descriptor'
    
    # Use environment variables for Docker deployment
    GRAPHDB_URL = os.environ.get('FLYOVER_GRAPHDB_URL', 'http://graphdb:7200')
    REPOSITORY_NAME = os.environ.get('FLYOVER_REPOSITORY_NAME', 'defaultRepo')


# Configuration mapping
config_map: Dict[str, Any] = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'docker': DockerConfig,
    'default': DevelopmentConfig
}


def get_config(config_name: str = None) -> Config:
    """
    Get the configuration class based on environment.
    
    Args:
        config_name (str, optional): Configuration name. If None, auto-detects.
        
    Returns:
        Config: Configuration class instance
    """
    if config_name is None:
        # Auto-detect environment
        if os.getenv('FLYOVER_GRAPHDB_URL') and os.getenv('FLYOVER_REPOSITORY_NAME'):
            config_name = 'docker'
        elif os.getenv('FLASK_ENV') == 'production':
            config_name = 'production'
        elif os.getenv('FLASK_ENV') == 'testing':
            config_name = 'testing'
        else:
            config_name = 'development'
    
    config_class = config_map.get(config_name, config_map['default'])
    return config_class()