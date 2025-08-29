"""
Flask blueprints for the Flyover data descriptor application.

This package organizes Flask routes into logical blueprints for better
maintainability and modular structure.
"""

from .main import main_bp
from .ingest import ingest_bp
from .describe import describe_bp
from .annotate import annotate_bp
from .api import api_bp

__all__ = [
    'main_bp',
    'ingest_bp', 
    'describe_bp',
    'annotate_bp',
    'api_bp'
]