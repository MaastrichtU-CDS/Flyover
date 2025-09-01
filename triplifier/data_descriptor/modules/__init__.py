"""
Modular components for the Flyover data descriptor application.

This package contains the core utility functions and classes that have been
extracted from the main application file to improve modularity and maintainability.
"""

from .app_config import setup_logging
from .session_management import Cache
from .file_operations import allowed_file
from .graphdb_operations import (
    check_graph_exists,
    api_check_graph_exists,
    execute_query,
    upload_file_to_graphdb,
    upload_ontology_then_data
)
from .data_operations import (
    retrieve_categories,
    retrieve_global_names,
    formulate_local_semantic_map,
    handle_postgres_data,
    clean_column_names,
    preprocess_dataframe,
    get_original_column_name
)
from .triplification import run_triplifier

# TODO: Enable these imports after full extraction is complete
# from .relationship_processing import (
#     insert_equivalencies,
#     get_column_class_uri,
#     insert_fk_relation,
#     process_pk_fk_relationships,
#     background_pk_fk_processing,
#     get_existing_graph_structure,
#     get_existing_column_class_uri,
#     insert_cross_graph_relation,
#     process_cross_graph_relationships,
#     background_cross_graph_processing
# )

__all__ = [
    # App configuration
    'setup_logging',
    
    # Session management
    'Cache',
    
    # File operations
    'allowed_file',
    
    # GraphDB operations
    'check_graph_exists',
    'api_check_graph_exists',
    'execute_query',
    'upload_file_to_graphdb',
    'upload_ontology_then_data',
    
    # Data operations
    'retrieve_categories',
    'retrieve_global_names',
    'formulate_local_semantic_map',
    'handle_postgres_data',
    'clean_column_names',
    'preprocess_dataframe',
    'get_original_column_name',
    
    # Triplification
    'run_triplifier',
    
    # TODO: Enable these after full extraction
    # # Relationship processing
    # 'insert_equivalencies',
    # 'get_column_class_uri',
    # 'insert_fk_relation',
    # 'process_pk_fk_relationships',
    # 'background_pk_fk_processing',
    # 'get_existing_graph_structure',
    # 'get_existing_column_class_uri',
    # 'insert_cross_graph_relation',
    # 'process_cross_graph_relationships',
    # 'background_cross_graph_processing',
]