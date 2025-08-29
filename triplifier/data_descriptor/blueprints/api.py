"""
API blueprint for REST API endpoints and AJAX handlers.

Contains routes for API calls, status checks, and asynchronous operations.
"""

import json
import logging
from io import StringIO
from flask import Blueprint, request, jsonify, current_app
import pandas as pd

# Import modular components
from modules import (
    Cache,
    api_check_graph_exists,
    execute_query
)

api_bp = Blueprint('api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)


@api_bp.route('/check-graph-exists', methods=['GET'])
def check_graph_exists():
    """
    API endpoint to check if a graph exists in GraphDB.
    
    This endpoint provides a RESTful way to check if data already exists
    in the GraphDB repository, commonly used by frontend JavaScript.
    
    Workflow:
        1. Extract repository and graph URI from request parameters
        2. Use modular api_check_graph_exists() function for checking
        3. Return JSON response with existence status
        4. Handle GraphDB connection errors gracefully
    
    Returns:
        flask.jsonify: JSON response with graph existence status
        
    Route:
        GET /api/check-graph-exists - Graph existence check endpoint
        
    Query Parameters:
        repo (str, optional): Repository name (defaults to session cache)
        graph_uri (str, optional): Graph URI to check (defaults to "http://data.local/")
        
    Response:
        JSON: {
            "exists": bool,
            "repository": str,
            "graph_uri": str,
            "message": str (optional error message)
        }
        
    Status Codes:
        200: Success (graph check completed)
        400: Bad request (missing parameters)
        500: Server error (GraphDB connection issues)
    """
    session_cache = current_app.session_cache
    graphdb_url = current_app.config['GRAPHDB_URL']
    
    try:
        # Get parameters from request or use defaults
        repo = request.args.get('repo', session_cache.repo)
        graph_uri = request.args.get('graph_uri', "http://data.local/")
        
        if not repo:
            return jsonify({
                "error": "Repository name is required",
                "exists": False
            }), 400
        
        # Check graph existence using modular function
        exists = api_check_graph_exists(repo, graph_uri, graphdb_url)
        
        logger.info(f"Graph existence check: {repo}/{graph_uri} -> {exists}")
        
        return jsonify({
            "exists": exists,
            "repository": repo,
            "graph_uri": graph_uri
        })
        
    except Exception as e:
        logger.error(f"Error checking graph existence: {e}")
        return jsonify({
            "error": f"Failed to check graph existence: {e}",
            "exists": False,
            "repository": repo if 'repo' in locals() else None,
            "graph_uri": graph_uri if 'graph_uri' in locals() else None
        }), 500


@api_bp.route('/existing-graph-structure', methods=['GET'])
def get_existing_graph_structure():
    """
    API endpoint to retrieve the structure of existing graph data.
    
    This endpoint returns information about existing tables and columns
    in the GraphDB repository for data linking and relationship mapping.
    
    Workflow:
        1. Execute SPARQL query to retrieve existing data structure
        2. Parse query results to extract table and column information
        3. Organize data into tables and table-column mappings
        4. Return structured JSON response
    
    Returns:
        flask.jsonify: JSON response with graph structure data
        
    Route:
        GET /api/existing-graph-structure - Graph structure information endpoint
        
    Response:
        JSON: {
            "tables": [str],
            "tableColumns": {
                "table_name": [str]  # column names
            },
            "totalTables": int,
            "totalColumns": int
        }
        
    Status Codes:
        200: Success (structure retrieved)
        500: Server error (query execution issues)
    """
    session_cache = current_app.session_cache
    
    try:
        # SPARQL query to get existing data structure
        structure_query = """
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        SELECT ?uri ?column 
        WHERE {             
                ?uri dbo:column ?column .
            }
        """
        
        result = execute_query(session_cache.repo, structure_query)
        
        if not result or result.strip() == "":
            logger.info("No existing graph structure found")
            return jsonify({
                "tables": [],
                "tableColumns": {},
                "totalTables": 0,
                "totalColumns": 0
            })
        
        # Parse the CSV result into a DataFrame
        structure_info = pd.read_csv(StringIO(result))
        
        if structure_info.empty:
            return jsonify({
                "tables": [],
                "tableColumns": {},
                "totalTables": 0,
                "totalColumns": 0
            })
        
        # Extract table names from URIs
        structure_info['table'] = structure_info['uri'].str.extract(
            r'.*/(.*?)\.', expand=False
        ).fillna('unknown')
        
        # Get unique tables
        tables = structure_info['table'].unique().tolist()
        
        # Create table-column mapping
        table_columns = {}
        total_columns = 0
        
        for table in tables:
            columns = structure_info[structure_info['table'] == table]['column'].tolist()
            table_columns[table] = columns
            total_columns += len(columns)
        
        logger.info(f"Retrieved graph structure: {len(tables)} tables, {total_columns} columns")
        
        return jsonify({
            "tables": tables,
            "tableColumns": table_columns,
            "totalTables": len(tables),
            "totalColumns": total_columns
        })
        
    except Exception as e:
        logger.error(f"Error getting existing graph structure: {e}")
        return jsonify({
            "error": f"Failed to retrieve graph structure: {e}",
            "tables": [],
            "tableColumns": {},
            "totalTables": 0,
            "totalColumns": 0
        }), 500


@api_bp.route('/session-status', methods=['GET'])
def get_session_status():
    """
    API endpoint to retrieve current session status and progress.
    
    This endpoint provides information about the current state of the
    user's workflow, including uploaded data, processing status, etc.
    
    Returns:
        flask.jsonify: JSON response with session status
        
    Route:
        GET /api/session-status - Session status information endpoint
        
    Response:
        JSON: {
            "hasData": bool,
            "hasSemanticMap": bool,
            "hasAnnotations": bool,
            "processingStatus": str,
            "currentStep": str,
            "dataInfo": {
                "columns": int,
                "rows": int,
                "source": str
            }
        }
    """
    session_cache = current_app.session_cache
    
    try:
        # Gather session information
        has_data = hasattr(session_cache, 'cleaned_df') and session_cache.cleaned_df is not None
        has_semantic_map = hasattr(session_cache, 'uploaded_semantic_map')
        has_annotations = hasattr(session_cache, 'uploaded_annotations')
        
        # Determine current workflow step
        current_step = "ingest"
        if has_annotations:
            current_step = "annotate"
        elif has_semantic_map or hasattr(session_cache, 'column_mapping'):
            current_step = "describe"
        elif has_data:
            current_step = "describe"
        
        # Get data information if available
        data_info = {}
        if has_data:
            df = session_cache.cleaned_df
            data_info = {
                "columns": len(df.columns),
                "rows": len(df),
                "source": "file" if hasattr(session_cache, 'file_path') else "database"
            }
        
        # Get processing status
        processing_status = getattr(session_cache, 'annotation_status', 'not_started')
        
        return jsonify({
            "hasData": has_data,
            "hasSemanticMap": has_semantic_map,
            "hasAnnotations": has_annotations,
            "processingStatus": processing_status,
            "currentStep": current_step,
            "dataInfo": data_info,
            "repository": session_cache.repo
        })
        
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        return jsonify({
            "error": f"Failed to retrieve session status: {e}",
            "hasData": False,
            "hasSemanticMap": False,
            "hasAnnotations": False,
            "processingStatus": "error",
            "currentStep": "unknown",
            "dataInfo": {}
        }), 500


@api_bp.route('/execute-query', methods=['POST'])
def execute_sparql_query():
    """
    API endpoint to execute custom SPARQL queries.
    
    This endpoint allows frontend applications to execute custom
    SPARQL queries against the GraphDB repository.
    
    Returns:
        flask.jsonify: JSON response with query results
        
    Route:
        POST /api/execute-query - SPARQL query execution endpoint
        
    Request Body:
        JSON: {
            "query": str,  # SPARQL query string
            "repository": str (optional),  # Repository name
            "format": str (optional)  # Response format preference
        }
        
    Response:
        JSON: {
            "success": bool,
            "results": str or dict,  # Query results
            "query": str,  # Original query
            "executionTime": float (optional)
        }
        
    Status Codes:
        200: Success (query executed)
        400: Bad request (invalid query or parameters)
        500: Server error (query execution issues)
    """
    session_cache = current_app.session_cache
    
    try:
        # Parse request JSON
        data = request.get_json()
        if not data:
            return jsonify({
                "success": False,
                "error": "Request body must be JSON"
            }), 400
        
        query = data.get('query')
        if not query:
            return jsonify({
                "success": False,
                "error": "SPARQL query is required"
            }), 400
        
        repository = data.get('repository', session_cache.repo)
        
        # Execute the query using modular function
        import time
        start_time = time.time()
        
        result = execute_query(repository, query)
        
        execution_time = time.time() - start_time
        
        logger.info(f"SPARQL query executed successfully in {execution_time:.2f}s")
        
        return jsonify({
            "success": True,
            "results": result,
            "query": query,
            "repository": repository,
            "executionTime": execution_time
        })
        
    except Exception as e:
        logger.error(f"Error executing SPARQL query: {e}")
        return jsonify({
            "success": False,
            "error": f"Query execution failed: {e}",
            "query": data.get('query') if 'data' in locals() else None
        }), 500


@api_bp.route('/validate-connection', methods=['POST'])
def validate_database_connection():
    """
    API endpoint to validate database connection parameters.
    
    This endpoint tests database connections without performing
    full data ingestion, useful for validation workflows.
    
    Returns:
        flask.jsonify: JSON response with connection validation results
        
    Route:
        POST /api/validate-connection - Database connection validation endpoint
        
    Request Body:
        JSON: {
            "host": str,
            "port": int,
            "database": str,
            "username": str,
            "password": str,
            "connection_type": str  # "postgresql", etc.
        }
        
    Response:
        JSON: {
            "valid": bool,
            "message": str,
            "connection_info": dict (optional)
        }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                "valid": False,
                "message": "Request body must be JSON"
            }), 400
        
        required_fields = ['host', 'database', 'username', 'password']
        missing_fields = [field for field in required_fields if not data.get(field)]
        
        if missing_fields:
            return jsonify({
                "valid": False,
                "message": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400
        
        # TODO: Implement actual database connection validation
        # This would involve testing the connection without data retrieval
        
        return jsonify({
            "valid": True,
            "message": "Connection validation not yet implemented",
            "connection_info": {
                "host": data.get('host'),
                "port": data.get('port', 5432),
                "database": data.get('database')
            }
        })
        
    except Exception as e:
        logger.error(f"Error validating database connection: {e}")
        return jsonify({
            "valid": False,
            "message": f"Connection validation failed: {e}"
        }), 500