"""
GraphDB operations utilities for the Flyover data descriptor.

This module contains functions for interacting with GraphDB repositories,
including graph existence checking and SPARQL query execution.
"""

import os
import requests
import logging
from typing import Optional, Any
from flask import flash, render_template, jsonify

# Use the centrally configured logger
logger = logging.getLogger(__name__)


def get_graphdb_config() -> tuple[str, str]:
    """
    Get GraphDB configuration from environment variables or defaults.
    
    Returns:
        tuple: (graphdb_url, repo) configuration values
    """
    if os.getenv('FLYOVER_GRAPHDB_URL') and os.getenv('FLYOVER_REPOSITORY_NAME'):
        # Running in Docker
        graphdb_url = os.getenv('FLYOVER_GRAPHDB_URL')
        repo = os.getenv('FLYOVER_REPOSITORY_NAME')
    else:
        # Not running in Docker
        graphdb_url = 'http://localhost:7200'
        repo = 'userRepo'
    
    return graphdb_url, repo


def api_check_graph_exists(session_cache: Any) -> Any:
    """
    API endpoint to check if graph data exists in the repository.
    
    This function provides a JSON API endpoint for checking the existence
    of graph data in the configured GraphDB repository.
    
    Args:
        session_cache: The session cache object containing repository configuration
        
    Returns:
        flask.jsonify: JSON response with exists boolean and optional error message
    """
    try:
        if not session_cache.repo:
            return jsonify({'exists': False, 'error': 'No repository configured'})

        # Check if the main data graph exists
        graph_uri = "http://data.local/"
        exists = check_graph_exists(session_cache.repo, graph_uri)

        return jsonify({'exists': exists})
    except Exception as e:
        return jsonify({'exists': False, 'error': str(e)})


def check_graph_exists(repo: str, graph_uri: str, graphdb_url: Optional[str] = None) -> bool:
    """
    Check if a graph exists in a GraphDB repository.
    
    This function verifies the existence of a specific graph in the GraphDB
    repository by executing an ASK SPARQL query.
    
    Args:
        repo (str): The name of the repository in GraphDB
        graph_uri (str): The URI of the graph to check
        graphdb_url (Optional[str]): GraphDB base URL, if None uses config
        
    Returns:
        bool: True if the graph exists, False otherwise
        
    Raises:
        Exception: If the request to the GraphDB instance fails
    """
    if graphdb_url is None:
        graphdb_url, _ = get_graphdb_config()
    
    # Construct the SPARQL query
    query = f"ASK WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"

    # Send a GET request to the GraphDB instance
    response = requests.get(
        f"{graphdb_url}/repositories/{repo}",
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )

    # If the request is successful, return the result of the ASK query
    if response.status_code == 200:
        return response.json()['boolean']
    # If the request fails, raise an exception with the status code
    else:
        raise Exception(f"Query failed with status code {response.status_code}")


def execute_query(repo: str, query: str, query_type: Optional[str] = None, 
                 endpoint_appendices: Optional[str] = None, 
                 graphdb_url: Optional[str] = None) -> Any:
    """
    Execute a SPARQL query on a specified GraphDB repository.
    
    This function executes SPARQL queries (SELECT, ASK, CONSTRUCT, etc.) on the
    specified GraphDB repository and returns the results.
    
    Args:
        repo (str): The name of the GraphDB repository
        query (str): The SPARQL query to be executed
        query_type (Optional[str]): The type of the SPARQL query. Defaults to "query"
        endpoint_appendices (Optional[str]): Additional endpoint parameters
        graphdb_url (Optional[str]): GraphDB base URL, if None uses config
        
    Returns:
        str: The result of the query execution as a string if successful
        flask.render_template: Error template if execution fails
        
    Raises:
        Exception: If an error occurs during query execution
        
    Note:
        The function performs the following steps:
        1. Sets default values for optional parameters
        2. Constructs the endpoint URL using the repository name and appendices
        3. Executes the SPARQL query on the constructed endpoint
        4. Returns the result as a string if successful
        5. Flashes error message and renders template if execution fails
    """
    if graphdb_url is None:
        graphdb_url, _ = get_graphdb_config()
        
    if query_type is None:
        query_type = "query"

    if endpoint_appendices is None:
        endpoint_appendices = ""
        
    try:
        # Construct the endpoint URL
        endpoint = f"{graphdb_url}/repositories/" + repo + endpoint_appendices
        
        # Execute the query
        response = requests.post(endpoint,
                                data={query_type: query},
                                headers={"Content-Type": "application/x-www-form-urlencoded"})
        
        # Return the result of the query execution
        return response.text
    except Exception as e:
        # If an error occurs, flash the error message to the user and render the 'ingest.html' template
        flash(f'Unexpected error when connecting to GraphDB, error: {e}.')
        return render_template('ingest.html')