"""
Database and SPARQL operations module.

This module contains functions for interacting with GraphDB repositories,
executing SPARQL queries, and performing database-related operations.
"""

import logging
import requests
from typing import Optional, Union
from flask import flash, render_template

logger = logging.getLogger(__name__)


def check_graph_exists(repo: str, graph_uri: str, graphdb_url: str) -> bool:
    """
    Check if a graph exists in a GraphDB repository.

    Args:
        repo: The name of the repository in GraphDB
        graph_uri: The URI of the graph to check
        graphdb_url: The base URL of the GraphDB instance

    Returns:
        True if the graph exists, False otherwise

    Raises:
        Exception: If the request to the GraphDB instance fails
    """
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


def execute_query(repo: str, query: str, graphdb_url: str, 
                 query_type: Optional[str] = None, 
                 endpoint_appendices: Optional[str] = None) -> Union[str, object]:
    """
    Execute a SPARQL query on a specified GraphDB repository.

    Args:
        repo: The name of the GraphDB repository on which the query is to be executed
        query: The SPARQL query to be executed
        graphdb_url: The base URL of the GraphDB instance
        query_type: The type of the SPARQL query. Defaults to "query"
        endpoint_appendices: Additional endpoint parameters. Defaults to ""

    Returns:
        The result of the query execution as a string if successful,
        or a Flask template render if an error occurs

    Raises:
        Exception: If an error occurs during the query execution
    """
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


def retrieve_categories(repo: str, column_name: str, graphdb_url: str) -> str:
    """
    Execute a SPARQL query to retrieve the categories of a given column.

    Args:
        repo: The name of the GraphDB repository on which the query is to be executed
        column_name: The name of the column for which the categories are to be retrieved
        graphdb_url: The base URL of the GraphDB instance

    Returns:
        The result of the query execution as a string if successful

    The function constructs a SPARQL query that selects the value and count of each 
    category in the specified column, grouped by the value of the category.
    """
    query_categories = f"""
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        PREFIX db: <http://{repo}.local/rdf/ontology/>
        PREFIX roo: <http://www.cancerdata.org/roo/>
        SELECT ?value (COUNT(?value) as ?count)
        WHERE 
        {{  
           ?a a ?v.
           ?v dbo:column '{column_name}'.
           ?a dbo:has_cell ?cell.
           ?cell dbo:has_value ?value
        }} 
        GROUP BY (?value)
    """
    return execute_query(repo, query_categories, graphdb_url)


def get_column_class_uri(table_name: str, column_name: str, repo: str, graphdb_url: str) -> Optional[str]:
    """
    Retrieve the class URI for a specific column in a table.

    Args:
        table_name: Name of the database table
        column_name: Name of the column
        repo: The GraphDB repository name
        graphdb_url: The base URL of the GraphDB instance

    Returns:
        The class URI as a string, or None if not found
    """
    query = f"""
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        PREFIX db: <http://{repo}.local/rdf/ontology/>
        
        SELECT ?class WHERE {{
            ?class dbo:table "{table_name}" .
            ?class dbo:column "{column_name}" .
        }}
    """
    
    try:
        result = execute_query(repo, query, graphdb_url)
        # Parse the result to extract the class URI
        # This is a simplified extraction - in practice you'd parse the SPARQL JSON response
        if "db:" in result:
            # Extract the class URI from the result
            lines = result.split('\n')
            for line in lines:
                if 'db:' in line and table_name in line and column_name in line:
                    # Extract the class URI - this is a simplified approach
                    parts = line.split()
                    for part in parts:
                        if part.startswith('db:') and table_name in part and column_name in part:
                            return part.strip()
        return None
    except Exception as e:
        logger.error(f"Error retrieving column class URI: {e}")
        return None


def get_existing_column_class_uri(table_name: str, column_name: str, repo: str, graphdb_url: str) -> Optional[str]:
    """
    Get the class URI for an existing column in the graph.

    Args:
        table_name: Name of the database table
        column_name: Name of the column  
        repo: The GraphDB repository name
        graphdb_url: The base URL of the GraphDB instance

    Returns:
        The existing class URI as a string, or None if not found
    """
    query = f"""
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        PREFIX db: <http://{repo}.local/rdf/ontology/>
        
        SELECT ?class WHERE {{
            GRAPH <http://data.local/> {{
                ?class dbo:table "{table_name}" .
                ?class dbo:column "{column_name}" .
            }}
        }}
    """
    
    try:
        result = execute_query(repo, query, graphdb_url)
        # Parse result to extract class URI
        if "db:" in result:
            lines = result.split('\n')
            for line in lines:
                if 'db:' in line and table_name in line and column_name in line:
                    parts = line.split()
                    for part in parts:
                        if part.startswith('db:') and table_name in part and column_name in part:
                            return part.strip()
        return None
    except Exception as e:
        logger.error(f"Error retrieving existing column class URI: {e}")
        return None