"""
Session helper functions for the Flyover data descriptor application.

This module contains utility functions for initialising and managing session state,
particularly for database information that needs to be fetched from the RDF store.
"""

import copy
import logging
import numpy
import requests

import pandas as pd

from io import StringIO
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Constants for SPARQL queries and regex patterns used in database initialisation
COLUMN_INFO_QUERY = """
PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    SELECT ?uri ?column 
    WHERE {
    ?uri dbo:column ?column .
    }
"""
DATABASE_NAME_PATTERN = r".*/(.*?)\."


def check_graph_exists(repo: str, graph_uri: str, graphdb_url: str) -> bool:
    """
    This function checks if a graph exists in a GraphDB repository.

    Args:
        repo: The name of the repository in GraphDB.
        graph_uri: The URI of the graph to check.
        graphdb_url: The base URL of the GraphDB instance.

    Returns:
        bool: True if the graph exists, False otherwise.

    Raises:
        Exception: If the request to the GraphDB instance fails,
        an exception is raised with the status code of the failed request.
    """
    # Construct the SPARQL query
    query = f"ASK WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"

    # Send a GET request to the GraphDB instance
    response = requests.get(
        f"{graphdb_url}/repositories/{repo}",
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"},
    )

    # If the request is successful, return the result of the ASK query
    if response.status_code == 200:
        return response.json()["boolean"]
    # If the request fails, raise an exception with the status code
    else:
        raise Exception(f"Query failed with status code {response.status_code}")


def ensure_databases_initialised(
    session_cache, execute_query_func: Callable[[str, str], str]
) -> bool:
    """
    Ensure that session_cache.databases is populated from the RDF-store.

    Args:
        session_cache: The session cache object
        execute_query_func: The function to execute SPARQL queries

    Returns:
        bool: True if databases are available, False otherwise
    """
    databases = fetch_databases_from_rdf(session_cache.repo, execute_query_func)

    if databases is None or len(databases) == 0:
        return False

    session_cache.databases = databases
    return True


def fetch_databases_from_rdf(
    repo: str, execute_query_func: Callable[[str, str], str]
) -> Optional[numpy.ndarray]:
    """
    Fetch database names from the RDF-store.

    This function queries the GraphDB repository to get column information and extracts
    the unique database names from the URIs. This is useful when users navigate directly
    to annotation routes without going through the 'describe' step.

    Args:
        repo: The repository name to query
        execute_query_func: The function to execute SPARQL queries

    Returns:
        numpy.ndarray or None: Array of unique database names, or None if fetching fails
    """
    try:
        # Execute the query and read the results into a pandas DataFrame
        query_result = execute_query_func(
            repo, COLUMN_INFO_QUERY
        )  # TODO simplify this query and make solely for database retrieval
        column_info = pd.read_csv(StringIO(query_result))

        # Check if we have valid results
        if column_info.empty or "uri" not in column_info.columns:
            logger.warning("No column information found in the RDF store")
            return None

        # Extract the database name from the URI
        column_info["database"] = column_info["uri"].str.extract(
            DATABASE_NAME_PATTERN, expand=False
        )

        # Filter out None/NaN values from the extracted database names
        unique_values = column_info["database"].dropna().unique()
        if len(unique_values) == 0:
            logger.warning("No valid database names could be extracted from URIs")
            return None

        logger.info(f"Fetched {len(unique_values)} database(s) from RDF store")
        return unique_values

    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse SPARQL query result as CSV: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching databases from RDF store: {e}")
        return None


def process_variable_for_annotation(
    var_name: str,
    var_data: Dict[str, Any],
    global_semantic_map: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Any], bool]:
    """
    Process a variable for annotation by checking and enriching local definitions.

    This function handles the logic of checking if a variable has a local definition,
    and if not, attempts to get it from the original uploaded JSON. This is a common
    pattern used in annotation_review, start_annotation, and annotation_verify routes.

    TODO: Remove this function once we migrate to JSON-LD, as the variable processing
    will be handled differently.

    Args:
        var_name: The name of the variable to process
        var_data: The variable data dictionary from the local semantic map
        global_semantic_map: The global semantic map (uploaded JSON)

    Returns:
        tuple: (var_copy, has_local_def) where var_copy is the enriched variable data
               and has_local_def indicates if a local definition was found
    """
    # Create a deep copy of the variable data to avoid reference issues
    var_copy = copy.deepcopy(var_data)

    # Check if this variable has a local definition
    has_local_def = var_copy.get("local_definition") is not None

    # If no local definition from the formulated map, check the original uploaded JSON
    # This handles the case when a user uploads JSON directly for annotation
    if not has_local_def and isinstance(global_semantic_map, dict):
        original_var_info = global_semantic_map.get("variable_info", {}).get(
            var_name, {}
        )
        if original_var_info.get("local_definition"):
            var_copy["local_definition"] = original_var_info["local_definition"]
            has_local_def = True

            # Also copy value mappings if they exist in the original JSON
            # Only copy if the current var_copy doesn't have local_term values already
            if "value_mapping" in original_var_info:
                # Check if the formulated map already has local_term values (from the 'describe' step)
                current_value_mapping = var_copy.get("value_mapping", {})
                has_local_terms = False
                if current_value_mapping.get("terms"):
                    for term_data in current_value_mapping["terms"].values():
                        if term_data.get("local_term") is not None:
                            has_local_terms = True
                            break

                # Only overwrite if no local_terms exist yet
                if not has_local_terms:
                    var_copy["value_mapping"] = original_var_info["value_mapping"]

    return var_copy, has_local_def
