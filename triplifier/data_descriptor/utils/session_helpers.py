"""
Session helper functions for the Flyover data descriptor application.

This module contains utility functions for initializing and managing session state,
particularly for database information that needs to be fetched from the RDF store.

TODO: These functions should be removed once we migrate to JSON-LD, as the session
state management will be handled differently.
"""

import copy
import logging

import pandas as pd
from io import StringIO

logger = logging.getLogger(__name__)

# Constants for SPARQL queries and regex patterns used in database initialization
COLUMN_INFO_QUERY = """
PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    SELECT ?uri ?column 
    WHERE {
    ?uri dbo:column ?column .
    }
"""
DATABASE_NAME_PATTERN = r".*/(.*?)\."


def fetch_databases_from_rdf(repo, execute_query_func):
    """
    Fetch database names from the RDF store.

    This function queries the GraphDB repository to get column information and extracts
    the unique database names from the URIs. This is useful when users navigate directly
    to annotation routes without going through the describe step.

    Args:
        repo: The repository name to query
        execute_query_func: The function to execute SPARQL queries

    Returns:
        numpy.ndarray or None: Array of unique database names, or None if fetching fails
    """
    try:
        # Execute the query and read the results into a pandas DataFrame
        query_result = execute_query_func(repo, COLUMN_INFO_QUERY)
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

        logger.info(
            f"Fetched {len(unique_values)} database(s) from RDF store"
        )
        return unique_values

    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse SPARQL query result as CSV: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching databases from RDF store: {e}")
        return None


def process_variable_for_annotation(var_name, var_data, global_semantic_map):
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

    # If no local definition from formulated map, check the original uploaded JSON
    # This handles the case when user uploads JSON directly for annotation
    if not has_local_def and isinstance(global_semantic_map, dict):
        original_var_info = global_semantic_map.get(
            "variable_info", {}
        ).get(var_name, {})
        if original_var_info.get("local_definition"):
            var_copy["local_definition"] = original_var_info["local_definition"]
            has_local_def = True

            # Also copy value mappings if they exist in the original JSON
            # Only copy if the current var_copy doesn't have local_term values already
            if "value_mapping" in original_var_info:
                # Check if formulated map already has local_term values (from describe step)
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
