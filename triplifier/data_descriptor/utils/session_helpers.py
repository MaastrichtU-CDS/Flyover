"""
Session helper functions for the Flyover data descriptor application.

This module contains utility functions for initialising and managing session state,
particularly for database information that needs to be fetched from the RDF store.
"""

import copy
import logging
import requests

import polars as pl

from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Tuple

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

# Query to fetch unique database names
DATABASE_NAME_QUERY = """
PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
select ?db where {
    ?s dbo:table ?db.
    ?s rdfs:subClassOf dbo:TableRow.
}
"""


def check_any_data_graph_exists(repo: str, graphdb_url: str) -> bool:
    """
    This function checks if any data graph exists in a GraphDB repository.
    It checks for graphs matching the pattern http://data.local/* which includes
    both the legacy single graph (http://data.local/) and new per-table graphs
    (http://data.local/tablename/).

    Args:
        repo: The name of the repository in GraphDB.
        graphdb_url: The base URL of the GraphDB instance.

    Returns:
        bool: True if any data graph exists, False otherwise.

    Raises:
        Exception: If the request to the GraphDB instance fails,
        an exception is raised with the status code of the failed request.
    """
    # Construct a SPARQL query that checks for any graph starting with http://data.local/
    query = """
    ASK WHERE {
        GRAPH ?g {
            ?s ?p ?o
        }
        FILTER(STRSTARTS(STR(?g), "http://data.local/"))
    }
    """

    # Send a GET request to the GraphDB instance
    response = requests.get(
        f"{graphdb_url}/repositories/{repo}",
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=30,
    )

    # If the request is successful, return the result of the ASK query
    if response.status_code == 200:
        return response.json()["boolean"]
    # If the request fails, raise an exception with the status code
    else:
        raise Exception(f"Query failed with status code {response.status_code}")


def graph_database_ensure_backend_initialisation(
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
    databases = graph_database_fetch_from_rdf(session_cache.repo, execute_query_func)

    if databases is None or len(databases) == 0:
        return False

    session_cache.databases = databases
    return True


def graph_database_fetch_from_rdf(
    repo: str, execute_query_func: Callable[[str, str], str]
) -> Optional[List[str]]:
    """
    Fetch database names from the RDF-store.

    This function queries the GraphDB repository to get column information and extracts
    the unique database names from the URIs. This is useful when users navigate directly
    to annotation routes without going through the 'describe' step.

    Args:
        repo: The repository name to query
        execute_query_func: The function to execute SPARQL queries

    Returns:
        List[str] or None: List of unique database names, or None if fetching fails
    """
    try:
        # Execute the query and read the results into a polars DataFrame
        query_result = execute_query_func(repo, DATABASE_NAME_QUERY)
        database_info = pl.read_csv(
            StringIO(query_result),
            infer_schema_length=0,
            null_values=[],
            try_parse_dates=False,
        )

        # Check if we have valid results
        if database_info.is_empty() or "db" not in database_info.columns:
            logger.warning("No column information found in the RDF store")
            return None

        # Filter out None/null values and get unique database names
        unique_values = database_info.get_column("db").drop_nulls().unique().to_list()
        # Filter out empty strings
        unique_values = [v for v in unique_values if v and str(v).strip()]

        if len(unique_values) == 0:
            logger.warning("No valid database names could be extracted from URIs")
            return None

        logger.info(f"Fetched {len(unique_values)} database(s) from RDF store")
        return unique_values

    except pl.exceptions.ComputeError as e:
        logger.error(f"Failed to parse SPARQL query result as CSV: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching databases from RDF store: {e}")
        return None


def graph_database_find_name_match(
    map_database_name: Optional[str], target_database: str
) -> bool:
    """
    Check if a semantic map's database_name matches a target database.

    This function implements strict matching with a fallback for .csv extension handling.
    If map_database_name is None or empty, the map is considered a global template
    and applies to all databases.

    Args:
        map_database_name: The database_name field from the semantic map JSON (can be None)
        target_database: The database name to match against

    Returns:
        bool: True if the map should apply to this database, False otherwise
    """
    # If map_database_name is None or empty, it's a global template - applies to all
    if map_database_name is None or map_database_name == "":
        return True

    # Strict exact match first
    if map_database_name == target_database:
        return True

    # Fallback: try matching with/without .csv extension
    map_name_no_ext = (
        map_database_name.rstrip(".csv")
        if map_database_name.endswith(".csv")
        else map_database_name
    )
    target_no_ext = (
        target_database.rstrip(".csv")
        if target_database.endswith(".csv")
        else target_database
    )

    if map_name_no_ext == target_no_ext:
        logger.debug(
            f"Database name matched with .csv extension fallback: "
            f"'{map_database_name}' matches '{target_database}'"
        )
        return True

    return False


def graph_database_find_matching(
    map_database_name: Optional[str], available_databases
) -> Optional[str]:
    """
    Find a matching database from the available databases for a given map database name.

    Args:
        map_database_name: The database_name field from the semantic map JSON
        available_databases: Array/list of available database names

    Returns:
        str or None: The matching database name, or None if no match was found
    """
    if available_databases is None or len(available_databases) == 0:
        return None

    # If map_database_name is None, it's a global template - return the first database as an indication it applies
    if map_database_name is None or map_database_name == "":
        return str(available_databases[0]) if len(available_databases) > 0 else None

    for db in available_databases:
        if graph_database_find_name_match(map_database_name, str(db)):
            return str(db)

    return None


def get_table_names_from_mapping(session_cache) -> List[str]:
    """
    Get table names from the available semantic map for matching against RDF store databases.

    For JSON-LD:  extracts sourceFile from databases → tables → sourceFile
    For legacy JSON: returns database_name as a single-item list

    Args:
        session_cache: The session cache object

    Returns:
        list: List of table names to match against RDF store databases
    """
    # Prefer jsonld_mapping when available
    if session_cache.jsonld_mapping is not None:
        # Try to get table names from the jsonld_mapping object
        # If it has a method for this, use it; otherwise extract from raw data
        if hasattr(session_cache.jsonld_mapping, 'get_table_names'):
            return session_cache.jsonld_mapping.get_table_names()
        # Fallback:  extract from the raw jsonld data if available
        if hasattr(session_cache.jsonld_mapping, 'raw_data'):
            return get_table_names_from_jsonld(session_cache.jsonld_mapping.raw_data)
        # Last resort: try to_legacy_format and check for sourceFile info
        try:
            legacy = session_cache.jsonld_mapping.to_legacy_format()
            if isinstance(legacy, dict) and "databases" in legacy:
                return get_table_names_from_jsonld(legacy)
        except Exception:
            pass

    # Check global_semantic_map for JSON-LD format
    if isinstance(session_cache.global_semantic_map, dict):
        if is_jsonld_semantic_map(session_cache.global_semantic_map):
            table_names = get_table_names_from_jsonld(session_cache.global_semantic_map)
            if table_names:
                return table_names

        # Legacy format:  return database_name as a list
        database_name = session_cache.global_semantic_map.get("database_name")
        if isinstance(database_name, str) and database_name:
            return [database_name]

    return []


def get_table_names_from_jsonld(semantic_map:  dict) -> list:
    """
    Extract all table names (sourceFile values) from a JSON-LD semantic map.

    Args:
        semantic_map: The semantic map dictionary (could be JSON or JSON-LD format)

    Returns:
        list: List of table names (sourceFile values) from all databases/tables
    """
    table_names = []

    # Check if this is JSON-LD format (has 'databases' key)
    databases = semantic_map.get("databases")
    if not isinstance(databases, dict):
        return table_names

    for db_key, db_data in databases. items():
        if not isinstance(db_data, dict):
            continue
        tables = db_data. get("tables")
        if not isinstance(tables, dict):
            continue
        for table_key, table_data in tables. items():
            if not isinstance(table_data, dict):
                continue
            source_file = table_data.get("sourceFile")
            if isinstance(source_file, str) and source_file:
                table_names.append(source_file)

    return table_names


def is_jsonld_semantic_map(semantic_map: dict) -> bool:
    """
    Check if a semantic map is in JSON-LD format.

    Args:
        semantic_map: The semantic map dictionary

    Returns:
        bool: True if JSON-LD format, False otherwise
    """
    if not isinstance(semantic_map, dict):
        return False
    return "@context" in semantic_map or "databases" in semantic_map


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


def get_semantic_map_for_annotation(session_cache, database_key: Optional[str] = None):
    """
    Get the effective semantic map for annotation, preferring jsonld_mapping.

    This function provides a unified interface for getting semantic map data,
    automatically handling the transition from global_semantic_map to jsonld_mapping.

    Args:
        session_cache: The session cache object containing mappings
        database_key: Optional database key to filter local mappings

    Returns:
        tuple: (semantic_map_dict, database_name, is_jsonld)
            - semantic_map_dict: The semantic map in legacy format
            - database_name: The database name from the mapping
            - is_jsonld: True if source is jsonld_mapping, False if global_semantic_map
    """
    # Prefer jsonld_mapping when available
    if session_cache.jsonld_mapping is not None:
        legacy_map = session_cache.jsonld_mapping.to_legacy_format(
            database_key=database_key
        )
        db_name = session_cache.jsonld_mapping.get_first_database_name()
        return legacy_map, db_name, True

    # Fall back to global_semantic_map
    if isinstance(session_cache.global_semantic_map, dict):
        db_name = session_cache.global_semantic_map.get("database_name")
        return session_cache.global_semantic_map, db_name, False

    return None, None, False


def has_semantic_map(session_cache) -> bool:
    """
    Check if any semantic map (jsonld_mapping or global_semantic_map) is available.

    Args:
        session_cache: The session cache object

    Returns:
        bool: True if a semantic map is available, False otherwise
    """
    if session_cache.jsonld_mapping is not None:
        return True
    if isinstance(session_cache.global_semantic_map, dict):
        return True
    return False


def get_database_name_from_mapping(session_cache) -> Optional[str]:
    """
    Get the database name from the available semantic map.

    Args:
        session_cache: The session cache object

    Returns:
        str or None: The database name, or None if not available
    """
    if session_cache.jsonld_mapping is not None:
        return session_cache.jsonld_mapping.get_first_database_name()
    if isinstance(session_cache.global_semantic_map, dict):
        return session_cache.global_semantic_map.get("database_name")
    return None
