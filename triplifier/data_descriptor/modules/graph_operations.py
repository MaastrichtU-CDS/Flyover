"""
Graph operations module.

This module contains functions for managing graph relationships,
including primary key/foreign key relationships and cross-graph connections.
"""

import time
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


def insert_fk_relation(fk_predicate: str, column_class_uri: str, target_class_uri: str, 
                      repo: str, execute_query_func) -> str:
    """
    Insert primary key/foreign key relationship into the data graph.

    Args:
        fk_predicate: The predicate URI for the foreign key relationship
        column_class_uri: The URI of the source column class
        target_class_uri: The URI of the target column class
        repo: The GraphDB repository name
        execute_query_func: Function to execute SPARQL queries

    Returns:
        The result of the query execution

    This function creates SPARQL INSERT query to establish relationships between
    data instances that share the same values across foreign key and primary key columns.
    """
    insert_query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT {{
        ?sources <{fk_predicate}> ?targets .
    }} WHERE {{
        ?sources rdf:type <{column_class_uri}> ;
                 dbo:has_cell ?sourceCell .
        ?sourceCell dbo:has_value ?columnValue .

        ?targets rdf:type <{target_class_uri}> ;
                 dbo:has_cell ?targetCell .
        ?targetCell dbo:has_value ?columnValue .
    }}
    """

    return execute_query_func(repo, insert_query, "update", "/statements")


def insert_cross_graph_relation(predicate: str, new_column_uri: str, existing_column_uri: str,
                               repo: str, execute_query_func) -> str:
    """
    Insert cross-graph relationship into the data graph.

    Args:
        predicate: The predicate URI for the cross-graph relationship
        new_column_uri: The URI of the new column class
        existing_column_uri: The URI of the existing column class
        repo: The GraphDB repository name
        execute_query_func: Function to execute SPARQL queries

    Returns:
        The result of the query execution

    This function creates relationships between data instances from newly uploaded data
    and existing data in the graph that share the same values.
    """
    insert_query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    INSERT {{
        ?newSources <{predicate}> ?existingSources .
    }} WHERE {{
        ?newSources rdf:type <{new_column_uri}> ;
                    dbo:has_cell ?newCell .
        ?newCell dbo:has_value ?columnValue .

        ?existingSources rdf:type <{existing_column_uri}> ;
                         dbo:has_cell ?existingCell .
        ?existingCell dbo:has_value ?columnValue .
    }}
    """

    return execute_query_func(repo, insert_query, "update", "/statements")


def process_pk_fk_relationships(pk_fk_data: List[Dict[str, Any]], repo: str, 
                               get_column_class_uri_func, execute_query_func) -> bool:
    """
    Process all primary key/foreign key relationships after successful triplification.

    Args:
        pk_fk_data: List of dictionaries containing PK/FK relationship data
        repo: The GraphDB repository name
        get_column_class_uri_func: Function to get column class URI
        execute_query_func: Function to execute SPARQL queries

    Returns:
        True if processing was successful, False otherwise

    This function processes the uploaded PK/FK relationship data and creates
    appropriate graph connections between related data instances.
    """
    if not pk_fk_data:
        return True

    try:
        logger.info("Starting PK/FK relationship processing...")

        # Create mapping of files with their PK/FK info
        file_map = {rel['fileName']: rel for rel in pk_fk_data}

        # Process each relationship
        for rel in pk_fk_data:
            if not all([rel.get('foreignKey'), rel.get('foreignKeyTable'),
                        rel.get('foreignKeyColumn')]):
                continue

            # Find the target table's PK info
            target_file = rel['foreignKeyTable']
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get('primaryKey'):
                continue

            # Generate FK configuration
            fk_config = {
                'foreignKeyTable': rel['fileName'].replace('.csv', ''),
                'foreignKeyColumn': rel['foreignKey'],
                'primaryKeyTable': target_file.replace('.csv', ''),
                'primaryKeyColumn': target_rel['primaryKey']
            }

            source_uri = get_column_class_uri_func(
                fk_config['foreignKeyTable'],
                fk_config['foreignKeyColumn']
            )

            target_uri = get_column_class_uri_func(
                fk_config['primaryKeyTable'],
                fk_config['primaryKeyColumn']
            )

            if not source_uri or not target_uri:
                logger.warning(f"Could not find URIs for FK relationship: {fk_config}")
                continue

            fk_predicate = f"http://um-cds/ontologies/databaseontology/fk_refers_to"

            # Insert the relationship
            insert_fk_relation(fk_predicate, source_uri, target_uri, repo, execute_query_func)
            logger.info(
                f"Created FK relationship: {fk_config['foreignKeyTable']}.{fk_config['foreignKeyColumn']} -> {fk_config['primaryKeyTable']}.{fk_config['primaryKeyColumn']}")

        logger.info("PK/FK relationship processing completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error processing PK/FK relationships: {e}")
        return False


def process_cross_graph_relationships(cross_graph_link_data: Dict[str, str], repo: str,
                                    get_column_class_uri_func, get_existing_column_class_uri_func,
                                    execute_query_func) -> bool:
    """
    Process cross-graph relationships after successful triplification.

    Args:
        cross_graph_link_data: Dictionary containing cross-graph relationship data
        repo: The GraphDB repository name
        get_column_class_uri_func: Function to get column class URI
        get_existing_column_class_uri_func: Function to get existing column class URI
        execute_query_func: Function to execute SPARQL queries

    Returns:
        True if processing was successful, False otherwise

    This function processes cross-graph linking data to establish relationships
    between newly uploaded data and existing data in the knowledge graph.
    """
    if not cross_graph_link_data:
        return True

    try:
        logger.info("Starting cross-graph relationship processing...")

        link_data = cross_graph_link_data

        # Get URIs for new and existing columns
        new_column_uri = get_column_class_uri_func(
            link_data['newTableName'],
            link_data['newColumnName']
        )

        existing_column_uri = get_existing_column_class_uri_func(
            link_data['existingTableName'],
            link_data['existingColumnName']
        )

        if not new_column_uri or not existing_column_uri:
            logger.error(f"Could not find URIs for cross-graph relationship: {link_data}")
            return False

        predicate = f"http://um-cds/ontologies/databaseontology/fk_refers_to"

        # Insert the relationship
        insert_cross_graph_relation(predicate, new_column_uri, existing_column_uri, repo, execute_query_func)

        logger.info(
            f"Created cross-graph relationship: {link_data['newTableName']}.{link_data['newColumnName']} -> {link_data['existingTableName']}.{link_data['existingColumnName']}")

        logger.info("Cross-graph relationship processing completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error processing cross-graph relationships: {e}")
        return False


def background_pk_fk_processing(pk_fk_data: List[Dict[str, Any]], repo: str,
                               get_column_class_uri_func, execute_query_func,
                               status_callback) -> None:
    """
    Background function to process primary key/foreign key relationships.

    Args:
        pk_fk_data: List of dictionaries containing PK/FK relationship data
        repo: The GraphDB repository name
        get_column_class_uri_func: Function to get column class URI
        execute_query_func: Function to execute SPARQL queries
        status_callback: Callback function to update processing status

    This function runs in the background to process PK/FK relationships after
    a small delay to ensure GraphDB has processed the uploaded data.
    """
    try:
        # Small delay to ensure GraphDB has processed the uploaded data
        time.sleep(3)
        status_callback("processing")
        success = process_pk_fk_relationships(pk_fk_data, repo, get_column_class_uri_func, execute_query_func)
        status_callback("success" if success else "failed")
    except Exception as e:
        logger.error(f"Background PK/FK processing error: {e}")
        status_callback("failed")


def background_cross_graph_processing(cross_graph_link_data: Dict[str, str], repo: str,
                                     get_column_class_uri_func, get_existing_column_class_uri_func,
                                     execute_query_func, status_callback) -> None:
    """
    Background function to process cross-graph relationships.

    Args:
        cross_graph_link_data: Dictionary containing cross-graph relationship data
        repo: The GraphDB repository name
        get_column_class_uri_func: Function to get column class URI
        get_existing_column_class_uri_func: Function to get existing column class URI
        execute_query_func: Function to execute SPARQL queries
        status_callback: Callback function to update processing status

    This function runs in the background to process cross-graph relationships after
    a delay to ensure GraphDB has processed the uploaded data and any PK/FK processing.
    """
    try:
        # Slightly longer delay than PK/FK to ensure it runs after
        time.sleep(5)
        status_callback("processing")
        success = process_cross_graph_relationships(cross_graph_link_data, repo,
                                                   get_column_class_uri_func, get_existing_column_class_uri_func,
                                                   execute_query_func)
        status_callback("success" if success else "failed")
    except Exception as e:
        logger.error(f"Background cross-graph processing error: {e}")
        status_callback("failed")