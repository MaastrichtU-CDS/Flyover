"""
Relationship processing utilities for the Flyover data descriptor.

This module contains functions for processing primary key/foreign key relationships
and cross-graph relationship handling.
"""

from typing import Any, Optional, Dict, List
import logging
import pandas as pd
import time
from io import StringIO

# Use the centrally configured logger
logger = logging.getLogger(__name__)


def insert_equivalencies(descriptive_info: dict, variable: str, session_cache: Any, execute_query_func: callable) -> Optional[str]:
    """
    Insert equivalencies into a GraphDB repository.
    
    This function inserts an owl:equivalentClass triple into the ontology graph
    for the specified variable based on its descriptive information.
    
    Args:
        descriptive_info (dict): Dictionary containing descriptive information about variables
        variable (str): The name of the variable for which the equivalency is to be inserted
        session_cache: Session cache object containing repository information
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        Optional[str]: Result of the query execution or None if skipped
    """
    try:
        # Skip if variable missing or empty
        if variable not in descriptive_info or not descriptive_info[variable]:
            logger.info(f"Skipping equivalency for {variable}: no descriptive info")
            return None

        var_info = descriptive_info[variable]

        # Get the three main fields
        type_value = var_info.get('type', '')
        description_value = var_info.get('description', '')
        comments_value = var_info.get('comments', '')

        # Check if any of these fields has meaningful content
        has_type = type_value not in ['', 'Variable type: ', 'Variable type: None']
        has_description = description_value not in ['', 'Variable description: ', 'Variable description: None']
        has_comments = comments_value not in ['', 'Variable comment: No comment provided']

        # Skip if none of the fields has meaningful content
        if not (has_type or has_description or has_comments):
            logger.info(f"Skipping equivalency for {variable}: no meaningful content")
            return None

        query = f"""
                    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
                    PREFIX db: <http://{session_cache.repo}.local/rdf/ontology/>
                    PREFIX roo: <http://www.cancerdata.org/roo/>
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>

                    INSERT  
                    {{
                        GRAPH <http://ontology.local/>
                        {{ ?s owl:equivalentClass "{list(var_info.values())}". }}
                    }}
                    WHERE 
                    {{
                        ?s dbo:column '{variable}'.
                    }}        
                """
        
        result = execute_query_func(session_cache.repo, query, "update", "/statements")
        logger.info(f"Successfully inserted equivalency for variable: {variable}")
        return result
        
    except Exception as e:
        logger.error(f"Error inserting equivalencies for {variable}: {e}")
        return None


def get_column_class_uri(table_name: str, column_name: str, session_cache: Any, execute_query_func: callable) -> Optional[str]:
    """
    Retrieve column class URI from the GraphDB repository.
    
    This function queries the GraphDB repository to find the URI of a specific
    column in a table based on the table and column names.
    
    Args:
        table_name (str): Name of the database table
        column_name (str): Name of the column
        session_cache: Session cache object containing repository information
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        Optional[str]: URI of the column class or None if not found
    """
    query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?uri WHERE {{
        ?uri dbo:column '{column_name}' .
        FILTER(CONTAINS(LCASE(STR(?uri)), LCASE('{table_name}')))
    }}
    LIMIT 1
    """

    try:
        query_result = execute_query_func(session_cache.repo, query)

        if not query_result or query_result.strip() == "":
            logger.warning(f"No results found for column {table_name}.{column_name}")
            return None

        column_info = pd.read_csv(StringIO(query_result))

        if column_info.empty:
            logger.warning(f"Empty result set for column {table_name}.{column_name}")
            return None

        if 'uri' not in column_info.columns:
            logger.error(f"Query result format error: no 'uri' column found")
            return None

        uri = column_info['uri'].iloc[0]
        logger.info(f"Found URI for {table_name}.{column_name}: {uri}")
        return uri

    except Exception as e:
        logger.error(f"Error fetching column URI for {table_name}.{column_name}: {e}")
        return None


def insert_fk_relation(fk_predicate: str, column_class_uri: str, target_class_uri: str, 
                      session_cache: Any, execute_query_func: callable) -> str:
    """
    Insert PK/FK relationship into the data graph.
    
    This function creates relationships between foreign key columns and their
    corresponding primary key targets in the knowledge graph.
    
    Args:
        fk_predicate (str): The foreign key predicate URI
        column_class_uri (str): URI of the column class
        target_class_uri (str): URI of the target class
        session_cache: Session cache object containing repository information
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        str: Result of the query execution
        
    Raises:
        Exception: If the SPARQL query execution fails
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

    try:
        result = execute_query_func(session_cache.repo, insert_query, "update", "/statements")
        logger.info(f"Successfully inserted FK relation: {column_class_uri} -> {target_class_uri}")
        return result
    except Exception as e:
        logger.error(f"Error inserting FK relation: {e}")
        raise


def process_pk_fk_relationships(session_cache: Any) -> bool:
    """
    Process all PK/FK relationships after successful triplification.
    
    This function processes primary key/foreign key relationships stored in the
    session cache and creates the appropriate links in the knowledge graph.
    
    Args:
        session_cache: Session cache object containing PK/FK data
        
    Returns:
        bool: True if processing successful
    """
    if not session_cache.pk_fk_data:
        logger.info("No PK/FK data to process")
        return True

    try:
        logger.info("Starting PK/FK relationship processing...")
        session_cache.pk_fk_status = "processing"

        # Import execute_query function from graphdb_operations
        from .graphdb_operations import execute_query

        # Create mapping of files with their PK/FK info
        file_map = {rel['fileName']: rel for rel in session_cache.pk_fk_data}

        # Process each relationship
        for rel in session_cache.pk_fk_data:
            if not all([rel.get('foreignKey'), rel.get('foreignKeyTable'),
                        rel.get('foreignKeyColumn')]):
                logger.warning(f"Incomplete FK relationship data: {rel}")
                continue

            # Find the target table's PK info
            target_file = rel['foreignKeyTable']
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get('primaryKey'):
                logger.warning(f"No target PK found for {target_file}")
                continue

            # Generate FK configuration
            fk_config = {
                'foreignKeyTable': rel['fileName'].replace('.csv', ''),
                'foreignKeyColumn': rel['foreignKey'],
                'primaryKeyTable': target_file.replace('.csv', ''),
                'primaryKeyColumn': target_rel['primaryKey']
            }

            source_uri = get_column_class_uri(
                fk_config['foreignKeyTable'],
                fk_config['foreignKeyColumn'],
                session_cache,
                execute_query
            )

            target_uri = get_column_class_uri(
                fk_config['primaryKeyTable'],
                fk_config['primaryKeyColumn'],
                session_cache,
                execute_query
            )

            if not source_uri or not target_uri:
                logger.warning(f"Could not find URIs for FK relationship: {fk_config}")
                continue

            fk_predicate = f"http://um-cds/ontologies/databaseontology/fk_refers_to"

            # Insert the relationship
            insert_fk_relation(fk_predicate, source_uri, target_uri, session_cache, execute_query)
            logger.info(
                f"Created FK relationship: {fk_config['foreignKeyTable']}.{fk_config['foreignKeyColumn']} -> {fk_config['primaryKeyTable']}.{fk_config['primaryKeyColumn']}")

        session_cache.pk_fk_status = "success"
        logger.info("PK/FK relationship processing completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error processing PK/FK relationships: {e}")
        session_cache.pk_fk_status = "failed"
        return False


def background_pk_fk_processing(session_cache: Any) -> None:
    """
    Background processing for PK/FK relationships.
    
    This function handles background processing of primary key/foreign key
    relationships with appropriate delays and error handling.
    
    Args:
        session_cache: Session cache object
    """
    try:
        # Small delay to ensure GraphDB has processed the uploaded data
        logger.info("Starting background PK/FK processing...")
        time.sleep(3)
        process_pk_fk_relationships(session_cache)
    except Exception as e:
        logger.error(f"Background PK/FK processing error: {e}")
        session_cache.pk_fk_status = "failed"


def get_existing_graph_structure(session_cache: Any, execute_query_func: callable) -> Dict[str, Any]:
    """
    Get existing graph structure for cross-graph relationship processing.
    
    This function queries the GraphDB repository to retrieve the structure
    of existing graph data, including tables and their columns.
    
    Args:
        session_cache: Session cache object
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        Dict[str, Any]: Graph structure information with tables and tableColumns
    """
    try:
        # Query to get existing tables and their columns
        structure_query = """
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
        SELECT ?uri ?column 
        WHERE {             
                ?uri dbo:column ?column .
            }
        """

        result = execute_query_func(session_cache.repo, structure_query)

        if not result or result.strip() == "":
            logger.warning("No existing graph structure found")
            return {"tables": [], "tableColumns": {}}

        # Parse the result
        structure_info = pd.read_csv(StringIO(result))

        if structure_info.empty:
            logger.warning("Empty structure result set")
            return {"tables": [], "tableColumns": {}}

        # Extract table names from URIs and organize by table
        structure_info['table'] = structure_info['uri'].str.extract(r'.*/(.*?)\.', expand=False).fillna('unknown')

        # Get unique tables
        tables = structure_info['table'].unique().tolist()

        # Create table-column mapping
        table_columns = {}
        for table in tables:
            columns = structure_info[structure_info['table'] == table]['column'].tolist()
            table_columns[table] = columns

        logger.info(f"Retrieved graph structure: {len(tables)} tables")
        return {
            "tables": tables,
            "tableColumns": table_columns
        }

    except Exception as e:
        logger.error(f"Error getting existing graph structure: {e}")
        return {"tables": [], "tableColumns": {}}


def get_existing_column_class_uri(table_name: str, column_name: str, 
                                 session_cache: Any, execute_query_func: callable) -> Optional[str]:
    """
    Get existing column class URI for cross-graph relationships.
    
    This function retrieves the URI of an existing column class from the
    graph database for use in cross-graph relationship processing.
    
    Args:
        table_name (str): Name of the database table
        column_name (str): Name of the column
        session_cache: Session cache object
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        Optional[str]: URI of the existing column class or None
    """
    query = f"""
    PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?uri WHERE {{
            ?uri dbo:column '{column_name}' .
            FILTER(CONTAINS(LCASE(STR(?uri)), LCASE('{table_name}')))
    }}
    LIMIT 1
    """

    try:
        query_result = execute_query_func(session_cache.repo, query)

        if not query_result or query_result.strip() == "":
            logger.warning(f"No existing results found for column {table_name}.{column_name}")
            return None

        column_info = pd.read_csv(StringIO(query_result))

        if column_info.empty or 'uri' not in column_info.columns:
            logger.warning(f"No URI found in results for {table_name}.{column_name}")
            return None

        uri = column_info['uri'].iloc[0]
        logger.info(f"Found existing URI for {table_name}.{column_name}: {uri}")
        return uri

    except Exception as e:
        logger.error(f"Error fetching existing column URI for {table_name}.{column_name}: {e}")
        return None


def insert_cross_graph_relation(predicate: str, new_column_uri: str, existing_column_uri: str,
                               session_cache: Any, execute_query_func: callable) -> str:
    """
    Insert cross-graph relationship.
    
    This function creates relationships between columns from different graphs
    or datasets within the knowledge graph.
    
    Args:
        predicate (str): Relationship predicate
        new_column_uri (str): URI of the new column
        existing_column_uri (str): URI of the existing column
        session_cache: Session cache object
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        str: Result of the query execution
        
    Raises:
        Exception: If the SPARQL query execution fails
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

    try:
        result = execute_query_func(session_cache.repo, insert_query, "update", "/statements")
        logger.info(f"Successfully inserted cross-graph relation: {new_column_uri} -> {existing_column_uri}")
        return result
    except Exception as e:
        logger.error(f"Error inserting cross-graph relation: {e}")
        raise


def process_cross_graph_relationships(session_cache: Any) -> bool:
    """
    Process cross-graph relationships.
    
    This function processes cross-graph relationships stored in the session
    cache and creates appropriate links between different datasets.
    
    Args:
        session_cache: Session cache object
        
    Returns:
        bool: True if processing successful
    """
    if not session_cache.cross_graph_link_data:
        logger.info("No cross-graph link data to process")
        return True

    try:
        logger.info("Starting cross-graph relationship processing...")
        session_cache.cross_graph_link_status = "processing"

        # Import execute_query function from graphdb_operations
        from .graphdb_operations import execute_query

        link_data = session_cache.cross_graph_link_data

        # Get URIs for new and existing columns
        new_column_uri = get_column_class_uri(
            link_data['newTableName'],
            link_data['newColumnName'],
            session_cache,
            execute_query
        )

        existing_column_uri = get_existing_column_class_uri(
            link_data['existingTableName'],
            link_data['existingColumnName'],
            session_cache,
            execute_query
        )

        if not new_column_uri or not existing_column_uri:
            logger.warning(f"Could not find URIs for cross-graph relationship: {link_data}")
            session_cache.cross_graph_link_status = "failed"
            return False

        predicate = f"http://um-cds/ontologies/databaseontology/fk_refers_to"

        # Insert the relationship
        insert_cross_graph_relation(predicate, new_column_uri, existing_column_uri, session_cache, execute_query)

        logger.info(
            f"Created cross-graph relationship: {link_data['newTableName']}.{link_data['newColumnName']} -> {link_data['existingTableName']}.{link_data['existingColumnName']}")

        session_cache.cross_graph_link_status = "success"
        logger.info("Cross-graph relationship processing completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error processing cross-graph relationships: {e}")
        session_cache.cross_graph_link_status = "failed"
        return False


def background_cross_graph_processing(session_cache: Any) -> None:
    """
    Background processing for cross-graph relationships.
    
    This function handles background processing of cross-graph relationships
    with appropriate delays and error handling.
    
    Args:
        session_cache: Session cache object
    """
    try:
        # Small delay to ensure GraphDB has processed the uploaded data
        # Slightly longer delay than PK/FK to ensure it runs after
        logger.info("Starting background cross-graph processing...")
        time.sleep(5)  
        process_cross_graph_relationships(session_cache)
    except Exception as e:
        logger.error(f"Background cross-graph processing error: {e}")
        session_cache.cross_graph_link_status = "failed"