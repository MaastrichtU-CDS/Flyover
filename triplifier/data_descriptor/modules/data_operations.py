"""
Data operations utilities for the Flyover data descriptor.

This module contains functions for data retrieval, category processing,
semantic mapping, and PostgreSQL data handling.
"""

import copy
import os
from typing import Any, List, Optional, Union
from flask import flash, render_template
from psycopg2 import connect
import logging

# Use the centrally configured logger
logger = logging.getLogger(__name__)


def retrieve_categories(repo: str, column_name: str, execute_query_func: callable) -> str:
    """
    Execute a SPARQL query to retrieve the categories of a given column.
    
    This function constructs and executes a SPARQL query that selects the value
    and count of each category in the specified column from the GraphDB repository.
    
    Args:
        repo (str): The name of the GraphDB repository
        column_name (str): The name of the column for which categories are retrieved
        execute_query_func (callable): Function to execute SPARQL queries
        
    Returns:
        str: The result of the query execution as a string
        
    Note:
        The SPARQL query:
        1. Selects the value and count of each category in the specified column
        2. Groups the results by the value of the category
        3. Uses database ontology and RDF ontology prefixes
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
    return execute_query_func(repo, query_categories)


def retrieve_global_names(session_cache: Any) -> Union[List[str], Any]:
    """
    Retrieve the names of global variables from the session cache.
    
    This function extracts global variable names from the global semantic map
    stored in the session cache. If no semantic map is available, it returns
    a default list of common variable names.
    
    Args:
        session_cache: The session cache object containing global semantic map
        
    Returns:
        List[str]: A list of global variable names, or default names if none available
        flask.render_template: Error template if processing fails
        
    Note:
        The function:
        1. Checks if the global semantic map is a dictionary
        2. If not, returns default global variable names
        3. If available, extracts keys from 'variable_info' field
        4. Capitalizes names and replaces underscores with spaces
        5. Adds 'Other' as an additional option
    """
    if not isinstance(session_cache.global_semantic_map, dict):
        return ['Research subject identifier', 'Biological sex', 'Age at inclusion', 'Other']
    else:
        try:
            return [name.capitalize().replace('_', ' ') for name in
                    session_cache.global_semantic_map['variable_info'].keys()] + ['Other']
        except Exception as e:
            flash(f"Failed to read the global semantic map. Error: {e}")
            return render_template('ingest.html', error=True)


def formulate_local_semantic_map(database: str, session_cache: Any) -> dict:
    """
    Modify the global semantic map by updating local definitions and setting unmapped variables to null.
    
    This function creates a database-specific semantic map by processing user input
    from the UI and mapping local variable names to global semantic definitions.
    It handles duplicate variables and value mappings appropriately.
    
    Args:
        database (str): The name of the database for which the local semantic map is formulated
        session_cache: The session cache containing global semantic map and descriptive info
        
    Returns:
        dict: A dictionary representing the modified semantic map with proper null handling
        
    Note:
        The function:
        1. Creates a deep copy of the global semantic map
        2. Updates the database_name field
        3. Resets all local_definitions to null initially
        4. Processes only variables filled in the UI
        5. Handles duplicate global variables by creating new entries with suffixes
        6. Updates local definitions and data types based on UI input
        7. Processes value mappings for categorical variables
    """
    # Create a deep copy of the global semantic map
    modified_semantic_map = copy.deepcopy(session_cache.global_semantic_map)

    # Update the 'database_name' field in the semantic map
    if isinstance(modified_semantic_map.get('database_name'), str):
        modified_semantic_map['database_name'] = database
    else:
        modified_semantic_map.update({'database_name': database})

    # Reset all local_definitions to null and datatypes to empty string
    # This ensures that unmapped fields are properly cleared
    for variable_name, variable_info in modified_semantic_map['variable_info'].items():
        modified_semantic_map['variable_info'][variable_name]['local_definition'] = None

        # Reset all local_terms in value_mapping to null
        if 'value_mapping' in variable_info and 'terms' in variable_info['value_mapping']:
            for term_key in variable_info['value_mapping']['terms']:
                modified_semantic_map['variable_info'][variable_name]['value_mapping']['terms'][term_key][
                    'local_term'] = None

    # Process only the variables that are filled in the UI
    # Process local definitions and update the existing semantic map
    used_global_variables = {}  # Track usage for duplicate handling

    for local_variable, local_value in session_cache.descriptive_info[database].items():
        # Skip if no description is provided (empty field in UI)
        if 'description' not in local_value or not local_value['description']:
            continue

        global_variable = local_value['description'].split('Variable description: ')[1].lower().replace(' ', '_')

        if global_variable and global_variable in session_cache.global_semantic_map['variable_info']:
            # Handle duplicate global variables by creating new entries with suffix
            if global_variable in used_global_variables:
                suffix = used_global_variables[global_variable] + 1
                new_global_variable = f"{global_variable}_{suffix}"
                used_global_variables[global_variable] = suffix

                # Create new entry based on original
                modified_semantic_map['variable_info'][new_global_variable] = copy.deepcopy(
                    session_cache.global_semantic_map['variable_info'][global_variable]
                )

                # Reset the new entry's local fields to null initially
                modified_semantic_map['variable_info'][new_global_variable]['local_definition'] = None
                modified_semantic_map['variable_info'][new_global_variable]['data_type'] = None
            else:
                new_global_variable = global_variable
                used_global_variables[global_variable] = 0

            # Update local definition (only if field was filled in UI)
            modified_semantic_map['variable_info'][new_global_variable]['local_definition'] = local_variable

            # Extract and add datatype information from UI
            datatype_value = local_value['type'].split('Variable type: ')[1].lower().replace(' ', '_')
            # Only set datatype if it's not empty
            if datatype_value and datatype_value.strip():
                modified_semantic_map['variable_info'][new_global_variable]['data_type'] = datatype_value
            else:
                # Try to extract from request data or set default
                modified_semantic_map['variable_info'][new_global_variable]['data_type'] = None

            # Process value mapping if it exists
            if 'value_mapping' in modified_semantic_map['variable_info'][new_global_variable]:
                original_terms = modified_semantic_map['variable_info'][new_global_variable]['value_mapping']['terms']
                used_global_terms = {}  # Track usage for duplicate term handling

                # Reset all local_terms to null first (already done above, but being explicit here)
                # Reset local_term for all terms first
                for term_key in original_terms:
                    original_terms[term_key]['local_term'] = None

                # Update local terms based on UI input (only for filled categories)
                for category, value in local_value.items():
                    if category.startswith('Category: ') and value and value.strip():
                        global_term = value.split(': ')[1].split(', comment')[0].lower().replace(' ', '_')
                        local_term_value = category.split(': ')[1]

                        if global_term in original_terms:
                            # Handle duplicate terms
                            if global_term in used_global_terms:
                                suffix = used_global_terms[global_term] + 1
                                new_global_term = f"{global_term}_{suffix}"
                                used_global_terms[global_term] = suffix

                                # Create new term entry
                                original_terms[new_global_term] = copy.deepcopy(original_terms[global_term])
                                original_terms[new_global_term]['local_term'] = local_term_value
                            else:
                                original_terms[global_term]['local_term'] = local_term_value
                                used_global_terms[global_term] = 0

    return modified_semantic_map


def handle_postgres_data(username: str, password: str, postgres_url: str, 
                        postgres_db: str, table: str, session_cache: Any,
                        root_dir: str, child_dir: str) -> Optional[Any]:
    """
    Handle PostgreSQL data by caching connection info and establishing database connection.
    
    This function processes PostgreSQL connection parameters, caches them in the session,
    establishes a database connection, and writes connection details to a properties file
    for use by the triplification process.
    
    Args:
        username (str): The username for the PostgreSQL database
        password (str): The password for the PostgreSQL database
        postgres_url (str): The URL of the PostgreSQL database
        postgres_db (str): The name of the PostgreSQL database
        table (str): The name of the table in the PostgreSQL database
        session_cache: The session cache object to store connection details
        root_dir (str): Root directory path for file operations
        child_dir (str): Child directory path for file operations
        
    Returns:
        Optional[Any]: None if connection successful, error template if connection fails
        
    Note:
        The function:
        1. Caches connection information in session
        2. Attempts to establish PostgreSQL connection
        3. Writes connection details to triplifierSQL.properties file
        4. Returns error template if connection fails
    """
    # Cache information
    session_cache.username, session_cache.password, session_cache.url, session_cache.db_name, session_cache.table = (
        username, password, postgres_url, postgres_db, table)

    try:
        # Establish PostgreSQL connection
        session_cache.conn = connect(dbname=session_cache.db_name, user=session_cache.username,
                                     host=session_cache.url,
                                     password=session_cache.password)
        print("Connection:", session_cache.conn)
    except Exception as err:
        print("connect() ERROR:", err)
        session_cache.conn = None
        flash('Attempting to connect to PostgreSQL datasource unsuccessful. Please check your details!')
        return render_template('ingest.html', error=True)

    # Write connection details to properties file
    with open(f"{root_dir}{child_dir}/triplifierSQL.properties", "w") as f:
        f.write(f"jdbc.url = jdbc:postgresql://{session_cache.url}/{session_cache.db_name}\n"
                f"jdbc.user = {session_cache.username}\n"
                f"jdbc.password = {session_cache.password}\n"
                f"jdbc.driver = org.postgresql.Driver\n\n"
                # f"repo.type = rdf4j\n"
                # f"repo.url = {graphdb_url}\n"
                # f"repo.id = {repo}"
                )

    return None