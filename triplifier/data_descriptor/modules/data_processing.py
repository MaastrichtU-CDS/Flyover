"""
Data processing module.

This module contains functions for processing data, handling semantic maps,
managing PostgreSQL connections, and performing data transformation operations.
"""

import copy
import logging
from typing import Dict, Any, Optional, Union
from flask import flash, render_template
from psycopg2 import connect

logger = logging.getLogger(__name__)


def formulate_local_semantic_map(database: str, global_semantic_map: Dict[str, Any], 
                                descriptive_info: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Modify the global semantic map by updating local definitions and setting unmapped variables to null.

    Args:
        database: The name of the database for which the local semantic map is to be formulated
        global_semantic_map: The original global semantic mapping data
        descriptive_info: Dictionary containing descriptive information about variables

    Returns:
        A dictionary representing the modified semantic map with proper null handling

    This function creates a deep copy of the global semantic map, updates the database name,
    resets all local definitions to null, and then processes only the variables that are
    filled in the UI to update their local definitions and datatypes.
    """
    # Create a deep copy of the global semantic map
    modified_semantic_map = copy.deepcopy(global_semantic_map)

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

    for local_variable, local_value in descriptive_info[database].items():
        # Skip if no description is provided (empty field in UI)
        if 'description' not in local_value or not local_value['description']:
            continue

        global_variable = local_value['description'].split('Variable description: ')[1].lower().replace(' ', '_')

        if global_variable and global_variable in global_semantic_map['variable_info']:
            # Handle duplicate global variables by creating new entries with suffix
            if global_variable in used_global_variables:
                suffix = used_global_variables[global_variable] + 1
                new_global_variable = f"{global_variable}_{suffix}"
                used_global_variables[global_variable] = suffix

                # Create new entry based on original
                modified_semantic_map['variable_info'][new_global_variable] = copy.deepcopy(
                    global_semantic_map['variable_info'][global_variable]
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
                        postgres_db: str, table: str, root_dir: str, 
                        child_dir: str) -> Optional[object]:
    """
    Handle PostgreSQL data by establishing connection and writing configuration.

    Args:
        username: The username for the PostgreSQL database
        password: The password for the PostgreSQL database
        postgres_url: The URL of the PostgreSQL database
        postgres_db: The name of the PostgreSQL database
        table: The name of the table in the PostgreSQL database
        root_dir: Root directory path for configuration files
        child_dir: Child directory path for configuration files

    Returns:
        Flask response object if connection fails, None if successful

    This function establishes a connection to the PostgreSQL database and writes
    the connection details to a properties file for use by the triplifier.
    """
    try:
        # Establish PostgreSQL connection
        conn = connect(dbname=postgres_db, user=username, host=postgres_url, password=password)
        logger.info(f"PostgreSQL connection established: {conn}")
        
        # Write connection details to properties file
        with open(f"{root_dir}{child_dir}/triplifierSQL.properties", "w") as f:
            f.write(f"jdbc.url = jdbc:postgresql://{postgres_url}/{postgres_db}\n"
                    f"jdbc.user = {username}\n"
                    f"jdbc.password = {password}\n"
                    f"jdbc.driver = org.postgresql.Driver\n\n")
        
        return conn
        
    except Exception as err:
        logger.error(f"PostgreSQL connection failed: {err}")
        flash('Attempting to connect to PostgreSQL datasource unsuccessful. Please check your details!')
        return render_template('ingest.html', error=True)


def insert_equivalencies(descriptive_info: Dict[str, Dict[str, Any]], variable: str, 
                        repo: str, execute_query_func) -> Optional[str]:
    """
    Insert equivalencies into a GraphDB repository.

    Args:
        descriptive_info: Dictionary containing descriptive information about variables
        variable: The name of the variable for which the equivalency is to be inserted
        repo: The GraphDB repository name
        execute_query_func: Function to execute SPARQL queries

    Returns:
        The result of the query execution as a string if successful, None if skipped

    This function constructs a SPARQL INSERT query that inserts an owl:equivalentClass
    triple into the ontology graph. It only processes variables that have meaningful
    content in their type, description, or comments fields.
    """
    # Skip if variable missing or empty
    if variable not in descriptive_info or not descriptive_info[variable]:
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
        return None

    query = f"""
                PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
                PREFIX db: <http://{repo}.local/rdf/ontology/>
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
    return execute_query_func(repo, query, "update", "/statements")