"""
Session cache utility functions for the Flyover data descriptor application.
"""

import logging
import pandas as pd
from io import StringIO

logger = logging.getLogger(__name__)


def populate_databases_from_rdf(session_cache, execute_query):
    """
    Populate session_cache.databases from the RDF store if it's not already set.
    This function queries the RDF store for database information and extracts unique database names.
    It's called by annotation routes to ensure databases are available even when users
    skip the describe step.
    
    Parameters:
        session_cache: The session cache object containing the databases attribute and repo name
        execute_query: Function to execute SPARQL queries on the GraphDB repository
    
    Returns:
        bool: True if databases were successfully populated or already exist, False if no databases found
    """
    # If databases are already populated, return True
    if session_cache.databases is not None:
        return True
    
    try:
        # SPARQL query to fetch the URI and column name of each column in the GraphDB repository
        column_query = """
        PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            SELECT ?uri ?column 
            WHERE {
            ?uri dbo:column ?column .
            }
        """
        # Execute the query and read the results into a pandas DataFrame
        result = execute_query(session_cache.repo, column_query)
        column_info = pd.read_csv(StringIO(result))
        
        # Check if we got any results
        if column_info.empty:
            logger.warning("No database columns found in RDF store")
            return False
        
        # Extract the database name from the URI and add it as a new column in the DataFrame
        column_info['database'] = column_info['uri'].str.extract(r'.*/(.*?)\.', expand=False)
        
        # Get unique values in 'database' column and store them in the session cache
        unique_values = column_info['database'].unique()
        session_cache.databases = unique_values
        
        logger.info(f"Populated session_cache.databases with {len(unique_values)} databases: {list(unique_values)}")
        return True
        
    except Exception as e:
        logger.error(f"Error populating databases from RDF store: {str(e)}")
        return False
