import os
import tempfile
import sqlite3
import yaml
import socket
from typing import Tuple, Union
from io import StringIO

import pandas as pd

import logging

logger = logging.getLogger(__name__)


class PythonTriplifierIntegration:
    """Integration layer for the Python Triplifier package."""
    
    def __init__(self, root_dir='', child_dir='.'):
        self.root_dir = root_dir
        self.child_dir = child_dir
        self.hostname = socket.gethostname()
    
    def run_triplifier_csv(self, csv_data_list, csv_paths, base_uri=None):
        """
        Process CSV data using Python Triplifier API directly.
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            # Import triplifier modules
            from pythonTool.main_app import run_triplifier
            
            # Create a temporary SQLite database
            temp_db_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'temp_triplifier.db')
            os.makedirs(os.path.dirname(temp_db_path), exist_ok=True)
            
            # Remove existing temp database
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            
            # Create SQLite connection and load CSV data
            conn = sqlite3.connect(temp_db_path)
            
            for i, (csv_data, csv_path) in enumerate(zip(csv_data_list, csv_paths)):
                # Derive table name from CSV filename
                table_name = os.path.splitext(os.path.basename(csv_path))[0]
                # Clean table name to be SQLite compatible
                table_name = table_name.replace('-', '_').replace(' ', '_')
                
                # Write DataFrame to SQLite
                csv_data.to_sql(table_name, conn, if_exists='replace', index=False)
                logger.info(f"Loaded CSV data into SQLite table: {table_name}")
            
            conn.close()
            
            # Create YAML configuration
            config = {
                'db': {
                    'url': f'sqlite:///{temp_db_path}'
                }
            }
            
            config_path = os.path.join(self.root_dir, self.child_dir, 'triplifier_csv_config.yaml')
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            
            # Set up file paths
            ontology_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'ontology.owl')
            output_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            # Create arguments object for the triplifier
            class Args:
                def __init__(self):
                    self.config = config_path
                    self.output = output_path
                    self.ontology = ontology_path
                    self.baseuri = base_uri
                    self.ontologyAndOrData = None  # Convert both ontology and data
            
            args = Args()
            
            # Run Python Triplifier directly using the API
            run_triplifier(args)
            
            logger.info(f"Python Triplifier executed successfully")
            
            # Clean up temporary files
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            
            return True, "CSV data triplified successfully using Python Triplifier."
            
        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            return False, f"Error processing CSV data: {str(e)}"
    
    def run_triplifier_sql(self, base_uri=None):
        """
        Process PostgreSQL data using Python Triplifier API directly.
        
        Args:
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            # Import triplifier modules
            from pythonTool.main_app import run_triplifier
            
            # Create YAML configuration dynamically
            config = {
                'db': {
                    'url': "postgresql://postgres:postgres@postgres/opc"
                }
            }
            
            config_path = os.path.join(self.root_dir, self.child_dir, 'triplifier_sql_config.yaml')
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            
            ontology_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'ontology.owl')
            output_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            # Create arguments object for the triplifier
            class Args:
                def __init__(self):
                    self.config = config_path
                    self.output = output_path
                    self.ontology = ontology_path
                    self.baseuri = base_uri
                    self.ontologyAndOrData = None  # Convert both ontology and data
            
            args = Args()
            
            # Run Python Triplifier directly using the API
            run_triplifier(args)
            
            logger.info(f"Python Triplifier executed successfully")
            
            # Clean up temporary config file
            if os.path.exists(config_path):
                os.remove(config_path)
            
            return True, "PostgreSQL data triplified successfully using Python Triplifier."
            
        except Exception as e:
            logger.error(f"Error in SQL triplification: {e}")
            return False, f"Error processing PostgreSQL data: {str(e)}"