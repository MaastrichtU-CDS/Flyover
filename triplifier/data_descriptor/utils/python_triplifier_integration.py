import os
import tempfile
import sqlite3
import yaml
import socket
import sys
import subprocess
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
        Process CSV data using Python Triplifier.
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
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
            
            # Set up path for pythonTool module
            pythontool_path = os.path.join(os.path.dirname(__file__), '..')
            
            # Run Python Triplifier as subprocess
            env = os.environ.copy()
            env['PYTHONPATH'] = pythontool_path + ':' + env.get('PYTHONPATH', '')
            
            cmd = [
                sys.executable, '-m', 'pythonTool.main_app',
                '-c', config_path,
                '-o', output_path,
                '-t', ontology_path,
                '-b', base_uri
            ]
            
            result = subprocess.run(
                cmd,
                cwd=pythontool_path,
                env=env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                logger.info(f"Python Triplifier executed successfully")
                logger.info(f"Output: {result.stdout}")
                
                # Clean up temporary files
                if os.path.exists(config_path):
                    os.remove(config_path)
                if os.path.exists(temp_db_path):
                    os.remove(temp_db_path)
                
                return True, "CSV data triplified successfully using Python Triplifier."
            else:
                logger.error(f"Python Triplifier failed with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                return False, f"Python Triplifier error: {result.stderr}"
            
        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            return False, f"Error processing CSV data: {str(e)}"
    
    def run_triplifier_sql(self, base_uri=None):
        """
        Process PostgreSQL data using Python Triplifier.
        
        Args:
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            config_path = os.path.join(self.root_dir, self.child_dir, 'triplifierSQL.yaml')
            ontology_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'ontology.owl')
            output_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            # Set up path for pythonTool module
            pythontool_path = os.path.join(os.path.dirname(__file__), '..')
            
            # Run Python Triplifier as subprocess
            env = os.environ.copy()
            env['PYTHONPATH'] = pythontool_path + ':' + env.get('PYTHONPATH', '')
            
            cmd = [
                sys.executable, '-m', 'pythonTool.main_app',
                '-c', config_path,
                '-o', output_path,
                '-t', ontology_path,
                '-b', base_uri
            ]
            
            result = subprocess.run(
                cmd,
                cwd=pythontool_path,
                env=env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                logger.info(f"Python Triplifier executed successfully")
                logger.info(f"Output: {result.stdout}")
                return True, "PostgreSQL data triplified successfully using Python Triplifier."
            else:
                logger.error(f"Python Triplifier failed with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                return False, f"Python Triplifier error: {result.stderr}"
            
        except Exception as e:
            logger.error(f"Error in SQL triplification: {e}")
            return False, f"Error processing PostgreSQL data: {str(e)}"