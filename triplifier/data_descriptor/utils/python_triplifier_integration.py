import os
import tempfile
import sqlite3
import yaml
import socket
import uuid
from typing import Tuple, Union
from io import StringIO
from markupsafe import Markup

import pandas as pd

import logging

from .progress_tracker import progress_tracker

logger = logging.getLogger(__name__)


class PythonTriplifierIntegration:
    """Integration layer for the Python Triplifier package."""
    
    def __init__(self, root_dir='', child_dir='.'):
        self.root_dir = root_dir
        self.child_dir = child_dir
        self.hostname = socket.gethostname()
    
    def run_triplifier_csv(self, csv_data_list, csv_paths, base_uri=None, task_id=None):
        """
        Process CSV data using Python Triplifier API directly.
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            task_id: Optional task ID for progress tracking
            
        Returns:
            Tuple[bool, str, str]: (success, message/error, task_id)
        """
        # Generate task ID if not provided
        if task_id is None:
            task_id = str(uuid.uuid4())
        
        try:
            # Calculate total rows across all tables for progress tracking
            total_rows = sum(len(df) for df in csv_data_list)
            progress_tracker.start_task(task_id, "all_tables", total_rows)
            
            # Import triplifier modules
            from pythonTool.main_app import run_triplifier
            
            # Create a temporary SQLite database
            temp_db_path = os.path.join(self.root_dir, self.child_dir, 'static', 'files', 'temp_triplifier.db')
            os.makedirs(os.path.dirname(temp_db_path), exist_ok=True)
            
            # Remove existing temp database
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            
            # Update progress: database preparation
            progress_tracker.update_progress(task_id, 0, 'preparing_database')
            
            # Create SQLite connection and load CSV data
            conn = sqlite3.connect(temp_db_path)
            
            rows_loaded = 0
            for i, (csv_data, csv_path) in enumerate(zip(csv_data_list, csv_paths)):
                # Derive table name from CSV filename
                table_name = os.path.splitext(os.path.basename(csv_path))[0]
                # Clean table name to be SQLite compatible
                table_name = table_name.replace('-', '_').replace(' ', '_')
                
                # Write DataFrame to SQLite
                csv_data.to_sql(table_name, conn, if_exists='replace', index=False)
                logger.info(f"Loaded CSV data into SQLite table: {table_name}")
                
                # Update progress after loading each table
                rows_loaded += len(csv_data)
                progress_tracker.update_progress(task_id, rows_loaded, 'loading_data')
            
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
            
            # Set up file paths - output to root_dir for upload_ontology_then_data compatibility
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            # Update progress: starting triplification
            progress_tracker.update_progress(task_id, total_rows, 'triplifying')
            
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
            logger.info(f"Generated files: {ontology_path}, {output_path}")
            
            # Update progress: completed
            progress_tracker.complete_task(task_id, success=True)
            
            # Clean up temporary files
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            
            return True, "CSV data triplified successfully using Python Triplifier.", task_id
            
        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            import traceback
            traceback.print_exc()
            
            # Mark task as failed
            progress_tracker.complete_task(task_id, success=False)
            
            return False, f"Error processing CSV data: {str(e)}", task_id
    
    def run_triplifier_sql(self, base_uri=None, db_url=None, db_user=None, db_password=None):
        """
        Process PostgreSQL data using Python Triplifier API directly.
        
        Args:
            base_uri: Base URI for RDF generation
            db_url: Database connection URL (can be set from environment variable)
            db_user: Database user (can be set from environment variable)
            db_password: Database password (can be set from environment variable)
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            # Import triplifier modules
            from pythonTool.main_app import run_triplifier
            
            # Get database configuration from environment variables if not provided
            if db_url is None:
                db_url = os.getenv('TRIPLIFIER_DB_URL', 'postgresql://postgres/opc')
            if db_user is None:
                db_user = os.getenv('TRIPLIFIER_DB_USER', 'postgres')
            if db_password is None:
                db_password = os.getenv('TRIPLIFIER_DB_PASSWORD', 'postgres')
            
            # Create YAML configuration dynamically
            config = {
                'db': {
                    'url': db_url
                }
            }
            
            # Add user and password if they're not in the URL
            if db_user and '://' in db_url and '@' not in db_url:
                # Insert credentials into URL
                parts = db_url.split('://')
                config['db']['url'] = f"{parts[0]}://{db_user}:{db_password}@{parts[1]}"
            
            config_path = os.path.join(self.root_dir, self.child_dir, 'triplifier_sql_config.yaml')
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            
            # Set up file paths - output to root_dir for upload_ontology_then_data compatibility
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
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
            logger.info(f"Generated files: {ontology_path}, {output_path}")
            
            # Clean up temporary config file
            if os.path.exists(config_path):
                os.remove(config_path)
            
            return True, "PostgreSQL data triplified successfully using Python Triplifier."
            
        except Exception as e:
            logger.error(f"Error in SQL triplification: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error processing PostgreSQL data: {str(e)}"


def run_triplifier(properties_file=None, root_dir='', child_dir='.', csv_data_list=None, csv_paths=None, task_id=None):
    """
    Run the Python Triplifier for CSV or SQL data.
    This function is the main entry point for triplification.
    
    Args:
        properties_file: Legacy parameter for backwards compatibility ('triplifierCSV.properties' or 'triplifierSQL.properties')
        root_dir: Root directory for file operations
        child_dir: Child directory for file operations
        csv_data_list: List of pandas DataFrames (for CSV mode)
        csv_paths: List of CSV file paths (for CSV mode)
        task_id: Optional task ID for progress tracking
        
    Returns:
        Tuple[bool, Union[str, Markup], str]: (success, message, task_id)
    """
    try:
        # Initialize Python Triplifier integration
        triplifier = PythonTriplifierIntegration(root_dir, child_dir)
        
        if properties_file == 'triplifierCSV.properties':
            # Use Python Triplifier for CSV processing
            success, message, task_id = triplifier.run_triplifier_csv(
                csv_data_list, 
                csv_paths,
                task_id=task_id
            )
            
        elif properties_file == 'triplifierSQL.properties':
            # Use Python Triplifier for PostgreSQL processing
            success, message = triplifier.run_triplifier_sql()
            # For SQL, we return None as task_id since tracking isn't implemented yet
            task_id = None
        else:
            return False, f"Unknown properties file: {properties_file}", None

        return success, message, task_id
            
    except Exception as e:
        logger.error(f'Unexpected error attempting to run the Python Triplifier: {e}')
        import traceback
        traceback.print_exc()
        return False, f'Unexpected error attempting to run the Triplifier, error: {e}', None
