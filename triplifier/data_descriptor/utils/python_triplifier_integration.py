import os
import tempfile
import sqlite3
import yaml
import socket
from typing import Tuple, Union
from io import StringIO
from markupsafe import Markup

import pandas as pd

import logging

logger = logging.getLogger(__name__)


class PythonTriplifierIntegration:
    """Integration layer for the Python Triplifier package."""
    
    def __init__(self, root_dir='', child_dir='.'):
        self.root_dir = root_dir
        self.child_dir = child_dir
        self.hostname = socket.gethostname()
    
    def _prepare_csv_database(self, csv_data_list, csv_paths):
        """
        Prepare SQLite database from CSV data.
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            
        Returns:
            Tuple[str, str]: (temp_db_path, config_path)
        """
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
        
        return temp_db_path, config_path
    
    def _prepare_sql_config(self, db_url=None, db_user=None, db_password=None):
        """
        Prepare SQL database configuration.
        
        Args:
            db_url: Database connection URL
            db_user: Database user
            db_password: Database password
            
        Returns:
            str: config_path
        """
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
        
        return config_path
    
    def _run_triplifier_step(self, config_path, ontology_path, output_path, base_uri, mode=None):
        """
        Run a specific triplifier step (ontology or data).
        
        Args:
            config_path: Path to YAML configuration
            ontology_path: Path for ontology output
            output_path: Path for data output
            base_uri: Base URI for RDF generation
            mode: 'ontology' to generate only ontology, 'data' to generate only data, None for both
            
        Returns:
            None (raises exception on error)
        """
        from pythonTool.main_app import run_triplifier
        
        # Create arguments object for the triplifier
        class Args:
            def __init__(self):
                self.config = config_path
                self.output = output_path
                self.ontology = ontology_path
                self.baseuri = base_uri
                self.ontologyAndOrData = mode
        
        args = Args()
        
        # Run Python Triplifier directly using the API
        run_triplifier(args)
    
    def generate_ontology_csv(self, csv_data_list, csv_paths, base_uri=None):
        """
        Generate ontology file from CSV data.
        
        NOTE: Due to a bug in triplifier package (COLUMNREFERENCE not defined),
        this function is not currently usable. Use run_triplifier_csv() instead
        which generates both ontology and data in a single call.
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            temp_db_path, config_path = self._prepare_csv_database(csv_data_list, csv_paths)
            
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            self._run_triplifier_step(config_path, ontology_path, output_path, base_uri, mode='ontology')
            
            logger.info(f"Ontology generation successful: {ontology_path}")
            
            # Note: Don't clean up temp files yet, data generation will need them
            return True, "Ontology generated successfully"
            
        except Exception as e:
            logger.error(f"Error in ontology generation: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error generating ontology: {str(e)}"
    
    def generate_data_csv(self, csv_data_list, csv_paths, base_uri=None):
        """
        Generate data file from CSV data (requires ontology to exist).
        
        NOTE: Due to a bug in triplifier package (COLUMNREFERENCE not defined),
        this function is not currently usable. Use run_triplifier_csv() instead
        which generates both ontology and data in a single call.
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            temp_db_path, config_path = self._prepare_csv_database(csv_data_list, csv_paths)
            
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            self._run_triplifier_step(config_path, ontology_path, output_path, base_uri, mode='data')
            
            logger.info(f"Data generation successful: {output_path}")
            
            # Clean up temporary files after data generation
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            
            return True, "Data generated successfully"
            
        except Exception as e:
            logger.error(f"Error in data generation: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error generating data: {str(e)}"
    
    def generate_ontology_sql(self, base_uri=None, db_url=None, db_user=None, db_password=None):
        """
        Generate ontology file from SQL database.
        
        NOTE: Due to a bug in triplifier package (COLUMNREFERENCE not defined),
        this function is not currently usable. Use run_triplifier_sql() instead
        which generates both ontology and data in a single call.
        
        Args:
            base_uri: Base URI for RDF generation
            db_url: Database connection URL
            db_user: Database user
            db_password: Database password
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            config_path = self._prepare_sql_config(db_url, db_user, db_password)
            
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            self._run_triplifier_step(config_path, ontology_path, output_path, base_uri, mode='ontology')
            
            logger.info(f"Ontology generation successful: {ontology_path}")
            
            # Note: Don't clean up config yet, data generation will need it
            return True, "Ontology generated successfully"
            
        except Exception as e:
            logger.error(f"Error in ontology generation: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error generating ontology: {str(e)}"
    
    def generate_data_sql(self, base_uri=None, db_url=None, db_user=None, db_password=None):
        """
        Generate data file from SQL database (requires ontology to exist).
        
        NOTE: Due to a bug in triplifier package (COLUMNREFERENCE not defined),
        this function is not currently usable. Use run_triplifier_sql() instead
        which generates both ontology and data in a single call.
        
        Args:
            base_uri: Base URI for RDF generation
            db_url: Database connection URL
            db_user: Database user
            db_password: Database password
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            config_path = self._prepare_sql_config(db_url, db_user, db_password)
            
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            self._run_triplifier_step(config_path, ontology_path, output_path, base_uri, mode='data')
            
            logger.info(f"Data generation successful: {output_path}")
            
            # Clean up temporary config file
            if os.path.exists(config_path):
                os.remove(config_path)
            
            return True, "Data generated successfully"
            
        except Exception as e:
            logger.error(f"Error in data generation: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error generating data: {str(e)}"
    
    def run_triplifier_csv(self, csv_data_list, csv_paths, base_uri=None):
        """
        Process CSV data using Python Triplifier API directly.
        Generates both ontology and data files in a single call (workaround for triplifier bug).
        
        Args:
            csv_data_list: List of pandas DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            temp_db_path, config_path = self._prepare_csv_database(csv_data_list, csv_paths)
            
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            # Generate both ontology and data in single call to avoid triplifier bug
            # when loading ontology separately
            self._run_triplifier_step(config_path, ontology_path, output_path, base_uri, mode=None)
            
            logger.info(f"Python Triplifier executed successfully")
            logger.info(f"Generated files: {ontology_path}, {output_path}")
            
            # Clean up temporary files
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            
            return True, "CSV data triplified successfully using Python Triplifier."
            
        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            import traceback
            traceback.print_exc()
            return False, f"Error processing CSV data: {str(e)}"
    
    def run_triplifier_sql(self, base_uri=None, db_url=None, db_user=None, db_password=None):
        """
        Process PostgreSQL data using Python Triplifier API directly.
        Generates both ontology and data files in a single call (workaround for triplifier bug).
        
        Args:
            base_uri: Base URI for RDF generation
            db_url: Database connection URL (can be set from environment variable)
            db_user: Database user (can be set from environment variable)
            db_password: Database password (can be set from environment variable)
            
        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            config_path = self._prepare_sql_config(db_url, db_user, db_password)
            
            ontology_path = os.path.join(self.root_dir, 'ontology.owl')
            output_path = os.path.join(self.root_dir, 'output.ttl')
            base_uri = base_uri or f"http://{self.hostname}/"
            
            # Generate both ontology and data in single call to avoid triplifier bug
            # when loading ontology separately
            self._run_triplifier_step(config_path, ontology_path, output_path, base_uri, mode=None)
            
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


def run_triplifier(properties_file=None, root_dir='', child_dir='.', csv_data_list=None, csv_paths=None):
    """
    Run the Python Triplifier for CSV or SQL data.
    This function is the main entry point for triplification.
    
    Args:
        properties_file: Legacy parameter for backwards compatibility ('triplifierCSV.properties' or 'triplifierSQL.properties')
        root_dir: Root directory for file operations
        child_dir: Child directory for file operations
        csv_data_list: List of pandas DataFrames (for CSV mode)
        csv_paths: List of CSV file paths (for CSV mode)
        
    Returns:
        Tuple[bool, Union[str, Markup]]: (success, message)
    """
    try:
        # Initialize Python Triplifier integration
        triplifier = PythonTriplifierIntegration(root_dir, child_dir)
        
        if properties_file == 'triplifierCSV.properties':
            # Use Python Triplifier for CSV processing
            success, message = triplifier.run_triplifier_csv(
                csv_data_list, 
                csv_paths
            )
            
        elif properties_file == 'triplifierSQL.properties':
            # Use Python Triplifier for PostgreSQL processing
            success, message = triplifier.run_triplifier_sql()
        else:
            return False, f"Unknown properties file: {properties_file}"

        return success, message
            
    except Exception as e:
        logger.error(f'Unexpected error attempting to run the Python Triplifier: {e}')
        import traceback
        traceback.print_exc()
        return False, f'Unexpected error attempting to run the Triplifier, error: {e}'
