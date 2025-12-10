import os
import sqlite3
import yaml
import socket
import time
import gc
import logging
from typing import Tuple, Union
from markupsafe import Markup

logger = logging.getLogger(__name__)


class PythonTriplifierIntegration:
    """Integration layer for the Python Triplifier package."""

    def __init__(self, root_dir="", child_dir="."):
        self.root_dir = root_dir
        self.child_dir = child_dir
        self.hostname = socket.gethostname()

    def run_triplifier_csv(self, csv_data_list, csv_paths, base_uri=None):
        """
        Process CSV data using Python Triplifier API directly.

        Args:
            csv_data_list: List of polars DataFrames
            csv_paths: List of CSV file paths
            base_uri: Base URI for RDF generation

        Returns:
            Tuple[bool, str]: (success, message/error)
        """
        try:
            # Import triplifier modules
            from pythonTool.main_app import run_triplifier

            # Create a temporary SQLite database
            temp_db_path = os.path.join(
                self.root_dir, self.child_dir, "static", "files", "temp_triplifier.db"
            )
            os.makedirs(os.path.dirname(temp_db_path), exist_ok=True)

            # Remove existing temp database
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)

            # Create SQLite connection and load CSV data
            conn = sqlite3.connect(temp_db_path)

            try:
                for i, (csv_data, csv_path) in enumerate(zip(csv_data_list, csv_paths)):
                    # Derive table name from CSV filename
                    table_name = os.path.splitext(os.path.basename(csv_path))[0]
                    # Clean table name to be SQLite compatible
                    table_name = table_name.replace("-", "_").replace(" ", "_")

                    # Write polars DataFrame to SQLite using bulk insertion
                    col_defs = ", ".join([f'"{col}" TEXT' for col in csv_data.columns])
                    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                    conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
                    insert_sql = f'INSERT INTO "{table_name}" VALUES ({", ".join(["?" for _ in csv_data.columns])})'
                    # Use executemany for efficient batch insertion
                    conn.executemany(insert_sql, csv_data.iter_rows())
                    conn.commit()
                    logger.info(f"Loaded CSV data into SQLite table: {table_name}")

            finally:
                conn.close()

            # Create YAML configuration
            config = {
                "db": {"url": f"sqlite:///{temp_db_path}"},
                "repo.dataUri": (
                    base_uri if base_uri else f"http://{self.hostname}/rdf/data/"
                ),
            }

            config_path = os.path.join(
                self.root_dir, self.child_dir, "triplifier_csv_config.yaml"
            )
            with open(config_path, "w") as f:
                yaml.dump(config, f)

            # Set up file paths - output to root_dir for upload_ontology_then_data compatibility
            ontology_path = os.path.join(self.root_dir, "ontology.owl")
            output_path = os.path.join(self.root_dir, "output.ttl")
            base_uri = base_uri or f"http://{self.hostname}/rdf/ontology/"

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

            # Clean up temporary files
            if os.path.exists(config_path):
                os.remove(config_path)
            if os.path.exists(temp_db_path):
                gc.collect()  # Force garbage collection
                time.sleep(0.5)  # Wait for file handles to close

                try:
                    os.remove(temp_db_path)
                except PermissionError as pe:
                    logger.warning(
                        f"Could not delete temp database (will be cleaned up on next run): {pe}"
                    )
                    # Not critical - file will be overwritten on next run

            return True, "CSV data triplified successfully using Python Triplifier."

        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            import traceback

            traceback.print_exc()
            return False, f"Error processing CSV data: {str(e)}"

    def run_triplifier_sql(
        self, base_uri=None, db_url=None, db_user=None, db_password=None
    ):
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
                db_url = os.getenv("TRIPLIFIER_DB_URL", "postgresql://postgres/opc")
            if db_user is None:
                db_user = os.getenv("TRIPLIFIER_DB_USER", "postgres")
            if db_password is None:
                db_password = os.getenv("TRIPLIFIER_DB_PASSWORD", "postgres")

            # Create YAML configuration dynamically
            config = {
                "db": {"url": db_url},
                "repo.dataUri": (
                    base_uri if base_uri else f"http://{self.hostname}/rdf/data/"
                ),
            }

            # Add user and password if they're not in the URL
            if db_user and "://" in db_url and "@" not in db_url:
                # Insert credentials into URL
                parts = db_url.split("://")
                config["db"]["url"] = f"{parts[0]}://{db_user}:{db_password}@{parts[1]}"

            config_path = os.path.join(
                self.root_dir, self.child_dir, "triplifier_sql_config.yaml"
            )
            with open(config_path, "w") as f:
                yaml.dump(config, f)

            # Set up file paths - output to root_dir for upload_ontology_then_data compatibility
            ontology_path = os.path.join(self.root_dir, "ontology.owl")
            output_path = os.path.join(self.root_dir, "output.ttl")
            base_uri = base_uri or f"http://{self.hostname}/rdf/ontology/"

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

            return (
                True,
                "PostgreSQL data triplified successfully using Python Triplifier.",
            )

        except Exception as e:
            logger.error(f"Error in SQL triplification: {e}")
            import traceback

            traceback.print_exc()
            return False, f"Error processing PostgreSQL data: {str(e)}"


def run_triplifier(
    properties_file=None, root_dir="", child_dir=".", csv_data_list=None, csv_paths=None
):
    """
    Run the Python Triplifier for CSV or SQL data.
    This function is the main entry point for triplification.

    Args:
        properties_file: Legacy parameter for backwards compatibility ('triplifierCSV.properties' or 'triplifierSQL.properties')
        root_dir: Root directory for file operations
        child_dir: Child directory for file operations
        csv_data_list: List of polars DataFrames (for CSV mode)
        csv_paths: List of CSV file paths (for CSV mode)

    Returns:
        Tuple[bool, Union[str, Markup]]: (success, message)
    """
    try:
        # Initialize Python Triplifier integration
        triplifier = PythonTriplifierIntegration(root_dir, child_dir)

        if properties_file == "triplifierCSV.properties":
            # Use Python Triplifier for CSV processing
            success, message = triplifier.run_triplifier_csv(csv_data_list, csv_paths)

        elif properties_file == "triplifierSQL.properties":
            # Use Python Triplifier for PostgreSQL processing
            success, message = triplifier.run_triplifier_sql()
        else:
            return False, f"Unknown properties file: {properties_file}"

        return success, message

    except Exception as e:
        logger.error(f"Unexpected error attempting to run the Python Triplifier: {e}")
        import traceback

        traceback.print_exc()
        return False, f"Unexpected error attempting to run the Triplifier, error: {e}"
