import os
import sqlite3
import yaml
import socket
import time
import gc
import logging
from urllib.parse import urlparse

import polars as pl

from pythonTool.main_app import run_triplifier as triplifier_run
from typing import List, Tuple, Union

# Import table name sanitization function to avoid duplication
from .data_preprocessing import sanitise_table_name

logger = logging.getLogger(__name__)


class PythonTriplifierIntegration:
    """Integration layer for the Python Triplifier package."""

    def __init__(self, root_dir="", child_dir="."):
        self.root_dir = root_dir
        self.child_dir = child_dir
        self.hostname = socket.gethostname()

    def run_triplifier_csv(
        self,
        csv_data_list: List[pl.DataFrame],
        csv_table_names: List[str],
        base_uri: str | None = None,
    ) -> Tuple[bool, str, List[dict]]:
        """
        Process CSV data using Python Triplifier API directly.

        DataFrames are loaded directly into SQLite for triplification - no
        intermediate CSV files are needed.

        Args:
            csv_data_list: List of polars DataFrames
            csv_table_names: List of table names (derived from original filenames)
            base_uri: Base URI for RDF generation

        Returns:
            Tuple[bool, str, List[dict]]: (success, message/error, output_files)
            output_files is a list of dicts with keys: data_file, ontology_file, table_name
        """
        try:
            output_files = []
            static_files_dir = os.path.join(
                self.root_dir, self.child_dir, "static", "files"
            )
            os.makedirs(static_files_dir, exist_ok=True)

            # Process each CSV table individually
            for csv_data, table_name in zip(csv_data_list, csv_table_names):
                # Clean table name to be SQLite compatible
                clean_table_name = sanitise_table_name(table_name)

                # Create a temporary SQLite database for this table
                temp_db_path = os.path.join(
                    static_files_dir, f"temp_{clean_table_name}.db"
                )

                # Remove existing temp database
                if os.path.exists(temp_db_path):
                    os.remove(temp_db_path)

                # Create SQLite connection and load CSV data
                conn = sqlite3.connect(temp_db_path)

                try:
                    # Write polars DataFrame to SQLite using bulk insertion
                    col_defs = ", ".join([f'"{col}" TEXT' for col in csv_data.columns])
                    conn.execute(f'DROP TABLE IF EXISTS "{clean_table_name}"')
                    conn.execute(f'CREATE TABLE "{clean_table_name}" ({col_defs})')
                    insert_sql = f'INSERT INTO "{clean_table_name}" VALUES ({", ".join(["?" for _ in csv_data.columns])})'
                    # Use executemany for efficient batch insertion
                    conn.executemany(insert_sql, csv_data.iter_rows())
                    conn.commit()
                    logger.info(
                        f"Loaded CSV data into SQLite table: {clean_table_name}"
                    )

                finally:
                    conn.close()

                # Create YAML configuration for this table
                config = {
                    "db": {"url": f"sqlite:///{temp_db_path}"},
                    "repo.dataUri": (
                        base_uri if base_uri else f"http://{self.hostname}/rdf/data/"
                    ),
                }

                config_path = os.path.join(
                    self.root_dir,
                    self.child_dir,
                    f"triplifier_csv_config_{clean_table_name}.yaml",
                )
                with open(config_path, "w") as f:
                    yaml.dump(config, f)

                # Set up file paths - output to root_dir with table-specific names
                ontology_path = os.path.join(
                    self.root_dir, f"ontology_{clean_table_name}.owl"
                )
                output_path = os.path.join(
                    self.root_dir, f"output_{clean_table_name}.ttl"
                )
                base_uri_value = base_uri or f"http://{self.hostname}/rdf/ontology/"

                # Create arguments object for the triplifier
                class Args:
                    def __init__(self):
                        self.config = config_path
                        self.output = output_path
                        self.ontology = ontology_path
                        self.baseuri = base_uri_value
                        self.ontologyAndOrData = None  # Convert both ontology and data

                args = Args()

                # Run Python Triplifier directly using the API
                triplifier_run(args)

                logger.info(
                    f"Python Triplifier executed successfully for table: {clean_table_name}"
                )
                logger.info(f"Generated files: {ontology_path}, {output_path}")

                # Store output file information
                output_files.append(
                    {
                        "data_file": output_path,
                        "ontology_file": ontology_path,
                        "table_name": clean_table_name,
                    }
                )

                # Clean up temporary files for this table
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
                        # Not critical - file will be overwritten on the next run

            return (
                True,
                "CSV data triplified successfully using Python Triplifier.",
                output_files,
            )

        except Exception as e:
            logger.error(f"Error in CSV triplification: {e}")
            import traceback

            traceback.print_exc()
            return False, f"Error processing CSV data: {str(e)}", []

    def run_triplifier_sql(
        self,
        base_uri: str | None = None,
        db_url: str | None = None,
        db_user: str | None = None,
        db_password: str | None = None,
        db_name: str | None = None,
    ) -> Tuple[bool, str, List[dict]]:
        """
        Process PostgreSQL data using Python Triplifier API directly.

        Args:
            base_uri: Base URI for RDF generation
            db_url: Database connection URL (required; falls back to environment variable if not provided)
            db_user: Database user (required; falls back to environment variable if not provided)
            db_password: Database password (required; falls back to environment variable if not provided)
            db_name: Database name to use for named graph (extracted from db_url if not provided)

        Returns:
            Tuple[bool, str, List[dict]]: (success, message/error, output_files)
            - success (bool): Whether the triplification was successful
            - message (str): Success message or error description
            - output_files (List[dict]): List with a single dict containing:
                * 'data_file' (str): Path to the generated data file (output_{db_name}.ttl)
                * 'ontology_file' (str): Path to the generated ontology file (ontology_{db_name}.owl)
                * 'table_name' (str): Database name used for named graph identification
        """
        try:
            # Get database configuration from parameters first, then fall back to environment variables
            if db_url is None:
                db_url = os.getenv("TRIPLIFIER_DB_URL")
                if db_url is None:
                    return False, "Database URL is required. Please provide it via the form or set the TRIPLIFIER_DB_URL environment variable.", []
            if db_user is None:
                db_user = os.getenv("TRIPLIFIER_DB_USER")
                if db_user is None:
                    return False, "Database user is required. Please provide it via the form or set the TRIPLIFIER_DB_USER environment variable.", []
            if db_password is None:
                db_password = os.getenv("TRIPLIFIER_DB_PASSWORD")
                if db_password is None:
                    return False, "Database password is required. Please provide it via the form or set the TRIPLIFIER_DB_PASSWORD environment variable.", []

            # Extract database name from db_url if not provided
            # Expected format: postgresql://host/database_name
            if db_name is None:
                try:
                    # Use proper URL parsing to handle query parameters and fragments
                    parsed_url = urlparse(db_url)
                    # Extract database name from path (remove leading / and take first segment)
                    path = parsed_url.path.lstrip('/')
                    if path:
                        # Take only the first path segment (database name)
                        # This handles cases like /database or /database/schema
                        db_name = path.split('/')[0]
                    else:
                        db_name = "default"
                    # Sanitize database name for use in file names and graph URIs
                    # Note: sanitise_table_name is used for both table and database names
                    # as they require the same sanitization rules for filesystem and URI safety
                    db_name = sanitise_table_name(db_name)
                except Exception as e:
                    logger.warning(f"Could not extract database name from URL: {e}. Using 'default'")
                    db_name = "default"

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

            # Set up file paths with database-specific naming (similar to CSV approach)
            # This allows using upload_multiple_graphs for consistent named graph structure
            ontology_path = os.path.join(self.root_dir, f"ontology_{db_name}.owl")
            output_path = os.path.join(self.root_dir, f"output_{db_name}.ttl")
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
            triplifier_run(args)

            logger.info("Python Triplifier executed successfully")
            logger.info(f"Generated files: {ontology_path}, {output_path}")

            # Create output_files structure similar to CSV for consistent upload handling
            output_files = [
                {
                    "data_file": output_path,
                    "ontology_file": ontology_path,
                    "table_name": db_name,
                }
            ]

            # Clean up temporary config file
            if os.path.exists(config_path):
                os.remove(config_path)

            return (
                True,
                "PostgreSQL data triplified successfully using Python Triplifier.",
                output_files,
            )

        except Exception as e:
            logger.error(f"Error in SQL triplification: {e}")
            import traceback

            traceback.print_exc()
            return False, f"Error processing PostgreSQL data: {str(e)}", []


def run_triplifier(
    properties_file=None,  # TODO remove?
    root_dir: str = "",
    child_dir: str = ".",
    csv_data_list: List[pl.DataFrame] | None = None,
    csv_table_names: List[str] | None = None,
    db_url: str | None = None,
    db_user: str | None = None,
    db_password: str | None = None,
) -> Tuple[bool, Union[str], List[dict]]:
    """
    Run the Python Triplifier for CSV or SQL data.
    This function is the main entry point for triplification.

    Args:
        properties_file: Legacy parameter for backwards compatibility
        ('triplifierCSV.properties' or 'triplifierSQL.properties')
        root_dir: Root directory for file operations
        child_dir: Child directory for file operations
        csv_data_list: List of polars DataFrames (for CSV mode)
        csv_table_names: List of table names derived from CSV filenames (for CSV mode)
        db_url: Database connection URL (for SQL mode)
        db_user: Database user (for SQL mode)
        db_password: Database password (for SQL mode)

    Returns:
        Tuple[bool, Union[str], List[dict]]: (success, message, output_files)
        For CSV: output_files is a list of dicts with data_file, ontology_file, table_name
        For SQL: output_files is an empty list
    """
    try:
        # Initialise Python Triplifier integration
        triplifier = PythonTriplifierIntegration(root_dir, child_dir)

        if properties_file == "triplifierCSV.properties":
            # Use Python Triplifier for CSV processing
            success, message, output_files = triplifier.run_triplifier_csv(
                csv_data_list, csv_table_names
            )
            return success, message, output_files

        elif properties_file == "triplifierSQL.properties":
            # Use Python Triplifier for PostgreSQL processing
            success, message, output_files = triplifier.run_triplifier_sql(
                db_url=db_url, db_user=db_user, db_password=db_password
            )
            return success, message, output_files
        else:
            return False, f"Unknown properties file: {properties_file}", []

    except Exception as e:
        logger.error(f"Unexpected error attempting to run the Python Triplifier: {e}")
        import traceback

        traceback.print_exc()
        return (
            False,
            f"Unexpected error attempting to run the Triplifier, error: {e}",
            [],
        )
