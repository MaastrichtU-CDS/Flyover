"""
Ingest service for data ingestion operations.

This service handles business logic for file uploads,
data validation, and triplification.
"""

import json
import logging
import os
import requests
import time
from typing import Any, Dict, List, Optional, Tuple

import gevent
from gevent import subprocess as gevent_subprocess
import polars as pl
from psycopg2 import connect
from werkzeug.utils import secure_filename

try:
    from ..utils.data_preprocessing import preprocess_dataframe, sanitise_table_name
except ImportError:
    from utils.data_preprocessing import preprocess_dataframe, sanitise_table_name

logger = logging.getLogger(__name__)


class IngestService:
    """
    Service class for data ingestion operations.

    Handles file validation, CSV parsing, and preparation
    for triplification.
    """

    ALLOWED_EXTENSIONS = {"csv", "jsonld"}

    @staticmethod
    def allowed_file(filename: str, allowed_extensions: Optional[set] = None) -> bool:
        """
        Check if the file has an allowed extension.

        Args:
            filename: Name of the file to check.
            allowed_extensions: Set of allowed extensions.

        Returns:
            True if extension is allowed, False otherwise.
        """
        if allowed_extensions is None:
            allowed_extensions = IngestService.ALLOWED_EXTENSIONS

        return (
            "." in filename and filename.rsplit(".", 1)[1].lower() in allowed_extensions
        )

    @staticmethod
    def validate_csv_files(files: List[Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate uploaded CSV files.

        Args:
            files: List of uploaded file objects.

        Returns:
            Tuple of (is_valid, error_message).
        """
        if not files:
            return False, "No files provided"

        if not any(f.filename for f in files):
            return (
                False,
                "If opting to submit a CSV data source, please upload it as a '.csv' file.",
            )

        for csv_file in files:
            if not IngestService.allowed_file(csv_file.filename, {"csv"}):
                return (
                    False,
                    "If opting to submit a CSV data source, please upload it as a '.csv' file.",
                )

        return True, None

    @staticmethod
    def parse_csv_files(
        files: List[Any],
        separator: str = ",",
        decimal: str = ".",
    ) -> Tuple[List[pl.DataFrame], List[str], Optional[str]]:
        """
        Parse uploaded CSV files into DataFrames.

        Args:
            files: List of uploaded file objects.
            separator: CSV field separator.
            decimal: Decimal separator.

        Returns:
            Tuple of (dataframes, table_names, error_message).
        """
        dataframes = []
        table_names = []

        try:
            for csv_file in files:
                # Read CSV with minimal inference
                df = pl.read_csv(
                    csv_file,
                    separator=separator,
                    infer_schema_length=0,
                    null_values=[],
                    try_parse_dates=False,
                )

                # Handle decimal conversion if needed
                if decimal != ".":
                    df = df.with_columns(
                        [pl.col(c).str.replace_all(decimal, ".") for c in df.columns]
                    )

                dataframes.append(preprocess_dataframe(df))

                # Extract table name from filename
                table_name = os.path.splitext(secure_filename(csv_file.filename))[0]
                table_names.append(table_name)

        except Exception as e:
            return [], [], f"Error parsing CSV files: {e}"

        return dataframes, table_names, None

    @staticmethod
    def parse_pk_fk_data(pk_fk_json: Optional[str]) -> Optional[List[Dict]]:
        """
        Parse PK/FK relationship data from JSON string.

        Args:
            pk_fk_json: JSON string with PK/FK data.

        Returns:
            Parsed list of relationships, or None.
        """
        if not pk_fk_json:
            return None

        try:
            return json.loads(pk_fk_json)
        except Exception as e:
            logger.error(f"Error parsing PK/FK data: {e}")
            return None

    @staticmethod
    def parse_cross_graph_data(cross_graph_json: Optional[str]) -> Optional[Dict]:
        """
        Parse cross-graph linking data from JSON string.

        Args:
            cross_graph_json: JSON string with cross-graph data.

        Returns:
            Parsed dict with cross-graph data, or None.
        """
        if not cross_graph_json:
            return None

        try:
            return json.loads(cross_graph_json)
        except Exception as e:
            logger.error(f"Error parsing cross-graph data: {e}")
            return None

    def handle_postgres_connection(
        self,
        username: str,
        password: str,
        postgres_url: str,
        postgres_db: str,
        table: str,
        root_dir: str,
        child_dir: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Handle PostgreSQL connection and properties file creation.

        Args:
            username: PostgreSQL username
            password: PostgreSQL password
            postgres_url: PostgreSQL URL
            postgres_db: PostgreSQL database name
            table: Table name
            root_dir: Root directory for file paths
            child_dir: Child directory for file paths

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Test PostgreSQL connection
            conn = connect(
                dbname=postgres_db,
                user=username,
                host=postgres_url,
                password=password,
            )
            conn.close()  # Close connection after testing
            logger.info("PostgreSQL connection successful")

            # Write connection details to properties file
            properties_path = os.path.join(root_dir, child_dir, "triplifierSQL.properties")
            os.makedirs(os.path.dirname(properties_path), exist_ok=True)

            with open(properties_path, "w") as f:
                f.write(
                    f"jdbc.url = jdbc:postgresql://{postgres_url}/{postgres_db}\n"
                    f"jdbc.user = {username}\n"
                    f"jdbc.password = {password}\n"
                    f"jdbc.driver = org.postgresql.Driver\n\n"
                )

            return True, None

        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            return False, f"PostgreSQL connection failed: {e}"

    def run_triplifier(
        self,
        properties_file: str,
        root_dir: str,
        child_dir: str,
        csv_data_list: Optional[List[pl.DataFrame]] = None,
        csv_table_names: Optional[List[str]] = None,
    ) -> Tuple[bool, str, Optional[List[dict]]]:
        """
        Run the Python Triplifier for data processing.

        Args:
            properties_file: Name of properties file (triplifierCSV.properties or triplifierSQL.properties)
            root_dir: Root directory for file paths
            child_dir: Child directory for file paths
            csv_data_list: List of CSV DataFrames (for CSV processing)
            csv_table_names: List of CSV table names (for CSV processing)

        Returns:
            Tuple of (success, message, output_files)
        """
        try:
            try:
                from ..utils.python_triplifier_integration import (
                    run_triplifier as run_triplifier_impl,
                )
            except ImportError:
                from utils.python_triplifier_integration import (
                    run_triplifier as run_triplifier_impl,
                )

            if properties_file == "triplifierCSV.properties":
                # Use Python Triplifier for CSV processing
                success, message, output_files = run_triplifier_impl(
                    properties_file=properties_file,
                    root_dir=root_dir,
                    child_dir=child_dir,
                    csv_data_list=csv_data_list,
                    csv_table_names=csv_table_names,
                )

            elif properties_file == "triplifierSQL.properties":
                # Use Python Triplifier for PostgreSQL processing
                success, message, output_files = run_triplifier_impl(
                    properties_file=properties_file, root_dir=root_dir, child_dir=child_dir
                )
            else:
                return False, f"Unknown properties file: {properties_file}", None

            if success:
                return True, (
                    "The data you have submitted was triplified successfully and "
                    "is now available in GraphDB."
                ), output_files
            else:
                return False, message, None

        except Exception as e:
            logger.error(f"Unexpected error attempting to run the Python Triplifier: {e}")
            return False, f"Unexpected error attempting to run the Triplifier, error: {e}", None

    @staticmethod
    def process_pk_fk_relationships(
        pk_fk_data: List[Dict],
        graphdb_service: Any,
    ) -> Tuple[bool, List[str]]:
        """
        Process all PK/FK relationships.

        Args:
            pk_fk_data: List of relationship definitions.
            graphdb_service: GraphDB service instance.

        Returns:
            Tuple of (overall_success, list of messages).
        """
        if not pk_fk_data:
            return True, []

        messages = []
        success = True

        # Create mapping of files with their PK/FK info
        file_map = {rel["fileName"]: rel for rel in pk_fk_data}

        for rel in pk_fk_data:
            if not all(
                [
                    rel.get("foreignKey"),
                    rel.get("foreignKeyTable"),
                    rel.get("foreignKeyColumn"),
                ]
            ):
                logger.warning(
                    f"Skipping incomplete FK relationship for file '{rel.get('fileName', 'unknown')}': "
                    f"missing foreignKey, foreignKeyTable, or foreignKeyColumn"
                )
                continue

            # Find the target table's PK info
            target_file = rel["foreignKeyTable"]
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get("primaryKey"):
                logger.warning(
                    f"Skipping FK relationship: target table '{target_file}' not found or has no primaryKey"
                )
                continue

            # Sanitise table names
            fk_table = sanitise_table_name(rel["fileName"])
            pk_table = sanitise_table_name(target_file)

            result = graphdb_service.process_pk_fk_relationship(
                fk_table,
                rel["foreignKey"],
                pk_table,
                target_rel["primaryKey"],
            )

            if result:
                messages.append(
                    f"Created FK relationship: {fk_table}.{rel['foreignKey']} -> "
                    f"{pk_table}.{target_rel['primaryKey']}"
                )
            else:
                success = False
                messages.append(
                    f"Failed FK relationship: {fk_table}.{rel['foreignKey']} -> "
                    f"{pk_table}.{target_rel['primaryKey']}"
                )

        return success, messages

    def upload_file_to_graphdb(
        self,
        file_path: str,
        url: str,
        content_type: str,
        wait_for_completion: bool = True,
        timeout_seconds: int = 300,
    ) -> Tuple[bool, str, str]:
        """
        Upload a single file to GraphDB with an intelligent fallback strategy.
        Uses gevent subprocess for better integration with gevent worker.

        Args:
            file_path: Path to the file to upload
            url: GraphDB endpoint URL
            content_type: MIME type (e.g. 'application/rdf+xml')
            wait_for_completion: Whether to wait for upload completion
            timeout_seconds: Timeout for each upload attempt

        Returns:
            Tuple of (success: bool, message: str, method_used: str)
        """
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            return False, error_msg, "none"

        # Get file info
        file_name = os.path.basename(file_path)

        if not wait_for_completion:
            # Start upload in background using gevent spawn
            logger.info(f"🚀 Starting background upload for {file_name}")
            gevent.spawn(
                self._background_upload_with_fallback,
                file_path,
                url,
                content_type,
                timeout_seconds,
                file_name,
            )
            return True, f"Background upload started for {file_name}", "background"

        # Synchronous upload with fallback
        return self._synchronous_upload_with_fallback(
            file_path, url, content_type, timeout_seconds, file_name
        )

    def _background_upload_with_fallback(
        self,
        file_path: str,
        url: str,
        content_type: str,
        timeout_seconds: int,
        file_name: str,
    ) -> None:
        """Execute upload in background using gevent greenlet"""
        try:
            start_time = time.time()
            logger.info(f"🔄 Background upload: trying data-binary first for {file_name}")

            success, message = self._try_data_binary_upload(
                file_path, url, content_type, timeout_seconds
            )

            if success:
                elapsed = time.time() - start_time
                logger.info(
                    f"✅ Background upload (data-binary) successful for {file_name} in {elapsed:.1f}s"
                )
                return
            else:
                logger.warning(
                    f"⚠️ Background data-binary failed for {file_name}: {message}"
                )
                logger.info(
                    f"🔄 Background upload: falling back to streaming for {file_name}"
                )

            # Fallback to streaming upload
            success, message = self._try_streaming_upload(
                file_path, url, content_type, timeout_seconds
            )
            elapsed = time.time() - start_time

            if success:
                logger.info(
                    f"✅ Background upload (streaming) successful for {file_name} in {elapsed:.1f}s"
                )
            else:
                logger.error(
                    f"❌ Background upload (both methods) failed for {file_name} after {elapsed:.1f}s: {message}"
                )

        except Exception as e:
            logger.error(f"❌ Background upload crashed for {file_name}: {str(e)}")

    def _synchronous_upload_with_fallback(
        self,
        file_path: str,
        url: str,
        content_type: str,
        timeout_seconds: int,
        file_name: str,
    ) -> Tuple[bool, str, str]:
        """Execute synchronous upload with intelligent fallback using gevent"""
        start_time = time.time()

        logger.info(f"Attempting --data-binary upload for {file_name}")
        success, message = self._try_data_binary_upload(
            file_path, url, content_type, timeout_seconds
        )

        if success:
            elapsed = time.time() - start_time
            logger.info(
                f"✅ --data-binary upload successful for {file_name} in {elapsed:.1f}s"
            )
            return True, message, "data-binary"
        else:
            logger.warning(f"⚠️ --data-binary failed for {file_name}: {message}")
            logger.info(f"🔄 Falling back to streaming upload for {file_name}")

        # Fallback to streaming upload
        success, message = self._try_streaming_upload(
            file_path, url, content_type, timeout_seconds
        )
        elapsed = time.time() - start_time

        if success:
            logger.info(f"✅ Streaming upload successful for {file_name} in {elapsed:.1f}s")
            return True, message, "streaming"
        else:
            logger.error(
                f"❌ Both upload methods failed for {file_name} after {elapsed:.1f}s"
            )
            return False, f"All upload methods failed. Last error: {message}", "failed"

    def _try_data_binary_upload(
        self, file_path: str, url: str, content_type: str, timeout_seconds: int
    ) -> Tuple[bool, str]:
        """Attempt upload using the --data-binary method with gevent subprocess"""
        try:
            result = gevent_subprocess.run(
                [
                    "curl",
                    "-X",
                    "POST",
                    "-H",
                    f"Content-Type: {content_type}",
                    "--data-binary",
                    f"@{file_path}",
                    "--fail",
                    "--silent",
                    "--show-error",
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )

            if result.returncode == 0:
                return True, "Upload successful via data-binary method"
            else:
                error_msg = result.stderr.strip() or "Unknown curl error"
                return False, f"curl data-binary failed: {error_msg}"

        except gevent_subprocess.TimeoutExpired:
            return False, "Upload timeout (data-binary method)"
        except Exception as e:
            return False, f"Unexpected error (data-binary method): {str(e)}"

    def _try_streaming_upload(
        self, file_path: str, url: str, content_type: str, timeout_seconds: int
    ) -> Tuple[bool, str]:
        """Attempt upload using -T streaming method with gevent subprocess"""
        try:
            result = gevent_subprocess.run(
                [
                    "curl",
                    "-X",
                    "POST",
                    "-H",
                    f"Content-Type: {content_type}",
                    "-T",
                    file_path,
                    "--fail",
                    "--silent",
                    "--show-error",
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )

            if result.returncode == 0:
                return True, "Upload successful via streaming method"
            else:
                error_msg = result.stderr.strip() or "Unknown curl error"
                return False, f"curl streaming failed: {error_msg}"

        except gevent_subprocess.TimeoutExpired:
            return False, "Upload timeout (streaming method)"
        except Exception as e:
            return False, f"Unexpected error (streaming method): {str(e)}"

    def upload_multiple_graphs(
        self,
        root_dir: str,
        graphdb_url: str,
        repo: str,
        output_files: List[dict],
        ontology_timeout: int = 300,
        data_timeout: int = 43200,
        data_background: bool = True,
    ) -> Tuple[bool, List[str]]:
        """
        Upload multiple ontology and data files as separate named graphs.
        Used for CSV files where each table gets its own named graph.

        Args:
            root_dir: Directory containing the files
            graphdb_url: GraphDB base URL
            repo: Repository name
            output_files: List of dicts with keys: data_file, ontology_file, table_name
            ontology_timeout: Timeout for ontology upload
            data_timeout: Timeout for data upload
            data_background: Whether to upload data in background

        Returns:
            Tuple of (success, list of messages)
        """
        messages = []
        overall_success = True

        try:
            for file_info in output_files:
                table_name = file_info.get("table_name")
                ontology_path = file_info.get("ontology_file")
                data_path = file_info.get("data_file")

                if not data_path or not ontology_path:
                    messages.append(f"Missing files for table {table_name}")
                    continue

                # Upload ontology file
                ontology_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://ontology.local/{table_name}/"

                success, status, method = self.upload_file_to_graphdb(
                    ontology_path,
                    ontology_url,
                    "application/rdf+xml",
                    True,
                    ontology_timeout,
                )

                if success:
                    messages.append(
                        f"Ontology uploaded for {table_name} ({method}): {status}"
                    )
                else:
                    messages.append(
                        f"Ontology upload failed for {table_name} ({method}): {status}"
                    )
                    overall_success = False
                    continue

                # Upload data file
                data_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://data.local/{table_name}/"

                success, status, method = self.upload_file_to_graphdb(
                    data_path,
                    data_url,
                    "application/x-turtle",
                    not data_background,
                    data_timeout,
                )

                if success:
                    messages.append(
                        f"Data upload started for {table_name} ({method}): {status}"
                    )
                else:
                    messages.append(
                        f"Data upload failed for {table_name} ({method}): {status}"
                    )
                    overall_success = False

            return overall_success, messages

        except Exception as e:
            logger.error(f"Failed to upload multiple graphs: {e}")
            return False, [f"Upload error: {e}"]

    def upload_ontology_then_data(
        self,
        root_dir: str,
        graphdb_url: str,
        repo: str,
        upload_ontology: bool = True,
        upload_data: bool = True,
        data_background: bool = True,
        ontology_timeout: int = 300,
        data_timeout: int = 43200,
    ) -> Tuple[bool, List[str]]:
        """
        Upload ontology first, then data (configurable background/foreground).

        Args:
            root_dir: Directory containing the files
            graphdb_url: GraphDB base URL
            repo: Repository name
            upload_ontology: Whether to upload ontology file
            upload_data: Whether to upload data file
            data_background: Whether to upload data in background
            ontology_timeout: Timeout for ontology upload
            data_timeout: Timeout for data upload

        Returns:
            Tuple of (success, list of messages)
        """
        messages = []
        overall_success = True

        try:
            # Step 1: Upload ontology and WAIT for completion
            if upload_ontology:
                ontology_path = os.path.join(root_dir, "ontology.owl")
                if os.path.exists(ontology_path):
                    ontology_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://ontology.local/"
                    success, status, method = self.upload_file_to_graphdb(
                        ontology_path,
                        ontology_url,
                        "application/rdf+xml",
                        True,
                        ontology_timeout,
                    )

                    if success:
                        messages.append(f"Ontology uploaded ({method}): {status}")
                    else:
                        messages.append(f"Ontology upload failed ({method}): {status}")
                        return False, messages
                else:
                    messages.append("No ontology file found (ontology.owl)")

            # Step 2: Upload data (background/foreground configurable)
            if upload_data:
                data_path = os.path.join(root_dir, "output.ttl")
                if os.path.exists(data_path):
                    data_url = f"{graphdb_url}/repositories/{repo}/rdf-graphs/service?graph=http://data.local/"

                    success, status, method = self.upload_file_to_graphdb(
                        data_path,
                        data_url,
                        "application/x-turtle",
                        not data_background,
                        data_timeout,
                    )

                    if success:
                        messages.append(f"Data upload started ({method}): {status}")
                    else:
                        messages.append(f"Data upload failed ({method}): {status}")
                        overall_success = False
                else:
                    messages.append("No data file found (output.ttl)")

            return overall_success, messages

        except Exception as e:
            logger.error(f"Failed to upload ontology and data: {e}")
            return False, [f"Upload error: {e}"]

    def background_pk_fk_processing(self, session_cache: Any) -> None:
        """
        Background function to process PK/FK relationships

        Args:
            session_cache: Session cache object
        """
        try:
            # Small delay to ensure GraphDB has processed the uploaded data
            import time
            time.sleep(3)
            
            # Get PK/FK data from session
            pk_fk_data = session_cache.pk_fk_data if hasattr(session_cache, 'pk_fk_data') else []
            
            if pk_fk_data:
                success, messages = self.process_pk_fk_relationships(pk_fk_data)
                session_cache.pk_fk_status = "completed" if success else "failed"
                session_cache.pk_fk_messages = messages
            else:
                session_cache.pk_fk_status = "no_data"
                
        except Exception as e:
            logger.error(f"Background PK/FK processing error: {e}")
            session_cache.pk_fk_status = "failed"

    def background_cross_graph_processing(self, session_cache: Any) -> None:
        """
        Background function to process cross-graph relationships

        Args:
            session_cache: Session cache object
        """
        try:
            # Small delay to ensure GraphDB has processed the uploaded data
            import time
            time.sleep(3)
            
            # Get cross-graph data from session
            cross_graph_data = session_cache.cross_graph_link_data if hasattr(session_cache, 'cross_graph_link_data') else None
            
            if cross_graph_data:
                success, message = self.process_cross_graph_relationships(cross_graph_data)
                session_cache.cross_graph_status = "completed" if success else "failed"
                session_cache.cross_graph_message = message
            else:
                session_cache.cross_graph_status = "no_data"
                
        except Exception as e:
            logger.error(f"Background cross-graph processing error: {e}")
            session_cache.cross_graph_status = "failed"

    @staticmethod
    def process_cross_graph_relationships(
        cross_graph_data: Dict,
        graphdb_service: Any,
    ) -> Tuple[bool, str]:
        """
        Process cross-graph relationships.

        Args:
            cross_graph_data: Cross-graph relationship definition.
            graphdb_service: GraphDB service instance.

        Returns:
            Tuple of (success, message).
        """
        if not cross_graph_data:
            return True, ""

        new_table = sanitise_table_name(cross_graph_data["newTableName"])
        existing_table = sanitise_table_name(cross_graph_data["existingTableName"])

        result = graphdb_service.process_cross_graph_relationship(
            new_table,
            cross_graph_data["newColumnName"],
            existing_table,
            cross_graph_data["existingColumnName"],
        )

        if result:
            return True, (
                f"Created cross-graph relationship: "
                f"{new_table}.{cross_graph_data['newColumnName']} -> "
                f"{existing_table}.{cross_graph_data['existingColumnName']}"
            )
        else:
            return False, (
                f"Failed cross-graph relationship: "
                f"{new_table}.{cross_graph_data['newColumnName']} -> "
                f"{existing_table}.{cross_graph_data['existingColumnName']}"
            )

    def get_column_class_uri(self, table_name: str, column_name: str) -> Optional[str]:
        """
        Get the class URI for a specific column in a table.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            Optional[str]: Class URI or None if not found
        """
        try:
            query = f"""
            PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
            SELECT ?class_uri
            WHERE {{
                ?uri dbo:table_name "{table_name}" .
                ?uri dbo:column "{column_name}" .
                ?uri dbo:class_uri ?class_uri .
            }}
            """
            
            result = self.execute_query(query)
            if result and result.strip():
                # Parse the result to extract the class URI
                lines = result.strip().split('\n')
                if len(lines) > 1:
                    # Skip header line and get the first result
                    return lines[1].split('|')[0].strip()
            return None
            
        except Exception as e:
            logger.error(f"Failed to get class URI for {table_name}.{column_name}: {e}")
            return None

    def insert_fk_relation(
        self,
        fk_table: str,
        fk_column: str,
        pk_table: str,
        pk_column: str,
    ) -> bool:
        """
        Insert a foreign key relationship between tables.

        Args:
            fk_table: Foreign key table name
            fk_column: Foreign key column name
            pk_table: Primary key table name
            pk_column: Primary key column name

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get class URIs for both columns
            fk_class_uri = self.get_column_class_uri(fk_table, fk_column)
            pk_class_uri = self.get_column_class_uri(pk_table, pk_column)
            
            if not fk_class_uri or not pk_class_uri:
                logger.warning(f"Could not find URIs for FK relationship: {fk_table}.{fk_column} -> {pk_table}.{pk_column}")
                return False

            # Insert the relationship
            predicate = "http://um-cds/ontologies/databaseontology/fk_refers_to"
            result = self.graphdb_service.process_pk_fk_relationship(
                predicate, fk_class_uri, pk_class_uri
            )
            return result is not None
            
        except Exception as e:
            logger.error(f"Failed to insert FK relationship: {fk_table}.{fk_column} -> {pk_table}.{pk_column}: {e}")
            return False

    def process_pk_fk_relationships(self, pk_fk_data: List[Dict]) -> Tuple[bool, List[str]]:
        """
        Process all PK/FK relationships from the given data.

        Args:
            pk_fk_data: List of relationship definitions

        Returns:
            Tuple of (overall_success, list of messages)
        """
        if not pk_fk_data:
            return True, []

        messages = []
        success = True

        # Create mapping of files with their PK/FK info
        file_map = {rel["fileName"]: rel for rel in pk_fk_data}

        for rel in pk_fk_data:
            if not all([
                rel.get("foreignKey"),
                rel.get("foreignKeyTable"),
                rel.get("foreignKeyColumn"),
            ]):
                continue

            # Find the target table's PK info
            target_file = rel["foreignKeyTable"]
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get("primaryKey"):
                continue

            # Sanitise table names
            fk_table = sanitise_table_name(rel["fileName"])
            pk_table = sanitise_table_name(target_file)

            result = self.insert_fk_relation(
                fk_table,
                rel["foreignKey"],
                pk_table,
                target_rel["primaryKey"],
            )

            if result:
                messages.append(
                    f"Created FK relationship: {fk_table}.{rel['foreignKey']} -> {pk_table}.{target_rel['primaryKey']}"
                )
            else:
                success = False
                messages.append(
                    f"Failed FK relationship: {fk_table}.{rel['foreignKey']} -> {pk_table}.{target_rel['primaryKey']}"
                )

        return success, messages
