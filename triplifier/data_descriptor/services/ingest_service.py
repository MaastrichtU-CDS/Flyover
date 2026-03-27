"""
Ingest service for data ingestion operations.

This service handles business logic for file uploads,
data validation, and triplification.
"""

import json
import logging
import os
import requests
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
from werkzeug.utils import secure_filename

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
        Upload a file to GraphDB using the Workbench API.

        Args:
            file_path: Path to the file to upload
            url: GraphDB Workbench URL
            content_type: Content type of the file
            wait_for_completion: Whether to wait for upload completion
            timeout_seconds: Timeout for the upload operation

        Returns:
            Tuple of (success, status_message, error_message)
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                return False, "", f"File not found: {file_path}"

            # Read the file content
            with open(file_path, 'rb') as f:
                file_content = f.read()

            # Prepare headers
            headers = {
                'Content-Type': content_type,
            }

            # Upload the file
            response = requests.post(
                url,
                data=file_content,
                headers=headers,
                timeout=timeout_seconds
            )

            # Check response
            if response.status_code in [200, 201, 202]:
                return True, f"Upload successful: {response.text}", ""
            else:
                return False, "", f"Upload failed with status {response.status_code}: {response.text}"

        except requests.exceptions.RequestException as e:
            logger.error(f"GraphDB upload failed: {e}")
            return False, "", f"Request exception: {e}"
        except Exception as e:
            logger.error(f"GraphDB upload error: {e}")
            return False, "", f"Unexpected error: {e}"

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
