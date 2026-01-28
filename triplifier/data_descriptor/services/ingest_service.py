"""
Ingest service for data ingestion operations.

This service handles business logic for file uploads,
data validation, and triplification.
"""

import logging
import os
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

        import json

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

        import json

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
                continue

            # Find the target table's PK info
            target_file = rel["foreignKeyTable"]
            target_rel = file_map.get(target_file)

            if not target_rel or not target_rel.get("primaryKey"):
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
