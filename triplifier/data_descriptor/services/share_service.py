"""
Share service for handling share-related business logic.

This service handles operations related to sharing data, including
semantic map downloads, ontology downloads, and mock data generation.
"""

import json
import logging
import os
import zipfile
import random
import requests

import polars as pl

from flask import Response, abort, after_this_request
from typing import Any, Dict, List, Optional, Union, Tuple

try:
    from ..utils.data_preprocessing import preprocess_mixed_type_data
except ImportError:
    from utils.data_preprocessing import preprocess_mixed_type_data

logger = logging.getLogger(__name__)

# Constants for mock data generation
_DEFAULT_MIN_VALUE = 1
_DEFAULT_MAX_VALUE = 100
_MISSING_VALUE_PROBABILITY = 0.1
_IDENTIFIER_FORMAT = "ID_{:05d}"


def generate_mock_data_from_semantic_map(
        jsonld_map: Dict[str, Any],
        num_rows: int = 100,
        random_seed: Optional[int] = None,
        database_id: Optional[str] = None,
        table_id: Optional[str] = None,
) -> Dict[str, pl.DataFrame]:
    """
    Generate mock data from a JSON-LD semantic mapping.

    This function creates DataFrames of mock data based on the provided JSON-LD mapping,
    respecting local column mappings, value mappings, and data types from the schema.

    Supports multiple databases and multiple tables per database. Can generate data for:
    - All databases and all tables (default)
    - A specific database (all tables within it)
    - A specific table within a specific database

    Args:
        jsonld_map: A dictionary representing the JSON-LD mapping with schema and database sections.
        num_rows: Number of rows to generate in the mock dataset. Defaults to 100.
        random_seed: Optional seed for reproducibility.
                     If provided, random number generation will be deterministic.
        database_id: Optional database ID to generate data for. If None, generates for all databases.
        table_id: Optional table ID to generate data for. Requires database_id to be specified.

    Returns:
        A dictionary where keys are in the format "database_id.table_id" and values are
        polars DataFrames containing the mock data for that table.

    Raises:
        ValueError: If the jsonld_map is malformed or missing required keys.
        ValueError: If table_id is specified without database_id.
    """
    if table_id is not None and database_id is None:
        raise ValueError("table_id requires database_id to be specified")

    if random_seed is not None:
        random.seed(random_seed)

    # Extract schema variables
    schema = jsonld_map.get("schema", {})
    variables = schema.get("variables", {})

    # Extract databases
    databases = jsonld_map.get("databases", {})

    result: Dict[str, pl.DataFrame] = {}

    # Iterate through databases
    for db_id, db_info in databases.items():
        # Skip if specific database requested and this isn't it
        if database_id is not None and db_id != database_id:
            continue

        tables = db_info.get("tables", {})

        # Iterate through tables
        for tbl_id, tbl_info in tables.items():
            # Skip if specific table requested and this isn't it
            if table_id is not None and tbl_id != table_id:
                continue

            # Generate mock data for this table
            columns = tbl_info.get("columns", {})
            table_data: Dict[str, List[Union[str, int, float, None]]] = {}

            for col_id, col_info in columns.items():
                # Get the variable this column maps to
                maps_to = col_info.get("mapsTo", "")
                if not maps_to:
                    continue  # Skip if no mapping defined

                # Extract variable name from the full IRI (e.g., "schema:variable/identifier" -> "identifier")
                var_name = maps_to.split("/")[-1] if "/" in maps_to else maps_to
                var_name = var_name.strip()  # Clean up any whitespace

                if not var_name or var_name not in variables:
                    continue  # Skip if variable not found in schema

                var_info = variables[var_name]
                local_column = col_info.get("localColumn")

                if local_column is None:
                    continue  # Skip if no local column defined

                data_type = var_info.get("dataType")
                local_mappings = col_info.get("localMappings", {})

                if data_type == "categorical":
                    # Collect all local values from localMappings
                    # localMappings can have single values or be grouped
                    local_values = []
                    for semantic_term, local_value in local_mappings.items():
                        if isinstance(local_value, list):
                            # Multiple local values map to same concept
                            local_values.extend(local_value)
                        else:
                            # Single local value
                            local_values.append(local_value)

                    if not local_values:
                        continue  # Skip if no valid local values

                    table_data[local_column] = random.choices(local_values, k=num_rows)

                elif data_type == "continuous":
                    # Check if there's a missing/unspecified mapping
                    has_missing = "missing_or_unspecified" in local_mappings
                    missing_value = local_mappings.get("missing_or_unspecified", None)

                    # Generate random numbers or missing values
                    table_data[local_column] = [
                        (
                            random.randint(_DEFAULT_MIN_VALUE, _DEFAULT_MAX_VALUE)
                            if random.random() > _MISSING_VALUE_PROBABILITY
                               or not has_missing
                            else missing_value
                        )
                        for _ in range(num_rows)
                    ]

                elif data_type == "identifier":
                    # Generate sequential identifiers
                    table_data[local_column] = [
                        _IDENTIFIER_FORMAT.format(i) for i in range(1, num_rows + 1)
                    ]

                elif data_type == "standardised":
                    # TODO: improve handling of standardised data; e.g. using ontology informed synthetic data
                    # For now, treat similar to continuous
                    has_missing = "missing_or_unspecified" in local_mappings
                    missing_value = local_mappings.get("missing_or_unspecified", None)

                    table_data[local_column] = [
                        (
                            random.randint(_DEFAULT_MIN_VALUE, _DEFAULT_MAX_VALUE)
                            if random.random() > _MISSING_VALUE_PROBABILITY
                               or not has_missing
                            else missing_value
                        )
                        for _ in range(num_rows)
                    ]
                else:
                    raise ValueError(f"Unsupported data type: {data_type}")

            # Create polars DataFrame with proper type inference
            if table_data:  # Only create DataFrame if we have data
                try:
                    # Preprocess data to handle mixed types and list-type missing values
                    processed_table_data = preprocess_mixed_type_data(table_data)

                    # Create DataFrame with the processed data
                    result[f"{db_id}.{tbl_id}"] = pl.DataFrame(processed_table_data)
                except Exception as e:
                    # If creation still fails, try with explicit schema as fallback
                    schema_overrides = {}
                    for col_name, col_values in processed_table_data.items():
                        # Check if we have mixed types (e.g., integers and None)
                        has_none = any(v is None for v in col_values)
                        has_numeric = any(
                            isinstance(v, (int, float))
                            for v in col_values
                            if v is not None
                        )

                        if has_none and has_numeric:
                            schema_overrides[col_name] = (
                                pl.Float64
                            )  # Use Float64 to handle None
                        elif has_none:
                            schema_overrides[col_name] = (
                                pl.Utf8
                            )  # Use string type for mixed None/string

                    if schema_overrides:
                        result[f"{db_id}.{tbl_id}"] = pl.DataFrame(
                            processed_table_data, schema=schema_overrides
                        )
                    else:
                        raise e

    return result


class ShareService:
    """
    Service class for share-related operations.

    Handles business logic for downloading semantic maps, ontologies,
    and other share-related functionality.
    """

    @staticmethod
    def download_semantic_map(session_cache, formulate_local_map) -> Response:
        """
        Download the semantic map in JSON-LD or legacy JSON format.

        Args:
            session_cache: Application session cache
            formulate_local_map: Function to formulate local semantic maps

        Returns:
            Flask Response with semantic map file
        """
        try:
            # Prefer JSON-LD format
            if session_cache.jsonld_mapping is not None:
                mapping_dict = session_cache.jsonld_mapping.to_dict()
                filename = "semantic_mapping.jsonld"

                return Response(
                    json.dumps(mapping_dict, indent=2, ensure_ascii=False),
                    mimetype="application/ld+json",
                    headers={"Content-Disposition": f"attachment;filename={filename}"},
                )

            # Fall back to legacy format
            if len(session_cache.databases) > 1:
                return ShareService._download_multiple_semantic_maps(
                    session_cache, formulate_local_map
                )
            else:
                return ShareService._download_single_semantic_map(
                    session_cache, formulate_local_map
                )

        except Exception as e:
            abort(500, description=f"Error processing semantic map: {e}")

    @staticmethod
    def _download_multiple_semantic_maps(session_cache, formulate_local_map) -> Response:
        """Create zip file with multiple semantic maps."""
        zip_filename = "local_semantic_maps.zip"

        for database in session_cache.databases:
            filename = f"local_semantic_map_{database}.json"
            with zipfile.ZipFile(zip_filename, "a") as zipf:
                modified_map = formulate_local_map(database)
                zipf.writestr(filename, json.dumps(modified_map, indent=4))

        @after_this_request
        def remove_file(response):
            try:
                os.remove(zip_filename)
            except Exception as error:
                logger.error(f"Error removing zip file: {error}")
            return response

        with open(zip_filename, "rb") as f:
            return Response(
                f.read(),
                mimetype="application/zip",
                headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
            )

    @staticmethod
    def _download_single_semantic_map(session_cache, formulate_local_map) -> Response:
        """Download single semantic map."""
        database = session_cache.databases[0]
        filename = f"local_semantic_map_{database}.json"

        try:
            modified_map = formulate_local_map(database)
            return Response(
                json.dumps(modified_map, indent=4),
                mimetype="application/json",
                headers={"Content-Disposition": f"attachment;filename={filename}"},
            )
        except Exception as e:
            abort(500, description=f"Error processing semantic map: {e}")

    @staticmethod
    def download_ontology(
            session_cache,
            graphdb_service,
            graphdb_url: str,
            named_graph: str = "http://ontology.local/",
            filename: str = None
    ) -> Response:
        """
        Download ontology files from GraphDB.

        Args:
            session_cache: Application session cache
            graphdb_service: GraphDB service instance
            graphdb_url: GraphDB base URL
            named_graph: Base URL for ontology graphs
            filename: Optional filename override

        Returns:
            Flask Response with ontology file(s)
        """
        try:
            # Determine databases to process
            databases_to_process = []

            # First, try to get databases from session cache
            if session_cache.databases and len(session_cache.databases) > 1:
                databases_to_process = session_cache.databases
            else:
                # Query GraphDB to find all ontology graphs
                query_graphs = """
                    SELECT DISTINCT ?g WHERE {
                        GRAPH ?g {
                            ?s ?p ?o .
                        }
                        FILTER(STRSTARTS(STR(?g), "http://ontology.local/"))
                    }
                """
                result = graphdb_service.repository.execute_query(query_graphs)

                if result and result.strip():
                    # Parse result to extract database names
                    # This is a simplified version - actual implementation would parse the CSV result
                    # For now, we'll use a basic approach
                    lines = result.strip().split('\n')
                    if len(lines) > 1:  # Skip header
                        for line in lines[1:]:
                            if line.strip():
                                parts = line.split(',')
                                if len(parts) > 0:
                                    graph_uri = parts[0].strip('"')
                                    # Extract database name from URI like "http://ontology.local/database_name/"
                                    db_name = graph_uri.replace(named_graph, "").rstrip("/")
                                    if db_name:
                                        databases_to_process.append(db_name)

            # If we have multiple databases, create a zip file
            if len(databases_to_process) > 1:
                return ShareService._download_multiple_ontologies(
                    databases_to_process, graphdb_service, named_graph, graphdb_url
                )
            else:
                return ShareService._download_single_ontology(
                    databases_to_process, graphdb_service, named_graph, filename
                )

        except Exception as e:
            abort(500, description=f"Error downloading ontology: {e}")

    @staticmethod
    def _download_multiple_ontologies(
            databases: list,
            graphdb_service,
            named_graph: str,
            graphdb_url: str
    ) -> Response:
        """Create zip file with multiple ontologies."""
        zip_filename = "local_ontologies.zip"
        files_added = 0

        with zipfile.ZipFile(zip_filename, "w") as zipf:
            for database in databases:
                table_graph = f"{named_graph}{database}/"
                ontology_filename = f"local_ontology_{database}.nt"

                content, status = graphdb_service.repository.download_ontology(table_graph)

                if status == 200 and content and content.strip():
                    zipf.writestr(ontology_filename, content)
                    files_added += 1
                else:
                    logger.warning(f"No ontology data for graph: {table_graph}")

        if files_added == 0:
            if os.path.exists(zip_filename):
                os.remove(zip_filename)
            abort(404, description="No ontology data found.")

        @after_this_request
        def remove_file(response):
            try:
                os.remove(zip_filename)
            except Exception as error:
                logger.error(f"Error removing zip file: {error}")
            return response

        with open(zip_filename, "rb") as f:
            return Response(
                f.read(),
                mimetype="application/zip",
                headers={"Content-Disposition": f"attachment;filename={zip_filename}"},
            )

    @staticmethod
    def _download_single_ontology(
            databases: list,
            graphdb_service,
            named_graph: str,
            filename: str = None
    ) -> Response:
        """Download single ontology."""
        if len(databases) == 1:
            database = databases[0]
            ontology_graph = f"{named_graph}{database}/"
            filename = f"local_ontology_{database}.nt"
        else:
            ontology_graph = named_graph
            filename = filename or "local_ontology.nt"

        content, status = graphdb_service.repository.download_ontology(ontology_graph)

        if status == 200:
            return Response(
                content,
                mimetype="application/n-triples",
                headers={"Content-Disposition": f"attachment;filename={filename}"},
            )
        else:
            abort(500, description=f"Failed to download ontology. Status: {status}")

    @staticmethod
    def generate_mock_data_from_semantic_map(
            semantic_map_data: dict,
            num_rows: int = 100,
            random_seed: int = None,
            database_id: str = None,
            table_id: str = None
    ) -> dict:
        """
        Generate mock data based on semantic map structure using the data_synthetisation utility.

        Args:
            semantic_map_data: Semantic map data structure
            num_rows: Number of rows to generate
            random_seed: Optional random seed for reproducibility
            database_id: Optional specific database to generate for
            table_id: Optional specific table to generate for

        Returns:
            Dictionary with generated mock data in the format {"database.table": DataFrame}
        """
        try:
            # Convert polars DataFrames to dictionaries for JSON serialization
            mock_data_result = generate_mock_data_from_semantic_map(
                semantic_map_data,
                num_rows=num_rows,
                random_seed=random_seed,
                database_id=database_id,
                table_id=table_id
            )

            # Convert DataFrames to JSON-serializable format
            json_serializable_result = {}
            for table_key, dataframe in mock_data_result.items():
                json_serializable_result[table_key] = dataframe.to_dicts()

            return {
                "success": True,
                "data": json_serializable_result,
                "metadata": {
                    "generated_from": "semantic_map",
                    "privacy_preserving": True,
                    "row_count": num_rows,
                    "tables_generated": len(json_serializable_result)
                }
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "metadata": {
                    "generated_from": "semantic_map",
                    "privacy_preserving": True
                }
            }
