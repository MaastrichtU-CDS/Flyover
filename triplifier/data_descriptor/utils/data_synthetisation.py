import random

import polars as pl

from typing import Any, Dict, List, Optional, Union

# Constants for mock data generation
_DEFAULT_MIN_VALUE = 1
_DEFAULT_MAX_VALUE = 100
_MISSING_VALUE_PROBABILITY = 0.1
_IDENTIFIER_FORMAT = "ID_{:05d}"


def preprocess_mixed_type_data(
    table_data: Dict[str, List[Union[str, int, float, None]]],
) -> Dict[str, List[Union[str, int, float, None]]]:
    """
    Pre-process table data to handle mixed types by converting to appropriate types.
    Specifically handles the case where missing values are lists like ["NULL"].

    Args:
        table_data: Dictionary where keys are column names and values are lists of data

    Returns:
        Processed table data with consistent types for each column
    """
    processed_data = {}

    for col_name, col_values in table_data.items():
        processed_values = []
        has_list_values = False
        has_numeric = False
        has_string = False

        # First pass: detect what types we have
        for value in col_values:
            if isinstance(value, list):
                has_list_values = True
            elif isinstance(value, (int, float)):
                has_numeric = True
            elif isinstance(value, str):
                has_string = True

        # Second pass: convert values appropriately
        if has_list_values:
            # If we have list values, convert everything to string
            for value in col_values:
                if isinstance(value, list):
                    # Extract single item from list or join multiple items
                    if len(value) == 1:
                        processed_values.append(str(value[0]))
                    else:
                        processed_values.append(", ".join(str(v) for v in value))
                else:
                    processed_values.append(str(value))
        elif has_numeric and has_string:
            # Mixed numeric and string - convert everything to string
            for value in col_values:
                processed_values.append(str(value))
        elif has_numeric:
            # All numeric - keep as is but handle None values
            for value in col_values:
                if value is None:
                    processed_values.append(None)
                else:
                    processed_values.append(value)
        else:
            # All strings or other types - keep as is
            processed_values = col_values

        processed_data[col_name] = processed_values

    return processed_data


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
