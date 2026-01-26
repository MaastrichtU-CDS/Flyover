import random

import pandas as pd

from typing import Any, Dict, List, Optional, Union

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
    table_id: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
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
        pandas DataFrames containing the mock data for that table.

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
    
    result: Dict[str, pd.DataFrame] = {}
    
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
                    missing_value = local_mappings.get("missing_or_unspecified", "")
                    
                    # Generate random numbers or missing values
                    table_data[local_column] = [
                        (
                            random.randint(_DEFAULT_MIN_VALUE, _DEFAULT_MAX_VALUE)
                            if random.random() > _MISSING_VALUE_PROBABILITY or not has_missing
                            else missing_value
                        )
                        for _ in range(num_rows)
                    ]
                
                elif data_type == "identifier":
                    # Generate sequential identifiers
                    table_data[local_column] = [_IDENTIFIER_FORMAT.format(i) for i in range(1, num_rows + 1)]
                
                elif data_type == "standardised":
                    # TODO: improve handling of standardised data; e.g. using ontology informed synthetic data
                    # For now, treat similar to continuous
                    has_missing = "missing_or_unspecified" in local_mappings
                    missing_value = local_mappings.get("missing_or_unspecified", "")
                    
                    table_data[local_column] = [
                        (
                            random.randint(_DEFAULT_MIN_VALUE, _DEFAULT_MAX_VALUE)
                            if random.random() > _MISSING_VALUE_PROBABILITY or not has_missing
                            else missing_value
                        )
                        for _ in range(num_rows)
                    ]
                else:
                    raise ValueError(f"Unsupported data type: {data_type}")
            
            # Store the DataFrame with a composite key
            result[f"{db_id}.{tbl_id}"] = pd.DataFrame(table_data)
    
    return result
