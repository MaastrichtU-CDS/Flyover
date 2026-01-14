import random

import pandas as pd

from typing import Any, Dict, List, Optional, Union


def generate_mock_data_from_semantic_map(
    semantic_map: Dict[str, Any], num_rows: int = 100, random_seed: Optional[int] = None
) -> pd.DataFrame:
    """
    Generate mock data from a semantic map dictionary.

    This function creates a DataFrame of mock data based on the provided semantic map,
    respecting local definitions, value mappings, and data types.

    Args:
        semantic_map: A dictionary representing the semantic map, with keys as variable names and values
                     containing metadata such as data type, local definition, and value mappings.
        num_rows: Number of rows to generate in the mock dataset. Defaults to 100.
        random_seed: Optional seed for reproducibility.
                     If provided, random number generation will be deterministic.

    Returns:
        A pandas' DataFrame containing the mock data, with columns corresponding to
        variables with valid local definitions.

    Raises:
        ValueError: If the semantic_map is malformed or missing required keys.
    """
    if random_seed is not None:
        random.seed(random_seed)

    data: Dict[str, List[Union[str, int, float, None]]] = {}
    variable_info = semantic_map.get("variable_info", {})

    for var_name, var_info in variable_info.items():
        local_def = var_info.get("local_definition")
        if local_def is None:
            continue  # Skip if no local definition

        data_type = var_info.get("data_type")
        value_mapping = var_info.get("value_mapping", {}).get("terms", {})

        if data_type == "categorical":
            # Collect all local terms, including empty strings for missing/unspecified
            local_terms = [
                term_info.get("local_term")
                for term_info in value_mapping.values()
                if term_info.get("local_term") is not None
                or term_info.get("local_term") == ""
            ]
            if not local_terms:
                continue  # Skip if no valid local terms
            data[local_def] = random.choices(local_terms, k=num_rows)

        elif data_type in (
            "continuous",
            "standardised",
            "identifier",
        ):  # TODO: improve handling of standardised data; e.g. using ontology informed synthetic data
            # Check for missing/unspecified annotations in value_mapping
            has_missing = any(
                term_info.get("local_term") == "" or term_info.get("local_term") is None
                for term_info in value_mapping.values()
            )
            # Generate random numbers or empty strings for missing data
            data[local_def] = [
                (
                    random.randint(1, 100)
                    if random.random() > 0.1 or not has_missing
                    else ""
                )
                for _ in range(num_rows)
            ]
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

    return pd.DataFrame(data)
