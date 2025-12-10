"""
Data preprocessing utilities for cleaning and preparing data for the Flyover application.
"""

import polars as pl
import re
import logging
from typing import Any, Dict, List, Tuple

# Setup logger for this module
logger = logging.getLogger(__name__)

# Global registry for column mappings (since polars does not have .attrs)
# Defined at module level for clearer visibility
_column_mapping_registry: Dict[int, Dict] = {}


def clean_column_names(df: pl.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Clean column names to make them HTML/JavaScript safe and meaningful.

    Args:
        df: polars DataFrame with potentially problematic column names

    Returns:
        Tuple of (cleaned_columns, original_columns)
    """
    logger.info(
        f"Starting column name cleaning for DataFrame with {len(df.columns)} columns"
    )

    original_columns = df.columns
    cleaned_columns = []
    problematic_count = 0

    for i, col in enumerate(original_columns):
        # Handle empty, None, or problematic column names
        if (
            col is None
            or str(col).strip() == ""
            or str(col) == "nan"
            or str(col).startswith("Unnamed:")
        ):
            cleaned_col = f"column_{i + 1}"
            problematic_count += 1
            logger.debug(
                f"Replaced problematic column at index {i} with '{cleaned_col}'"
            )
        else:
            cleaned_col = str(col).strip()

        # Replace problematic characters with safe alternatives
        original_cleaned = cleaned_col
        cleaned_col = _sanitise_column_name(cleaned_col)

        if original_cleaned != cleaned_col:
            logger.debug(f"Sanitized column '{original_cleaned}' -> '{cleaned_col}'")

        # Ensure it starts with a letter (required for HTML IDs)
        if cleaned_col and not cleaned_col[0].isalpha():
            old_name = cleaned_col
            cleaned_col = "col_" + cleaned_col
            logger.debug(f"Added prefix to column '{old_name}' -> '{cleaned_col}'")

        # Handle empty result after cleaning
        if not cleaned_col:
            cleaned_col = f"column_{i + 1}"
            logger.warning(
                f"Column at index {i} resulted in empty name, using '{cleaned_col}'"
            )

        cleaned_columns.append(cleaned_col)

    # Handle duplicate column names
    final_columns = _handle_duplicate_columns(cleaned_columns)

    duplicate_count = len(cleaned_columns) - len(set(cleaned_columns))
    if duplicate_count > 0:
        logger.info(f"Handled {duplicate_count} duplicate column names")

    logger.info(
        f"Column cleaning completed: {problematic_count} problematic columns fixed"
    )
    return final_columns, original_columns


def _sanitise_column_name(col_name: str) -> str:
    """
    Sanitise a single column name by replacing problematic characters.

    Args:
        col_name: Original column name

    Returns:
        Sanitised column name
    """
    original_name = col_name

    # Character replacement mapping
    replacements = {
        ".": "_dot_",
        " ": "_",
        "-": "_",
        "(": "_",
        ")": "_",
        "[": "_",
        "]": "_",
        "/": "_slash_",
        "\\": "_backslash_",
        "&": "_and_",
        "%": "_percent_",
        "#": "_hash_",
        "@": "_at_",
        "!": "_excl_",
        "?": "_quest_",
        "*": "_star_",
        "+": "_plus_",
        "=": "_eq_",
        "<": "_lt_",
        ">": "_gt_",
        "|": "_pipe_",
        "^": "_caret_",
        "~": "_tilde_",
        "`": "_backtick_",
        '"': "_quote_",
        "'": "_squote_",
        ",": "_comma_",
        ";": "_semicolon_",
        ":": "_colon_",
    }

    # Apply replacements
    for old_char, new_char in replacements.items():
        col_name = col_name.replace(old_char, new_char)

    # Remove any remaining non-alphanumeric characters except underscore
    col_name = re.sub(r"[^a-zA-Z0-9_]", "_", col_name)

    # Remove multiple consecutive underscores
    col_name = re.sub(r"_+", "_", col_name)

    # Remove leading/trailing underscores
    col_name = col_name.strip("_")

    if original_name != col_name:
        logger.debug(f"Sanitized '{original_name}' -> '{col_name}'")

    return col_name


def _handle_duplicate_columns(columns: List[str]) -> List[str]:
    """
    Handle duplicate column names by adding suffixes.

    Args:
        columns: List of potentially duplicate column names

    Returns:
        List of unique column names
    """
    seen: Dict[str, int] = {}
    final_columns = []
    duplicates_handled = 0

    for col in columns:
        if col in seen:
            seen[col] += 1
            new_name = f"{col}_{seen[col]}"
            final_columns.append(new_name)
            duplicates_handled += 1
            logger.debug(f"Renamed duplicate column '{col}' -> '{new_name}'")
        else:
            seen[col] = 0
            final_columns.append(col)

    if duplicates_handled > 0:
        logger.info(f"Handled {duplicates_handled} duplicate column names")

    return final_columns


def preprocess_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Preprocess a DataFrame by cleaning column names and preparing it for HTML rendering.

    Args:
        df: Original DataFrame

    Returns:
        DataFrame with cleaned column names and original names stored in metadata

    Note:
        Column mappings are stored in a global registry keyed by DataFrame id().
        Call clear_column_mapping_registry() when DataFrames are no longer needed
        to free memory.
    """
    logger.info(
        f"Starting DataFrame preprocessing: {df.height} rows, {df.width} columns"
    )

    cleaned_columns, original_columns = clean_column_names(df)

    # Rename columns using polars rename
    column_mapping = dict(zip(original_columns, cleaned_columns))
    processed_df = df.rename(column_mapping)

    # Store original column names for later reference using a global registry (Polars doesn't have .attrs like pandas)
    _column_mapping_registry[id(processed_df)] = {
        "original_columns": original_columns,
        "column_mapping": dict(zip(cleaned_columns, original_columns)),
    }

    logger.info("DataFrame preprocessing completed successfully")
    logger.debug(f"Column mapping created: {len(column_mapping)} entries")

    return processed_df


def clear_column_mapping_registry() -> None:
    """
    Clear all stored column mappings from the global registry.

    Call this function when DataFrames are no longer needed to free memory.
    """
    _column_mapping_registry.clear()
    logger.debug("Column mapping registry cleared")


def get_original_column_name(df: pl.DataFrame, cleaned_name: str) -> str:
    """
    Get the original column name from a cleaned column name.

    Args:
        df: DataFrame with column mapping in registry
        cleaned_name: Cleaned column name

    Returns:
        Original column name if found, otherwise the cleaned name
    """
    df_id = id(df)
    if (
        df_id in _column_mapping_registry
        and "column_mapping" in _column_mapping_registry[df_id]
    ):
        original_name = _column_mapping_registry[df_id]["column_mapping"].get(
            cleaned_name, cleaned_name
        )
        if original_name != cleaned_name:
            logger.debug(
                f"Retrieved original column name: '{cleaned_name}' -> '{original_name}'"
            )
        return original_name

    logger.debug(f"No column mapping found, returning cleaned name: '{cleaned_name}'")
    return cleaned_name


def get_column_mapping(df: pl.DataFrame) -> Dict[str, str]:
    """
    Get the column mapping for a preprocessed DataFrame.

    Args:
        df: DataFrame with column mapping in registry

    Returns:
        Dictionary mapping cleaned column names to original column names
    """
    df_id = id(df)
    if (
        df_id in _column_mapping_registry
        and "column_mapping" in _column_mapping_registry[df_id]
    ):
        return _column_mapping_registry[df_id]["column_mapping"]
    return {}


def dataframe_to_template_data(df: pl.DataFrame) -> Dict[str, Any]:
    """
    Convert a polars DataFrame to a template-friendly dictionary structure.

    Instead of wrapping DataFrames, this converts them to plain Python data
    structures that Jinja templates can work with directly.

    Args:
        df: polars DataFrame to convert

    Returns:
        Dictionary containing:
        - 'columns': list of unique column values (from 'column' field)
        - 'rows': list of row dicts
        - 'by_column': dict mapping column names to their rows for easy filtering
    """
    rows = df.to_dicts()

    # Get unique column values if 'column' exists in dataframe
    columns = []
    if "column" in df.columns:
        columns = df.get_column("column").unique().to_list()

    # Create a mapping of column name to rows for easy filtering in templates
    by_column: Dict[str, List[Dict]] = {}
    for row in rows:
        col_name = row.get("column", "")
        if col_name not in by_column:
            by_column[col_name] = []
        by_column[col_name].append(row)

    return {
        "columns": columns,
        "rows": rows,
        "by_column": by_column,
    }
