"""
Data preprocessing utilities for cleaning and preparing data for the Flyover application.
"""

import pandas as pd
import re
import logging
from typing import Tuple, List

# Setup logger for this module
logger = logging.getLogger(__name__)


def clean_column_names(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Clean column names to make them HTML/JavaScript safe and meaningful.

    Args:
        df: pandas DataFrame with potentially problematic column names

    Returns:
        Tuple of (cleaned_columns, original_columns)
    """
    logger.info(
        f"Starting column name cleaning for DataFrame with {len(df.columns)} columns"
    )

    original_columns = df.columns.tolist()
    cleaned_columns = []
    problematic_count = 0

    for i, col in enumerate(original_columns):
        # Handle empty, NaN, or pandas "Unnamed" column names
        if (
            pd.isna(col)
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
    seen = {}
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


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess a DataFrame by cleaning column names and preparing it for HTML rendering.

    Args:
        df: Original DataFrame

    Returns:
        DataFrame with cleaned column names and original names stored in attrs
    """
    logger.info(
        f"Starting DataFrame preprocessing: {df.shape[0]} rows, {df.shape[1]} columns"
    )

    cleaned_columns, original_columns = clean_column_names(df)

    # Create a copy to avoid modifying the original
    processed_df = df.copy()
    processed_df.columns = cleaned_columns

    # Store original column names for later reference
    processed_df.attrs["original_columns"] = original_columns
    processed_df.attrs["column_mapping"] = dict(zip(cleaned_columns, original_columns))

    logger.info(f"DataFrame preprocessing completed successfully")
    logger.debug(
        f"Column mapping created: {len(processed_df.attrs['column_mapping'])} entries"
    )

    return processed_df


def get_original_column_name(df: pd.DataFrame, cleaned_name: str) -> str:
    """
    Get the original column name from a cleaned column name.

    Args:
        df: DataFrame with column mapping in attrs
        cleaned_name: Cleaned column name

    Returns:
        Original column name if found, otherwise the cleaned name
    """
    if hasattr(df, "attrs") and "column_mapping" in df.attrs:
        original_name = df.attrs["column_mapping"].get(cleaned_name, cleaned_name)
        if original_name != cleaned_name:
            logger.debug(
                f"Retrieved original column name: '{cleaned_name}' -> '{original_name}'"
            )
        return original_name

    logger.debug(f"No column mapping found, returning cleaned name: '{cleaned_name}'")
    return cleaned_name
