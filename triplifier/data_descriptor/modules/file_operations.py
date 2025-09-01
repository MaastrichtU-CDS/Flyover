"""
File operations module.

This module contains functions for handling file uploads, validation,
and file-related operations in the data descriptor application.
"""

from typing import Set, Optional, Union, List
from flask import flash, render_template


def allowed_file(filename: str, allowed_extensions: Set[str]) -> bool:
    """
    Check if the uploaded file has an allowed extension.

    Args:
        filename: The name of the file to be checked
        allowed_extensions: A set of strings representing the allowed file extensions

    Returns:
        True if the file has an allowed extension, False otherwise
    """
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in allowed_extensions


def retrieve_global_names(global_semantic_map: Optional[dict]) -> Union[List[str], object]:
    """
    Retrieve the names of global variables from the semantic map.

    Args:
        global_semantic_map: Dictionary containing global semantic mapping data

    Returns:
        A list of strings representing the names of the global variables,
        or a Flask template render if an error occurs

    The function checks if the global semantic map is a dictionary.
    If it is not, it returns a list of default global variable names.
    If it is a dictionary, it attempts to retrieve the keys from the 'variable_info' field,
    capitalize them, replace underscores with spaces, and return them as a list.
    """
    if not isinstance(global_semantic_map, dict):
        return ['Research subject identifier', 'Biological sex', 'Age at inclusion', 'Other']
    else:
        try:
            return [name.capitalize().replace('_', ' ') for name in
                    global_semantic_map['variable_info'].keys()] + ['Other']
        except Exception as e:
            flash(f"Failed to read the global semantic map. Error: {e}")
            return render_template('ingest.html', error=True)