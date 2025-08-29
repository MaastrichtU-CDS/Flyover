"""
File operations utilities for the Flyover data descriptor.

This module contains functions for file validation and operations.
"""

from typing import Set


def allowed_file(filename: str, allowed_extensions: Set[str]) -> bool:
    """
    Check if the uploaded file has an allowed extension.
    
    This function validates whether a given filename has an extension that
    is included in the set of allowed extensions.
    
    Args:
        filename (str): The name of the file to be checked
        allowed_extensions (Set[str]): A set of strings representing the allowed file extensions
        
    Returns:
        bool: True if the file has an allowed extension, False otherwise
        
    Example:
        >>> allowed_file("data.csv", {"csv", "txt"})
        True
        >>> allowed_file("data.pdf", {"csv", "txt"})
        False
    """
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in allowed_extensions