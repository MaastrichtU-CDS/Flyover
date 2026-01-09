"""
Flyover JSON-LD Mapping Validation Module

This module provides comprehensive validation for JSON-LD semantic mapping files,
including schema validation, cross-reference checking, and user-friendly error reporting.

Classes:
    ValidationIssue: Represents a single validation issue with context.
    ValidationResult: Complete validation result with all issues.
    MappingValidator: Main validator class for JSON-LD mapping files.

Usage:
    from data_descriptor.validation import MappingValidator

    validator = MappingValidator()
    result = validator.validate_file('mapping.jsonld')
    if not result.is_valid:
        errors = validator.format_errors_for_ui(result)
"""

from .mapping_validator import (
    ValidationIssue,
    ValidationResult,
    MappingValidator,
)

__all__ = [
    "ValidationIssue",
    "ValidationResult",
    "MappingValidator",
]
