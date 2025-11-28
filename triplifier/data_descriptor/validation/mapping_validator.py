"""
Flyover JSON-LD Mapping Validator

This module validates Flyover semantic mapping files against the JSON-LD schema
and provides detailed, user-friendly feedback on any validation errors.

Classes:
    ValidationIssue: Represents a single validation issue with context.
    ValidationResult: Complete validation result with all issues and statistics.
    MappingValidator: Main validator class for JSON-LD mapping files.

Usage:
    validator = MappingValidator()
    result = validator.validate(mapping_data)
    if not result.is_valid:
        errors = validator.format_errors_for_ui(result)
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

try:
    from jsonschema import Draft7Validator, ValidationError
except ImportError:
    Draft7Validator = None
    ValidationError = Exception


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class ValidationIssue:
    """
    Represents a single validation issue with context.

    Attributes:
        severity: Issue severity level - "error", "warning", or "info".
        path: JSON path where the issue was found (e.g., "schema.variables.age").
        message: Human-readable description of the issue.
        value: The actual value that caused the issue (optional).
        expected: What was expected instead (optional).
        suggestion: Actionable suggestion for fixing the issue (optional).
        schema_path: Path in the JSON Schema where the validation failed (optional).
    """

    severity: str  # "error", "warning", "info"
    path: str
    message: str
    value: Any = None
    expected: Any = None
    suggestion: str = ""
    schema_path: str = ""


@dataclass
class ValidationResult:
    """
    Complete validation result with all issues and statistics.

    Attributes:
        is_valid: True if the mapping passed validation without errors.
        issues: List of error-level ValidationIssue objects.
        warnings: List of warning-level ValidationIssue objects.
        statistics: Dictionary with counts and metadata about the mapping.
    """

    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    warnings: List[ValidationIssue] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Helper Functions
# ============================================================================


def _format_path(path: list) -> str:
    """Format a JSON path for display."""
    if not path:
        return "(root)"

    parts = []
    for item in path:
        if isinstance(item, int):
            parts.append(f"[{item}]")
        else:
            if parts and not parts[-1].endswith("]"):
                parts.append(".")
            parts.append(str(item))

    return "".join(parts)


def _get_value_preview(value: Any, max_length: int = 50) -> str:
    """Get a preview of a value for display."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        if len(value) > max_length:
            return f'"{value[:max_length]}..."'
        return f'"{value}"'
    if isinstance(value, list):
        return f"[array with {len(value)} items]"
    if isinstance(value, dict):
        return f"{{object with {len(value)} keys}}"
    return str(value)[:max_length]


def _get_error_suggestion(error: ValidationError) -> str:
    """Generate a helpful suggestion based on the error type."""
    validator = error.validator

    suggestions = {
        "required": lambda e: f"Add the missing field(s): {', '.join(e.validator_value)}",
        "type": lambda e: f"Change the value to type: {e.validator_value}",
        "enum": lambda e: f"Use one of: {', '.join(repr(v) for v in e.validator_value)}",
        "pattern": lambda e: f"Value must match pattern: {e.validator_value}",
        "minLength": lambda e: f"Value must be at least {e.validator_value} character(s)",
        "minProperties": lambda e: f"Object must have at least {e.validator_value} property/properties",
        "format": lambda e: f"Value must be a valid {e.validator_value}",
        "additionalProperties": lambda e: "Remove unexpected properties or check for typos",
        "oneOf": lambda e: "Value must match exactly one of the allowed schemas",
    }

    if validator in suggestions:
        try:
            return suggestions[validator](error)
        except Exception:
            pass

    return ""


def _get_field_documentation(path: list) -> str:
    """Get documentation for a field based on its path."""
    docs = {
        "@context": "JSON-LD context defining namespace prefixes and term mappings",
        "@id": "Unique URI identifier for this resource",
        "@type": "Type declaration for this resource",
        "name": "Human-readable name",
        "description": "Description of this resource",
        "version": "Semantic version number (e.g., '1.0.0')",
        "created": "Creation date in YYYY-MM-DD format",
        "endpoint": "SPARQL endpoint URL for data queries",
        "schema": "Embedded semantic schema with variable definitions",
        "databases": "Database definitions with tables and column mappings",
        "variables": "Variable definitions with predicates, classes, and value mappings",
        "tables": "Table definitions with column mappings",
        "columns": "Column mappings linking local data to schema variables",
        "mapsTo": "Reference to schema variable (e.g., 'schema:variable/biological_sex')",
        "localColumn": "Column name in your local data source",
        "localMappings": "Mapping of schema terms to local values",
        "dataType": "Variable data type: identifier, categorical, continuous, ordinal, date, or text",
        "predicate": "RDF predicate (property) URI for this variable",
        "class": "RDF class (concept) URI for this variable",
        "schemaReconstruction": "List of nodes defining the RDF graph structure",
        "valueMapping": "Mapping of categorical values to ontology classes",
        "terms": "Individual term mappings within a value mapping",
        "targetClass": "Target ontology class URI for a term",
    }

    if path:
        last_key = str(path[-1])
        if last_key in docs:
            return docs[last_key]

    return ""


# ============================================================================
# MappingValidator Class
# ============================================================================


class MappingValidator:
    """
    Validator for Flyover JSON-LD semantic mapping files.

    This class provides comprehensive validation including:
    - JSON Schema validation against the mapping schema
    - Cross-reference checking (mapsTo references exist in schema)
    - User-friendly error messages with suggestions

    Attributes:
        schema_path: Path to the JSON Schema file.
        schema_data: Loaded JSON Schema data.

    Example:
        validator = MappingValidator()
        result = validator.validate_file('mapping.jsonld')
        if not result.is_valid:
            for issue in result.issues:
                print(f"{issue.path}: {issue.message}")
    """

    def __init__(self, schema_path: Optional[Union[str, Path]] = None):
        """
        Initialise the validator with an optional custom schema path.

        Args:
            schema_path: Path to custom JSON Schema file. If not provided,
                        uses the default mapping_schema.json in the schemas directory.
        """
        self.schema_path = self._resolve_schema_path(schema_path)
        self.schema_data = self._load_schema()

    def _resolve_schema_path(self, custom_path: Optional[Union[str, Path]] = None) -> Path:
        """Resolve the schema path, using default if not provided."""
        if custom_path:
            return Path(custom_path)

        # Default schema locations to check
        candidates = [
            Path(__file__).parent.parent / "schemas" / "mapping_schema.json",
            Path(__file__).parent / "mapping_schema.json",
            Path("mapping_schema.json"),
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        # Return default path even if it doesn't exist (will fail at load time)
        return candidates[0]

    def _load_schema(self) -> Optional[dict]:
        """Load the JSON Schema from the schema path."""
        if not self.schema_path.exists():
            return None

        try:
            with open(self.schema_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    def validate(
        self,
        mapping_data: dict,
        check_references: bool = True
    ) -> ValidationResult:
        """
        Validate a mapping dictionary against the JSON-LD schema.

        Args:
            mapping_data: The mapping data as a dictionary.
            check_references: If True, also check that mapsTo references
                            exist in schema.variables.

        Returns:
            ValidationResult with is_valid flag, issues, warnings, and statistics.
        """
        result = ValidationResult(
            is_valid=True,
            statistics={
                "total_errors": 0,
                "total_warnings": 0,
                "databases": 0,
                "tables": 0,
                "columns": 0,
                "variables": 0,
            },
        )

        # Check if jsonschema is available
        if Draft7Validator is None:
            result.warnings.append(
                ValidationIssue(
                    severity="warning",
                    path="(system)",
                    message="jsonschema package not installed, skipping schema validation",
                    suggestion="Install jsonschema with: pip install jsonschema",
                )
            )
            return result

        # Check if schema is loaded
        if not self.schema_data:
            result.warnings.append(
                ValidationIssue(
                    severity="warning",
                    path="(system)",
                    message=f"Schema file not found at {self.schema_path}",
                    suggestion="Ensure the mapping_schema.json file exists",
                )
            )
            return result

        # Create validator and collect errors
        validator = Draft7Validator(self.schema_data)
        errors = sorted(
            validator.iter_errors(mapping_data),
            key=lambda e: str(list(e.absolute_path))
        )

        for error in errors:
            path = _format_path(list(error.absolute_path))
            suggestion = _get_error_suggestion(error)
            field_doc = _get_field_documentation(list(error.absolute_path))

            issue = ValidationIssue(
                severity="error",
                path=path,
                message=error.message,
                value=(
                    error.instance
                    if not isinstance(error.instance, (dict, list))
                    else None
                ),
                expected=(
                    error.validator_value
                    if error.validator in ["type", "enum", "pattern"]
                    else None
                ),
                suggestion=suggestion,
                schema_path=_format_path(list(error.absolute_schema_path)),
            )

            if field_doc:
                issue.suggestion = (
                    f"{suggestion}\nField: {field_doc}"
                    if suggestion
                    else f"Field: {field_doc}"
                )

            result.issues.append(issue)
            result.is_valid = False

        result.statistics["total_errors"] = len(result.issues)

        # Cross-reference checks
        if check_references and result.is_valid:
            ref_issues = self._check_cross_references(mapping_data)
            for issue in ref_issues:
                if issue.severity == "warning":
                    result.warnings.append(issue)
                else:
                    result.issues.append(issue)
                    result.is_valid = False
            result.statistics["total_warnings"] = len(result.warnings)

        # Collect statistics
        self._collect_statistics(mapping_data, result)

        return result

    def validate_file(
        self,
        file_path: Union[str, Path],
        check_references: bool = True
    ) -> ValidationResult:
        """
        Validate a JSON-LD mapping file.

        Args:
            file_path: Path to the mapping file.
            check_references: If True, also check cross-references.

        Returns:
            ValidationResult with is_valid flag, issues, warnings, and statistics.
        """
        file_path = Path(file_path)

        # Check file exists
        if not file_path.exists():
            return ValidationResult(
                is_valid=False,
                issues=[
                    ValidationIssue(
                        severity="error",
                        path="(file)",
                        message=f"File not found: {file_path}",
                        suggestion="Check the file path and ensure the file exists",
                    )
                ],
            )

        # Load and parse JSON
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
        except IOError as e:
            return ValidationResult(
                is_valid=False,
                issues=[
                    ValidationIssue(
                        severity="error",
                        path="(file)",
                        message=f"Error reading file: {e}",
                    )
                ],
            )

        try:
            mapping_data = json.loads(content)
        except json.JSONDecodeError as e:
            # Provide detailed JSON parsing error
            lines = content.split("\n")
            error_line = lines[e.lineno - 1] if e.lineno <= len(lines) else ""

            return ValidationResult(
                is_valid=False,
                issues=[
                    ValidationIssue(
                        severity="error",
                        path=f"(line {e.lineno}, column {e.colno})",
                        message=f"Invalid JSON syntax: {e.msg}",
                        value=error_line.strip()[:50] if error_line else None,
                        suggestion="Check for missing commas, brackets, or quotes",
                    )
                ],
            )

        return self.validate(mapping_data, check_references=check_references)

    def _check_cross_references(self, data: dict) -> List[ValidationIssue]:
        """Check that column mappings reference existing schema variables."""
        issues = []

        # Get defined variables
        schema_vars = set()
        if "schema" in data and "variables" in data["schema"]:
            schema_vars = set(data["schema"]["variables"].keys())

        if not schema_vars:
            return issues

        # Check each column mapping
        databases = data.get("databases", {})
        for db_key, db_data in databases.items():
            if not isinstance(db_data, dict):
                continue

            tables = db_data.get("tables", {})
            for table_key, table_data in tables.items():
                if not isinstance(table_data, dict):
                    continue

                columns = table_data.get("columns", {})
                for col_key, col_data in columns.items():
                    if not isinstance(col_data, dict):
                        continue

                    maps_to = col_data.get("mapsTo", "")
                    if maps_to:
                        # Extract variable name from mapsTo
                        match = re.match(r"schema:variable/(.+)$", maps_to)
                        if match:
                            var_name = match.group(1)
                            if var_name not in schema_vars:
                                path = f"databases.{db_key}.tables.{table_key}.columns.{col_key}.mapsTo"
                                available = ", ".join(sorted(schema_vars)[:5])
                                if len(schema_vars) > 5:
                                    available += "..."
                                issues.append(
                                    ValidationIssue(
                                        severity="warning",
                                        path=path,
                                        message=f"References undefined schema variable: '{var_name}'",
                                        value=maps_to,
                                        suggestion=f"Available variables: {available}",
                                    )
                                )

                    # Check localMappings keys against schema terms
                    local_mappings = col_data.get("localMappings", {})
                    # Extract variable key from mapsTo to check schema terms
                    var_key_match = re.match(r"schema:variable/(.+)$", maps_to) if maps_to else None
                    var_key = var_key_match.group(1) if var_key_match else None

                    if local_mappings and var_key and var_key in schema_vars:
                        schema_var = data["schema"]["variables"].get(var_key, {})
                        schema_terms = set()
                        if (
                            "valueMapping" in schema_var
                            and "terms" in schema_var["valueMapping"]
                        ):
                            schema_terms = set(schema_var["valueMapping"]["terms"].keys())

                        if schema_terms:
                            for local_term in local_mappings.keys():
                                if local_term not in schema_terms:
                                    path = f"databases.{db_key}.tables.{table_key}.columns.{col_key}.localMappings.{local_term}"
                                    available = ", ".join(sorted(schema_terms)[:5])
                                    if len(schema_terms) > 5:
                                        available += "..."
                                    issues.append(
                                        ValidationIssue(
                                            severity="info",
                                            path=path,
                                            message=f"Local mapping key '{local_term}' not in schema terms",
                                            suggestion=f"Schema terms: {available}",
                                        )
                                    )

        return issues

    def _collect_statistics(self, data: dict, result: ValidationResult) -> None:
        """Collect statistics about the mapping."""
        if "databases" in data:
            result.statistics["databases"] = len(data["databases"])
            for db_data in data["databases"].values():
                if isinstance(db_data, dict) and "tables" in db_data:
                    result.statistics["tables"] += len(db_data["tables"])
                    for table_data in db_data["tables"].values():
                        if isinstance(table_data, dict) and "columns" in table_data:
                            result.statistics["columns"] += len(table_data["columns"])

        if "schema" in data and "variables" in data["schema"]:
            result.statistics["variables"] = len(data["schema"]["variables"])

    def format_errors_for_ui(self, result: ValidationResult) -> List[Dict[str, Any]]:
        """
        Format validation errors for UI display.

        Args:
            result: ValidationResult from validate() or validate_file().

        Returns:
            List of dictionaries suitable for JSON response, each containing:
            - path: JSON path where the issue occurred
            - severity: "error", "warning", or "info"
            - message: Human-readable error message
            - suggestion: Optional fix suggestion
            - value: Optional current value that caused the error
        """
        errors = []

        for issue in result.issues:
            errors.append({
                "path": issue.path,
                "severity": issue.severity,
                "message": issue.message,
                "suggestion": issue.suggestion,
                "value": _get_value_preview(issue.value) if issue.value is not None else None,
            })

        for warning in result.warnings:
            errors.append({
                "path": warning.path,
                "severity": warning.severity,
                "message": warning.message,
                "suggestion": warning.suggestion,
                "value": _get_value_preview(warning.value) if warning.value is not None else None,
            })

        return errors
