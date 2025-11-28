"""
Flyover JSON-LD Mapping Validator

This script validates Flyover semantic mapping files against the JSON-LD schema
and provides detailed, user-friendly feedback on any validation errors.

Usage:
    python validate_mapping.py <mapping_file. jsonld> [--schema <schema_file. json>]
    python validate_mapping.py --help

Examples:
    python validate_mapping. py my_mapping.jsonld
    python validate_mapping.py my_mapping.jsonld --schema custom_schema.json
    python validate_mapping.py --check-references my_mapping.jsonld
"""
import argparse
import json
import re
import sys

from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass, field

try:
    from jsonschema import Draft7Validator, ValidationError
    from jsonschema.exceptions import SchemaError
except ImportError:
    print("ERROR: jsonschema package is required.")
    print("Install it with: pip install jsonschema")
    sys.exit(1)

# ============================================================================
# Constants and Configuration
# ============================================================================

COLORS = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "red": "\033[91m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "blue": "\033[94m",
    "cyan": "\033[96m",
    "gray": "\033[90m",
}

DEFAULT_SCHEMA_PATH = Path(__file__).parent / "mapping_schema. json"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ValidationIssue:
    """Represents a single validation issue with context."""
    severity: str  # "error", "warning", "info"
    path: str
    message: str
    value: Any = None
    expected: Any = None
    suggestion: str = ""
    schema_path: str = ""


@dataclass
class ValidationResult:
    """Complete validation result with all issues."""
    is_valid: bool
    file_path: str
    issues: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    info: list = field(default_factory=list)
    statistics: dict = field(default_factory=dict)


# ============================================================================
# Helper Functions
# ============================================================================

def colorize(text: str, color: str, bold: bool = False) -> str:
    """Apply ANSI color codes to text."""
    if not sys.stdout.isatty():
        return text
    prefix = COLORS.get("bold", "") if bold else ""
    return f"{prefix}{COLORS.get(color, '')}{text}{COLORS['reset']}"


def format_path(path: list) -> str:
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


def get_value_preview(value: Any, max_length: int = 50) -> str:
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


def get_error_suggestion(error: ValidationError) -> str:
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
        return suggestions[validator](error)

    return ""


def get_field_documentation(path: list) -> str:
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
# Validation Logic
# ============================================================================

def load_json_file(file_path: Path) -> tuple[Optional[dict], Optional[str]]:
    """Load and parse a JSON file, returning (data, error_message)."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        return None, f"File not found: {file_path}"
    except PermissionError:
        return None, f"Permission denied: {file_path}"
    except Exception as e:
        return None, f"Error reading file: {e}"

    try:
        data = json.loads(content)
        return data, None
    except json.JSONDecodeError as e:
        # Provide detailed JSON parsing error
        lines = content.split("\n")
        error_line = lines[e.lineno - 1] if e.lineno <= len(lines) else ""

        error_msg = (
            f"Invalid JSON syntax at line {e.lineno}, column {e.colno}:\n"
            f"\n"
            f"  {e.lineno} | {error_line}\n"
            f"  {' ' * len(str(e.lineno))}   {' ' * (e.colno - 1)}^\n"
            f"\n"
            f"  Error: {e.msg}"
        )
        return None, error_msg


def check_cross_references(data: dict) -> list[ValidationIssue]:
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
                    match = re.match(r"schema:variable/(. +)$", maps_to)
                    if match:
                        var_name = match.group(1)
                        if var_name not in schema_vars:
                            path = f"databases. {db_key}. tables.{table_key}.columns.{col_key}. mapsTo"
                            issues.append(ValidationIssue(
                                severity="warning",
                                path=path,
                                message=f"References undefined schema variable: '{var_name}'",
                                value=maps_to,
                                suggestion=f"Available variables: {', '.join(sorted(schema_vars)[:5])}{'...' if len(schema_vars) > 5 else ''}"
                            ))

                # Check localMappings keys against schema terms
                local_mappings = col_data.get("localMappings", {})
                if local_mappings and col_key in schema_vars:
                    schema_var = data["schema"]["variables"].get(col_key, {})
                    schema_terms = set()
                    if "valueMapping" in schema_var and "terms" in schema_var["valueMapping"]:
                        schema_terms = set(schema_var["valueMapping"]["terms"].keys())

                    if schema_terms:
                        for local_term in local_mappings.keys():
                            if local_term not in schema_terms:
                                path = f"databases.{db_key}.tables.{table_key}.columns.{col_key}.localMappings.{local_term}"
                                issues.append(ValidationIssue(
                                    severity="info",
                                    path=path,
                                    message=f"Local mapping key '{local_term}' not in schema terms",
                                    suggestion=f"Schema terms: {', '.join(sorted(schema_terms)[:5])}{'...' if len(schema_terms) > 5 else ''}"
                                ))

    return issues


def validate_mapping(
        mapping_data: dict,
        schema_data: dict,
        check_references: bool = True
) -> ValidationResult:
    """Validate a mapping file against the schema."""
    result = ValidationResult(
        is_valid=True,
        file_path="",
        statistics={
            "total_errors": 0,
            "total_warnings": 0,
            "databases": 0,
            "tables": 0,
            "columns": 0,
            "variables": 0,
        }
    )

    # Create validator
    validator = Draft7Validator(schema_data)

    # Collect all errors
    errors = sorted(validator.iter_errors(mapping_data), key=lambda e: str(list(e.absolute_path)))

    for error in errors:
        path = format_path(list(error.absolute_path))
        suggestion = get_error_suggestion(error)
        field_doc = get_field_documentation(list(error.absolute_path))

        issue = ValidationIssue(
            severity="error",
            path=path,
            message=error.message,
            value=error.instance if not isinstance(error.instance, (dict, list)) else None,
            expected=error.validator_value if error.validator in ["type", "enum", "pattern"] else None,
            suggestion=suggestion,
            schema_path=format_path(list(error.absolute_schema_path)),
        )

        if field_doc:
            issue.suggestion = f"{suggestion}\n         Field: {field_doc}" if suggestion else f"Field: {field_doc}"

        result.issues.append(issue)
        result.is_valid = False

    result.statistics["total_errors"] = len(result.issues)

    # Cross-reference checks
    if check_references and result.is_valid:
        ref_issues = check_cross_references(mapping_data)
        for issue in ref_issues:
            if issue.severity == "warning":
                result.warnings.append(issue)
            else:
                result.info.append(issue)
        result.statistics["total_warnings"] = len(result.warnings)

    # Collect statistics
    if "databases" in mapping_data:
        result.statistics["databases"] = len(mapping_data["databases"])
        for db_data in mapping_data["databases"].values():
            if isinstance(db_data, dict) and "tables" in db_data:
                result.statistics["tables"] += len(db_data["tables"])
                for table_data in db_data["tables"].values():
                    if isinstance(table_data, dict) and "columns" in table_data:
                        result.statistics["columns"] += len(table_data["columns"])

    if "schema" in mapping_data and "variables" in mapping_data["schema"]:
        result.statistics["variables"] = len(mapping_data["schema"]["variables"])

    return result


# ============================================================================
# Output Formatting
# ============================================================================

def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{colorize('═' * 70, 'blue')}")
    print(colorize(f"  {text}", "blue", bold=True))
    print(colorize('═' * 70, 'blue'))


def print_issue(issue: ValidationIssue, index: int) -> None:
    """Print a formatted validation issue."""
    if issue.severity == "error":
        icon = colorize("✗", "red", bold=True)
        severity_label = colorize("ERROR", "red", bold=True)
    elif issue.severity == "warning":
        icon = colorize("⚠", "yellow", bold=True)
        severity_label = colorize("WARNING", "yellow", bold=True)
    else:
        icon = colorize("ℹ", "cyan")
        severity_label = colorize("INFO", "cyan")

    print(f"\n{icon} {severity_label} #{index}")
    print(colorize("─" * 60, "gray"))

    # Location
    print(f"  {colorize('Location:', 'bold')} {colorize(issue.path, 'cyan')}")

    # Message
    print(f"  {colorize('Problem:', 'bold')}  {issue.message}")

    # Current value
    if issue.value is not None:
        print(f"  {colorize('Value:', 'bold')}    {colorize(get_value_preview(issue.value), 'yellow')}")

    # Expected value
    if issue.expected is not None:
        expected_str = issue.expected
        if isinstance(expected_str, list):
            expected_str = ", ".join(repr(v) for v in expected_str[:5])
            if len(issue.expected) > 5:
                expected_str += ", ..."
        print(f"  {colorize('Expected:', 'bold')} {colorize(str(expected_str), 'green')}")

    # Suggestion
    if issue.suggestion:
        for i, line in enumerate(issue.suggestion.split("\n")):
            prefix = "Tip:" if i == 0 else "    "
            print(f"  {colorize(prefix, 'bold')}      {colorize(line, 'green')}")


def print_result(result: ValidationResult) -> None:
    """Print the complete validation result."""

    if result.is_valid and not result.warnings:
        print_header("Validation Successful")
        print(f"\n{colorize('✓', 'green', bold=True)} {colorize('All checks passed! ', 'green', bold=True)}")
    else:
        if result.issues:
            print_header("Validation Failed")
            print(
                f"\n{colorize('Found', 'red')} {colorize(str(len(result.issues)), 'red', bold=True)} {colorize('error(s)', 'red')}")

            for i, issue in enumerate(result.issues, 1):
                print_issue(issue, i)

        if result.warnings:
            if result.issues:
                print()
            print(
                f"\n{colorize('⚠', 'yellow', bold=True)} {colorize(f'Found {len(result.warnings)} warning(s)', 'yellow')}")

            for i, warning in enumerate(result.warnings, 1):
                print_issue(warning, i)

    # Print info items
    if result.info:
        print(f"\n{colorize('ℹ', 'cyan')} {colorize(f'Additional notes ({len(result.info)}):', 'cyan')}")
        for info in result.info[:5]:  # Limit to first 5
            print(f"  • {info.path}: {info.message}")
        if len(result.info) > 5:
            print(f"  ... and {len(result.info) - 5} more")

    # Print statistics
    print(f"\n{colorize('Statistics:', 'blue', bold=True)}")
    print(f"  • Schema variables: {result.statistics.get('variables', 0)}")
    print(f"  • Databases:        {result.statistics.get('databases', 0)}")
    print(f"  • Tables:           {result.statistics.get('tables', 0)}")
    print(f"  • Column mappings:  {result.statistics.get('columns', 0)}")

    # Final status
    print()
    if result.is_valid:
        if result.warnings:
            print(f"{colorize('⚠', 'yellow', bold=True)} {colorize('Valid with warnings', 'yellow', bold=True)}")
        else:
            print(f"{colorize('✓', 'green', bold=True)} {colorize('Valid', 'green', bold=True)}")
    else:
        print(f"{colorize('✗', 'red', bold=True)} {colorize('Invalid', 'red', bold=True)}")


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Validate Flyover JSON-LD mapping files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s my_mapping.jsonld
  %(prog)s my_mapping.jsonld --schema custom_schema.json
  %(prog)s my_mapping.jsonld --no-references
  %(prog)s my_mapping. jsonld --quiet
        """
    )

    parser.add_argument(
        "mapping_file",
        type=Path,
        help="Path to the mapping file to validate"
    )

    parser.add_argument(
        "-s", "--schema",
        type=Path,
        default=None,
        help="Path to the JSON Schema file (default: mapping_schema.json in same directory)"
    )

    parser.add_argument(
        "--no-references",
        action="store_true",
        help="Skip cross-reference validation"
    )

    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Only output errors, no decorative formatting"
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )

    args = parser.parse_args()

    # Find schema file
    schema_path = args.schema
    if not schema_path:
        # Try default locations
        candidates = [
            DEFAULT_SCHEMA_PATH,
            Path("mapping_schema.json"),
            Path(__file__).parent / "mapping_schema. json",
        ]
        for candidate in candidates:
            if candidate.exists():
                schema_path = candidate
                break

    if not schema_path or not schema_path.exists():
        print(colorize("ERROR: Schema file not found.", "red", bold=True))
        print("Please provide a schema file with --schema or place 'mapping_schema. json' in the current directory.")
        sys.exit(1)

    # Load schema
    schema_data, schema_error = load_json_file(schema_path)
    if schema_error:
        print(colorize("ERROR loading schema file:", "red", bold=True))
        print(schema_error)
        sys.exit(1)

    # Load mapping file
    if not args.quiet:
        print_header("Flyover Mapping Validator")
        print(f"\n  Mapping file: {colorize(str(args.mapping_file), 'cyan')}")
        print(f"  Schema file:  {colorize(str(schema_path), 'cyan')}")

    mapping_data, mapping_error = load_json_file(args.mapping_file)
    if mapping_error:
        if args.json:
            print(json.dumps({"valid": False, "error": mapping_error}))
        else:
            print(colorize("\nERROR loading mapping file:", "red", bold=True))
            print(mapping_error)
        sys.exit(1)

    # Validate
    result = validate_mapping(
        mapping_data,
        schema_data,
        check_references=not args.no_references
    )
    result.file_path = str(args.mapping_file)

    # Output results
    if args.json:
        output = {
            "valid": result.is_valid,
            "file": result.file_path,
            "errors": [
                {
                    "path": issue.path,
                    "message": issue.message,
                    "suggestion": issue.suggestion,
                }
                for issue in result.issues
            ],
            "warnings": [
                {
                    "path": warning.path,
                    "message": warning.message,
                }
                for warning in result.warnings
            ],
            "statistics": result.statistics,
        }
        print(json.dumps(output, indent=2))
    else:
        print_result(result)

    # Exit with an appropriate code
    sys.exit(0 if result.is_valid else 1)


if __name__ == "__main__":
    main()