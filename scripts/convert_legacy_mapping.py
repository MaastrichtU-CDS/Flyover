"""
Flyover Legacy Mapping Converter

This script converts legacy Flyover semantic mapping files (old JSON format)
to the new JSON-LD format with separated schema and local mappings.

Usage:
    python convert_legacy_mapping.py <legacy_file. json> [options]
    python convert_legacy_mapping.py --help

Examples:
    python convert_legacy_mapping. py old_mapping.json
    python convert_legacy_mapping.py old_mapping.json -o new_mapping. jsonld
    python convert_legacy_mapping. py old_mapping. json --database-name my_db --table-name patients
    python convert_legacy_mapping.py old_mapping.json --mapping-name "Centre A Mapping"

Options:
    -o, --output          Output file path (default: <input>_converted.jsonld)
    --database-name       Name for the database in the new format
    --table-name          Name for the table in the new format
    --mapping-name        Human-readable name for the mapping
    --mapping-id          URI identifier for the mapping
    --schema-id           URI identifier for the schema
    --dry-run             Show conversion result without writing file
    --validate            Validate output against schema after conversion
"""

import json
import sys
import argparse
import re
from pathlib import Path
from datetime import date
from typing import Any, Optional
from dataclasses import dataclass, field
from copy import deepcopy

# ============================================================================
# Constants
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

# Default JSON-LD context for new format
DEFAULT_CONTEXT = {
    "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/",
    "sio": "http://semanticscience.org/resource/",
    "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
    "mesh": "http://id.nlm.nih.gov/mesh/",
    "roo": "http://www.cancerdata.org/roo/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "schema": "schema/",
    "mapping": "mapping/",
    "variables": {"@id": "schema:hasVariable", "@container": "@index"},
    "schemaReconstruction": {
        "@id": "schema:hasSchemaReconstruction",
        "@container": "@list",
    },
    "valueMapping": {"@id": "schema:hasValueMapping"},
    "terms": {"@id": "schema:hasTerms", "@container": "@index"},
    "targetClass": {"@id": "schema:mapsToClass", "@type": "@id"},
    "databases": {"@id": "mapping:hasDatabase", "@container": "@index"},
    "tables": {"@id": "mapping:hasTable", "@container": "@index"},
    "columns": {"@id": "mapping:hasColumn", "@container": "@index"},
    "localMappings": {"@id": "mapping:hasLocalMappings", "@container": "@index"},
    "mapsTo": {"@id": "mapping:mapsToVariable", "@type": "@id"},
}

# Mapping from old data_type values to new @type values
DATA_TYPE_TO_VARIABLE_TYPE = {
    "identifier": "schema:IdentifierVariable",
    "categorical": "schema:CategoricalVariable",
    "continuous": "schema:ContinuousVariable",
    "ordinal": "schema:OrdinalVariable",
    "date": "schema:DateVariable",
    "text": "schema:TextVariable",
}

# Mapping from old schema_reconstruction type to new @type
RECONSTRUCTION_TYPE_MAP = {
    "class": "schema:ClassNode",
    "node": "schema:UnitNode",
    "property": "schema:PropertyNode",
}


# ============================================================================
# Helper Functions
# ============================================================================


def colorise(text: str, color: str, bold: bool = False) -> str:
    """Apply ANSI color codes to text."""
    if not sys.stdout.isatty():
        return text
    prefix = COLORS.get("bold", "") if bold else ""
    return f"{prefix}{COLORS.get(color, '')}{text}{COLORS['reset']}"


def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{colorise('═' * 70, 'blue')}")
    print(colorise(f"  {text}", "blue", bold=True))
    print(colorise("═" * 70, "blue"))


def print_success(text: str) -> None:
    """Print a success message."""
    print(f"{colorise('✓', 'green', bold=True)} {text}")


def print_warning(text: str) -> None:
    """Print a warning message."""
    print(f"{colorise('⚠', 'yellow', bold=True)} {colorise(text, 'yellow')}")


def print_error(text: str) -> None:
    """Print an error message."""
    print(f"{colorise('✗', 'red', bold=True)} {colorise(text, 'red')}")


def print_info(text: str) -> None:
    """Print an info message."""
    print(f"{colorise('ℹ', 'cyan')} {text}")


def sanitise_key(key: str) -> str:
    """Sanitise a string to be used as a JSON key/identifier."""
    # Replace spaces and special chars with underscores
    sanitised = re.sub(r"[^a-zA-Z0-9_]", "_", key)
    # Remove consecutive underscores
    sanitised = re.sub(r"_+", "_", sanitised)
    # Remove leading/trailing underscores
    sanitised = sanitised.strip("_")
    return sanitised.lower() if sanitised else "unnamed"


def extract_database_name(legacy_data: dict) -> str:
    """Extract or generate a database name from legacy data."""
    db_name = legacy_data.get("database_name")
    if db_name:
        # Remove file extension if present
        if isinstance(db_name, str):
            db_name = Path(db_name).stem
            return sanitise_key(db_name)
    return "default_database"


def extract_table_name(legacy_data: dict) -> str:
    """Extract or generate a table name from legacy data."""
    db_name = legacy_data.get("database_name")
    if db_name and isinstance(db_name, str):
        return sanitise_key(Path(db_name).stem)
    return "default_table"


# ============================================================================
# Conversion Logic
# ============================================================================


@dataclass
class ConversionResult:
    """Result of a conversion operation."""

    success: bool
    output: Optional[dict] = None
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    statistics: dict = field(default_factory=dict)


def convert_schema_reconstruction(legacy_reconstruction: list) -> list:
    """Convert legacy schema_reconstruction to new format."""
    new_reconstruction = []

    for item in legacy_reconstruction:
        if not isinstance(item, dict):
            continue

        old_type = item.get("type", "class")
        new_item = {
            "@type": RECONSTRUCTION_TYPE_MAP.get(old_type, "schema:ClassNode"),
            "predicate": item.get("predicate", ""),
            "class": item.get("class", ""),
        }

        # Copy optional fields
        if "placement" in item:
            new_item["placement"] = item["placement"]
        if "class_label" in item:
            new_item["classLabel"] = item["class_label"]
        if "node_label" in item:
            new_item["nodeLabel"] = item["node_label"]
        if "aesthetic_label" in item:
            new_item["aestheticLabel"] = item["aesthetic_label"]

        new_reconstruction.append(new_item)

    return new_reconstruction


def convert_value_mapping(legacy_value_mapping: dict) -> dict:
    """Convert legacy value_mapping to new schema format."""
    if not legacy_value_mapping or "terms" not in legacy_value_mapping:
        return {"terms": {}}

    new_terms = {}
    legacy_terms = legacy_value_mapping.get("terms", {})

    for term_key, term_value in legacy_terms.items():
        if isinstance(term_value, dict):
            target_class = term_value.get("target_class", "")
            if target_class:
                new_terms[term_key] = {"targetClass": target_class}

    return {"terms": new_terms}


def extract_local_mappings(legacy_value_mapping: dict) -> dict:
    """Extract local term mappings from legacy value_mapping."""
    if not legacy_value_mapping or "terms" not in legacy_value_mapping:
        return {}

    local_mappings = {}
    legacy_terms = legacy_value_mapping.get("terms", {})

    for term_key, term_value in legacy_terms.items():
        if isinstance(term_value, dict):
            local_term = term_value.get("local_term")
            # Include the mapping (even if None or empty string)
            if local_term is not None:
                local_mappings[term_key] = local_term if local_term else ""
            else:
                # If no local_term specified, use empty string as placeholder
                local_mappings[term_key] = ""

    return local_mappings


def convert_variable_to_schema(var_key: str, legacy_var: dict) -> dict:
    """Convert a legacy variable definition to new schema format."""
    data_type = legacy_var.get("data_type", "categorical")
    var_type = DATA_TYPE_TO_VARIABLE_TYPE.get(data_type, "schema:CategoricalVariable")

    new_var = {
        "@id": f"schema:variable/{var_key}",
        "@type": var_type,
        "dataType": data_type,
        "predicate": legacy_var.get("predicate", "").strip(),
        "class": legacy_var.get("class", ""),
    }

    # Convert schema_reconstruction if present
    if "schema_reconstruction" in legacy_var:
        reconstruction = legacy_var["schema_reconstruction"]
        if isinstance(reconstruction, list) and reconstruction:
            new_var["schemaReconstruction"] = convert_schema_reconstruction(
                reconstruction
            )

    # Convert value_mapping if present
    if "value_mapping" in legacy_var:
        value_mapping = legacy_var["value_mapping"]
        if isinstance(value_mapping, dict):
            converted_mapping = convert_value_mapping(value_mapping)
            if converted_mapping.get("terms"):
                new_var["valueMapping"] = converted_mapping

    return new_var


def convert_variable_to_column_mapping(var_key: str, legacy_var: dict) -> dict:
    """Convert a legacy variable to a column mapping."""
    column_mapping = {
        "mapsTo": f"schema:variable/{var_key}",
        "localColumn": legacy_var.get("local_definition", "") or "",
    }

    # Extract local value mappings
    if "value_mapping" in legacy_var:
        local_mappings = extract_local_mappings(legacy_var["value_mapping"])
        if local_mappings:
            column_mapping["localMappings"] = local_mappings

    return column_mapping


def convert_legacy_mapping(
    legacy_data: dict,
    mapping_name: Optional[str] = None,
    mapping_id: Optional[str] = None,
    schema_id: Optional[str] = None,
    database_name: Optional[str] = None,
    table_name: Optional[str] = None,
) -> ConversionResult:
    """
    Convert a legacy Flyover mapping to the new JSON-LD format.

    Args:
        legacy_data: The legacy mapping data as a dictionary
        mapping_name: Optional human-readable name for the mapping
        mapping_id: Optional URI identifier for the mapping
        schema_id: Optional URI identifier for the schema
        database_name: Optional name for the database
        table_name: Optional name for the table

    Returns:
        ConversionResult with the converted data or errors
    """
    result = ConversionResult(success=False)
    result.statistics = {
        "variables_converted": 0,
        "schema_reconstructions": 0,
        "value_mappings": 0,
        "local_mappings": 0,
    }

    # Validate input
    if not isinstance(legacy_data, dict):
        result.errors.append("Input must be a JSON object")
        return result

    if "variable_info" not in legacy_data:
        result.errors.append("Legacy format must contain 'variable_info' field")
        return result

    variable_info = legacy_data.get("variable_info", {})
    if not isinstance(variable_info, dict) or not variable_info:
        result.errors.append("'variable_info' must be a non-empty object")
        return result

    # Determine names and IDs
    db_name = database_name or extract_database_name(legacy_data)
    tbl_name = table_name or extract_table_name(legacy_data)

    if not mapping_name:
        mapping_name = f"Converted Mapping - {db_name}"

    if not mapping_id:
        mapping_id = (
            f"https://flyover.maastrichtuniversity.nl/mapping/{sanitise_key(db_name)}"
        )

    if not schema_id:
        schema_id = "schema:clinical-oncology/v1"

    # Build the new structure
    new_mapping = {
        "@context": deepcopy(DEFAULT_CONTEXT),
        "@id": mapping_id,
        "@type": "mapping:DataMapping",
        "name": mapping_name,
        "description": f"Converted from legacy format on {date.today().isoformat()}",
        "version": "1.0.0",
        "created": date.today().isoformat(),
    }

    # Copy endpoint if present
    if "endpoint" in legacy_data:
        new_mapping["endpoint"] = legacy_data["endpoint"]

    # Build schema section
    schema_variables = {}
    column_mappings = {}

    for var_key, var_data in variable_info.items():
        if not isinstance(var_data, dict):
            result.warnings.append(
                f"Skipping invalid variable '{var_key}': not an object"
            )
            continue

        # Convert to schema variable
        try:
            schema_var = convert_variable_to_schema(var_key, var_data)
            schema_variables[var_key] = schema_var
            result.statistics["variables_converted"] += 1

            if "schemaReconstruction" in schema_var:
                result.statistics["schema_reconstructions"] += len(
                    schema_var["schemaReconstruction"]
                )

            if "valueMapping" in schema_var:
                result.statistics["value_mappings"] += len(
                    schema_var["valueMapping"].get("terms", {})
                )
        except Exception as e:
            result.warnings.append(f"Error converting schema for '{var_key}': {e}")
            continue

        # Convert to column mapping
        try:
            col_mapping = convert_variable_to_column_mapping(var_key, var_data)
            column_mappings[var_key] = col_mapping

            if "localMappings" in col_mapping:
                result.statistics["local_mappings"] += len(col_mapping["localMappings"])
        except Exception as e:
            result.warnings.append(
                f"Error converting column mapping for '{var_key}': {e}"
            )

    if not schema_variables:
        result.errors.append("No variables could be converted")
        return result

    # Build schema section
    new_mapping["schema"] = {
        "@id": schema_id,
        "@type": "schema:SemanticSchema",
        "name": "Semantic Schema",
        "version": "1.0.0",
        "description": "Semantic schema extracted from legacy mapping",
        "prefixes": {
            "mesh": "http://id.nlm.nih.gov/mesh/",
            "sio": "http://semanticscience.org/resource/",
            "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
            "roo": "http://www.cancerdata.org/roo/",
        },
        "variables": schema_variables,
    }

    # Extract prefixes from legacy format if available
    if "prefixes" in legacy_data and isinstance(legacy_data["prefixes"], str):
        # Parse PREFIX declarations
        prefix_pattern = r"PREFIX\s+(\w+):\s*<([^>]+)>"
        for match in re.finditer(prefix_pattern, legacy_data["prefixes"]):
            prefix_name = match.group(1)
            prefix_uri = match.group(2)
            new_mapping["schema"]["prefixes"][prefix_name] = prefix_uri

    # Build databases section
    source_file = legacy_data.get("database_name", "")

    new_mapping["databases"] = {
        db_name: {
            "@id": f"mapping:database/{db_name}",
            "@type": "mapping:Database",
            "name": db_name.replace("_", " ").title(),
            "description": "Converted from legacy mapping",
            "tables": {
                tbl_name: {
                    "@id": f"mapping:table/{tbl_name}",
                    "@type": "mapping:Table",
                    "sourceFile": source_file if source_file else "",
                    "description": (
                        f"Table converted from {source_file}"
                        if source_file
                        else "Converted table"
                    ),
                    "columns": column_mappings,
                }
            },
        }
    }

    result.success = True
    result.output = new_mapping
    return result


# ============================================================================
# Validation (Optional)
# ============================================================================


def validate_output(
    converted_data: dict, schema_path: Optional[Path] = None
) -> tuple[bool, list]:
    """
    Validate the converted output against the JSON-LD schema.

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    try:
        from jsonschema import Draft7Validator
    except ImportError:
        return True, ["jsonschema not installed, skipping validation"]

    # Find schema file
    if schema_path and schema_path.exists():
        pass
    elif Path("mapping_schema.json").exists():
        schema_path = Path("mapping_schema.json")
    else:
        return True, ["Schema file not found, skipping validation"]

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        return True, [f"Could not load schema: {e}"]

    validator = Draft7Validator(schema)
    errors = list(validator.iter_errors(converted_data))

    if not errors:
        return True, []

    error_messages = []
    for error in errors[:10]:  # Limit to first 10 errors
        path = " → ".join(str(p) for p in error.absolute_path) or "(root)"
        error_messages.append(f"  • {path}: {error.message}")

    if len(errors) > 10:
        error_messages.append(f"  ... and {len(errors) - 10} more errors")

    return False, error_messages


# ============================================================================
# Main Entry Point
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Convert legacy Flyover mapping files to JSON-LD format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s old_mapping.json
  %(prog)s old_mapping.json -o new_mapping. jsonld
  %(prog)s old_mapping.json --database-name hospital_a --table-name patients
  %(prog)s old_mapping. json --dry-run --validate
        """,
    )

    parser.add_argument(
        "input_file", type=Path, help="Path to the legacy mapping file (JSON)"
    )

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output file path (default: <input>_converted.jsonld)",
    )

    parser.add_argument(
        "--database-name", type=str, help="Name for the database in the new format"
    )

    parser.add_argument(
        "--table-name", type=str, help="Name for the table in the new format"
    )

    parser.add_argument(
        "--mapping-name", type=str, help="Human-readable name for the mapping"
    )

    parser.add_argument("--mapping-id", type=str, help="URI identifier for the mapping")

    parser.add_argument("--schema-id", type=str, help="URI identifier for the schema")

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show conversion result without writing file",
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate output against schema after conversion",
    )

    parser.add_argument(
        "--schema-file",
        type=Path,
        help="Path to validation schema file (for --validate)",
    )

    parser.add_argument(
        "--pretty",
        action="store_true",
        default=True,
        help="Pretty-print JSON output (default: True)",
    )

    parser.add_argument(
        "--compact",
        action="store_true",
        help="Output compact JSON (overrides --pretty)",
    )

    args = parser.parse_args()

    # Print header
    print_header("Flyover Legacy Mapping Converter")

    # Check input file
    if not args.input_file.exists():
        print_error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    print_info(f"Input file: {args.input_file}")

    # Load legacy data
    try:
        with open(args.input_file, "r", encoding="utf-8") as f:
            legacy_data = json.load(f)
    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON in input file: {e}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error reading input file: {e}")
        sys.exit(1)

    print_success("Loaded legacy mapping file")

    # Perform conversion
    print_info("Converting to JSON-LD format...")

    result = convert_legacy_mapping(
        legacy_data,
        mapping_name=args.mapping_name,
        mapping_id=args.mapping_id,
        schema_id=args.schema_id,
        database_name=args.database_name,
        table_name=args.table_name,
    )

    # Print warnings
    for warning in result.warnings:
        print_warning(warning)

    # Check for errors
    if not result.success:
        print_error("Conversion failed:")
        for error in result.errors:
            print(f"  • {error}")
        sys.exit(1)

    print_success("Conversion completed successfully")

    # Print statistics
    print(f"\n{colorise('Conversion Statistics:', 'cyan', bold=True)}")
    print(f"  • Variables converted:     {result.statistics['variables_converted']}")
    print(f"  • Schema reconstructions:  {result.statistics['schema_reconstructions']}")
    print(f"  • Value mappings:          {result.statistics['value_mappings']}")
    print(f"  • Local mappings:          {result.statistics['local_mappings']}")

    # Validate if requested
    if args.validate:
        print_info("\nValidating converted output...")
        is_valid, validation_errors = validate_output(result.output, args.schema_file)

        if is_valid:
            print_success("Validation passed")
        else:
            print_warning("Validation issues found:")
            for error in validation_errors:
                print(error)

    # Prepare output
    indent = None if args.compact else 2
    output_json = json.dumps(result.output, indent=indent, ensure_ascii=False)

    # Dry run - just print
    if args.dry_run:
        print(f"\n{colorise('Converted Output (dry-run):', 'cyan', bold=True)}")
        print(colorise("─" * 70, "gray"))

        # Print first/last parts if too long
        lines = output_json.split("\n")
        if len(lines) > 50:
            print("\n".join(lines[:25]))
            print(colorise(f"\n... ({len(lines) - 50} lines omitted) ...\n", "gray"))
            print("\n".join(lines[-25:]))
        else:
            print(output_json)

        print(colorise("─" * 70, "gray"))
        print_info("Dry run complete - no file written")
        sys.exit(0)

    # Determine output path
    output_path = args.output
    if not output_path:
        output_path = args.input_file.with_suffix(". converted.jsonld")

    # Write output
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(output_json)
        print_success(f"Output written to: {output_path}")
    except Exception as e:
        print_error(f"Error writing output file: {e}")
        sys.exit(1)

    # Final summary
    print(f"\n{colorise('Conversion Summary:', 'green', bold=True)}")
    print(f"  Input:  {args.input_file}")
    print(f"  Output: {output_path}")
    print(f"  Variables: {result.statistics['variables_converted']}")

    if result.warnings:
        print(f"  Warnings: {len(result.warnings)}")

    print(f"\n{colorise('Done!', 'green', bold=True)}")


if __name__ == "__main__":
    main()
