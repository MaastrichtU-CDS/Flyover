"""
Unit tests for the MappingValidator class.

Tests cover:
- Valid JSON-LD file validation
- Missing required fields (@context, schema, databases)
- Pattern validation failures
- Cross-reference validation (mapsTo references)
- JSON syntax error handling
- Error formatting for UI
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from validation import MappingValidator, ValidationIssue, ValidationResult


class TestValidationIssue(unittest.TestCase):
    """Test the ValidationIssue dataclass."""

    def test_create_error_issue(self):
        """Test creating an error-level issue."""
        issue = ValidationIssue(
            severity="error",
            path="schema.variables.test",
            message="Missing required field",
            value=None,
            suggestion="Add the required field"
        )
        self.assertEqual(issue.severity, "error")
        self.assertEqual(issue.path, "schema.variables.test")
        self.assertEqual(issue.message, "Missing required field")

    def test_create_warning_issue(self):
        """Test creating a warning-level issue."""
        issue = ValidationIssue(
            severity="warning",
            path="databases.db1.tables.t1.mapsTo",
            message="References undefined variable",
            value="schema:variable/undefined"
        )
        self.assertEqual(issue.severity, "warning")
        self.assertIn("undefined", issue.message)


class TestValidationResult(unittest.TestCase):
    """Test the ValidationResult dataclass."""

    def test_valid_result(self):
        """Test a valid result."""
        result = ValidationResult(is_valid=True)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.issues), 0)
        self.assertEqual(len(result.warnings), 0)

    def test_invalid_result_with_issues(self):
        """Test an invalid result with issues."""
        result = ValidationResult(
            is_valid=False,
            issues=[
                ValidationIssue(
                    severity="error",
                    path="@context",
                    message="Missing @context"
                )
            ]
        )
        self.assertFalse(result.is_valid)
        self.assertEqual(len(result.issues), 1)


class TestMappingValidator(unittest.TestCase):
    """Test the MappingValidator class."""

    def setUp(self):
        """Set up test fixtures."""
        self.validator = MappingValidator()

        # Minimal valid mapping
        self.valid_mapping = {
            "@context": {
                "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"
            },
            "@type": "mapping:DataMapping",
            "name": "Test Mapping",
            "schema": {
                "@type": "schema:SemanticSchema",
                "variables": {
                    "identifier": {
                        "@type": "schema:IdentifierVariable",
                        "dataType": "identifier",
                        "predicate": "sio:SIO_000673",
                        "class": "ncit:C25364"
                    }
                }
            },
            "databases": {
                "test_db": {
                    "@type": "mapping:Database",
                    "name": "Test Database",
                    "tables": {
                        "test_table": {
                            "@type": "mapping:Table",
                            "columns": {
                                "identifier": {
                                    "mapsTo": "schema:variable/identifier",
                                    "localColumn": "id"
                                }
                            }
                        }
                    }
                }
            }
        }

    def test_validate_valid_mapping(self):
        """Test validation of a valid mapping."""
        result = self.validator.validate(self.valid_mapping)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.issues), 0)

    def test_validate_missing_context(self):
        """Test validation fails when @context is missing."""
        invalid_mapping = {k: v for k, v in self.valid_mapping.items() if k != "@context"}
        result = self.validator.validate(invalid_mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("@context" in issue.message or "@context" in issue.path
                           for issue in result.issues))

    def test_validate_missing_schema(self):
        """Test validation fails when schema is missing."""
        invalid_mapping = {k: v for k, v in self.valid_mapping.items() if k != "schema"}
        result = self.validator.validate(invalid_mapping)
        self.assertFalse(result.is_valid)

    def test_validate_missing_databases(self):
        """Test validation fails when databases is missing."""
        invalid_mapping = {k: v for k, v in self.valid_mapping.items() if k != "databases"}
        result = self.validator.validate(invalid_mapping)
        self.assertFalse(result.is_valid)

    def test_validate_invalid_type_pattern(self):
        """Test validation fails for invalid @type pattern."""
        invalid_mapping = dict(self.valid_mapping)
        invalid_mapping["@type"] = "InvalidType"
        result = self.validator.validate(invalid_mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("@type" in issue.path for issue in result.issues))

    def test_validate_invalid_version_format(self):
        """Test validation fails for invalid version format."""
        invalid_mapping = dict(self.valid_mapping)
        invalid_mapping["version"] = "1.0"  # Should be 1.0.0
        result = self.validator.validate(invalid_mapping)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("version" in issue.path for issue in result.issues))

    def test_cross_reference_warning(self):
        """Test warning for mapsTo referencing undefined variable."""
        mapping = dict(self.valid_mapping)
        mapping["databases"]["test_db"]["tables"]["test_table"]["columns"]["bad_ref"] = {
            "mapsTo": "schema:variable/undefined_var",
            "localColumn": "bad_col"
        }
        result = self.validator.validate(mapping, check_references=True)
        # Should still be valid but have warnings
        self.assertTrue(len(result.warnings) > 0 or not result.is_valid)

    def test_validate_file_not_found(self):
        """Test validation returns error for non-existent file."""
        result = self.validator.validate_file("/non/existent/file.jsonld")
        self.assertFalse(result.is_valid)
        self.assertTrue(any("not found" in issue.message.lower()
                           for issue in result.issues))

    def test_validate_file_json_syntax_error(self):
        """Test validation handles JSON syntax errors."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonld', delete=False) as f:
            f.write('{"invalid": json,}')
            f.flush()
            temp_path = f.name

        try:
            result = self.validator.validate_file(temp_path)
            self.assertFalse(result.is_valid)
            self.assertTrue(any("syntax" in issue.message.lower() or "json" in issue.message.lower()
                               for issue in result.issues))
        finally:
            os.unlink(temp_path)

    def test_format_errors_for_ui(self):
        """Test formatting errors for UI display."""
        result = ValidationResult(
            is_valid=False,
            issues=[
                ValidationIssue(
                    severity="error",
                    path="@context",
                    message="Missing @context",
                    suggestion="Add a @context object"
                )
            ],
            warnings=[
                ValidationIssue(
                    severity="warning",
                    path="databases.db.tables.t.columns.c.mapsTo",
                    message="References undefined variable"
                )
            ]
        )

        formatted = self.validator.format_errors_for_ui(result)
        self.assertEqual(len(formatted), 2)
        self.assertEqual(formatted[0]["severity"], "error")
        self.assertEqual(formatted[0]["path"], "@context")
        self.assertEqual(formatted[1]["severity"], "warning")

    def test_statistics_collection(self):
        """Test that statistics are collected correctly."""
        result = self.validator.validate(self.valid_mapping)
        self.assertEqual(result.statistics["variables"], 1)
        self.assertEqual(result.statistics["databases"], 1)
        self.assertEqual(result.statistics["tables"], 1)
        self.assertEqual(result.statistics["columns"], 1)


class TestMappingValidatorWithRealFiles(unittest.TestCase):
    """Test MappingValidator with actual example files."""

    def setUp(self):
        """Set up validator and file paths."""
        self.validator = MappingValidator()
        self.example_dir = Path(__file__).parent.parent.parent.parent / "example_data"

    def test_validate_centre_a_mapping(self):
        """Test validation of Centre A English mapping."""
        file_path = self.example_dir / "centre_a_english" / "mapping_centre_a.jsonld"
        if file_path.exists():
            result = self.validator.validate_file(file_path)
            self.assertTrue(result.is_valid, f"Errors: {[i.message for i in result.issues]}")
            self.assertEqual(result.statistics["variables"], 11)

    def test_validate_centre_b_mapping(self):
        """Test validation of Centre B Dutch mapping."""
        file_path = self.example_dir / "centre_b_dutch" / "mapping_centre_b.jsonld"
        if file_path.exists():
            result = self.validator.validate_file(file_path)
            self.assertTrue(result.is_valid, f"Errors: {[i.message for i in result.issues]}")

    def test_validate_template_mapping(self):
        """Test validation of template mapping."""
        file_path = self.example_dir / "mapping_template.jsonld"
        if file_path.exists():
            result = self.validator.validate_file(file_path)
            self.assertTrue(result.is_valid, f"Errors: {[i.message for i in result.issues]}")


if __name__ == "__main__":
    unittest.main()
