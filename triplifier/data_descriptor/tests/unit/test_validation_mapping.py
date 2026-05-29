"""
Extended unit tests for MappingValidator — semantic and cross-field validation.

Tests cover:
- _format_path and _get_value_preview helper functions
- Cross-field validation: localMappings keys vs schema terms, mapsTo existence
- Semantic validation: empty predicate/class URIs
- Boundary cases: large mappings, Unicode in variable names, special characters,
  XSS-like and SQL-injection-like string payloads
- Statistics collection for complex mappings
- format_errors_for_ui output structure (new cases not in test_validation.py)
- Graceful degradation when the schema file is absent

These tests complement test_validation.py and do not duplicate its cases.
"""

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add data_descriptor to path so local imports resolve
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from validation import MappingValidator, ValidationIssue, ValidationResult
from validation.mapping_validator import _format_path, _get_value_preview

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_valid_mapping(**overrides) -> dict:
    """Return a minimal valid mapping, optionally overriding top-level keys."""
    base = {
        "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
        "@type": "mapping:DataMapping",
        "name": "Validation Test Mapping",
        "schema": {
            "@type": "schema:SemanticSchema",
            "variables": {
                "identifier": {
                    "@type": "schema:IdentifierVariable",
                    "dataType": "identifier",
                    "predicate": "sio:SIO_000673",
                    "class": "ncit:C25364",
                },
                "biological_sex": {
                    "@type": "schema:CategoricalVariable",
                    "dataType": "categorical",
                    "predicate": "sio:SIO_000008",
                    "class": "ncit:C28421",
                    "valueMapping": {
                        "terms": {
                            "male": {"targetClass": "ncit:C20197"},
                            "female": {"targetClass": "ncit:C16576"},
                        }
                    },
                },
            },
        },
        "databases": {
            "db1": {
                "@type": "mapping:Database",
                "name": "Test Database",
                "tables": {
                    "t1": {
                        "@type": "mapping:Table",
                        "columns": {
                            "id_col": {
                                "mapsTo": "schema:variable/identifier",
                                "localColumn": "id",
                            },
                            "sex_col": {
                                "mapsTo": "schema:variable/biological_sex",
                                "localColumn": "sex",
                                "localMappings": {"male": "M", "female": "F"},
                            },
                        },
                    }
                },
            }
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Helper function unit tests — _format_path
# ---------------------------------------------------------------------------


class TestFormatPath(unittest.TestCase):
    """Unit tests for the _format_path helper."""

    def test_empty_path_returns_root(self):
        """An empty path list must return '(root)'."""
        self.assertEqual(_format_path([]), "(root)")

    def test_single_key_returns_key(self):
        """A single string key must be returned as-is."""
        self.assertEqual(_format_path(["schema"]), "schema")

    def test_nested_keys_are_dot_separated(self):
        """Nested string keys must be joined with dots."""
        self.assertEqual(_format_path(["schema", "variables"]), "schema.variables")

    def test_integer_index_uses_bracket_notation(self):
        """Integer path segments must be formatted as bracket notation."""
        self.assertEqual(_format_path(["items", 0]), "items[0]")

    def test_array_index_followed_by_key_uses_dot_separator(self):
        """A dot must be inserted between a bracket index and the following key."""
        result = _format_path(["schemaReconstruction", 2, "class"])
        self.assertEqual(result, "schemaReconstruction[2].class")


# ---------------------------------------------------------------------------
# Helper function unit tests — _get_value_preview
# ---------------------------------------------------------------------------


class TestGetValuePreview(unittest.TestCase):
    """Unit tests for the _get_value_preview helper."""

    def test_none_returns_null_string(self):
        """None must return the string 'null'."""
        self.assertEqual(_get_value_preview(None), "null")

    def test_bool_true_returns_true_string(self):
        """True must return the string 'true'."""
        self.assertEqual(_get_value_preview(True), "true")

    def test_bool_false_returns_false_string(self):
        """False must return the string 'false'."""
        self.assertEqual(_get_value_preview(False), "false")

    def test_integer_returns_string_representation(self):
        """An integer must be converted to its string representation."""
        self.assertEqual(_get_value_preview(42), "42")

    def test_short_string_is_quoted(self):
        """A short string must be returned wrapped in double quotes."""
        self.assertEqual(_get_value_preview("hello"), '"hello"')

    def test_long_string_is_truncated_with_ellipsis(self):
        """A string longer than max_length must be truncated with '...'."""
        long_str = "a" * 100
        preview = _get_value_preview(long_str)
        self.assertIn("...", preview)
        self.assertLessEqual(len(preview), 60)

    def test_list_describes_item_count(self):
        """A list must be described by its item count."""
        preview = _get_value_preview([1, 2, 3])
        self.assertIn("3", preview)
        self.assertIn("array", preview)

    def test_dict_describes_key_count(self):
        """A dict must be described by its key count."""
        preview = _get_value_preview({"a": 1, "b": 2})
        self.assertIn("2", preview)
        self.assertIn("object", preview)


# ---------------------------------------------------------------------------
# Cross-reference validation
# ---------------------------------------------------------------------------


class TestCrossReferenceValidation(unittest.TestCase):
    """Tests for the cross-reference checking of mapsTo values."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_valid_references_produce_no_warnings(self):
        """Columns referencing existing variables must produce no warnings."""
        result = self.validator.validate(_make_valid_mapping())
        self.assertEqual(len(result.warnings), 0)

    def test_undefined_variable_reference_produces_warning(self):
        """A mapsTo referencing a non-existent variable must produce a warning
        that mentions the undefined variable name."""
        mapping = _make_valid_mapping()
        mapping["databases"]["db1"]["tables"]["t1"]["columns"]["id_col"][
            "mapsTo"
        ] = "schema:variable/does_not_exist"
        result = self.validator.validate(mapping, check_references=True)
        self.assertGreaterEqual(len(result.warnings), 1)
        self.assertTrue(
            any("does_not_exist" in w.message for w in result.warnings),
            [w.message for w in result.warnings],
        )

    def test_check_references_false_skips_cross_reference_check(self):
        """When check_references=False, undefined references must not produce warnings."""
        mapping = _make_valid_mapping()
        mapping["databases"]["db1"]["tables"]["t1"]["columns"]["id_col"][
            "mapsTo"
        ] = "schema:variable/undefined"
        result = self.validator.validate(mapping, check_references=False)
        self.assertEqual(len(result.warnings), 0)

    def test_local_mapping_key_not_in_schema_terms_produces_info_issue(self):
        """A localMappings key not in the schema's valueMapping terms must produce
        an info-level issue in result.issues."""
        mapping = _make_valid_mapping()
        mapping["databases"]["db1"]["tables"]["t1"]["columns"]["sex_col"][
            "localMappings"
        ]["unknown_term"] = "?"
        result = self.validator.validate(mapping, check_references=True)
        info_issues = [i for i in result.issues if i.severity == "info"]
        self.assertGreaterEqual(
            len(info_issues),
            1,
            "Expected at least one info-level issue for unknown localMappings key",
        )

    def test_multiple_undefined_references_each_produce_a_warning(self):
        """Multiple columns with undefined variable references must each warn."""
        mapping = _make_valid_mapping()
        for col in ("id_col", "sex_col"):
            mapping["databases"]["db1"]["tables"]["t1"]["columns"][col][
                "mapsTo"
            ] = f"schema:variable/missing_{col}"
        result = self.validator.validate(mapping, check_references=True)
        self.assertGreaterEqual(len(result.warnings), 2)


# ---------------------------------------------------------------------------
# Semantic validation: predicate and class URI strings
# ---------------------------------------------------------------------------


class TestOntologyURIHandling(unittest.TestCase):
    """The validator must accept any non-empty string for predicate and class."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_ncit_uri_accepted(self):
        """An ncit-prefixed URI must pass schema validation."""
        mapping = _make_valid_mapping()
        mapping["schema"]["variables"]["identifier"]["class"] = "ncit:C25364"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_mesh_uri_accepted(self):
        """A mesh-prefixed URI must pass schema validation."""
        mapping = _make_valid_mapping()
        mapping["schema"]["variables"]["identifier"]["class"] = "mesh:D000091569"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_minimal_prefixed_predicate_accepted(self):
        """A minimal non-empty predicate string must pass schema validation."""
        mapping = _make_valid_mapping()
        mapping["schema"]["variables"]["identifier"]["predicate"] = "x:y"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_empty_predicate_is_rejected(self):
        """An empty string predicate (minLength 1) must be rejected."""
        mapping = _make_valid_mapping()
        mapping["schema"]["variables"]["identifier"]["predicate"] = ""
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)

    def test_empty_class_uri_is_rejected(self):
        """An empty class URI (minLength 1) must be rejected."""
        mapping = _make_valid_mapping()
        mapping["schema"]["variables"]["identifier"]["class"] = ""
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)


# ---------------------------------------------------------------------------
# Boundary: large mappings
# ---------------------------------------------------------------------------


class TestLargeMapping(unittest.TestCase):
    """The validator must handle large mappings without errors."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_mapping_with_fifty_continuous_variables_passes(self):
        """A mapping with 50 continuous variables must pass schema validation."""
        mapping = _make_valid_mapping()
        for i in range(50):
            var_key = f"variable_{i:03d}"
            mapping["schema"]["variables"][var_key] = {
                "@type": "schema:ContinuousVariable",
                "dataType": "continuous",
                "predicate": f"sio:SIO_{i:06d}",
                "class": f"ncit:C{i:05d}",
            }
            mapping["databases"]["db1"]["tables"]["t1"]["columns"][f"col_{i:03d}"] = {
                "mapsTo": f"schema:variable/{var_key}",
                "localColumn": f"local_col_{i}",
            }
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])
        # 2 baseline variables + 50 added = 52
        self.assertEqual(result.statistics["variables"], 52)

    def test_mapping_with_ten_databases_passes(self):
        """A mapping with 10 databases must pass schema validation."""
        mapping = _make_valid_mapping()
        for i in range(10):
            db_key = f"database_{i:02d}"
            mapping["databases"][db_key] = {
                "@type": "mapping:Database",
                "name": f"Database {i}",
                "tables": {
                    f"table_{i:02d}": {
                        "@type": "mapping:Table",
                        "columns": {
                            "id_col": {
                                "mapsTo": "schema:variable/identifier",
                                "localColumn": "id",
                            }
                        },
                    }
                },
            }
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])
        # 1 baseline database + 10 added = 11
        self.assertEqual(result.statistics["databases"], 11)


# ---------------------------------------------------------------------------
# Boundary: Unicode and special characters in free-text fields
# ---------------------------------------------------------------------------


class TestUnicodeAndSpecialCharacters(unittest.TestCase):
    """The validator must accept Unicode and special characters in string fields."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_unicode_in_name_field_is_accepted(self):
        """Unicode characters in the name field must pass schema validation."""
        mapping = _make_valid_mapping()
        mapping["name"] = "Cartographie Sémantique — Données Cliniques"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_unicode_variable_keys_are_accepted(self):
        """Unicode variable keys must pass schema validation."""
        mapping = _make_valid_mapping()
        mapping["schema"]["variables"]["identifiant_患者"] = {
            "@type": "schema:IdentifierVariable",
            "dataType": "identifier",
            "predicate": "sio:SIO_000673",
            "class": "ncit:C25364",
        }
        mapping["databases"]["db1"]["tables"]["t1"]["columns"]["unicode_col"] = {
            "mapsTo": "schema:variable/identifiant_患者",
            "localColumn": "id",
        }
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_xss_like_payload_in_description_is_accepted_by_schema(self):
        """XSS-like payload strings must pass schema validation.

        The schema validates structure, not string content; sanitisation is
        handled separately by the application layer.
        """
        mapping = _make_valid_mapping()
        mapping["description"] = "<script>alert('xss')</script>"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])

    def test_sql_injection_like_payload_in_name_is_accepted_by_schema(self):
        """SQL injection-like payloads in string fields must pass schema validation.

        Input sanitisation is handled by the application layer, not the schema.
        """
        mapping = _make_valid_mapping()
        mapping["name"] = "'; DROP TABLE patients; --"
        result = self.validator.validate(mapping)
        self.assertTrue(result.is_valid, [i.message for i in result.issues])


# ---------------------------------------------------------------------------
# Statistics collection
# ---------------------------------------------------------------------------


class TestStatisticsCollection(unittest.TestCase):
    """Validate that the statistics in ValidationResult are accurate."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_statistics_counts_two_databases(self):
        """statistics['databases'] must count all database entries."""
        mapping = _make_valid_mapping()
        mapping["databases"]["db2"] = copy.deepcopy(mapping["databases"]["db1"])
        result = self.validator.validate(mapping)
        self.assertEqual(result.statistics["databases"], 2)

    def test_statistics_counts_two_tables(self):
        """statistics['tables'] must total tables across all databases."""
        mapping = _make_valid_mapping()
        mapping["databases"]["db1"]["tables"]["t2"] = copy.deepcopy(
            mapping["databases"]["db1"]["tables"]["t1"]
        )
        result = self.validator.validate(mapping)
        self.assertEqual(result.statistics["tables"], 2)

    def test_statistics_counts_two_columns(self):
        """statistics['columns'] must total columns across all tables."""
        result = self.validator.validate(_make_valid_mapping())
        self.assertEqual(result.statistics["columns"], 2)

    def test_statistics_counts_two_variables(self):
        """statistics['variables'] must count schema variable definitions."""
        result = self.validator.validate(_make_valid_mapping())
        self.assertEqual(result.statistics["variables"], 2)

    def test_invalid_mapping_statistics_are_still_populated(self):
        """statistics must be populated even when validation fails."""
        mapping = {k: v for k, v in _make_valid_mapping().items() if k != "@context"}
        result = self.validator.validate(mapping)
        self.assertFalse(result.is_valid)
        self.assertIn("databases", result.statistics)


# ---------------------------------------------------------------------------
# format_errors_for_ui — new cases only (basic structure in test_validation.py)
# ---------------------------------------------------------------------------


class TestFormatErrorsForUI(unittest.TestCase):
    """format_errors_for_ui edge cases not covered in test_validation.py."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_valid_mapping_produces_empty_list(self):
        """A valid mapping must produce an empty list from format_errors_for_ui."""
        result = self.validator.validate(_make_valid_mapping())
        formatted = self.validator.format_errors_for_ui(result)
        self.assertEqual(formatted, [])

    def test_each_item_has_required_keys(self):
        """Every item from format_errors_for_ui must have the required keys."""
        mapping = {k: v for k, v in _make_valid_mapping().items() if k != "@context"}
        result = self.validator.validate(mapping)
        formatted = self.validator.format_errors_for_ui(result)
        required_keys = {"path", "severity", "message", "suggestion", "value"}
        for item in formatted:
            self.assertTrue(required_keys.issubset(item.keys()), item)

    def test_suggestion_field_is_always_a_string(self):
        """The suggestion field in each item must be a string, not None."""
        mapping = {k: v for k, v in _make_valid_mapping().items() if k != "@context"}
        result = self.validator.validate(mapping)
        formatted = self.validator.format_errors_for_ui(result)
        for item in formatted:
            self.assertIsInstance(item["suggestion"], str)

    def test_format_errors_does_not_raise_on_empty_result(self):
        """format_errors_for_ui must not raise when the result has no issues."""
        result = ValidationResult(is_valid=True)
        try:
            self.validator.format_errors_for_ui(result)
        except Exception as exc:  # noqa: BLE001
            self.fail(f"format_errors_for_ui raised unexpectedly: {exc}")


# ---------------------------------------------------------------------------
# Schema not loaded / absent schema file
# ---------------------------------------------------------------------------


class TestValidatorWithMissingSchema(unittest.TestCase):
    """The validator must degrade gracefully when the schema file is absent."""

    def test_missing_schema_file_does_not_raise(self):
        """Validating with an absent schema file must not raise an exception."""
        missing = Path(tempfile.gettempdir()) / "nonexistent_schema_abc123.json"
        validator = MappingValidator(schema_path=str(missing))
        try:
            validator.validate(_make_valid_mapping())
        except Exception as exc:  # noqa: BLE001
            self.fail(f"validate() raised with missing schema: {exc}")

    def test_missing_schema_file_produces_warning_or_passes(self):
        """A validator pointing to a non-existent schema must not crash."""
        missing = Path(tempfile.gettempdir()) / "nonexistent_schema_abc123.json"
        validator = MappingValidator(schema_path=str(missing))
        result = validator.validate(_make_valid_mapping())
        # The validator may return valid (skip schema check) or add a warning
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# validate_file edge cases
# ---------------------------------------------------------------------------


class TestValidateFileEdgeCases(unittest.TestCase):
    """Edge-case tests for the validate_file method."""

    def setUp(self):
        """Set up a shared validator instance."""
        self.validator = MappingValidator()

    def test_path_object_is_accepted_by_validate_file(self):
        """validate_file must accept a pathlib.Path object."""
        valid = _make_valid_mapping()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False, encoding="utf-8"
        ) as fh:
            json.dump(valid, fh)
            tmp = fh.name
        try:
            result = self.validator.validate_file(Path(tmp))
            self.assertTrue(result.is_valid, [i.message for i in result.issues])
        finally:
            os.unlink(tmp)

    def test_string_path_is_accepted_by_validate_file(self):
        """validate_file must accept a plain string path."""
        valid = _make_valid_mapping()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False, encoding="utf-8"
        ) as fh:
            json.dump(valid, fh)
            tmp = fh.name
        try:
            result = self.validator.validate_file(tmp)
            self.assertTrue(result.is_valid, [i.message for i in result.issues])
        finally:
            os.unlink(tmp)


if __name__ == "__main__":
    unittest.main()
