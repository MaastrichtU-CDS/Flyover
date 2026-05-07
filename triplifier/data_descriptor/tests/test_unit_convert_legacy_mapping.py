"""
Unit tests for scripts/convert_legacy_mapping.py.

Tests cover all conversion helper functions and the main convert_legacy_mapping
function with a variety of legacy JSON inputs including edge cases.
"""

import sys
import unittest
from pathlib import Path

# Make the scripts directory importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "scripts"))

from convert_legacy_mapping import (
    ConversionResult,
    convert_legacy_mapping,
    convert_schema_reconstruction,
    convert_value_mapping,
    extract_database_name,
    extract_local_mappings,
    extract_table_name,
    sanitise_key,
)

# ---------------------------------------------------------------------------
# Tests for sanitise_key
# ---------------------------------------------------------------------------


class TestSanitiseKey(unittest.TestCase):
    """Tests for sanitise_key."""

    def test_plain_string_unchanged(self):
        """Alphanumeric string returned as lowercase."""
        self.assertEqual(sanitise_key("patients"), "patients")

    def test_spaces_replaced_with_underscore(self):
        """Spaces are replaced with underscores."""
        self.assertEqual(sanitise_key("Centre A Data"), "centre_a_data")

    def test_special_chars_replaced(self):
        """Special characters are replaced with underscores."""
        self.assertEqual(sanitise_key("table-name!"), "table_name")

    def test_consecutive_underscores_collapsed(self):
        """Consecutive underscores are collapsed to a single one."""
        self.assertEqual(sanitise_key("a__b"), "a_b")

    def test_leading_trailing_underscores_stripped(self):
        """Leading and trailing underscores are stripped."""
        self.assertEqual(sanitise_key("_table_"), "table")

    def test_empty_string_returns_unnamed(self):
        """Empty string returns 'unnamed'."""
        self.assertEqual(sanitise_key(""), "unnamed")


# ---------------------------------------------------------------------------
# Tests for extract_database_name / extract_table_name
# ---------------------------------------------------------------------------


class TestExtractDatabaseName(unittest.TestCase):
    """Tests for extract_database_name."""

    def test_extracts_stem_from_filename(self):
        """Strips .csv extension and sanitises the name."""
        data = {"database_name": "patients.csv"}
        self.assertEqual(extract_database_name(data), "patients")

    def test_sanitises_name(self):
        """Sanitises the database name."""
        data = {"database_name": "My Data.csv"}
        self.assertEqual(extract_database_name(data), "my_data")

    def test_returns_default_when_missing(self):
        """Returns 'default_database' when database_name key is absent."""
        self.assertEqual(extract_database_name({}), "default_database")

    def test_returns_default_when_none(self):
        """Returns 'default_database' when database_name is None."""
        self.assertEqual(
            extract_database_name({"database_name": None}), "default_database"
        )


class TestExtractTableName(unittest.TestCase):
    """Tests for extract_table_name."""

    def test_extracts_stem(self):
        """Returns the stem of the database_name as table name."""
        data = {"database_name": "patients.csv"}
        self.assertEqual(extract_table_name(data), "patients")

    def test_default_when_missing(self):
        """Returns 'default_table' when database_name is missing."""
        self.assertEqual(extract_table_name({}), "default_table")


# ---------------------------------------------------------------------------
# Tests for convert_schema_reconstruction
# ---------------------------------------------------------------------------


class TestConvertSchemaReconstruction(unittest.TestCase):
    """Tests for the scripts version of convert_schema_reconstruction."""

    def test_class_type_conversion(self):
        """'class' type items are converted to schema:ClassNode."""
        items = [
            {
                "type": "class",
                "predicate": "roo:P100018",
                "class": "mesh:D000091569",
                "class_label": "SocioLabel",
                "aesthetic_label": "Socio",
                "placement": "before",
            }
        ]
        result = convert_schema_reconstruction(items)
        self.assertEqual(result[0]["@type"], "schema:ClassNode")
        self.assertEqual(result[0]["placement"], "before")
        self.assertEqual(result[0]["classLabel"], "SocioLabel")

    def test_node_type_conversion(self):
        """'node' type items are converted to schema:UnitNode."""
        items = [{"type": "node", "predicate": "p", "class": "c", "node_label": "nl"}]
        result = convert_schema_reconstruction(items)
        self.assertEqual(result[0]["@type"], "schema:UnitNode")
        self.assertEqual(result[0]["nodeLabel"], "nl")

    def test_unknown_type_defaults_to_class_node(self):
        """Unknown type defaults to schema:ClassNode."""
        items = [{"type": "unknown", "predicate": "p", "class": "c"}]
        result = convert_schema_reconstruction(items)
        self.assertEqual(result[0]["@type"], "schema:ClassNode")

    def test_non_dict_items_skipped(self):
        """Non-dict items in the list are silently skipped."""
        result = convert_schema_reconstruction(["not a dict", None, 42])
        self.assertEqual(result, [])

    def test_empty_list_returns_empty(self):
        """Empty list returns empty list."""
        self.assertEqual(convert_schema_reconstruction([]), [])


# ---------------------------------------------------------------------------
# Tests for convert_value_mapping
# ---------------------------------------------------------------------------


class TestConvertValueMappingScript(unittest.TestCase):
    """Tests for the scripts version of convert_value_mapping."""

    def test_converts_target_class_to_camel_case(self):
        """Legacy target_class is moved to targetClass."""
        legacy = {
            "terms": {
                "male": {"target_class": "ncit:C20197"},
                "female": {"target_class": "ncit:C16576"},
            }
        }
        result = convert_value_mapping(legacy)
        self.assertIn("male", result["terms"])
        self.assertEqual(result["terms"]["male"]["targetClass"], "ncit:C20197")

    def test_terms_without_target_class_excluded(self):
        """Terms with no target_class are excluded from the result."""
        legacy = {
            "terms": {
                "known": {"target_class": "ncit:C1"},
                "unknown": {},
            }
        }
        result = convert_value_mapping(legacy)
        self.assertIn("known", result["terms"])
        self.assertNotIn("unknown", result["terms"])

    def test_none_input_returns_empty_terms(self):
        """None input returns dict with empty terms."""
        result = convert_value_mapping(None)
        self.assertEqual(result, {"terms": {}})

    def test_missing_terms_key_returns_empty(self):
        """Dict without 'terms' key returns empty terms."""
        result = convert_value_mapping({})
        self.assertEqual(result, {"terms": {}})


# ---------------------------------------------------------------------------
# Tests for extract_local_mappings
# ---------------------------------------------------------------------------


class TestExtractLocalMappings(unittest.TestCase):
    """Tests for extract_local_mappings."""

    def test_extracts_local_terms(self):
        """Local terms are extracted from value_mapping."""
        legacy = {
            "terms": {
                "male": {"target_class": "ncit:C20197", "local_term": "M"},
                "female": {"target_class": "ncit:C16576", "local_term": "F"},
            }
        }
        result = extract_local_mappings(legacy)
        self.assertEqual(result["male"], "M")
        self.assertEqual(result["female"], "F")

    def test_none_local_term_included_as_empty_string(self):
        """Terms without local_term get an empty string placeholder."""
        legacy = {"terms": {"male": {"target_class": "ncit:C20197"}}}
        result = extract_local_mappings(legacy)
        self.assertEqual(result["male"], "")

    def test_none_input_returns_empty(self):
        """None input returns empty dict."""
        self.assertEqual(extract_local_mappings(None), {})

    def test_empty_terms_returns_empty(self):
        """Empty terms returns empty dict."""
        self.assertEqual(extract_local_mappings({"terms": {}}), {})


# ---------------------------------------------------------------------------
# Tests for convert_legacy_mapping (main conversion function)
# ---------------------------------------------------------------------------


class TestConvertLegacyMapping(unittest.TestCase):
    """Tests for convert_legacy_mapping."""

    def _base_legacy(self):
        """Return a minimal valid legacy mapping dict."""
        return {
            "endpoint": "http://localhost:7200/repositories/test/statements",
            "database_name": "patients.csv",
            "prefixes": "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
            "variable_info": {
                "biological_sex": {
                    "predicate": "roo:P100018",
                    "class": "ncit:C28421",
                    "local_definition": "sex",
                    "data_type": "categorical",
                    "value_mapping": {
                        "terms": {
                            "male": {"target_class": "ncit:C20197", "local_term": "M"},
                            "female": {
                                "target_class": "ncit:C16576",
                                "local_term": "F",
                            },
                        }
                    },
                },
                "age": {
                    "predicate": "roo:P100027",
                    "class": "ncit:C25150",
                    "local_definition": "age",
                    "data_type": "continuous",
                },
            },
        }

    def test_valid_input_returns_success(self):
        """Valid legacy mapping converts successfully."""
        result = convert_legacy_mapping(self._base_legacy())
        self.assertTrue(result.success)
        self.assertIsNotNone(result.output)

    def test_output_has_required_keys(self):
        """Converted output contains all required top-level keys."""
        result = convert_legacy_mapping(self._base_legacy())
        output = result.output
        self.assertIn("@context", output)
        self.assertIn("@id", output)
        self.assertIn("schema", output)
        self.assertIn("databases", output)

    def test_schema_contains_variables(self):
        """Schema section contains the converted variables."""
        result = convert_legacy_mapping(self._base_legacy())
        variables = result.output["schema"]["variables"]
        self.assertIn("biological_sex", variables)
        self.assertIn("age", variables)

    def test_databases_section_contains_columns(self):
        """Databases section contains tables with column mappings."""
        result = convert_legacy_mapping(self._base_legacy())
        db_name = list(result.output["databases"].keys())[0]
        tables = result.output["databases"][db_name]["tables"]
        tbl_name = list(tables.keys())[0]
        columns = tables[tbl_name]["columns"]
        self.assertIn("biological_sex", columns)
        self.assertIn("age", columns)

    def test_statistics_are_populated(self):
        """Conversion statistics track variables_converted and value_mappings."""
        result = convert_legacy_mapping(self._base_legacy())
        self.assertEqual(result.statistics["variables_converted"], 2)
        self.assertGreater(result.statistics["value_mappings"], 0)

    def test_endpoint_is_copied(self):
        """Endpoint is preserved in the output."""
        result = convert_legacy_mapping(self._base_legacy())
        self.assertEqual(
            result.output["endpoint"],
            "http://localhost:7200/repositories/test/statements",
        )

    def test_custom_mapping_name(self):
        """Custom mapping_name is used in the output."""
        result = convert_legacy_mapping(
            self._base_legacy(), mapping_name="My Custom Mapping"
        )
        self.assertEqual(result.output["name"], "My Custom Mapping")

    def test_custom_database_name(self):
        """Custom database_name overrides extracted value."""
        result = convert_legacy_mapping(self._base_legacy(), database_name="custom_db")
        self.assertIn("custom_db", result.output["databases"])

    def test_non_dict_input_fails(self):
        """Non-dict input returns failure result."""
        result = convert_legacy_mapping("not a dict")
        self.assertFalse(result.success)
        self.assertTrue(len(result.errors) > 0)

    def test_missing_variable_info_fails(self):
        """Missing variable_info key returns failure result."""
        result = convert_legacy_mapping({"endpoint": "http://ep"})
        self.assertFalse(result.success)
        self.assertTrue(len(result.errors) > 0)

    def test_empty_variable_info_fails(self):
        """Empty variable_info returns failure result."""
        result = convert_legacy_mapping({"variable_info": {}})
        self.assertFalse(result.success)

    def test_invalid_variable_entries_generate_warnings(self):
        """Invalid (non-dict) variable entries are skipped with warnings."""
        legacy = self._base_legacy()
        legacy["variable_info"]["bad_var"] = "not a dict"
        result = convert_legacy_mapping(legacy)
        self.assertTrue(result.success)
        self.assertTrue(any("bad_var" in w for w in result.warnings))

    def test_legacy_prefixes_are_parsed(self):
        """PREFIX declarations in legacy data are included in schema prefixes."""
        result = convert_legacy_mapping(self._base_legacy())
        schema_prefixes = result.output["schema"]["prefixes"]
        self.assertIn("ncit", schema_prefixes)

    def test_variable_with_schema_reconstruction(self):
        """Variables with schema_reconstruction are converted."""
        legacy = self._base_legacy()
        legacy["variable_info"]["bio_sex_reconstructed"] = {
            "predicate": "roo:P100018",
            "class": "ncit:C28421",
            "local_definition": "sex",
            "data_type": "categorical",
            "schema_reconstruction": [
                {
                    "type": "class",
                    "predicate": "roo:P100018",
                    "class": "mesh:D000091569",
                    "class_label": "Social",
                    "aesthetic_label": "Social",
                    "placement": "before",
                }
            ],
        }
        result = convert_legacy_mapping(legacy)
        self.assertTrue(result.success)
        var = result.output["schema"]["variables"]["bio_sex_reconstructed"]
        self.assertIn("schemaReconstruction", var)

    def test_all_variables_are_invalid_fails(self):
        """When all variables are invalid, conversion fails with no output."""
        legacy = {
            "variable_info": {
                "bad1": "string",
                "bad2": 42,
            }
        }
        result = convert_legacy_mapping(legacy)
        self.assertFalse(result.success)

    def test_source_file_from_database_name(self):
        """sourceFile in the table mapping comes from database_name."""
        result = convert_legacy_mapping(self._base_legacy())
        db_name = list(result.output["databases"].keys())[0]
        tbl_name = list(result.output["databases"][db_name]["tables"].keys())[0]
        source_file = result.output["databases"][db_name]["tables"][tbl_name][
            "sourceFile"
        ]
        self.assertEqual(source_file, "patients.csv")


if __name__ == "__main__":
    unittest.main()
