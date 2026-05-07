"""
Unit tests for annotation_helper/main.py.

Tests cover:
- is_jsonld_file — extension and content detection
- extract_schema_variable_name — parsing various mapsTo formats
- convert_schema_reconstruction — ClassNode / UnitNode conversion
- convert_value_mapping — merging schema terms with local mappings
- parse_jsonld_for_table — full extraction from a JSON-LD dict
- get_all_tables — listing all (database, table) pairs
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent directory to path so the annotation_helper package is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from annotation_helper.main import (
    convert_schema_reconstruction,
    convert_value_mapping,
    extract_schema_variable_name,
    get_all_tables,
    is_jsonld_file,
    parse_jsonld_for_table,
)


class TestIsJsonldFile(unittest.TestCase):
    """Tests for is_jsonld_file."""

    def test_jsonld_extension_returns_true(self):
        """Files ending in .jsonld are identified as JSON-LD."""
        self.assertTrue(is_jsonld_file("mapping.jsonld", {}))

    def test_jsonld_extension_case_insensitive(self):
        """Extension check is case-insensitive."""
        self.assertTrue(is_jsonld_file("MAPPING.JSONLD", {}))

    def test_context_key_returns_true(self):
        """Dicts containing @context are identified as JSON-LD."""
        self.assertTrue(is_jsonld_file("data.json", {"@context": {}}))

    def test_no_extension_no_context_returns_false(self):
        """Files without .jsonld extension and no @context return False."""
        self.assertFalse(is_jsonld_file("data.json", {"key": "value"}))

    def test_non_dict_content_with_no_extension_returns_false(self):
        """Non-dict content without .jsonld extension returns False."""
        self.assertFalse(is_jsonld_file("data.json", "raw string content"))

    def test_empty_content_without_extension_returns_false(self):
        """Empty dict without .jsonld extension returns False."""
        self.assertFalse(is_jsonld_file("data.json", {}))


class TestExtractSchemaVariableName(unittest.TestCase):
    """Tests for extract_schema_variable_name."""

    def test_slash_format(self):
        """Extracts name after the last slash."""
        result = extract_schema_variable_name("schema:variable/biological_sex")
        self.assertEqual(result, "biological_sex")

    def test_colon_format(self):
        """Extracts name after ': ' when no slash present."""
        result = extract_schema_variable_name("schema:variable: biological_sex")
        self.assertEqual(result, "biological_sex")

    def test_plain_string(self):
        """Returns the string unchanged when no slash or colon present."""
        result = extract_schema_variable_name("biological_sex")
        self.assertEqual(result, "biological_sex")

    def test_non_string_returns_none(self):
        """Non-string input returns None."""
        self.assertIsNone(extract_schema_variable_name(None))
        self.assertIsNone(extract_schema_variable_name(123))

    def test_nested_slash_uses_last_segment(self):
        """Multiple slashes returns only the last segment."""
        result = extract_schema_variable_name("a/b/c/variable_name")
        self.assertEqual(result, "variable_name")


class TestConvertSchemaReconstruction(unittest.TestCase):
    """Tests for convert_schema_reconstruction."""

    def test_class_node_conversion(self):
        """ClassNode items are converted to type 'class'."""
        input_data = [
            {
                "@type": "schema:ClassNode",
                "predicate": "roo:P100018",
                "class": "ncit:C28421",
                "classLabel": "MyLabel",
                "aestheticLabel": "Pretty",
                "placement": "before",
            }
        ]
        result = convert_schema_reconstruction(input_data)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["type"], "class")
        self.assertEqual(result[0]["predicate"], "roo:P100018")
        self.assertEqual(result[0]["class"], "ncit:C28421")
        self.assertEqual(result[0]["class_label"], "MyLabel")
        self.assertEqual(result[0]["aesthetic_label"], "Pretty")
        self.assertEqual(result[0]["placement"], "before")

    def test_unit_node_conversion(self):
        """UnitNode items are converted to type 'node'."""
        input_data = [
            {
                "@type": "schema:UnitNode",
                "predicate": "roo:hasUnit",
                "class": "ncit:C25301",
                "nodeLabel": "NodeLabel",
                "aestheticLabel": "Unit",
            }
        ]
        result = convert_schema_reconstruction(input_data)
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["type"], "node")
        self.assertEqual(result[0]["node_label"], "NodeLabel")

    def test_empty_list_returns_none(self):
        """Empty list returns None."""
        self.assertIsNone(convert_schema_reconstruction([]))

    def test_non_list_returns_none(self):
        """Non-list input returns None."""
        self.assertIsNone(convert_schema_reconstruction(None))
        self.assertIsNone(convert_schema_reconstruction({"key": "value"}))

    def test_unknown_type_is_skipped(self):
        """Items with unknown @type are silently skipped."""
        input_data = [{"@type": "schema:UnknownNode", "predicate": "x"}]
        result = convert_schema_reconstruction(input_data)
        self.assertIsNone(result)

    def test_multiple_nodes_converted(self):
        """Multiple nodes in a list are all converted."""
        input_data = [
            {"@type": "schema:ClassNode", "predicate": "p1", "class": "c1"},
            {"@type": "schema:UnitNode", "predicate": "p2", "class": "c2"},
        ]
        result = convert_schema_reconstruction(input_data)
        self.assertEqual(len(result), 2)


class TestConvertValueMapping(unittest.TestCase):
    """Tests for convert_value_mapping."""

    def test_basic_conversion_with_local_mappings(self):
        """Terms with matching local mappings are included."""
        schema_vm = {
            "terms": {
                "male": {"targetClass": "ncit:C20197"},
                "female": {"targetClass": "ncit:C16576"},
            }
        }
        local_mappings = {"male": "M", "female": "F"}

        result = convert_value_mapping(schema_vm, local_mappings)
        self.assertIsNotNone(result)
        self.assertIn("male", result["terms"])
        self.assertIn("female", result["terms"])
        self.assertEqual(result["terms"]["male"]["local_term"], "M")
        self.assertEqual(result["terms"]["female"]["local_term"], "F")

    def test_terms_without_local_mapping_excluded(self):
        """Terms without a local mapping are excluded from the result."""
        schema_vm = {
            "terms": {
                "male": {"targetClass": "ncit:C20197"},
                "female": {"targetClass": "ncit:C16576"},
            }
        }
        local_mappings = {"male": "M"}  # female has no local mapping

        result = convert_value_mapping(schema_vm, local_mappings)
        self.assertIn("male", result["terms"])
        self.assertNotIn("female", result["terms"])

    def test_empty_local_term_becomes_none(self):
        """Empty string local terms are converted to None."""
        schema_vm = {"terms": {"male": {"targetClass": "ncit:C20197"}}}
        local_mappings = {"male": ""}

        result = convert_value_mapping(schema_vm, local_mappings)
        self.assertIsNone(result["terms"]["male"]["local_term"])

    def test_non_dict_schema_returns_none(self):
        """Non-dict schema value mapping returns None."""
        self.assertIsNone(convert_value_mapping(None, {}))
        self.assertIsNone(convert_value_mapping("string", {}))

    def test_no_terms_in_schema_returns_none(self):
        """Schema value mapping with no terms returns None."""
        self.assertIsNone(convert_value_mapping({}, {}))
        self.assertIsNone(convert_value_mapping({"terms": {}}, {}))

    def test_none_local_mappings_treated_as_empty(self):
        """None local_mappings means no terms have local values, so all excluded."""
        schema_vm = {"terms": {"male": {"targetClass": "ncit:C20197"}}}
        result = convert_value_mapping(schema_vm, None)
        self.assertIsNone(result)


class TestParseJsonldForTable(unittest.TestCase):
    """Tests for parse_jsonld_for_table."""

    def setUp(self):
        """Build a minimal JSON-LD content dict for testing."""
        self.jsonld_content = {
            "endpoint": "http://localhost:7200/repositories/test/statements",
            "schema": {
                "prefixes": {
                    "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
                    "roo": "http://www.cancerdata.org/roo/",
                },
                "variables": {
                    "biological_sex": {
                        "predicate": "roo:P100018",
                        "class": "ncit:C28421",
                        "valueMapping": {
                            "terms": {
                                "male": {"targetClass": "ncit:C20197"},
                                "female": {"targetClass": "ncit:C16576"},
                            }
                        },
                    },
                    "age": {
                        "predicate": "roo:P100027",
                        "class": "ncit:C25150",
                    },
                },
            },
            "databases": {
                "test_db": {
                    "tables": {
                        "patients": {
                            "sourceFile": "patients.csv",
                            "columns": {
                                "sex_col": {
                                    "mapsTo": "schema:variable/biological_sex",
                                    "localColumn": "sex",
                                    "localMappings": {"male": "M", "female": "F"},
                                },
                                "age_col": {
                                    "mapsTo": "schema:variable/age",
                                    "localColumn": "age",
                                },
                            },
                        }
                    }
                }
            },
        }

    def test_extracts_endpoint(self):
        """Endpoint is extracted correctly."""
        endpoint, _, _, _ = parse_jsonld_for_table(
            self.jsonld_content, "test_db", "patients"
        )
        self.assertEqual(
            endpoint, "http://localhost:7200/repositories/test/statements"
        )

    def test_extracts_table_name(self):
        """sourceFile is used as the table name."""
        _, table_name, _, _ = parse_jsonld_for_table(
            self.jsonld_content, "test_db", "patients"
        )
        self.assertEqual(table_name, "patients.csv")

    def test_builds_prefixes_string(self):
        """Prefixes are formatted as PREFIX declarations."""
        _, _, prefixes, _ = parse_jsonld_for_table(
            self.jsonld_content, "test_db", "patients"
        )
        self.assertIn("PREFIX ncit:", prefixes)
        self.assertIn("PREFIX roo:", prefixes)

    def test_builds_variable_info(self):
        """Variable info contains entries for mapped columns."""
        _, _, _, variable_info = parse_jsonld_for_table(
            self.jsonld_content, "test_db", "patients"
        )
        self.assertIn("biological_sex", variable_info)
        self.assertIn("age", variable_info)
        self.assertEqual(variable_info["age"]["local_definition"], "age")

    def test_unknown_database_returns_empty_variable_info(self):
        """Unknown database key results in empty variable_info."""
        _, _, _, variable_info = parse_jsonld_for_table(
            self.jsonld_content, "nonexistent_db", "patients"
        )
        self.assertEqual(variable_info, {})

    def test_column_with_list_local_column(self):
        """localColumn as a list uses the first element."""
        content = {
            "endpoint": "http://ep",
            "schema": {
                "prefixes": {},
                "variables": {
                    "bio_sex": {"predicate": "p", "class": "c"},
                },
            },
            "databases": {
                "db": {
                    "tables": {
                        "tbl": {
                            "sourceFile": "file.csv",
                            "columns": {
                                "col": {
                                    "mapsTo": "schema:variable/bio_sex",
                                    "localColumn": ["primary_col", "alt_col"],
                                }
                            },
                        }
                    }
                }
            },
        }
        _, _, _, variable_info = parse_jsonld_for_table(content, "db", "tbl")
        self.assertEqual(variable_info["bio_sex"]["local_definition"], "primary_col")


class TestGetAllTables(unittest.TestCase):
    """Tests for get_all_tables."""

    def test_single_database_single_table(self):
        """Single database with one table returns one tuple."""
        content = {
            "databases": {
                "db1": {"tables": {"tbl1": {}}},
            }
        }
        result = get_all_tables(content)
        self.assertEqual(result, [("db1", "tbl1")])

    def test_multiple_databases_multiple_tables(self):
        """Multiple databases with multiple tables returns all combinations."""
        content = {
            "databases": {
                "db1": {"tables": {"t1": {}, "t2": {}}},
                "db2": {"tables": {"t3": {}}},
            }
        }
        result = get_all_tables(content)
        self.assertIn(("db1", "t1"), result)
        self.assertIn(("db1", "t2"), result)
        self.assertIn(("db2", "t3"), result)
        self.assertEqual(len(result), 3)

    def test_no_databases_returns_empty(self):
        """Content without databases key returns empty list."""
        result = get_all_tables({})
        self.assertEqual(result, [])

    def test_database_without_tables_returns_empty(self):
        """Database with no tables contributes nothing."""
        content = {"databases": {"db1": {}}}
        result = get_all_tables(content)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
