"""
Unit tests for the service layer.

Tests cover IngestService, DescribeService, and AnnotateService
functionality for business logic operations.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services import IngestService, DescribeService, AnnotateService


class TestIngestServiceValidation(unittest.TestCase):
    """Test IngestService file validation methods."""

    def test_allowed_file_csv(self):
        """Test CSV file extension validation."""
        self.assertTrue(IngestService.allowed_file("test.csv", {"csv"}))
        self.assertTrue(IngestService.allowed_file("data.CSV", {"csv"}))

    def test_allowed_file_jsonld(self):
        """Test JSON-LD file extension validation."""
        self.assertTrue(IngestService.allowed_file("mapping.jsonld", {"jsonld"}))

    def test_allowed_file_invalid(self):
        """Test invalid file extension."""
        self.assertFalse(IngestService.allowed_file("test.txt", {"csv"}))
        self.assertFalse(IngestService.allowed_file("data.json", {"jsonld"}))

    def test_allowed_file_no_extension(self):
        """Test file without extension."""
        self.assertFalse(IngestService.allowed_file("noextension", {"csv"}))

    def test_validate_csv_files_empty(self):
        """Test validation with no files."""
        is_valid, error = IngestService.validate_csv_files([])
        self.assertFalse(is_valid)
        self.assertIn("No files", error)

    def test_validate_csv_files_no_filename(self):
        """Test validation with files but no filenames."""
        mock_file = MagicMock()
        mock_file.filename = ""
        is_valid, error = IngestService.validate_csv_files([mock_file])
        self.assertFalse(is_valid)

    def test_validate_csv_files_wrong_extension(self):
        """Test validation with wrong file extension."""
        mock_file = MagicMock()
        mock_file.filename = "test.txt"
        is_valid, error = IngestService.validate_csv_files([mock_file])
        self.assertFalse(is_valid)

    def test_validate_csv_files_valid(self):
        """Test validation with valid CSV files."""
        mock_file = MagicMock()
        mock_file.filename = "test.csv"
        is_valid, error = IngestService.validate_csv_files([mock_file])
        self.assertTrue(is_valid)
        self.assertIsNone(error)


class TestIngestServiceDataParsing(unittest.TestCase):
    """Test IngestService data parsing methods."""

    def test_parse_pk_fk_data_none(self):
        """Test parsing None PK/FK data."""
        result = IngestService.parse_pk_fk_data(None)
        self.assertIsNone(result)

    def test_parse_pk_fk_data_valid(self):
        """Test parsing valid PK/FK data."""
        json_str = '[{"fileName": "test.csv", "primaryKey": "id"}]'
        result = IngestService.parse_pk_fk_data(json_str)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["fileName"], "test.csv")

    def test_parse_pk_fk_data_invalid_json(self):
        """Test parsing invalid JSON."""
        result = IngestService.parse_pk_fk_data("invalid json")
        self.assertIsNone(result)

    def test_parse_cross_graph_data_none(self):
        """Test parsing None cross-graph data."""
        result = IngestService.parse_cross_graph_data(None)
        self.assertIsNone(result)

    def test_parse_cross_graph_data_valid(self):
        """Test parsing valid cross-graph data."""
        json_str = '{"newTableName": "new", "existingTableName": "old"}'
        result = IngestService.parse_cross_graph_data(json_str)
        self.assertEqual(result["newTableName"], "new")


class TestDescribeServiceFormParsing(unittest.TestCase):
    """Test DescribeService form parsing methods."""

    def test_parse_form_data_for_database(self):
        """Test parsing form data for a specific database."""
        form_data = {
            "db1_col1": "categorical",
            "ncit_comment_db1_col1": "Test description",
            "comment_db1_col1": "Test comment",
            "db2_col2": "continuous",
        }
        databases = ["db1", "db2"]

        result = DescribeService.parse_form_data_for_database(
            form_data, "db1", databases
        )

        self.assertIn("col1", result)
        self.assertIn("Variable type: categorical", result["col1"]["type"])
        self.assertIn("Test description", result["col1"]["description"])

    def test_parse_form_data_for_database_skips_other(self):
        """Test that parsing skips other database's data."""
        form_data = {
            "db1_col1": "categorical",
            "db2_col2": "continuous",
        }
        databases = ["db1", "db2"]

        result = DescribeService.parse_form_data_for_database(
            form_data, "db1", databases
        )

        self.assertIn("col1", result)
        self.assertNotIn("col2", result)


class TestDescribeServiceLocalSemanticMap(unittest.TestCase):
    """Test DescribeService local semantic map formulation."""

    def setUp(self):
        """Set up test data."""
        self.global_map = {
            "database_name": "template",
            "variable_info": {
                "test_var": {
                    "predicate": "sio:test",
                    "class": "ncit:Test",
                    "local_definition": None,
                    "value_mapping": {
                        "terms": {
                            "option_a": {"target_class": "ncit:A", "local_term": None},
                            "option_b": {"target_class": "ncit:B", "local_term": None},
                        }
                    },
                }
            },
        }

    def test_formulate_local_semantic_map_no_descriptive_info(self):
        """Test formulating map without descriptive info."""
        result = DescribeService.formulate_local_semantic_map(
            self.global_map, "test_db"
        )

        self.assertEqual(result["database_name"], "test_db")
        self.assertIsNone(result["variable_info"]["test_var"]["local_definition"])

    def test_formulate_local_semantic_map_with_descriptive_info(self):
        """Test formulating map with descriptive info."""
        descriptive_info = {
            "local_column": {
                "description": "Variable description: test_var",
                "type": "Variable type: categorical",
            }
        }

        result = DescribeService.formulate_local_semantic_map(
            self.global_map, "test_db", descriptive_info
        )

        self.assertEqual(result["database_name"], "test_db")
        self.assertEqual(
            result["variable_info"]["test_var"]["local_definition"], "local_column"
        )


class TestDescribeServiceGlobalNames(unittest.TestCase):
    """Test DescribeService global variable name retrieval."""

    def test_get_global_variable_names_no_mapping(self):
        """Test getting default names when no mapping."""
        result = DescribeService.get_global_variable_names()

        self.assertIn("Research subject identifier", result)
        self.assertIn("Other", result)

    def test_get_global_variable_names_with_semantic_map(self):
        """Test getting names from semantic map."""
        semantic_map = {
            "variable_info": {
                "test_var_one": {},
                "test_var_two": {},
            }
        }

        result = DescribeService.get_global_variable_names(
            global_semantic_map=semantic_map
        )

        self.assertIn("Test var one", result)
        self.assertIn("Test var two", result)
        self.assertIn("Other", result)

    def test_get_global_variable_names_with_jsonld_mapping(self):
        """Test getting names from JSON-LD mapping."""
        mock_mapping = MagicMock()
        mock_mapping.get_all_variable_keys.return_value = ["bio_sex", "age"]

        result = DescribeService.get_global_variable_names(jsonld_mapping=mock_mapping)

        self.assertIn("Bio sex", result)
        self.assertIn("Age", result)
        self.assertIn("Other", result)


class TestAnnotateServiceGetAnnotatableVariables(unittest.TestCase):
    """Test AnnotateService variable filtering."""

    def test_get_annotatable_variables_jsonld(self):
        """Test filtering variables for JSON-LD format."""
        variable_info = {
            "var1": {"local_definition": "col1", "class": "ncit:Test"},
            "var2": {"local_definition": None, "class": "ncit:Test2"},
            "var3": {"local_definition": "col3", "class": "ncit:Test3"},
        }

        result = AnnotateService.get_annotatable_variables(
            variable_info, is_jsonld=True
        )

        self.assertEqual(len(result), 2)
        self.assertIn("var1", result)
        self.assertIn("var3", result)
        self.assertNotIn("var2", result)

    def test_get_annotatable_variables_empty(self):
        """Test with no annotatable variables."""
        variable_info = {
            "var1": {"local_definition": None},
            "var2": {"local_definition": None},
        }

        result = AnnotateService.get_annotatable_variables(
            variable_info, is_jsonld=True
        )

        self.assertEqual(len(result), 0)


class TestAnnotateServiceVerification(unittest.TestCase):
    """Test AnnotateService verification methods."""

    def test_get_verification_data_structure(self):
        """Test verification data structure."""
        databases = ["test_db"]
        session_cache = MagicMock()
        session_cache.jsonld_mapping = None
        session_cache.global_semantic_map = None

        def mock_name_matcher(a, b):
            return a == b

        def mock_get_semantic_map(cache, database_key=None):
            return (
                {
                    "variable_info": {
                        "var1": {"local_definition": "col1", "class": "ncit:Test"},
                        "var2": {"local_definition": None, "class": "ncit:Test2"},
                    },
                    "prefixes": "PREFIX test: <http://test/>",
                },
                "test_db",
                True,
            )

        def mock_formulate(db):
            return {}

        annotated, unannotated, data = AnnotateService.get_verification_data(
            databases,
            session_cache,
            ["test_db"],
            mock_name_matcher,
            mock_get_semantic_map,
            mock_formulate,
        )

        self.assertEqual(len(annotated), 1)
        self.assertEqual(len(unannotated), 1)
        self.assertIn("test_db.var1", annotated)
        self.assertIn("test_db.var2", unannotated)


if __name__ == "__main__":
    unittest.main()
