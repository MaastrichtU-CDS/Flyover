"""
Unit tests for session helper functions.

Tests cover the new JSON-LD support helper functions:
- get_semantic_map_for_annotation
- has_semantic_map
- get_database_name_from_mapping
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.session_helpers import (
    get_semantic_map_for_annotation,
    has_semantic_map,
    get_database_name_from_mapping,
)


class MockSessionCache:
    """Mock session cache for testing."""

    def __init__(self):
        self.jsonld_mapping = None
        self.global_semantic_map = None


class TestHasSemanticMap(unittest.TestCase):
    """Test the has_semantic_map function."""

    def test_no_semantic_map(self):
        """Test when no semantic map is available."""
        cache = MockSessionCache()
        self.assertFalse(has_semantic_map(cache))

    def test_has_global_semantic_map(self):
        """Test when global_semantic_map is available."""
        cache = MockSessionCache()
        cache.global_semantic_map = {"variable_info": {}}
        self.assertTrue(has_semantic_map(cache))

    def test_has_jsonld_mapping(self):
        """Test when jsonld_mapping is available."""
        cache = MockSessionCache()
        cache.jsonld_mapping = MagicMock()
        self.assertTrue(has_semantic_map(cache))

    def test_both_mappings_available(self):
        """Test when both mappings are available."""
        cache = MockSessionCache()
        cache.jsonld_mapping = MagicMock()
        cache.global_semantic_map = {"variable_info": {}}
        self.assertTrue(has_semantic_map(cache))


class TestGetDatabaseNameFromMapping(unittest.TestCase):
    """Test the get_database_name_from_mapping function."""

    def test_no_mapping(self):
        """Test when no mapping is available."""
        cache = MockSessionCache()
        self.assertIsNone(get_database_name_from_mapping(cache))

    def test_global_semantic_map(self):
        """Test with global_semantic_map."""
        cache = MockSessionCache()
        cache.global_semantic_map = {"database_name": "test_db"}
        self.assertEqual(get_database_name_from_mapping(cache), "test_db")

    def test_jsonld_mapping(self):
        """Test with jsonld_mapping."""
        cache = MockSessionCache()
        cache.jsonld_mapping = MagicMock()
        cache.jsonld_mapping.get_first_database_name.return_value = "jsonld_db"
        self.assertEqual(get_database_name_from_mapping(cache), "jsonld_db")

    def test_jsonld_takes_precedence(self):
        """Test that jsonld_mapping takes precedence over global_semantic_map."""
        cache = MockSessionCache()
        cache.jsonld_mapping = MagicMock()
        cache.jsonld_mapping.get_first_database_name.return_value = "jsonld_db"
        cache.global_semantic_map = {"database_name": "legacy_db"}
        self.assertEqual(get_database_name_from_mapping(cache), "jsonld_db")


class TestGetSemanticMapForAnnotation(unittest.TestCase):
    """Test the get_semantic_map_for_annotation function."""

    def test_no_mapping(self):
        """Test when no mapping is available."""
        cache = MockSessionCache()
        result, db_name, is_jsonld = get_semantic_map_for_annotation(cache)
        self.assertIsNone(result)
        self.assertIsNone(db_name)
        self.assertFalse(is_jsonld)

    def test_global_semantic_map(self):
        """Test with global_semantic_map."""
        cache = MockSessionCache()
        cache.global_semantic_map = {
            "database_name": "test_db",
            "variable_info": {"var1": {"predicate": "test:pred"}},
        }
        result, db_name, is_jsonld = get_semantic_map_for_annotation(cache)
        self.assertEqual(result, cache.global_semantic_map)
        self.assertEqual(db_name, "test_db")
        self.assertFalse(is_jsonld)

    def test_jsonld_mapping(self):
        """Test with jsonld_mapping."""
        cache = MockSessionCache()
        mock_mapping = MagicMock()
        mock_mapping.to_legacy_format.return_value = {
            "variable_info": {"var1": {"predicate": "test:pred"}}
        }
        mock_mapping.get_first_database_name.return_value = "jsonld_db"
        cache.jsonld_mapping = mock_mapping

        result, db_name, is_jsonld = get_semantic_map_for_annotation(cache)
        self.assertIsNotNone(result)
        self.assertEqual(db_name, "jsonld_db")
        self.assertTrue(is_jsonld)
        mock_mapping.to_legacy_format.assert_called_once()

    def test_jsonld_takes_precedence(self):
        """Test that jsonld_mapping takes precedence over global_semantic_map."""
        cache = MockSessionCache()
        mock_mapping = MagicMock()
        mock_mapping.to_legacy_format.return_value = {"variable_info": {}}
        mock_mapping.get_first_database_name.return_value = "jsonld_db"
        cache.jsonld_mapping = mock_mapping
        cache.global_semantic_map = {"database_name": "legacy_db"}

        result, db_name, is_jsonld = get_semantic_map_for_annotation(cache)
        self.assertEqual(db_name, "jsonld_db")
        self.assertTrue(is_jsonld)


if __name__ == "__main__":
    unittest.main()
