"""
Integration tests for JSONLDMapping loading actual example files from disk.

These tests read real files from the example_data/ directory and are kept
separate from unit tests so CI can run them independently when the example
data is available.
"""

import sys
import unittest
from pathlib import Path

import pytest

# Add data_descriptor to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders import JSONLDMapping

_EXAMPLE_DIR = Path(__file__).parent.parent.parent.parent.parent / "example_data"


class TestJSONLDMappingWithRealFiles(unittest.TestCase):
    """Test JSONLDMapping with actual example files."""

    def setUp(self):
        """Set up file paths."""
        self.example_dir = _EXAMPLE_DIR

    @pytest.mark.skipif(
        not (_EXAMPLE_DIR / "centre_a_english" / "mapping_centre_a.jsonld").exists(),
        reason="Example data not available",
    )
    def test_load_centre_a_mapping(self):
        """Test loading Centre A English mapping."""
        file_path = self.example_dir / "centre_a_english" / "mapping_centre_a.jsonld"
        mapping = JSONLDMapping.from_file(file_path)
        self.assertEqual(mapping.name, "Centre A - English Hospital Mapping")
        self.assertEqual(len(mapping.variables), 11)

        # Test accessor methods
        var = mapping.get_variable("biological_sex")
        self.assertIsNotNone(var)
        self.assertEqual(var.data_type, "categorical")

        # Test local term retrieval
        local_term = mapping.get_local_term("biological_sex", "male")
        self.assertEqual(local_term, "male")

        # Test target class retrieval
        target = mapping.get_target_class("biological_sex", "male")
        self.assertEqual(target, "ncit:C20197")

    @pytest.mark.skipif(
        not (_EXAMPLE_DIR / "centre_b_dutch" / "mapping_centre_b.jsonld").exists(),
        reason="Example data not available",
    )
    def test_load_centre_b_mapping(self):
        """Test loading Centre B Dutch mapping."""
        file_path = self.example_dir / "centre_b_dutch" / "mapping_centre_b.jsonld"
        mapping = JSONLDMapping.from_file(file_path)

        # Test Dutch local mappings
        local_term = mapping.get_local_term("biological_sex", "male")
        self.assertEqual(local_term, "man")

        local_term = mapping.get_local_term("biological_sex", "female")
        self.assertEqual(local_term, "vrouw")

        # Test locale field
        db = mapping.get_database("centre_b_ehr")
        self.assertIsNotNone(db)
        self.assertEqual(db.locale, "nl_NL")

    @pytest.mark.skipif(
        not (_EXAMPLE_DIR / "centre_a_english" / "mapping_centre_a.jsonld").exists(),
        reason="Example data not available",
    )
    def test_locale_field(self):
        """Test that locale field is properly loaded and saved."""
        file_path = self.example_dir / "centre_a_english" / "mapping_centre_a.jsonld"
        mapping = JSONLDMapping.from_file(file_path)

        # Test locale field in Centre A
        db = mapping.get_database("centre_a_ehr")
        self.assertIsNotNone(db)
        self.assertEqual(db.locale, "en_GB")

        # Test that locale is preserved in to_dict
        mapping_dict = mapping.to_dict()
        self.assertEqual(mapping_dict["databases"]["centre_a_ehr"]["locale"], "en_GB")


if __name__ == "__main__":
    unittest.main()
