"""
Backend integration: MappingValidator using actual example files from disk.

These tests read real files from the example_data/ directory and are kept
separate from unit tests so CI can run them independently when the example
data is available.
"""

import sys
import unittest
from pathlib import Path

import pytest

# Add flyover package to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from validation import MappingValidator

_EXAMPLE_DIR = Path(__file__).parent.parent.parent.parent.parent / "example_data"


class TestMappingValidatorWithRealFiles(unittest.TestCase):
    """Test MappingValidator with actual example files."""

    def setUp(self):
        """Set up validator and file paths."""
        self.validator = MappingValidator()
        self.example_dir = _EXAMPLE_DIR

    @pytest.mark.skipif(
        not (_EXAMPLE_DIR / "centre_a_english" / "mapping_centre_a.jsonld").exists(),
        reason="Example data not available",
    )
    def test_validate_centre_a_mapping(self):
        """Test validation of Centre A English mapping."""
        file_path = self.example_dir / "centre_a_english" / "mapping_centre_a.jsonld"
        result = self.validator.validate_file(file_path)
        self.assertTrue(
            result.is_valid, f"Errors: {[i.message for i in result.issues]}"
        )
        self.assertEqual(result.statistics["variables"], 11)

    @pytest.mark.skipif(
        not (_EXAMPLE_DIR / "centre_b_dutch" / "mapping_centre_b.jsonld").exists(),
        reason="Example data not available",
    )
    def test_validate_centre_b_mapping(self):
        """Test validation of Centre B Dutch mapping."""
        file_path = self.example_dir / "centre_b_dutch" / "mapping_centre_b.jsonld"
        result = self.validator.validate_file(file_path)
        self.assertTrue(
            result.is_valid, f"Errors: {[i.message for i in result.issues]}"
        )

    @pytest.mark.skipif(
        not (_EXAMPLE_DIR / "mapping_template.jsonld").exists(),
        reason="Example data not available",
    )
    def test_validate_template_mapping(self):
        """Test validation of template mapping."""
        file_path = self.example_dir / "mapping_template.jsonld"
        result = self.validator.validate_file(file_path)
        self.assertTrue(
            result.is_valid, f"Errors: {[i.message for i in result.issues]}"
        )


if __name__ == "__main__":
    unittest.main()
