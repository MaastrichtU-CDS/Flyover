"""
Unit tests for data preprocessing utilities.

Tests cover encoding detection and conversion for CSV file ingestion.
"""

import sys
import unittest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.data_preprocessing import detect_and_convert_encoding


class TestDetectAndConvertEncoding(unittest.TestCase):
    """Tests for the detect_and_convert_encoding function."""

    def test_utf8_bytes_returned_unchanged(self):
        """UTF-8 encoded bytes should be returned as-is."""
        utf8_bytes = "hello,world\nfoo,bar".encode("utf-8")
        result = detect_and_convert_encoding(utf8_bytes)
        self.assertEqual(result, utf8_bytes)

    def test_ascii_bytes_returned_unchanged(self):
        """Plain ASCII bytes should be returned as-is."""
        ascii_bytes = b"col1,col2\n1,2\n3,4"
        result = detect_and_convert_encoding(ascii_bytes)
        self.assertEqual(result, ascii_bytes)

    def test_latin1_converted_to_utf8(self):
        """Latin-1 (ISO-8859-1) encoded bytes should be converted to UTF-8."""
        # "café" in Latin-1: 'é' is 0xe9
        latin1_bytes = "name,city\ncafé,Zürich\nnaïve,Malmö".encode("latin-1")
        result = detect_and_convert_encoding(latin1_bytes)
        # The result should be valid UTF-8
        decoded = result.decode("utf-8")
        self.assertIn("café", decoded)
        self.assertIn("Zürich", decoded)
        self.assertIn("naïve", decoded)
        self.assertIn("Malmö", decoded)

    def test_windows_1252_converted_to_utf8(self):
        """Windows-1252 encoded bytes should be converted to UTF-8."""
        # Windows-1252 has characters like curly quotes that differ from Latin-1
        text = "name,value\ntest,100\ndata,200"
        cp1252_bytes = text.encode("windows-1252")
        result = detect_and_convert_encoding(cp1252_bytes)
        decoded = result.decode("utf-8")
        self.assertIn("test", decoded)

    def test_utf8_with_bom_handled(self):
        """UTF-8 with BOM should be handled (BOM removed)."""
        text = "col1,col2\nval1,val2"
        bom_bytes = b"\xef\xbb\xbf" + text.encode("utf-8")
        result = detect_and_convert_encoding(bom_bytes)
        decoded = result.decode("utf-8")
        # BOM should not cause an error; content should be preserved
        self.assertIn("col1", decoded)
        self.assertIn("val1", decoded)

    def test_utf8_with_special_characters(self):
        """UTF-8 bytes with special characters should work correctly."""
        text = "name,description\npatient,données médicales\nétude,résultats"
        utf8_bytes = text.encode("utf-8")
        result = detect_and_convert_encoding(utf8_bytes)
        decoded = result.decode("utf-8")
        self.assertIn("données", decoded)
        self.assertIn("résultats", decoded)

    def test_empty_bytes(self):
        """Empty bytes should be returned without error."""
        result = detect_and_convert_encoding(b"")
        self.assertEqual(result, b"")

    def test_result_is_valid_utf8(self):
        """The result should always be valid UTF-8, regardless of input encoding."""
        # Create bytes with Latin-1 specific characters
        latin1_bytes = bytes([0xE4, 0xF6, 0xFC])  # äöü in Latin-1
        result = detect_and_convert_encoding(latin1_bytes)
        # Should not raise UnicodeDecodeError
        result.decode("utf-8")


if __name__ == "__main__":
    unittest.main()
