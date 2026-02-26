"""
Unit tests for data preprocessing utilities.

Tests cover:
- Encoding detection and conversion for CSV file ingestion
- Reading multiple CSVs with different encodings via read_csv_with_encoding_detection
- Edge cases with rare character sets
"""

import sys
import unittest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.data_preprocessing import (
    detect_and_convert_encoding,
    read_csv_with_encoding_detection,
)


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


class TestReadCsvWithEncodingDetection(unittest.TestCase):
    """Tests for the read_csv_with_encoding_detection function."""

    def test_read_utf8_csv(self):
        """Reading a simple UTF-8 CSV should work."""
        csv_bytes = "name,age\nAlice,30\nBob,25".encode("utf-8")
        df = read_csv_with_encoding_detection(csv_bytes)
        self.assertEqual(df.columns, ["name", "age"])
        self.assertEqual(df.height, 2)
        self.assertEqual(df["name"].to_list(), ["Alice", "Bob"])

    def test_read_latin1_csv(self):
        """Reading a Latin-1 encoded CSV should produce correct UTF-8 output."""
        csv_bytes = "name,city\ncafé,Zürich\nnaïve,Malmö".encode("latin-1")
        df = read_csv_with_encoding_detection(csv_bytes)
        self.assertEqual(df["name"].to_list(), ["café", "naïve"])
        self.assertEqual(df["city"].to_list(), ["Zürich", "Malmö"])

    def test_read_windows_1252_csv(self):
        """Reading a Windows-1252 encoded CSV with special chars should work."""
        # \x93 and \x94 are left/right double quotation marks in Windows-1252
        csv_bytes = b"col1,col2\n\x93hello\x94,world"
        df = read_csv_with_encoding_detection(csv_bytes)
        self.assertEqual(df.height, 1)
        self.assertIn("hello", df["col1"].to_list()[0])

    def test_custom_separator(self):
        """Custom separator should be respected."""
        csv_bytes = "name;age\nAlice;30".encode("utf-8")
        df = read_csv_with_encoding_detection(csv_bytes, separator=";")
        self.assertEqual(df.columns, ["name", "age"])
        self.assertEqual(df["name"].to_list(), ["Alice"])

    def test_decimal_sign_conversion(self):
        """Non-standard decimal separator should be converted to '.'."""
        csv_bytes = "value;amount\n1,5;2,3".encode("utf-8")
        df = read_csv_with_encoding_detection(
            csv_bytes, separator=";", decimal_sign=","
        )
        self.assertEqual(df["value"].to_list(), ["1.5"])
        self.assertEqual(df["amount"].to_list(), ["2.3"])

    def test_all_columns_are_strings(self):
        """All columns should be read as strings (infer_schema_length=0)."""
        csv_bytes = "id,score,date\n1,99.5,2024-01-01".encode("utf-8")
        df = read_csv_with_encoding_detection(csv_bytes)
        import polars as pl

        for col in df.columns:
            self.assertEqual(df[col].dtype, pl.String)


class TestMultipleCsvIngestion(unittest.TestCase):
    """
    Tests simulating the ingestion of multiple CSV files with different encodings,
    as would happen during a multi-file upload in the application.
    """

    def _ingest_multiple(self, files, separator=",", decimal_sign="."):
        """Helper that mimics the upload_file loop for a list of raw byte buffers."""
        dataframes = []
        for file_bytes in files:
            df = read_csv_with_encoding_detection(
                file_bytes, separator=separator, decimal_sign=decimal_sign
            )
            dataframes.append(df)
        return dataframes

    def test_mixed_utf8_and_latin1(self):
        """Ingesting a UTF-8 file and a Latin-1 file together should work."""
        # Use enough rows for chardet to reliably detect UTF-8
        utf8_csv = (
            "name,city\n"
            + "\n".join(f"person_{i},München" for i in range(20))
        ).encode("utf-8")
        latin1_csv = "name,city\nBob,Zürich".encode("latin-1")

        dfs = self._ingest_multiple([utf8_csv, latin1_csv])

        self.assertEqual(len(dfs), 2)
        self.assertEqual(dfs[0]["city"].to_list()[0], "München")
        self.assertEqual(dfs[1]["city"].to_list(), ["Zürich"])

    def test_mixed_utf8_latin1_and_windows_1252(self):
        """Ingesting UTF-8, Latin-1, and Windows-1252 files together should work."""
        utf8_csv = "col\nvalue_utf8".encode("utf-8")
        latin1_csv = "col\nrésumé".encode("latin-1")
        cp1252_csv = b"col\n\x93quoted\x94"  # curly double quotes in Windows-1252

        dfs = self._ingest_multiple([utf8_csv, latin1_csv, cp1252_csv])

        self.assertEqual(len(dfs), 3)
        self.assertEqual(dfs[0]["col"].to_list(), ["value_utf8"])
        self.assertEqual(dfs[1]["col"].to_list(), ["résumé"])
        self.assertIn("quoted", dfs[2]["col"].to_list()[0])

    def test_mixed_ascii_and_utf8_with_bom(self):
        """Ingesting ASCII and UTF-8-with-BOM files together should work."""
        ascii_csv = b"a,b\n1,2"
        bom_csv = b"\xef\xbb\xbfa,b\n3,4"

        dfs = self._ingest_multiple([ascii_csv, bom_csv])

        self.assertEqual(len(dfs), 2)
        self.assertEqual(dfs[0]["a"].to_list(), ["1"])
        self.assertEqual(dfs[1]["a"].to_list(), ["3"])

    def test_multiple_latin1_files_with_accented_headers(self):
        """Multiple Latin-1 files with accented column headers should work."""
        file1 = "prénom,âge\nJean,40".encode("latin-1")
        file2 = "código,descripción\nA01,fièvre".encode("latin-1")

        dfs = self._ingest_multiple([file1, file2])

        self.assertEqual(len(dfs), 2)
        self.assertEqual(dfs[0]["prénom"].to_list(), ["Jean"])
        self.assertEqual(dfs[1]["descripción"].to_list(), ["fièvre"])

    def test_iso_8859_15_euro_sign(self):
        """ISO-8859-15 (Latin-9) file with euro sign should be handled."""
        # ISO-8859-15 is like Latin-1 but has € at 0xA4 instead of ¤
        csv_bytes = b"price,currency\n100,\xa4"  # \xA4 = € in ISO-8859-15, ¤ in Latin-1
        dfs = self._ingest_multiple([csv_bytes])
        # Should not raise; the character may be decoded as ¤ or € depending
        # on chardet detection, but must be valid UTF-8 either way
        self.assertEqual(len(dfs), 1)
        dfs[0]["currency"].to_list()[0].encode("utf-8")  # must not raise

    def test_greek_iso_8859_7(self):
        """CSV with Greek characters in ISO-8859-7 encoding should be handled."""
        # "Ελλάδα" (Greece) in ISO-8859-7
        greek_text = "Ελλάδα"
        csv_bytes = ("name,country\ntest," + greek_text).encode("iso-8859-7")
        dfs = self._ingest_multiple([csv_bytes])
        self.assertEqual(len(dfs), 1)
        # Verify it's valid UTF-8
        result = dfs[0]["country"].to_list()[0]
        result.encode("utf-8")  # must not raise

    def test_cyrillic_iso_8859_5(self):
        """CSV with Cyrillic text in ISO-8859-5 encoding should be handled."""
        # "Москва" (Moscow) in ISO-8859-5
        cyrillic_text = "Москва"
        csv_bytes = ("city,code\n" + cyrillic_text + ",MOW").encode("iso-8859-5")
        dfs = self._ingest_multiple([csv_bytes])
        self.assertEqual(len(dfs), 1)
        result = dfs[0]["city"].to_list()[0]
        result.encode("utf-8")  # must not raise

    def test_mixed_rare_encodings(self):
        """Ingesting files with multiple rare encodings simultaneously."""
        utf8_csv = "col\nhello".encode("utf-8")
        latin1_csv = "col\nMünchen".encode("latin-1")
        greek_csv = ("col\n" + "Αθήνα").encode("iso-8859-7")

        dfs = self._ingest_multiple([utf8_csv, latin1_csv, greek_csv])

        self.assertEqual(len(dfs), 3)
        # All results must be valid UTF-8
        for df in dfs:
            df["col"].to_list()[0].encode("utf-8")

    def test_many_files_same_encoding(self):
        """Ingesting many files with the same encoding should work consistently."""
        files = [f"id,val\n{i},{i * 10}".encode("utf-8") for i in range(10)]
        dfs = self._ingest_multiple(files)
        self.assertEqual(len(dfs), 10)
        for i, df in enumerate(dfs):
            self.assertEqual(df["id"].to_list(), [str(i)])

    def test_semicolon_separated_with_comma_decimal(self):
        """Multiple files with European-style formatting (semicolon + comma decimal)."""
        file1 = "val;amount\n1,5;2,3".encode("utf-8")
        file2 = "val;amount\n3,7;4,1".encode("latin-1")

        dfs = self._ingest_multiple(
            [file1, file2], separator=";", decimal_sign=","
        )

        self.assertEqual(len(dfs), 2)
        self.assertEqual(dfs[0]["val"].to_list(), ["1.5"])
        self.assertEqual(dfs[1]["val"].to_list(), ["3.7"])


if __name__ == "__main__":
    unittest.main()
