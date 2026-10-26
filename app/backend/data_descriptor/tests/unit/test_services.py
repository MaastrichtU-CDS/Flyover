"""
Unit tests for the service layer.

Tests cover IngestService, DescribeService, and AnnotateService
functionality for business logic operations.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

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

    def test_validate_excel_files_accepts_xlsx_xls_and_ods(self):
        """Excel validation accepts Excel and OpenDocument workbooks alike."""
        for name in ("data.xlsx", "data.xls", "data.ods", "DATA.ODS"):
            mock_file = MagicMock()
            mock_file.filename = name
            is_valid, error = IngestService.validate_excel_files([mock_file])
            self.assertTrue(is_valid, name)
            self.assertIsNone(error, name)

    def test_validate_excel_files_rejects_other_extensions(self):
        """Excel validation rejects non-spreadsheet files and names .ods."""
        for name in ("data.csv", "data.txt", "data.odt"):
            mock_file = MagicMock()
            mock_file.filename = name
            is_valid, error = IngestService.validate_excel_files([mock_file])
            self.assertFalse(is_valid, name)
            self.assertIn("'.ods'", error)

    def test_allowed_file_ods(self):
        """.ods is part of the default allowed extensions."""
        self.assertTrue(IngestService.allowed_file("sheet.ods"))
        self.assertTrue(IngestService.allowed_file("sheet.ODS", {"ods"}))


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


def _minimal_ods(sheets):
    """Build a minimal OpenDocument spreadsheet in memory.

    ``sheets`` maps a sheet name to a list of rows; every cell is written as
    a string cell. Only the parts calamine needs are included (mimetype,
    manifest and content.xml), which keeps the fixture readable.
    """
    import io
    import zipfile

    def cell(value):
        return (
            '<table:table-cell office:value-type="string">'
            f"<text:p>{value}</text:p></table:table-cell>"
        )

    tables = ""
    for name, rows in sheets.items():
        body = "".join(
            "<table:table-row>" + "".join(cell(v) for v in row) + "</table:table-row>"
            for row in rows
        )
        tables += f'<table:table table:name="{name}">{body}</table:table>'

    content = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<office:document-content"
        ' xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"'
        ' xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0"'
        ' xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"'
        ' office:version="1.2">'
        f"<office:body><office:spreadsheet>{tables}</office:spreadsheet>"
        "</office:body></office:document-content>"
    )
    manifest = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<manifest:manifest"
        ' xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"'
        ' manifest:version="1.2">'
        '<manifest:file-entry manifest:full-path="/"'
        ' manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/>'
        '<manifest:file-entry manifest:full-path="content.xml"'
        ' manifest:media-type="text/xml"/>'
        "</manifest:manifest>"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(
            zipfile.ZipInfo("mimetype"),
            "application/vnd.oasis.opendocument.spreadsheet",
            compress_type=zipfile.ZIP_STORED,
        )
        archive.writestr("content.xml", content, compress_type=zipfile.ZIP_DEFLATED)
        archive.writestr(
            "META-INF/manifest.xml", manifest, compress_type=zipfile.ZIP_DEFLATED
        )
    return buf.getvalue()


class TestIngestServiceExcelParsing(unittest.TestCase):
    """Test IngestService.parse_excel_files across workbook formats."""

    def test_parse_ods_workbook_yields_one_table_per_sheet(self):
        """An .ods upload is parsed like an .xlsx: one table per sheet,
        named filename_sheetname, with every column read as text."""
        import io

        ods_bytes = _minimal_ods(
            {
                "Patients": [["id", "name"], ["1", "Ann"]],
                "Visits": [["visit_id", "patient_id"], ["10", "1"]],
            }
        )
        upload = io.BytesIO(ods_bytes)
        upload.filename = "clinic.ods"

        dataframes, table_names, error = IngestService.parse_excel_files([upload])

        self.assertIsNone(error)
        self.assertEqual(table_names, ["clinic_Patients", "clinic_Visits"])
        self.assertEqual(dataframes[0].columns, ["id", "name"])
        self.assertEqual(dataframes[1].columns, ["visit_id", "patient_id"])
        self.assertEqual(dataframes[0]["id"].to_list(), ["1"])
        self.assertTrue(all(dtype == pl.Utf8 for dtype in dataframes[1].dtypes))


class TestProcessPkFkRelationships(unittest.TestCase):
    """Test IngestService.process_pk_fk_relationships orchestration."""

    def _mock_rdf_store(self):
        store = MagicMock()
        store.process_pk_fk_relationship.return_value = True
        return store

    def test_empty_data_returns_success(self):
        """Empty PK/FK data should return (True, [])."""
        success, messages = IngestService.process_pk_fk_relationships(
            [], self._mock_rdf_store()
        )
        self.assertTrue(success)
        self.assertEqual(messages, [])

    def test_none_data_returns_success(self):
        """None PK/FK data should return (True, [])."""
        success, messages = IngestService.process_pk_fk_relationships(
            None, self._mock_rdf_store()
        )
        self.assertTrue(success)
        self.assertEqual(messages, [])

    def test_incomplete_fk_is_skipped(self):
        """FK with missing foreignKeyColumn should be skipped, not crash."""
        data = [
            {
                "fileName": "visits.csv",
                "primaryKey": None,
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": None,
            },
        ]
        store = self._mock_rdf_store()
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertTrue(success)
        self.assertEqual(messages, [])
        store.process_pk_fk_relationship.assert_not_called()

    def test_fk_without_target_pk_is_skipped(self):
        """FK referencing a table with no PK should be skipped gracefully."""
        data = [
            {
                "fileName": "patients.csv",
                "primaryKey": None,
                "foreignKey": None,
                "foreignKeyTable": None,
                "foreignKeyColumn": None,
            },
            {
                "fileName": "visits.csv",
                "primaryKey": None,
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "patient_id",
            },
        ]
        store = self._mock_rdf_store()
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertTrue(success)
        self.assertEqual(messages, [])
        store.process_pk_fk_relationship.assert_not_called()

    def test_complete_relationship_is_processed(self):
        """A complete FK→PK relationship should call the RDF store."""
        data = [
            {
                "fileName": "patients.csv",
                "primaryKey": "patient_id",
                "foreignKey": None,
                "foreignKeyTable": None,
                "foreignKeyColumn": None,
            },
            {
                "fileName": "visits.csv",
                "primaryKey": None,
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "patient_id",
            },
        ]
        store = self._mock_rdf_store()
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertTrue(success)
        self.assertEqual(len(messages), 1)
        self.assertIn("visits", messages[0])
        self.assertIn("patients", messages[0])
        store.process_pk_fk_relationship.assert_called_once()

    def test_failed_relationship_lowers_success(self):
        """A failed RDF store insert should set overall success to False."""
        data = [
            {
                "fileName": "patients.csv",
                "primaryKey": "patient_id",
                "foreignKey": None,
                "foreignKeyTable": None,
                "foreignKeyColumn": None,
            },
            {
                "fileName": "visits.csv",
                "primaryKey": None,
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "patient_id",
            },
        ]
        store = self._mock_rdf_store()
        store.process_pk_fk_relationship.return_value = False
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertFalse(success)
        self.assertIn("Failed", messages[0])

    def test_multiple_relationships_all_processed(self):
        """Multiple FK relationships should each be processed."""
        data = [
            {
                "fileName": "patients.csv",
                "primaryKey": "patient_id",
                "foreignKey": None,
                "foreignKeyTable": None,
                "foreignKeyColumn": None,
            },
            {
                "fileName": "doctors.csv",
                "primaryKey": "doctor_id",
                "foreignKey": None,
                "foreignKeyTable": None,
                "foreignKeyColumn": None,
            },
            {
                "fileName": "visits.csv",
                "primaryKey": "visit_id",
                "foreignKey": "patient_id",
                "foreignKeyTable": "patients.csv",
                "foreignKeyColumn": "patient_id",
            },
            {
                "fileName": "prescriptions.csv",
                "primaryKey": None,
                "foreignKey": "visit_id",
                "foreignKeyTable": "visits.csv",
                "foreignKeyColumn": "visit_id",
            },
        ]
        store = self._mock_rdf_store()
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertTrue(success)
        self.assertEqual(len(messages), 2)
        self.assertEqual(store.process_pk_fk_relationship.call_count, 2)

    def test_excel_sheet_table_names_are_passed_through(self):
        """Excel sheet-based table names (filename_sheetname) should be
        passed to the RDF store after sanitisation."""
        data = [
            {
                "fileName": "data_Patients",
                "primaryKey": "id",
                "foreignKey": None,
                "foreignKeyTable": None,
                "foreignKeyColumn": None,
            },
            {
                "fileName": "data_Visits",
                "primaryKey": None,
                "foreignKey": "patient_id",
                "foreignKeyTable": "data_Patients",
                "foreignKeyColumn": "id",
            },
        ]
        store = self._mock_rdf_store()
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertTrue(success)
        # The store should receive the table names (sanitised)
        call_args = store.process_pk_fk_relationship.call_args
        fk_table = call_args[0][0]  # first positional arg
        pk_table = call_args[0][2]
        self.assertIn("data_Visits", fk_table)
        self.assertIn("data_Patients", pk_table)

    def test_fk_referencing_unknown_table_is_skipped(self):
        """FK referencing a table not in the PK/FK data should be skipped."""
        data = [
            {
                "fileName": "visits.csv",
                "primaryKey": None,
                "foreignKey": "patient_id",
                "foreignKeyTable": "unknown.csv",
                "foreignKeyColumn": "patient_id",
            },
        ]
        store = self._mock_rdf_store()
        success, messages = IngestService.process_pk_fk_relationships(data, store)
        self.assertTrue(success)
        self.assertEqual(messages, [])
        store.process_pk_fk_relationship.assert_not_called()


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


class TestAnnotateServiceExecution(unittest.TestCase):
    """Test annotation execution result handling."""

    @patch("annotation_helper.src.miscellaneous.add_annotation")
    def test_execute_annotation_returns_per_variable_results(self, mock_add_annotation):
        """Test that annotation execution preserves helper result details."""
        mock_add_annotation.return_value = {
            "gender": {"success": True, "value_mappings_inserted": True}
        }

        result, error = AnnotateService.execute_annotation(
            endpoint="http://example.test/statements",
            database="test_db",
            prefixes="PREFIX ncit: <http://example.test/>",
            annotated_variables={"gender": {"class": "ncit:C28421"}},
            temp_dir="/tmp/flyover-test-annotation",
        )

        self.assertIsNone(error)
        self.assertEqual(
            result,
            {"gender": {"success": True, "value_mappings_inserted": True}},
        )

    @patch("annotation_helper.src.miscellaneous.add_annotation")
    def test_execute_annotation_returns_error_on_exception(self, mock_add_annotation):
        """Test that annotation execution surfaces helper exceptions."""
        mock_add_annotation.side_effect = RuntimeError("rdf4j update failed")

        result, error = AnnotateService.execute_annotation(
            endpoint="http://example.test/statements",
            database="test_db",
            prefixes="",
            annotated_variables={"gender": {"class": "ncit:C28421"}},
            temp_dir="/tmp/flyover-test-annotation",
        )

        self.assertIsNone(result)
        self.assertEqual(error, "rdf4j update failed")


class TestRDFStoreServiceNameMatcher(unittest.TestCase):
    """Test RDFStoreService database name matching."""

    def test_exact_match(self):
        """Test exact database name match."""
        from services import RDFStoreService

        self.assertTrue(RDFStoreService.graph_database_find_name_match("mydb", "mydb"))

    def test_none_matches_all(self):
        """Test that None database name matches all databases."""
        from services import RDFStoreService

        self.assertTrue(RDFStoreService.graph_database_find_name_match(None, "any_db"))

    def test_empty_matches_all(self):
        """Test that empty string database name matches all databases."""
        from services import RDFStoreService

        self.assertTrue(RDFStoreService.graph_database_find_name_match("", "any_db"))

    def test_csv_extension_match(self):
        """Test matching with/without .csv extension."""
        from services import RDFStoreService

        self.assertTrue(
            RDFStoreService.graph_database_find_name_match("mydb.csv", "mydb")
        )
        self.assertTrue(
            RDFStoreService.graph_database_find_name_match("mydb", "mydb.csv")
        )

    def test_csv_strip_uses_slice_not_rstrip(self):
        """Test that .csv removal uses slice, not rstrip (which strips chars)."""
        from services import RDFStoreService

        # rstrip(".csv") would incorrectly strip 'v','s','c','.' chars
        # from names like "table_csv.csv" → "table_" instead of "table_csv"
        self.assertTrue(
            RDFStoreService.graph_database_find_name_match("table_csv.csv", "table_csv")
        )
        self.assertFalse(
            RDFStoreService.graph_database_find_name_match("table_csv.csv", "table_")
        )

    def test_no_match(self):
        """Test that different database names don't match."""
        from services import RDFStoreService

        self.assertFalse(RDFStoreService.graph_database_find_name_match("db_a", "db_b"))

    def test_substring_no_match(self):
        """Test that substring database names don't match."""
        from services import RDFStoreService

        self.assertFalse(
            RDFStoreService.graph_database_find_name_match(
                "synthetic_dutch_150", "synthetic_dutch_1500"
            )
        )


if __name__ == "__main__":
    unittest.main()
