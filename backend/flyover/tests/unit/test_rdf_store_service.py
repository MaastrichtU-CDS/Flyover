"""
Backend unit: services/rdf_store_service.py.

Tests cover all public methods of RDFStoreService with the
rdf_store_repository dependency mocked out.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.rdf_store_service import RDFStoreService


def _make_service():
    """Create an RDFStoreService with a mocked repository."""
    service = RDFStoreService.__new__(RDFStoreService)
    service.rdf_store_url = "http://localhost:7200"
    service.repo = "test_repo"
    service.repository = MagicMock()
    return service


class TestRDFStoreServiceCheckDataExists(unittest.TestCase):
    """Tests for check_data_exists."""

    def test_returns_true_when_repository_returns_true(self):
        """Delegates to repository and returns True."""
        service = _make_service()
        service.repository.check_data_graph_exists.return_value = True
        self.assertTrue(service.check_data_exists())

    def test_returns_false_when_repository_returns_false(self):
        """Returns False when no data graph exists."""
        service = _make_service()
        service.repository.check_data_graph_exists.return_value = False
        self.assertFalse(service.check_data_exists())

    def test_returns_false_on_exception(self):
        """Returns False when repository raises an exception."""
        service = _make_service()
        service.repository.check_data_graph_exists.side_effect = RuntimeError("fail")
        self.assertFalse(service.check_data_exists())


class TestRDFStoreServiceGetDatabases(unittest.TestCase):
    """Tests for get_databases."""

    def test_returns_list_from_repository(self):
        """Returns the list returned by the repository."""
        service = _make_service()
        service.repository.get_database_names.return_value = ["db_a", "db_b"]
        result = service.get_databases()
        self.assertEqual(result, ["db_a", "db_b"])

    def test_returns_empty_list_when_none(self):
        """Returns empty list when repository returns empty."""
        service = _make_service()
        service.repository.get_database_names.return_value = []
        result = service.get_databases()
        self.assertEqual(result, [])


class TestRDFStoreServiceGetColumnInfoByDatabase(unittest.TestCase):
    """Tests for get_column_info_by_database."""

    def test_returns_empty_dict_when_repository_returns_none(self):
        """Returns {} when repository returns None."""
        service = _make_service()
        service.repository.get_column_info.return_value = None
        result = service.get_column_info_by_database()
        self.assertEqual(result, {})

    def test_groups_columns_by_database(self):
        """Parses DataFrame and groups columns by database extracted from URI."""
        service = _make_service()
        df = pl.DataFrame(
            {
                "uri": [
                    "http://data.local/patients.csv",
                    "http://data.local/patients.csv",
                    "http://data.local/labs.csv",
                ],
                "column": ["age", "sex", "result"],
            }
        )
        service.repository.get_column_info.return_value = df
        result = service.get_column_info_by_database()

        self.assertIn("patients", result)
        self.assertIn("labs", result)
        self.assertIn("age", result["patients"])
        self.assertIn("sex", result["patients"])
        self.assertIn("result", result["labs"])


class TestRDFStoreServiceInsertEquivalencies(unittest.TestCase):
    """Tests for insert_equivalencies."""

    def test_returns_false_for_empty_var_info(self):
        """Empty var_info returns False without calling repository."""
        service = _make_service()
        result = service.insert_equivalencies("var", "db", {})
        self.assertFalse(result)
        service.repository.insert_equivalency.assert_not_called()

    def test_returns_false_for_placeholder_only_info(self):
        """Info with only placeholder values returns False."""
        service = _make_service()
        var_info = {
            "type": "Variable type: ",
            "description": "Variable description: ",
            "comments": "Variable comment: No comment provided",
        }
        result = service.insert_equivalencies("var", "db", var_info)
        self.assertFalse(result)

    def test_returns_true_when_has_real_type(self):
        """Info with a real type inserts and returns True."""
        service = _make_service()
        service.repository.insert_equivalency.return_value = "ok"
        var_info = {"type": "Variable type: categorical"}
        result = service.insert_equivalencies("var", "db", var_info)
        self.assertTrue(result)
        service.repository.insert_equivalency.assert_called_once()

    def test_returns_true_when_has_real_description(self):
        """Info with a real description inserts and returns True."""
        service = _make_service()
        service.repository.insert_equivalency.return_value = "ok"
        var_info = {"description": "Variable description: Some meaningful text"}
        result = service.insert_equivalencies("var", "db", var_info)
        self.assertTrue(result)

    def test_returns_false_when_repository_returns_none(self):
        """Returns False when repository returns None."""
        service = _make_service()
        service.repository.insert_equivalency.return_value = None
        var_info = {"type": "Variable type: continuous"}
        result = service.insert_equivalencies("var", "db", var_info)
        self.assertFalse(result)


class TestRDFStoreServiceProcessPkFkRelationship(unittest.TestCase):
    """Tests for process_pk_fk_relationship."""

    def test_returns_false_when_source_uri_missing(self):
        """Returns False when fk column URI is not found."""
        service = _make_service()
        service.repository.get_column_class_uri.side_effect = [None, "uri://pk"]
        result = service.process_pk_fk_relationship("fk_t", "fk_c", "pk_t", "pk_c")
        self.assertFalse(result)

    def test_returns_false_when_target_uri_missing(self):
        """Returns False when pk column URI is not found."""
        service = _make_service()
        service.repository.get_column_class_uri.side_effect = ["uri://fk", None]
        result = service.process_pk_fk_relationship("fk_t", "fk_c", "pk_t", "pk_c")
        self.assertFalse(result)

    def test_returns_true_when_both_uris_found(self):
        """Returns True when relationship is successfully inserted."""
        service = _make_service()
        service.repository.get_column_class_uri.side_effect = [
            "uri://fk",
            "uri://pk",
        ]
        service.repository.insert_fk_relation.return_value = "ok"
        result = service.process_pk_fk_relationship("fk_t", "fk_c", "pk_t", "pk_c")
        self.assertTrue(result)

    def test_returns_false_when_insert_returns_none(self):
        """Returns False when insert operation returns None."""
        service = _make_service()
        service.repository.get_column_class_uri.side_effect = [
            "uri://fk",
            "uri://pk",
        ]
        service.repository.insert_fk_relation.return_value = None
        result = service.process_pk_fk_relationship("fk_t", "fk_c", "pk_t", "pk_c")
        self.assertFalse(result)


class TestRDFStoreServiceDownloadOntologies(unittest.TestCase):
    """Tests for download_ontologies."""

    def test_returns_content_for_successful_download(self):
        """Returns list with (filename, content) for a successful download."""
        service = _make_service()
        service.repository.get_ontology_graphs.return_value = ["patients"]
        service.repository.download_ontology.return_value = ("<rdf>...</rdf>", 200)

        results, failed = service.download_ontologies()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "local_ontology_patients.nt")
        self.assertEqual(failed, [])

    def test_records_failure_for_non_200_status(self):
        """Records a failure when the download returns non-200 status."""
        service = _make_service()
        service.repository.download_ontology.return_value = (None, 404)

        results, failed = service.download_ontologies(databases=["missing_db"])

        self.assertEqual(results, [])
        self.assertIn("missing_db", failed)

    def test_records_failure_for_empty_content(self):
        """Records a failure when content is empty."""
        service = _make_service()
        service.repository.download_ontology.return_value = ("   ", 200)

        results, failed = service.download_ontologies(databases=["empty_db"])

        self.assertEqual(results, [])
        self.assertIn("empty_db", failed)

    def test_queries_repository_when_databases_is_none(self):
        """Queries the repository for graphs when databases argument is None."""
        service = _make_service()
        service.repository.get_ontology_graphs.return_value = []

        results, failed = service.download_ontologies(databases=None)

        service.repository.get_ontology_graphs.assert_called_once()
        self.assertEqual(results, [])
        self.assertEqual(failed, [])


class TestRDFStoreServiceExecuteQuery(unittest.TestCase):
    """Tests for execute_query."""

    def test_returns_result_from_repository(self):
        """Returns query result string from repository."""
        service = _make_service()
        service.repository.execute_query.return_value = "query_result"
        result = service.execute_query("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(result, "query_result")

    def test_raises_exception_when_repository_returns_none(self):
        """Raises Exception when repository returns None."""
        service = _make_service()
        service.repository.execute_query.return_value = None
        with self.assertRaises(Exception):
            service.execute_query("SELECT ?s WHERE { ?s ?p ?o }")


class TestRDFStoreServiceProcessCrossGraphRelationships(unittest.TestCase):
    """Tests for process_cross_graph_relationships."""

    def test_empty_data_returns_success(self):
        """Empty cross-graph data returns (True, '') without calling repository."""
        service = _make_service()
        success, msg = service.process_cross_graph_relationships({})
        self.assertTrue(success)
        self.assertEqual(msg, "")

    def test_successful_relationship_returns_true_with_message(self):
        """Successful relationship returns True with a descriptive message."""
        service = _make_service()
        service.repository.get_column_class_uri.side_effect = [
            "uri://new",
            "uri://existing",
        ]
        service.repository.insert_cross_graph_relation.return_value = "ok"

        data = {
            "newTableName": "new_t",
            "newColumnName": "new_c",
            "existingTableName": "ex_t",
            "existingColumnName": "ex_c",
        }
        success, msg = service.process_cross_graph_relationships(data)
        self.assertTrue(success)
        self.assertIn("new_t", msg)

    def test_failed_relationship_returns_false_with_message(self):
        """Failed relationship returns False with an error message."""
        service = _make_service()
        service.repository.get_column_class_uri.side_effect = [None, None]

        data = {
            "newTableName": "new_t",
            "newColumnName": "new_c",
            "existingTableName": "ex_t",
            "existingColumnName": "ex_c",
        }
        success, msg = service.process_cross_graph_relationships(data)
        self.assertFalse(success)

    def test_missing_key_returns_false_with_error_message(self):
        """Missing key in cross_graph_data returns False."""
        service = _make_service()
        data = {"newTableName": "nt"}  # Missing required keys
        success, msg = service.process_cross_graph_relationships(data)
        self.assertFalse(success)
        self.assertIn("Error", msg)


class TestRDFStoreServiceGraphDatabaseFindNameMatch(unittest.TestCase):
    """Tests for the static graph_database_find_name_match method."""

    def test_none_matches_all(self):
        """None map_database_name matches any target."""
        self.assertTrue(RDFStoreService.graph_database_find_name_match(None, "any_db"))

    def test_empty_string_matches_all(self):
        """Empty string matches any target."""
        self.assertTrue(RDFStoreService.graph_database_find_name_match("", "any_db"))

    def test_exact_match(self):
        """Exact string match returns True."""
        self.assertTrue(
            RDFStoreService.graph_database_find_name_match("my_db", "my_db")
        )

    def test_csv_extension_fallback(self):
        """Matching with .csv extension stripped returns True."""
        self.assertTrue(
            RDFStoreService.graph_database_find_name_match("my_db.csv", "my_db")
        )
        self.assertTrue(
            RDFStoreService.graph_database_find_name_match("my_db", "my_db.csv")
        )

    def test_different_names_no_match(self):
        """Different database names return False."""
        self.assertFalse(RDFStoreService.graph_database_find_name_match("db_a", "db_b"))

    def test_substring_no_match(self):
        """Substring relationship does not match."""
        self.assertFalse(
            RDFStoreService.graph_database_find_name_match(
                "short_name", "short_name_extended"
            )
        )


class TestRDFStoreServiceVerifyAnnotation(unittest.TestCase):
    """Tests for verify_annotation."""

    def test_delegates_to_repository(self):
        """verify_annotation delegates to repository."""
        service = _make_service()
        service.repository.verify_annotation.return_value = (True, True, "ASK {}")
        result = service.verify_annotation("db", "col", "ncit:C123")
        self.assertEqual(result, (True, True, "ASK {}"))
        service.repository.verify_annotation.assert_called_once_with(
            "db", "col", "ncit:C123", None, None
        )

    def test_passes_optional_args(self):
        """Optional value_mapping and prefixes are passed through."""
        service = _make_service()
        service.repository.verify_annotation.return_value = (True, False, "ASK {}")
        value_mapping = {"terms": {"male": {"targetClass": "ncit:C20197"}}}
        prefixes = {"ncit": "http://ncicb.nci.nih.gov/..."}
        service.verify_annotation("db", "col", "ncit:C123", value_mapping, prefixes)
        service.repository.verify_annotation.assert_called_once_with(
            "db", "col", "ncit:C123", value_mapping, prefixes
        )


if __name__ == "__main__":
    unittest.main()
