"""
Unit tests for repositories/rdf_store_repository.py.

Tests cover all public methods of RDFStoreRepository with HTTP calls mocked.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from repositories.rdf_store_repository import RDFStoreRepository


def _make_repo() -> RDFStoreRepository:
    """Return an RDFStoreRepository with a concrete URL/repo."""
    return RDFStoreRepository("http://localhost:7200", "test_repo")


class TestParseCsvResult(unittest.TestCase):
    """Tests for _parse_csv_result (internal helper)."""

    def setUp(self):
        self.repo = _make_repo()

    def test_valid_csv_returns_dataframe(self):
        """Valid CSV string returns a Polars DataFrame."""
        csv = "uri,column\nhttp://data.local/t.csv,age\n"
        df = self.repo._parse_csv_result(csv)
        self.assertIsNotNone(df)
        self.assertIn("uri", df.columns)
        self.assertIn("column", df.columns)

    def test_none_returns_none(self):
        """None input returns None."""
        self.assertIsNone(self.repo._parse_csv_result(None))

    def test_empty_string_returns_none(self):
        """Empty string returns None."""
        self.assertIsNone(self.repo._parse_csv_result(""))

    def test_whitespace_only_returns_none(self):
        """Whitespace-only string returns None."""
        self.assertIsNone(self.repo._parse_csv_result("   "))

    def test_malformed_csv_returns_none(self):
        """Malformed CSV that Polars cannot parse returns None."""
        # Create input that causes Polars to raise (e.g., no data, only header)
        # An empty CSV with a header row is valid, but a completely unparseable one is not
        # We can force failure by monkeypatching
        with patch(
            "repositories.rdf_store_repository.pl.read_csv",
            side_effect=Exception("parse error"),
        ):
            result = self.repo._parse_csv_result("a,b\n1,2")
        self.assertIsNone(result)


class TestExecuteQuery(unittest.TestCase):
    """Tests for execute_query."""

    def setUp(self):
        self.repo = _make_repo()

    @patch("repositories.rdf_store_repository.requests.post")
    def test_returns_response_text_on_success(self, mock_post):
        """Returns response text when the request succeeds."""
        mock_response = MagicMock()
        mock_response.text = "query_result_csv"
        mock_post.return_value = mock_response

        result = self.repo.execute_query("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertEqual(result, "query_result_csv")

    @patch("repositories.rdf_store_repository.requests.post")
    def test_posts_to_correct_endpoint(self, mock_post):
        """Posts to the correct SPARQL endpoint URL."""
        mock_response = MagicMock()
        mock_response.text = ""
        mock_post.return_value = mock_response

        self.repo.execute_query(
            "SELECT ?s WHERE { ?s ?p ?o }", endpoint_suffix="/statements"
        )

        call_args = mock_post.call_args
        self.assertIn("/repositories/test_repo/statements", call_args[0][0])

    @patch("repositories.rdf_store_repository.requests.post")
    def test_returns_none_on_exception(self, mock_post):
        """Returns None when the request raises an exception."""
        mock_post.side_effect = ConnectionError("timeout")
        result = self.repo.execute_query("SELECT ?s WHERE { ?s ?p ?o }")
        self.assertIsNone(result)


class TestExecuteAskQuery(unittest.TestCase):
    """Tests for execute_ask_query."""

    def setUp(self):
        self.repo = _make_repo()

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_true_when_ask_is_true(self, mock_get):
        """Returns True when the ASK query evaluates to true."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"boolean": True}
        mock_get.return_value = mock_response

        result = self.repo.execute_ask_query("ASK { ?s ?p ?o }")
        self.assertTrue(result)

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_false_when_ask_is_false(self, mock_get):
        """Returns False when the ASK query evaluates to false."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"boolean": False}
        mock_get.return_value = mock_response

        result = self.repo.execute_ask_query("ASK { ?s ?p ?o }")
        self.assertFalse(result)

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_none_on_non_200_status(self, mock_get):
        """Returns None when the response status is not 200."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = self.repo.execute_ask_query("ASK { ?s ?p ?o }")
        self.assertIsNone(result)

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_none_on_exception(self, mock_get):
        """Returns None when the request raises an exception."""
        mock_get.side_effect = ConnectionError("timeout")
        result = self.repo.execute_ask_query("ASK { ?s ?p ?o }")
        self.assertIsNone(result)


class TestCheckDataGraphExists(unittest.TestCase):
    """Tests for check_data_graph_exists."""

    def setUp(self):
        self.repo = _make_repo()

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_true_when_data_exists(self, mock_get):
        """Returns True when the ASK query confirms data exists."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"boolean": True}
        mock_get.return_value = mock_response

        self.assertTrue(self.repo.check_data_graph_exists())

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_false_when_no_data(self, mock_get):
        """Returns False when the ASK query says no data exists."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"boolean": False}
        mock_get.return_value = mock_response

        self.assertFalse(self.repo.check_data_graph_exists())

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_false_on_non_200(self, mock_get):
        """Returns False for a non-200 HTTP response."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_get.return_value = mock_response

        self.assertFalse(self.repo.check_data_graph_exists())

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_false_on_exception(self, mock_get):
        """Returns False when an exception occurs."""
        mock_get.side_effect = RuntimeError("connection refused")
        self.assertFalse(self.repo.check_data_graph_exists())


class TestGetDatabaseNames(unittest.TestCase):
    """Tests for get_database_names."""

    def setUp(self):
        self.repo = _make_repo()

    def test_returns_list_of_database_names(self):
        """Extracts unique database names from CSV result."""
        csv = "db\npatients\nlabs\npatients\n"
        with patch.object(self.repo, "execute_query", return_value=csv):
            result = self.repo.get_database_names()
        self.assertIn("patients", result)
        self.assertIn("labs", result)

    def test_returns_empty_list_when_no_data(self):
        """Returns empty list when no query results."""
        with patch.object(self.repo, "execute_query", return_value=None):
            result = self.repo.get_database_names()
        self.assertEqual(result, [])

    def test_filters_empty_values(self):
        """Filters out empty or None database names."""
        csv = "db\npatients\n\n"
        with patch.object(self.repo, "execute_query", return_value=csv):
            result = self.repo.get_database_names()
        # Should only contain non-empty names
        self.assertTrue(all(n for n in result))


class TestGetColumnClassUri(unittest.TestCase):
    """Tests for get_column_class_uri."""

    def setUp(self):
        self.repo = _make_repo()

    def test_returns_uri_string(self):
        """Returns the URI string from the first result row."""
        csv = "uri\nhttp://ontology.local/patients.csv_age\n"
        with patch.object(self.repo, "execute_query", return_value=csv):
            result = self.repo.get_column_class_uri("patients", "age")
        self.assertEqual(result, "http://ontology.local/patients.csv_age")

    def test_returns_none_when_no_results(self):
        """Returns None when no results are found."""
        with patch.object(self.repo, "execute_query", return_value=None):
            result = self.repo.get_column_class_uri("patients", "age")
        self.assertIsNone(result)

    def test_returns_none_when_uri_column_missing(self):
        """Returns None when the CSV result has no 'uri' column."""
        csv = "other_column\nvalue\n"
        with patch.object(self.repo, "execute_query", return_value=csv):
            result = self.repo.get_column_class_uri("patients", "age")
        self.assertIsNone(result)


class TestGetGraphStructure(unittest.TestCase):
    """Tests for get_graph_structure."""

    def setUp(self):
        self.repo = _make_repo()

    def test_returns_tables_and_columns(self):
        """Returns dict with tables list and tableColumns mapping."""
        csv = (
            "uri,column\n"
            "http://ontology.local/patients.csv,age\n"
            "http://ontology.local/patients.csv,sex\n"
            "http://ontology.local/labs.csv,result\n"
        )
        with patch.object(self.repo, "execute_query", return_value=csv):
            result = self.repo.get_graph_structure()

        self.assertIn("tables", result)
        self.assertIn("tableColumns", result)
        self.assertIn("patients", result["tables"])
        self.assertIn("labs", result["tables"])
        self.assertIn("age", result["tableColumns"]["patients"])

    def test_returns_empty_structure_when_no_data(self):
        """Returns empty structure when no query results."""
        with patch.object(self.repo, "execute_query", return_value=None):
            result = self.repo.get_graph_structure()
        self.assertEqual(result, {"tables": [], "tableColumns": {}})


class TestGetOntologyGraphs(unittest.TestCase):
    """Tests for get_ontology_graphs."""

    def setUp(self):
        self.repo = _make_repo()

    def test_returns_database_names_from_graph_uris(self):
        """Extracts database names from ontology graph URIs."""
        csv = "g\nhttp://ontology.local/patients/\nhttp://ontology.local/labs/\n"
        with patch.object(self.repo, "execute_query", return_value=csv):
            result = self.repo.get_ontology_graphs()
        self.assertIn("patients", result)
        self.assertIn("labs", result)

    def test_returns_empty_list_when_no_results(self):
        """Returns empty list when no ontology graphs found."""
        with patch.object(self.repo, "execute_query", return_value=None):
            result = self.repo.get_ontology_graphs()
        self.assertEqual(result, [])


class TestDownloadOntology(unittest.TestCase):
    """Tests for download_ontology."""

    def setUp(self):
        self.repo = _make_repo()

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_content_and_status_200(self, mock_get):
        """Returns (content, 200) on a successful download."""
        mock_response = MagicMock()
        mock_response.text = "<rdf>...</rdf>"
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        content, status = self.repo.download_ontology("http://ontology.local/patients/")
        self.assertEqual(status, 200)
        self.assertEqual(content, "<rdf>...</rdf>")

    @patch("repositories.rdf_store_repository.requests.get")
    def test_returns_none_and_500_on_exception(self, mock_get):
        """Returns (None, 500) when the request raises an exception."""
        mock_get.side_effect = ConnectionError("timeout")
        content, status = self.repo.download_ontology("http://ontology.local/patients/")
        self.assertIsNone(content)
        self.assertEqual(status, 500)


class TestVerifyAnnotation(unittest.TestCase):
    """Tests for verify_annotation."""

    def setUp(self):
        self.repo = _make_repo()

    def test_returns_success_true_when_ask_returns_result(self):
        """Returns (True, boolean_result, query) when ASK succeeds."""
        with patch.object(self.repo, "execute_ask_query", return_value=True):
            success, is_valid, query = self.repo.verify_annotation(
                "patients", "sex", "ncit:C28421"
            )
        self.assertTrue(success)
        self.assertTrue(is_valid)
        self.assertIsInstance(query, str)

    def test_returns_success_false_when_ask_returns_none(self):
        """Returns (False, None, query) when ASK query fails."""
        with patch.object(self.repo, "execute_ask_query", return_value=None):
            success, is_valid, query = self.repo.verify_annotation(
                "patients", "sex", "ncit:C28421"
            )
        self.assertFalse(success)
        self.assertIsNone(is_valid)

    def test_builds_query_with_additional_prefixes(self):
        """Query is built with provided additional prefixes."""
        with patch.object(self.repo, "execute_ask_query", return_value=True):
            success, _, query = self.repo.verify_annotation(
                "patients",
                "sex",
                "ncit:C28421",
                additional_prefixes={"ncit": "http://ncicb.nci.nih.gov/..."},
            )
        self.assertTrue(success)
        self.assertIn("ncit", query)


if __name__ == "__main__":
    unittest.main()
