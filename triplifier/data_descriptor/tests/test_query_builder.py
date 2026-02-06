"""
Unit tests for the QueryBuilder class.

Tests cover all SPARQL query construction methods to ensure
correct query generation for various operations.
"""

import sys
import unittest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from repositories import QueryBuilder


class TestQueryBuilderSanitization(unittest.TestCase):
    """Test the SPARQL sanitization functionality."""

    def test_sanitize_sparql_value_none(self):
        """Test sanitizing None value."""
        result = QueryBuilder.sanitize_sparql_value(None)
        self.assertEqual(result, "")

    def test_sanitize_sparql_value_simple(self):
        """Test sanitizing simple string."""
        result = QueryBuilder.sanitize_sparql_value("simple_value")
        self.assertEqual(result, "simple_value")

    def test_sanitize_sparql_value_quotes(self):
        """Test sanitizing string with quotes."""
        result = QueryBuilder.sanitize_sparql_value("test'value")
        self.assertEqual(result, "test\\'value")

    def test_sanitize_sparql_value_double_quotes(self):
        """Test sanitizing string with double quotes."""
        result = QueryBuilder.sanitize_sparql_value('test"value')
        self.assertEqual(result, 'test\\"value')

    def test_sanitize_sparql_value_backslash(self):
        """Test sanitizing string with backslash."""
        result = QueryBuilder.sanitize_sparql_value("test\\value")
        self.assertEqual(result, "test\\\\value")

    def test_validate_identifier_valid(self):
        """Test validating a valid identifier."""
        result = QueryBuilder.validate_identifier("test_table_1")
        self.assertEqual(result, "test_table_1")

    def test_validate_identifier_with_hyphen(self):
        """Test validating identifier with hyphen."""
        result = QueryBuilder.validate_identifier("test-table")
        self.assertEqual(result, "test-table")

    def test_validate_identifier_empty(self):
        """Test validating empty identifier raises error."""
        with self.assertRaises(ValueError):
            QueryBuilder.validate_identifier("")

    def test_validate_identifier_invalid_chars(self):
        """Test validating identifier with invalid characters."""
        with self.assertRaises(ValueError):
            QueryBuilder.validate_identifier("test'; DROP TABLE users;--")


class TestQueryBuilderPrefixes(unittest.TestCase):
    """Test the PREFIX building functionality."""

    def test_build_default_prefixes(self):
        """Test building default prefixes."""
        prefixes = QueryBuilder.build_prefixes()

        self.assertIn("PREFIX dbo:", prefixes)
        self.assertIn("PREFIX rdf:", prefixes)
        self.assertIn("PREFIX rdfs:", prefixes)
        self.assertIn("PREFIX owl:", prefixes)
        self.assertIn("http://um-cds/ontologies/databaseontology/", prefixes)

    def test_build_prefixes_with_additional(self):
        """Test building prefixes with additional ones."""
        additional = {
            "custom": "http://example.org/custom/",
            "test": "http://example.org/test/",
        }
        prefixes = QueryBuilder.build_prefixes(additional)

        self.assertIn("PREFIX custom:", prefixes)
        self.assertIn("PREFIX test:", prefixes)
        self.assertIn("http://example.org/custom/", prefixes)


class TestQueryBuilderColumnQueries(unittest.TestCase):
    """Test column-related query construction."""

    def test_column_info_query(self):
        """Test column info query generation."""
        query = QueryBuilder.column_info_query()

        self.assertIn("SELECT ?uri ?column", query)
        self.assertIn("dbo:column", query)

    def test_column_class_uri_query(self):
        """Test column class URI query generation."""
        query = QueryBuilder.column_class_uri_query("test_table", "test_column")

        self.assertIn("test_table", query)
        self.assertIn("test_column", query)
        self.assertIn("SELECT ?uri", query)
        self.assertIn("FILTER", query)

    def test_categories_query(self):
        """Test categories query generation."""
        query = QueryBuilder.categories_query("test_repo", "test_column")

        self.assertIn("test_repo", query)
        self.assertIn("test_column", query)
        self.assertIn("SELECT ?value", query)
        self.assertIn("COUNT(?value)", query)
        self.assertIn("GROUP BY", query)


class TestQueryBuilderDatabaseQueries(unittest.TestCase):
    """Test database-related query construction."""

    def test_database_name_query(self):
        """Test database name query generation."""
        query = QueryBuilder.database_name_query()

        self.assertIn("SELECT ?db", query)
        self.assertIn("dbo:table", query)
        self.assertIn("rdfs:subClassOf", query)

    def test_check_data_graph_exists_query(self):
        """Test data graph existence check query."""
        query = QueryBuilder.check_data_graph_exists_query()

        self.assertIn("ASK", query)
        self.assertIn("GRAPH ?g", query)
        self.assertIn("FILTER", query)
        self.assertIn("http://data.local/", query)


class TestQueryBuilderInsertQueries(unittest.TestCase):
    """Test INSERT query construction."""

    def test_insert_equivalency_query(self):
        """Test equivalency INSERT query generation."""
        query = QueryBuilder.insert_equivalency_query(
            "test_repo", "test_var", "test_db", ["value1", "value2"]
        )

        self.assertIn("INSERT", query)
        self.assertIn("GRAPH", query)
        self.assertIn("http://ontology.local/test_db/", query)
        self.assertIn("owl:equivalentClass", query)
        self.assertIn("test_var", query)

    def test_fk_relation_insert_query(self):
        """Test FK relation INSERT query generation."""
        query = QueryBuilder.fk_relation_insert_query(
            "http://example.org/fk_predicate",
            "http://example.org/source_uri",
            "http://example.org/target_uri",
        )

        self.assertIn("INSERT", query)
        self.assertIn("GRAPH", query)
        self.assertIn("http://relationships.local/", query)
        self.assertIn("fk_predicate", query)
        self.assertIn("source_uri", query)
        self.assertIn("target_uri", query)

    def test_fk_relation_insert_query_custom_graph(self):
        """Test FK relation INSERT with custom graph."""
        query = QueryBuilder.fk_relation_insert_query(
            "http://example.org/pred",
            "http://example.org/src",
            "http://example.org/tgt",
            "http://custom.graph/",
        )

        self.assertIn("http://custom.graph/", query)

    def test_cross_graph_relation_insert_query(self):
        """Test cross-graph relation INSERT query."""
        query = QueryBuilder.cross_graph_relation_insert_query(
            "http://example.org/pred",
            "http://example.org/new_col",
            "http://example.org/existing_col",
        )

        self.assertIn("INSERT", query)
        self.assertIn("new_col", query)
        self.assertIn("existing_col", query)


class TestQueryBuilderGraphQueries(unittest.TestCase):
    """Test graph-related query construction."""

    def test_graph_structure_query(self):
        """Test graph structure query generation."""
        query = QueryBuilder.graph_structure_query()

        self.assertIn("SELECT ?uri ?column", query)
        self.assertIn("dbo:column", query)

    def test_ontology_graphs_query(self):
        """Test ontology graphs query generation."""
        query = QueryBuilder.ontology_graphs_query()

        self.assertIn("SELECT DISTINCT ?g", query)
        self.assertIn("GRAPH ?g", query)
        self.assertIn("http://ontology.local/", query)


class TestQueryBuilderAnnotationQueries(unittest.TestCase):
    """Test annotation-related query construction."""

    def test_annotation_ask_query_basic(self):
        """Test basic annotation ASK query."""
        query = QueryBuilder.annotation_ask_query(
            "test_db",
            "test_column",
            "ncit:C12345",
            prefixes="PREFIX db: <http://test/>",
        )

        self.assertIn("ASK", query)
        self.assertIn("test_db.test_column", query)
        self.assertIn("owl:equivalentClass", query)
        self.assertIn("ncit:C12345", query)

    def test_annotation_ask_query_with_value_mapping(self):
        """Test annotation ASK query with value mapping."""
        value_mapping = {
            "terms": {
                "male": {"local_term": "M", "target_class": "ncit:Male"},
                "female": {"local_term": "F", "target_class": "ncit:Female"},
            }
        }

        query = QueryBuilder.annotation_ask_query(
            "test_db",
            "sex_column",
            "ncit:Sex",
            value_mapping=value_mapping,
            prefixes="PREFIX ncit: <http://ncit/>",
        )

        self.assertIn("ASK", query)
        self.assertIn("rdfs:subClassOf", query)
        self.assertIn("ncit:Male", query)
        self.assertIn("ncit:Female", query)

    def test_annotation_ask_query_empty_value_mapping(self):
        """Test ASK query with empty value mapping."""
        query = QueryBuilder.annotation_ask_query(
            "test_db",
            "test_col",
            "ncit:Test",
            value_mapping={},
        )

        self.assertIn("ASK", query)
        self.assertNotIn("rdfs:subClassOf", query)


if __name__ == "__main__":
    unittest.main()
