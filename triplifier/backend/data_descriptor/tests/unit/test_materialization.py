"""
Unit tests for the materialization functionality.

Tests cover:
- SPARQL query generation for owl:equivalentClass materialization
- SPARQL query generation for value mapping ?term materialization
- Configuration flag behavior (materialize on/off)
- Graph URI construction for materialization
- Integration with add_annotation when materialization is enabled
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from annotation_helper.src.miscellaneous import (
    materialize_inferences,
    _materialize_equivalent_class,
    _materialize_schema_reconstruction_predicates,
    _materialize_value_mapping,
    read_file,
)


class TestMaterializeEquivalentClass(unittest.TestCase):
    """Test the owl:equivalentClass materialization query generation."""

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_replaces_graph_uris(self, mock_post):
        """Test that annotation and materialized graph URIs are correctly substituted."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_equivalent_class(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        self.assertIn("http://annotation.local/testdb/", query)
        self.assertIn("http://materialized.local/testdb/", query)
        self.assertNotIn("ANNOTATION_GRAPH_URI", query)
        self.assertNotIn("MATERIALIZED_GRAPH_URI", query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_contains_equivalent_class_pattern(self, mock_post):
        """Test that the query contains the owl:equivalentClass to rdf:type pattern."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_equivalent_class(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        self.assertIn("owl:equivalentClass", query)
        self.assertIn("rdf:type", query)
        self.assertIn("FILTER(!isBlank(?equivalentClass))", query)
        self.assertIn("GRAPH ?dataGraph", query)
        self.assertIn('FILTER(STRSTARTS(STR(?dataGraph), "http://data.local/"))', query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_inserts_into_materialized_graph(self, mock_post):
        """Test that the INSERT targets the materialized graph, not the annotation graph."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_equivalent_class(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        # The INSERT should target the materialized graph
        insert_section = query.split("WHERE")[0]
        self.assertIn("http://materialized.local/testdb/", insert_section)

        # The WHERE should read from the annotation graph
        where_section = query.split("WHERE")[1]
        self.assertIn("http://annotation.local/testdb/", where_section)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_posts_query_to_endpoint(self, mock_post):
        """Test that the query is posted to the endpoint."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_equivalent_class(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertEqual(
            call_args[0][0],
            "http://localhost:7200/repositories/test/statements",
        )
        self.assertEqual(call_args[1]["data"], {"update": query})

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_scopes_instance_lookup_to_data_graphs(self, mock_post):
        """Test that the query reads instance data from explicit data named graphs."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_equivalent_class(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        where_section = query.split("WHERE")[1]
        self.assertIn("GRAPH ?dataGraph", where_section)
        self.assertIn("?instance rdf:type ?class .", where_section)
        self.assertIn(
            'FILTER(STRSTARTS(STR(?dataGraph), "http://data.local/"))',
            where_section,
        )


class TestMaterializeValueMapping(unittest.TestCase):
    """Test the value mapping ?term materialization query generation."""

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_replaces_graph_uris(self, mock_post):
        """Test that annotation and materialized graph URIs are correctly substituted."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_value_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        self.assertIn("http://annotation.local/testdb/", query)
        self.assertIn("http://materialized.local/testdb/", query)
        self.assertNotIn("ANNOTATION_GRAPH_URI", query)
        self.assertNotIn("MATERIALIZED_GRAPH_URI", query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_contains_term_pattern(self, mock_post):
        """Test that the query targets the ?term variable with rdf:type."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_value_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        self.assertIn("?term", query)
        self.assertIn("?valueCell rdf:type ?term", query)
        self.assertIn("rdfs:subClassOf", query)
        self.assertIn("owl:intersectionOf", query)
        self.assertIn("GRAPH ?dataGraph", query)
        self.assertIn('FILTER(STRSTARTS(STR(?dataGraph), "http://data.local/"))', query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_uses_materialized_superclass_type(self, mock_post):
        """Test that the query reads ?superClass typing from the materialized graph."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_value_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        where_section = query.split("WHERE")[1]
        self.assertIn("http://materialized.local/testdb/", where_section)
        self.assertIn("?instance rdf:type ?superClass .", where_section)
        self.assertIn("dbo:has_value", where_section)
        self.assertIn("GRAPH ?dataGraph", where_section)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_scopes_value_matching_to_data_graphs(self, mock_post):
        """Test that value matching reads values from explicit data named graphs."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_value_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        where_section = query.split("WHERE")[1]
        self.assertIn("GRAPH ?dataGraph", where_section)
        self.assertIn("?instance dbo:has_cell ?valueCell .", where_section)
        self.assertIn("?valueCell dbo:has_value ?localValue .", where_section)
        self.assertIn(
            'FILTER(STRSTARTS(STR(?dataGraph), "http://data.local/"))',
            where_section,
        )

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_inserts_into_materialized_graph(self, mock_post):
        """Test that INSERT targets the materialized graph."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_value_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        insert_section = query.split("WHERE")[0]
        self.assertIn("http://materialized.local/testdb/", insert_section)


class TestMaterializeSchemaReconstructionPredicates(unittest.TestCase):
    """Test schema reconstruction predicate-edge materialization query generation."""

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_replaces_graph_uris(self, mock_post):
        """Test that annotation and materialized graph URIs are correctly substituted."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_schema_reconstruction_predicates(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        self.assertIn("http://annotation.local/testdb/", query)
        self.assertIn("http://materialized.local/testdb/", query)
        self.assertNotIn("ANNOTATION_GRAPH_URI", query)
        self.assertNotIn("MATERIALIZED_GRAPH_URI", query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_targets_instance_edges_and_excludes_meta_predicates(self, mock_post):
        """Test that query keeps instance edges and excludes schema/meta predicates."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_schema_reconstruction_predicates(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        self.assertIn("?subject ?predicate ?object .", query)
        self.assertIn("FILTER(isIRI(?subject) && isIRI(?object))", query)
        self.assertIn(
            'FILTER(STRSTARTS(STR(?subject), "http://data.local/"))',
            query,
        )
        self.assertIn(
            'FILTER(!STRSTARTS(STR(?subject), "http://data.local/rdf/ontology/"))',
            query,
        )
        self.assertIn("FILTER(?predicate != rdf:type)", query)
        self.assertIn("FILTER(?predicate != owl:equivalentClass)", query)
        self.assertIn("FILTER(?predicate != dbo:has_column)", query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_query_inserts_into_materialized_graph(self, mock_post):
        """Test that INSERT targets the materialized graph."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response, query = _materialize_schema_reconstruction_predicates(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

        insert_section = query.split("WHERE")[0]
        self.assertIn("http://materialized.local/testdb/", insert_section)


class TestMaterializeInferences(unittest.TestCase):
    """Test the main materialize_inferences function."""

    @patch(
        "annotation_helper.src.miscellaneous._materialize_schema_reconstruction_predicates"
    )
    @patch("annotation_helper.src.miscellaneous._materialize_value_mapping")
    @patch("annotation_helper.src.miscellaneous._materialize_equivalent_class")
    def test_calls_all_materializations(self, mock_eq_class, mock_val_map, mock_sr):
        """Test that all materialization queries are executed."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_eq_class.return_value = (mock_response, "eq_query")
        mock_val_map.return_value = (mock_response, "vm_query")
        mock_sr.return_value = (mock_response, "sr_query")

        materialize_inferences(
            endpoint="http://localhost:7200/repositories/test/statements",
            database_name="testdb",
        )

        mock_eq_class.assert_called_once_with(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )
        mock_val_map.assert_called_once_with(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )
        mock_sr.assert_called_once_with(
            endpoint="http://localhost:7200/repositories/test/statements",
            annotation_graph_uri="http://annotation.local/testdb/",
            materialized_graph_uri="http://materialized.local/testdb/",
        )

    @patch(
        "annotation_helper.src.miscellaneous._materialize_schema_reconstruction_predicates"
    )
    @patch("annotation_helper.src.miscellaneous._materialize_value_mapping")
    @patch("annotation_helper.src.miscellaneous._materialize_equivalent_class")
    def test_constructs_correct_graph_uris(self, mock_eq_class, mock_val_map, mock_sr):
        """Test that annotation and materialized graph URIs are correctly constructed."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_eq_class.return_value = (mock_response, "eq_query")
        mock_val_map.return_value = (mock_response, "vm_query")
        mock_sr.return_value = (mock_response, "sr_query")

        materialize_inferences(
            endpoint="http://localhost:7200/repositories/test/statements",
            database_name="my_database",
        )

        # Check that the correct URIs were passed
        eq_call_kwargs = mock_eq_class.call_args[1]
        self.assertEqual(
            eq_call_kwargs["annotation_graph_uri"],
            "http://annotation.local/my_database/",
        )
        self.assertEqual(
            eq_call_kwargs["materialized_graph_uri"],
            "http://materialized.local/my_database/",
        )
        sr_call_kwargs = mock_sr.call_args[1]
        self.assertEqual(
            sr_call_kwargs["annotation_graph_uri"],
            "http://annotation.local/my_database/",
        )
        self.assertEqual(
            sr_call_kwargs["materialized_graph_uri"],
            "http://materialized.local/my_database/",
        )

    @patch(
        "annotation_helper.src.miscellaneous._materialize_schema_reconstruction_predicates"
    )
    @patch("annotation_helper.src.miscellaneous._materialize_value_mapping")
    @patch("annotation_helper.src.miscellaneous._materialize_equivalent_class")
    def test_returns_responses_and_queries(self, mock_eq_class, mock_val_map, mock_sr):
        """Test that the function returns all responses and queries."""
        mock_eq_response = MagicMock()
        mock_eq_response.status_code = 200
        mock_val_response = MagicMock()
        mock_val_response.status_code = 200
        mock_sr_response = MagicMock()
        mock_sr_response.status_code = 200
        mock_eq_class.return_value = (mock_eq_response, "eq_query")
        mock_val_map.return_value = (mock_val_response, "vm_query")
        mock_sr.return_value = (mock_sr_response, "sr_query")

        eq_resp, eq_q, vm_resp, vm_q, sr_resp, sr_q = materialize_inferences(
            endpoint="http://localhost:7200/repositories/test/statements",
            database_name="testdb",
        )

        self.assertEqual(eq_resp, mock_eq_response)
        self.assertEqual(eq_q, "eq_query")
        self.assertEqual(vm_resp, mock_val_response)
        self.assertEqual(vm_q, "vm_query")
        self.assertEqual(sr_resp, mock_sr_response)
        self.assertEqual(sr_q, "sr_query")


class TestMaterializeConfigFlag(unittest.TestCase):
    """Test that the materialize configuration flag controls behavior."""

    @patch("annotation_helper.src.miscellaneous.materialize_inferences")
    @patch("annotation_helper.src.miscellaneous.requests.post")
    @patch("annotation_helper.src.miscellaneous.materialize", True)
    @patch("annotation_helper.src.miscellaneous.dry_run", False)
    def test_add_annotation_calls_materialize_when_enabled(
        self, mock_post, mock_materialize
    ):
        """Test that add_annotation calls materialize_inferences when flag is True."""
        from annotation_helper.src.miscellaneous import add_annotation

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"boolean": true}'
        mock_post.return_value = mock_response

        annotation_data = {
            "test_var": {
                "predicate": "roo:P100018",
                "class": "ncit:C28421",
                "local_definition": "test_column",
            }
        }

        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            add_annotation(
                endpoint="http://localhost:7200/repositories/test/statements",
                database="test_db",
                prefixes="PREFIX roo: <http://www.cancerdata.org/roo/>\nPREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
                annotation_data=annotation_data,
                path=tmpdir,
                save_query=False,
            )

        mock_materialize.assert_called_once_with(
            endpoint="http://localhost:7200/repositories/test/statements",
            database_name="test_db",
        )

    @patch("annotation_helper.src.miscellaneous.materialize_inferences")
    @patch("annotation_helper.src.miscellaneous.requests.post")
    @patch("annotation_helper.src.miscellaneous.materialize", False)
    @patch("annotation_helper.src.miscellaneous.dry_run", False)
    def test_add_annotation_skips_materialize_when_disabled(
        self, mock_post, mock_materialize
    ):
        """Test that add_annotation does NOT call materialize_inferences when flag is False."""
        from annotation_helper.src.miscellaneous import add_annotation

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"boolean": true}'
        mock_post.return_value = mock_response

        annotation_data = {
            "test_var": {
                "predicate": "roo:P100018",
                "class": "ncit:C28421",
                "local_definition": "test_column",
            }
        }

        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            add_annotation(
                endpoint="http://localhost:7200/repositories/test/statements",
                database="test_db",
                prefixes="PREFIX roo: <http://www.cancerdata.org/roo/>\nPREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
                annotation_data=annotation_data,
                path=tmpdir,
                save_query=False,
            )

        mock_materialize.assert_not_called()

    @patch("annotation_helper.src.miscellaneous.materialize_inferences")
    @patch("annotation_helper.src.miscellaneous.requests.post")
    @patch("annotation_helper.src.miscellaneous.materialize", True)
    @patch("annotation_helper.src.miscellaneous.dry_run", False)
    def test_add_annotation_survives_materialize_failure(
        self, mock_post, mock_materialize
    ):
        """A materialization failure must not fail the written annotation."""
        from annotation_helper.src.miscellaneous import add_annotation

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"boolean": true}'
        mock_post.return_value = mock_response

        # Materialization runs after the annotation is already written; even if
        # it blows up (e.g. the RDF store is briefly unreachable) the annotation
        # itself must still be reported as successful.
        mock_materialize.side_effect = RuntimeError("rdf store unreachable")

        annotation_data = {
            "test_var": {
                "predicate": "roo:P100018",
                "class": "ncit:C28421",
                "local_definition": "test_column",
            }
        }

        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            result = add_annotation(
                endpoint="http://localhost:7200/repositories/test/statements",
                database="test_db",
                prefixes="PREFIX roo: <http://www.cancerdata.org/roo/>\nPREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
                annotation_data=annotation_data,
                path=tmpdir,
                save_query=False,
            )

        mock_materialize.assert_called_once()
        self.assertIsInstance(result, dict)


class TestMaterializeTemplateFiles(unittest.TestCase):
    """Test that the SPARQL template files exist and are well-formed."""

    def setUp(self):
        """Set up the template directory path."""
        self.template_dir = os.path.join(
            Path(__file__).parent.parent.parent,
            "annotation_helper",
            "src",
            "sparql_templates",
        )

    def test_equivalent_class_template_exists(self):
        """Test that the equivalent class materialization template exists."""
        template_path = os.path.join(
            self.template_dir, "template_materialize_equivalent_class.rq"
        )
        self.assertTrue(
            os.path.exists(template_path),
            f"Template file not found: {template_path}",
        )

    def test_value_mapping_template_exists(self):
        """Test that the value mapping materialization template exists."""
        template_path = os.path.join(
            self.template_dir, "template_materialize_value_mapping.rq"
        )
        self.assertTrue(
            os.path.exists(template_path),
            f"Template file not found: {template_path}",
        )

    def test_equivalent_class_template_has_placeholders(self):
        """Test that the template has the required graph URI placeholders."""
        template_path = os.path.join(
            self.template_dir, "template_materialize_equivalent_class.rq"
        )
        content = read_file(template_path)
        self.assertIn("ANNOTATION_GRAPH_URI", content)
        self.assertIn("MATERIALIZED_GRAPH_URI", content)

    def test_value_mapping_template_has_placeholders(self):
        """Test that the template has the required graph URI placeholders."""
        template_path = os.path.join(
            self.template_dir, "template_materialize_value_mapping.rq"
        )
        content = read_file(template_path)
        self.assertIn("ANNOTATION_GRAPH_URI", content)
        self.assertIn("MATERIALIZED_GRAPH_URI", content)

    def test_equivalent_class_template_is_insert_query(self):
        """Test that the template is a valid INSERT query."""
        template_path = os.path.join(
            self.template_dir, "template_materialize_equivalent_class.rq"
        )
        content = read_file(template_path)
        self.assertIn("INSERT", content)
        self.assertIn("WHERE", content)
        self.assertIn("GRAPH", content)

    def test_value_mapping_template_is_insert_query(self):
        """Test that the template is a valid INSERT query."""
        template_path = os.path.join(
            self.template_dir, "template_materialize_value_mapping.rq"
        )
        content = read_file(template_path)
        self.assertIn("INSERT", content)
        self.assertIn("WHERE", content)
        self.assertIn("GRAPH", content)


if __name__ == "__main__":
    unittest.main()
