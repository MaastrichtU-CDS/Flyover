import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from annotation_helper.src.miscellaneous import (
    _check_for_value_mapping,
    add_annotation,
    add_mapping,
)
from annotation_helper.main import parse_jsonld_for_table


class TestValueMappingAnnotations(unittest.TestCase):
    def test_parse_jsonld_for_table_uses_variable_predicate_and_class(self):
        jsonld_content = {
            "endpoint": "http://localhost:7200/repositories/test/statements",
            "schema": {
                "prefixes": {
                    "sio": "http://semanticscience.org/resource/",
                    "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
                    "schema": "schema/",
                },
                "variables": {
                    "tumour_location": {
                        "@id": "schema:variable/tumour_location",
                        "@type": "schema:CategoricalVariable",
                        "dataType": "categorical",
                        "predicate": "sio:SIO_000008",
                        "class": "ncit:C3263",
                    }
                },
            },
            "databases": {
                "db1": {
                    "tables": {
                        "tbl1": {
                            "sourceFile": "clinical_table",
                            "columns": {
                                "tumour_location": {
                                    "mapsTo": "schema:variable/tumour_location",
                                    "localColumn": "tumour_location",
                                }
                            },
                        }
                    }
                }
            },
        }

        endpoint, table_name, prefixes, variable_info = parse_jsonld_for_table(
            jsonld_content, "db1", "tbl1"
        )

        self.assertEqual(
            endpoint, "http://localhost:7200/repositories/test/statements"
        )
        self.assertEqual(table_name, "clinical_table")
        self.assertIn("PREFIX sio:", prefixes)
        self.assertEqual(
            variable_info["tumour_location"]["predicate"], "sio:SIO_000008"
        )
        self.assertEqual(variable_info["tumour_location"]["class"], "ncit:C3263")
        self.assertEqual(
            variable_info["tumour_location"]["local_definition"], "tumour_location"
        )

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_check_for_value_mapping_queries_annotation_graph(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"boolean": true}'
        mock_post.return_value = mock_response

        response, query = _check_for_value_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            prefixes="PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
            database_name="testdb",
            target_class="ncit:C20197",
            super_class="ncit:C28421",
            local_term="male",
        )

        self.assertIn("GRAPH <http://annotation.local/testdb/>", query)
        self.assertIn("owl:intersectionOf", query)
        self.assertIn("owl:onProperty dbo:cell_of", query)
        self.assertIn('owl:hasValue "male"^^xsd:string', query)
        self.assertEqual(mock_post.call_args[1]["data"]["query"], query)

    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_add_mapping_returns_statuses(self, mock_post):
        insert_response = MagicMock()
        insert_response.status_code = 200
        insert_response.text = ""
        check_response = MagicMock()
        check_response.status_code = 200
        check_response.text = '{"boolean": true}'
        mock_post.side_effect = [insert_response, check_response]

        responses, queries, statuses = add_mapping(
            endpoint="http://localhost:7200/repositories/test/statements",
            prefixes="PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
            variable="biological_sex",
            super_class="ncit:C28421",
            value_map={
                "terms": {
                    "male": {
                        "target_class": "ncit:C20197",
                        "local_term": "male",
                    }
                }
            },
            database_name="testdb",
        )

        self.assertEqual(len(responses), 1)
        self.assertIn("male_male", queries)
        self.assertEqual(statuses, {"male_male": True})

    @patch("annotation_helper.src.miscellaneous.materialize", False)
    @patch("annotation_helper.src.miscellaneous.dry_run", False)
    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_add_annotation_reports_value_mapping_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"boolean": true}'
        mock_post.return_value = mock_response

        annotation_data = {
            "biological_sex": {
                "predicate": "roo:P100018",
                "class": "ncit:C28421",
                "local_definition": "test_column",
                "value_mapping": {
                    "terms": {
                        "male": {
                            "target_class": "ncit:C20197",
                            "local_term": "male",
                        }
                    }
                },
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result = add_annotation(
                endpoint="http://localhost:7200/repositories/test/statements",
                database="test_db",
                prefixes="\n".join(
                    [
                        "PREFIX roo: <http://www.cancerdata.org/roo/>",
                        "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
                    ]
                ),
                annotation_data=annotation_data,
                path=tmpdir,
                save_query=False,
            )

        self.assertTrue(result["biological_sex"]["success"])
        self.assertTrue(result["biological_sex"]["value_mappings_inserted"])

    @patch("annotation_helper.src.miscellaneous.materialize", False)
    @patch("annotation_helper.src.miscellaneous.dry_run", False)
    @patch("annotation_helper.src.miscellaneous.requests.post")
    def test_add_annotation_query_uses_variable_predicate_and_local_type(
        self, mock_post
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"boolean": true}'
        mock_post.return_value = mock_response

        annotation_data = {
            "tumour_location": {
                "predicate": "sio:SIO_000008",
                "class": "ncit:C3263",
                "local_definition": "tumour_location",
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            add_annotation(
                endpoint="http://localhost:7200/repositories/test/statements",
                database="clinical_table",
                prefixes="\n".join(
                    [
                        "PREFIX sio: <http://semanticscience.org/resource/>",
                        "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
                    ]
                ),
                annotation_data=annotation_data,
                path=tmpdir,
                save_query=False,
            )

        insert_query = mock_post.call_args_list[1][1]["data"]["update"]
        self.assertIn("?tablerow sio:SIO_000008 ?component1.", insert_query)
        self.assertIn("?tablerow dbo:has_column ?component1 .", insert_query)
        self.assertIn("?component1 rdf:type db:clinical_table.tumour_location .", insert_query)


if __name__ == "__main__":
    unittest.main()
