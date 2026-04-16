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


class TestValueMappingAnnotations(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
