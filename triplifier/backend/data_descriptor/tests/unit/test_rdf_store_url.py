"""
Unit tests for RDF store URL normalisation helpers.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "utils"))

from rdf_store_url import (
    build_graph_store_endpoint,
    build_repository_endpoint,
    normalise_rdf_store_base_url,
)


class TestRDFStoreURLHelpers(unittest.TestCase):
    """Test RDF store URL normalisation and endpoint building."""

    def test_normalise_keeps_graphdb_base_url(self):
        self.assertEqual(
            normalise_rdf_store_base_url("http://rdf-store:7200", "userRepo"),
            "http://rdf-store:7200",
        )

    def test_normalise_strips_repositories_suffix(self):
        self.assertEqual(
            normalise_rdf_store_base_url(
                "http://rdf-store:7200/rdf4j-server/repositories",
                "userRepo",
            ),
            "http://rdf-store:7200/rdf4j-server",
        )

    def test_normalise_strips_repository_path(self):
        self.assertEqual(
            normalise_rdf_store_base_url(
                "http://rdf-store:7200/rdf4j-server/repositories/userRepo",
                "userRepo",
            ),
            "http://rdf-store:7200/rdf4j-server",
        )

    def test_build_repository_endpoint_from_rdf4j_compose_url(self):
        self.assertEqual(
            build_repository_endpoint(
                "http://rdf-store:7200/rdf4j-server/repositories",
                "userRepo",
                "/statements",
            ),
            "http://rdf-store:7200/rdf4j-server/repositories/userRepo/statements",
        )

    def test_build_graph_store_endpoint_encodes_graph_uri(self):
        self.assertEqual(
            build_graph_store_endpoint(
                "http://rdf-store:7200/rdf4j-server/repositories",
                "userRepo",
                "http://data.local/test_table/",
            ),
            "http://rdf-store:7200/rdf4j-server/repositories/userRepo/rdf-graphs/service?graph=http%3A%2F%2Fdata.local%2Ftest_table%2F",
        )


if __name__ == "__main__":
    unittest.main()
