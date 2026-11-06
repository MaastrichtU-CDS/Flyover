"""
Unit tests for RDF store URL normalisation helpers.
"""

import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from utils.rdf_store_url import (
    build_graph_store_endpoint,
    build_repository_endpoint,
    names_this_machine,
    normalise_rdf_store_base_url,
    repository_of_endpoint,
    without_credentials,
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


# ---------------------------------------------------------------------------
# A map's endpoint: which store and repository it names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "endpoint, store",
    [
        # GraphDB's update endpoint, as the example maps write it.
        (
            "http://localhost:7200/repositories/userRepo/statements",
            ("http://localhost:7200", "userRepo"),
        ),
        # RDF4J under a path prefix, query endpoint, trailing slash.
        (
            "https://store.example.org/rdf4j-server/repositories/aya/",
            ("https://store.example.org/rdf4j-server", "aya"),
        ),
        # A query and a fragment are not part of the store's address.
        (
            "http://rdf-store:7200/repositories/userRepo?infer=false#x",
            ("http://rdf-store:7200", "userRepo"),
        ),
        # The scheme is case-insensitive; surrounding spaces are dropped.
        (
            "  HTTP://rdf-store:7200/repositories/userRepo  ",
            ("http://rdf-store:7200", "userRepo"),
        ),
        # Credentials stay: the store needs them.
        (
            "https://flyover:s3cret@store.example.org/repositories/aya/statements",
            ("https://flyover:s3cret@store.example.org", "aya"),
        ),
    ],
)
def test_an_endpoint_names_a_store_and_a_repository(endpoint, store):
    assert repository_of_endpoint(endpoint) == store


@pytest.mark.parametrize(
    "endpoint, reason",
    [
        ("ftp://store.example.org/repositories/aya", "http:// or https://"),
        ("file:///repositories/aya", "http:// or https://"),
        ("store.example.org/repositories/aya", "http:// or https://"),
        ("", "http:// or https://"),
        ("http:///repositories/aya", "names no host"),
        ("http://store.example.org:port/repositories/aya", "port is not a number"),
        ("http://store.example.org:7200/sparql", "/repositories/<name>"),
        ("http://store.example.org:7200/repositories", "/repositories/<name>"),
        ("http://store.example.org:7200/repositories/", "/repositories/<name>"),
        ("http://store.example.org:7200/", "/repositories/<name>"),
    ],
)
def test_an_endpoint_flyover_cannot_use_is_refused_with_the_reason(endpoint, reason):
    with pytest.raises(ValueError, match=re.escape(reason)):
        repository_of_endpoint(endpoint)


@pytest.mark.parametrize(
    "url, shown",
    [
        (
            "https://flyover:s3cret@store.example.org:7200/repositories/aya",
            "https://store.example.org:7200/repositories/aya",
        ),
        (
            "http://token@store.example.org/repositories/aya?x=1",
            "http://store.example.org/repositories/aya?x=1",
        ),
        (
            "http://user:pw@[::1]:7200/repositories/a",
            "http://[::1]:7200/repositories/a",
        ),
        # Nothing to take out: the URL comes back as it was.
        (
            "http://rdf-store:7200/repositories/userRepo",
            "http://rdf-store:7200/repositories/userRepo",
        ),
    ],
)
def test_credentials_never_reach_the_shown_url(url, shown):
    assert without_credentials(url) == shown


@pytest.mark.parametrize(
    "url, local",
    [
        ("http://localhost:7200", True),
        ("http://LOCALHOST:7200", True),
        ("http://graphdb.localhost:7200", True),
        ("http://127.0.0.1:7200", True),
        ("http://127.1.2.3", True),
        ("http://[::1]:7200", True),
        ("http://rdf-store:7200", False),
        ("http://10.0.0.5:7200", False),
        ("http://localhost.example.org", False),
        ("", False),
    ],
)
def test_localhost_and_loopback_addresses_name_this_machine(url, local):
    assert names_this_machine(url) is local


if __name__ == "__main__":
    unittest.main()
