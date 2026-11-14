"""
Unit tests of the store's datasets (``services/store_datasets.py``): what the
status bar's panel lists, the TriG it exports and the updates it sends to
delete, with the store faked at the ``requests`` level.
"""

import logging
import sys
from pathlib import Path
from urllib.parse import parse_qs

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services import store_datasets as module  # noqa: E402
from services.store_datasets import (  # noqa: E402
    DESCRIBED_QUERY,
    ROWS_QUERY,
    StoreDatasetError,
    StoreDatasets,
    dataset_graphs,
    trig_block,
    valid_name,
)

BASE = "http://rdf-store.unit:7200"
QUERY_URL = f"{BASE}/repositories/userRepo"
UPDATE_URL = f"{QUERY_URL}/statements"
GRAPH_URL = f"{QUERY_URL}/rdf-graphs/service"
SEPARATOR = " ;\n"


def _operations(update):
    return update.split(SEPARATOR)


def _form(request):
    body = request.body.decode() if isinstance(request.body, bytes) else request.body
    return {key: values[0] for key, values in parse_qs(body).items()}


class FakeStore:
    """Answers the queries the panel sends from a small model."""

    def __init__(self):
        self.rows = {"patients": 150, "Visits": 12, "labs": 0}
        self.described = {"patients"}
        self.graph_contents = {}
        self.links = ""
        self.extra_graphs = []
        self.updates = []
        self.queries = []
        self.update_status = 204
        # A store that takes one operation per request only.
        self.refuse_combined = False

    def install(self, web):
        web.add_callback(responses.POST, QUERY_URL, callback=self._query)
        web.add_callback(responses.POST, UPDATE_URL, callback=self._update)
        web.add_callback(responses.GET, GRAPH_URL, callback=self._graph)

    def _query(self, request):
        query = _form(request)["query"]
        self.queries.append(query)
        if query == ROWS_QUERY:
            body = "db,rows\n" + "".join(f"{n},{r}\n" for n, r in self.rows.items())
        elif query == DESCRIBED_QUERY:
            body = "g\n" + "".join(
                f"http://ontology.local/{n}/\n" for n in self.described
            )
        elif query.startswith("CONSTRUCT"):
            return 200, {"Content-Type": "application/n-triples"}, self.links
        elif "SELECT DISTINCT ?g" in query:
            present = [g for g in self.graph_contents if g] + self.extra_graphs
            prefixes = [p for p in module.FLYOVER_GRAPHS if f'"{p}"' in query]
            body = "g\n" + "".join(
                f"{g}\n" for g in present if any(g.startswith(p) for p in prefixes)
            )
        else:
            return 400, {}, "unexpected query"
        return 200, {"Content-Type": "text/csv"}, body

    def _update(self, request):
        update = _form(request)["update"]
        self.updates.append(update)
        if self.refuse_combined and SEPARATOR in update:
            return 400, {}, "one operation per request, please"
        return self.update_status, {}, ""

    def _graph(self, request):
        graph = parse_qs(request.url.split("?", 1)[1])["graph"][0]
        if graph not in self.graph_contents:
            return 404, {}, "no such graph"
        return (
            200,
            {"Content-Type": "application/n-triples"},
            self.graph_contents[graph],
        )


@pytest.fixture
def store():
    fake = FakeStore()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as web:
        fake.install(web)
        yield fake


@pytest.fixture
def datasets():
    return StoreDatasets(BASE, "userRepo")


# ---------------------------------------------------------------------------
# What is there
# ---------------------------------------------------------------------------


class TestListing:
    def test_every_dataset_with_its_rows_and_progress_by_name(self, store, datasets):
        store.graph_contents["http://annotation.local/Visits/"] = "<a> <b> <c> .\n"

        assert datasets.list() == [
            {"name": "labs", "rows": 0, "described": False, "annotated": False},
            {"name": "patients", "rows": 150, "described": True, "annotated": False},
            {"name": "Visits", "rows": 12, "described": False, "annotated": True},
        ]

    def test_a_count_the_store_writes_as_a_decimal_still_reads(self, store, datasets):
        store.rows = {"patients": "150.0", "odd": "many"}

        assert [d["rows"] for d in datasets.list()] == [0, 150]

    def test_it_knows_its_datasets_by_their_exact_name(self, store, datasets):
        assert datasets.has("Visits")
        assert not datasets.has("visits")


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------


class TestExports:
    def test_a_dataset_exports_its_graphs_and_its_links_as_trig(self, store, datasets):
        store.graph_contents = {
            "http://data.local/patients/": '<http://x/r1> <http://x/p> "1" .\n',
            "http://ontology.local/patients/": "<http://x/c> <http://x/q> <http://x/d> .\n",
        }
        store.links = "<http://x/r1> <http://x/fk> <http://y/r9> .\n"

        trig = datasets.export_dataset("patients")

        assert trig.startswith("# Flyover's export of the dataset patients")
        assert (
            '<http://data.local/patients/> {\n    <http://x/r1> <http://x/p> "1" .\n}\n'
            in trig
        )
        assert "<http://ontology.local/patients/> {" in trig
        assert (
            "<http://relationships.local/> {\n"
            "    <http://x/r1> <http://x/fk> <http://y/r9> .\n}\n"
        ) in trig
        # Graphs the dataset has none of are left out.
        assert "annotation.local" not in trig and "materialized.local" not in trig

    def test_the_links_are_found_through_the_datasets_data_graph(self, store, datasets):
        datasets.export_dataset("patients")

        [construct] = [q for q in store.queries if q.startswith("CONSTRUCT")]
        assert "GRAPH <http://relationships.local/> { ?s ?p ?o . }" in construct
        assert "EXISTS { GRAPH <http://data.local/patients/> { ?s " in construct
        assert "EXISTS { GRAPH <http://data.local/patients/> { ?o " in construct

    def test_every_dataset_exports_every_flyover_graph_and_nothing_else(
        self, store, datasets
    ):
        store.graph_contents = {
            "http://data.local/patients/": "<http://x/a> <http://x/b> <http://x/c> .\n",
            "http://relationships.local/": "<http://x/a> <http://x/fk> <http://x/d> .\n",
        }
        store.extra_graphs = ["http://other-tool.example/graph"]

        trig = datasets.export_all()

        assert "<http://data.local/patients/> {" in trig
        assert "<http://relationships.local/> {" in trig
        assert "other-tool" not in trig

    def test_the_annotations_export_holds_annotations_and_inferences_only(
        self, store, datasets
    ):
        store.graph_contents = {
            "http://data.local/patients/": "<http://x/a> <http://x/b> <http://x/c> .\n",
            "http://annotation.local/patients/": "<http://x/a> <http://x/e> <http://x/f> .\n",
            "http://materialized.local/patients/": "<http://x/a> <http://x/g> <http://x/h> .\n",
        }

        trig = datasets.export_annotations()

        assert "<http://annotation.local/patients/> {" in trig
        assert "<http://materialized.local/patients/> {" in trig
        assert "data.local" not in trig


class TestTrigBlocks:
    def test_blank_nodes_of_different_graphs_stay_apart(self):
        first = trig_block("http://g/1", "_:b0 <http://p> _:b1 .\n", "g0_")
        second = trig_block("http://g/2", "_:b0 <http://p> <http://o> .\n", "g1_")

        assert "_:g0_b0 <http://p> _:g0_b1 ." in first
        assert "_:g1_b0 <http://p> <http://o> ." in second

    def test_a_literal_that_looks_like_a_blank_node_is_left_alone(self):
        block = trig_block("http://g", '<http://s> <http://p> "a _:b0" .\n', "g0_")

        assert '<http://s> <http://p> "a _:b0" .' in block

    def test_comments_blank_lines_and_an_empty_graph_give_no_block(self):
        assert trig_block("http://g", "# nothing here\n\n", "g0_") == ""

    def test_a_line_that_is_not_a_statement_is_kept_as_it_is(self):
        assert "    odd" in trig_block("http://g", "odd\n", "g0_")


# ---------------------------------------------------------------------------
# Deletes
# ---------------------------------------------------------------------------


class TestDeletes:
    def test_a_dataset_goes_in_one_request_its_links_first_then_its_graphs(
        self, store, datasets
    ):
        datasets.delete_dataset("patients")

        [update] = store.updates
        links, *drops = _operations(update)
        assert links.startswith("DELETE { GRAPH <http://relationships.local/>")
        assert "GRAPH <http://data.local/patients/>" in links
        assert drops == [f"DROP SILENT GRAPH <{g}>" for g in dataset_graphs("patients")]
        assert dataset_graphs("patients") == [
            "http://data.local/patients/",
            "http://ontology.local/patients/",
            "http://annotation.local/patients/",
            "http://materialized.local/patients/",
        ]

    def test_every_dataset_goes_one_request_each_never_with_drop_all(
        self, store, datasets
    ):
        store.graph_contents = {
            "http://data.local/patients/": "",
            "http://ontology.local/patients/": "",
            "http://data.local/Visits/": "",
            "http://relationships.local/": "",
        }
        store.extra_graphs = ["http://other-tool.example/graph"]

        dropped = datasets.delete_all()

        assert sorted(dropped) == [
            "http://data.local/Visits/",
            "http://data.local/patients/",
            "http://ontology.local/patients/",
            "http://relationships.local/",
        ]
        # A request per dataset, in name order; the shared graphs last.
        assert [_operations(u) for u in store.updates] == [
            [
                "DROP SILENT GRAPH <http://data.local/patients/>",
                "DROP SILENT GRAPH <http://ontology.local/patients/>",
            ],
            ["DROP SILENT GRAPH <http://data.local/Visits/>"],
            ["DROP SILENT GRAPH <http://relationships.local/>"],
        ]
        assert not any("DROP ALL" in u or "other-tool" in u for u in store.updates)

    def test_the_annotations_go_with_their_inferences_and_nothing_else(
        self, store, datasets
    ):
        store.graph_contents = {
            "http://data.local/patients/": "",
            "http://annotation.local/patients/": "",
            "http://materialized.local/patients/": "",
        }

        dropped = datasets.delete_annotations()

        assert dropped == [
            "http://annotation.local/patients/",
            "http://materialized.local/patients/",
        ]
        assert store.updates == [
            "DROP SILENT GRAPH <http://annotation.local/patients/> ;\n"
            "DROP SILENT GRAPH <http://materialized.local/patients/>"
        ]

    def test_a_store_that_takes_one_operation_at_a_time_gets_them_so(
        self, store, datasets
    ):
        store.refuse_combined = True

        datasets.delete_dataset("patients")

        combined, *singles = store.updates
        assert singles == _operations(combined)
        assert len(singles) == 5

    def test_a_store_that_refuses_for_another_reason_is_not_asked_again(
        self, store, datasets
    ):
        store.update_status = 403

        with pytest.raises(StoreDatasetError, match=r"HTTP 403"):
            datasets.delete_dataset("patients")

        assert len(store.updates) == 1


# ---------------------------------------------------------------------------
# A store that refuses or is gone
# ---------------------------------------------------------------------------


class TestFailures:
    def test_a_refused_delete_says_so_with_the_status(self, store, datasets):
        store.update_status = 500

        with pytest.raises(StoreDatasetError) as raised:
            datasets.delete_dataset("patients")

        assert raised.value.message == "The store refused to delete (HTTP 500)."
        assert raised.value.status == 502
        assert len(store.updates) == 1

    def test_a_store_that_is_gone_is_said_to_be_unreachable_without_its_secrets(
        self, caplog
    ):
        caplog.set_level(logging.INFO, logger=module.logger.name)
        secret = "http://user:s3cret@gone.example:7200"
        with responses.RequestsMock() as web:
            web.add(
                responses.POST,
                f"{secret}/repositories/userRepo",
                body=requests.ConnectionError(f"refused: {secret}"),
            )
            with pytest.raises(StoreDatasetError) as raised:
                StoreDatasets(secret, "userRepo").list()

        assert (
            raised.value.message == "Currently, the store cannot be reached by Flyover."
        )
        assert "s3cret" not in caplog.text

    def test_a_refused_listing_says_so(self):
        with responses.RequestsMock() as web:
            web.add(responses.POST, QUERY_URL, status=403)
            with pytest.raises(
                StoreDatasetError, match=r"list the datasets \(HTTP 403\)"
            ):
                StoreDatasets(BASE, "userRepo").list()

    def test_a_graph_the_store_refuses_to_export_says_which(self):
        with responses.RequestsMock() as web:
            web.add(responses.GET, GRAPH_URL, status=500)
            with pytest.raises(
                StoreDatasetError, match="export http://data.local/patients/"
            ):
                StoreDatasets(BASE, "userRepo")._graph("http://data.local/patients/")


@pytest.mark.parametrize(
    "name, valid",
    [
        ("patients", True),
        ("NKI_prospective_data_IKNL_COMPRAYA2_T1_edited_v2_mock", True),
        ("synthetic-english.150", True),
        ("", False),
        ("two words", False),
        ("a/b", False),
        ("a>b", False),
        ('a"b', False),
        (None, False),
    ],
)
def test_a_name_must_fit_in_a_graph_iri(name, valid):
    assert valid_name(name) is valid
