"""
Integration tests of the datasets panel's routes, through the real dataset
service and the app, against the fake store: what reaches the store, the
TriG that comes back, and that a project's store is the one worked on.
"""

from urllib.parse import parse_qs, urlparse

import pytest
import responses

from tests.integration.support import _MAPPING, TABLE, FakeRdfStore

PROJECT_BASE_URL = "http://project-store.test:7200"

ROWS = "COUNT(DISTINCT ?row)"
DESCRIBED = "owl:equivalentClass ?answer"
GRAPH_LIST = "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o . }"
LINKS = "CONSTRUCT"


def _listing(store, graphs=()):
    """What the store answers about its datasets and graphs."""
    store.select_overrides.update(
        {
            ROWS: f"db,rows\r\n{TABLE},3\r\nvisits,7\r\n",
            DESCRIBED: f"g\r\nhttp://ontology.local/{TABLE}/\r\n",
            GRAPH_LIST: "g\r\n" + "".join(f"{g}\r\n" for g in graphs),
            LINKS: "<http://data.local/rdf/patients/r1> <http://x/fk> <http://data.local/rdf/visits/r9> .\n",
        }
    )


def _updates(store):
    return [c.update for c in store.calls if c.kind == "update"]


class TestTheDatasetsPanel:
    def test_it_lists_the_datasets_with_their_rows_and_progress(
        self, client, rdf_store
    ):
        _listing(rdf_store, graphs=[f"http://annotation.local/{TABLE}/"])

        response = client.get("/api/v1/store/datasets")

        assert response.get_json() == {
            "datasets": [
                {"name": TABLE, "rows": 3, "described": True, "annotated": True},
                {"name": "visits", "rows": 7, "described": False, "annotated": False},
            ]
        }

    def test_deleting_a_dataset_removes_its_links_then_its_graphs(
        self, client, rdf_store, session_cache
    ):
        _listing(rdf_store)
        session_cache.descriptive_info = {TABLE: {}, "visits": {}}

        response = client.delete(f"/api/v1/store/datasets/{TABLE}")

        assert response.status_code == 200, response.get_json()
        links, *drops = _updates(rdf_store)
        assert "GRAPH <http://relationships.local/>" in links
        assert f"GRAPH <http://data.local/{TABLE}/>" in links
        assert drops == [
            f"DROP SILENT GRAPH <http://data.local/{TABLE}/>",
            f"DROP SILENT GRAPH <http://ontology.local/{TABLE}/>",
            f"DROP SILENT GRAPH <http://annotation.local/{TABLE}/>",
            f"DROP SILENT GRAPH <http://materialized.local/{TABLE}/>",
        ]
        assert all(
            c.url == rdf_store.statements_url
            for c in rdf_store.calls
            if c.kind == "update"
        )
        assert session_cache.descriptive_info == {"visits": {}}

    def test_exporting_a_dataset_gives_its_graphs_and_links_as_trig(
        self, client, rdf_store
    ):
        _listing(rdf_store)
        rdf_store.graphs[f"http://data.local/{TABLE}/"] = (
            '<http://data.local/rdf/patients/r1> <http://x/value> "45" .\n'
        )
        rdf_store.graphs[f"http://ontology.local/{TABLE}/"] = (
            "<http://data.local/rdf/ontology/patients.age> "
            "<http://www.w3.org/2002/07/owl#equivalentClass> \"['age']\" .\n"
        )

        response = client.get(f"/api/v1/store/datasets/{TABLE}/export")

        assert response.status_code == 200
        trig = response.get_data(as_text=True)
        assert f"<http://data.local/{TABLE}/> {{" in trig
        assert f"<http://ontology.local/{TABLE}/> {{" in trig
        assert "<http://relationships.local/> {" in trig
        downloaded = sorted(
            parse_qs(urlparse(c.url).query)["graph"][0]
            for c in rdf_store.calls
            if c.kind == "graph"
        )
        assert downloaded == [
            f"http://annotation.local/{TABLE}/",
            f"http://data.local/{TABLE}/",
            f"http://materialized.local/{TABLE}/",
            f"http://ontology.local/{TABLE}/",
        ]

    def test_deleting_every_dataset_drops_flyovers_graphs_one_by_one(
        self, client, rdf_store
    ):
        _listing(
            rdf_store,
            graphs=[
                f"http://data.local/{TABLE}/",
                f"http://ontology.local/{TABLE}/",
                "http://relationships.local/",
            ],
        )

        response = client.delete("/api/v1/store/datasets")

        assert response.status_code == 200
        assert _updates(rdf_store) == [
            f"DROP SILENT GRAPH <http://data.local/{TABLE}/>",
            f"DROP SILENT GRAPH <http://ontology.local/{TABLE}/>",
            "DROP SILENT GRAPH <http://relationships.local/>",
        ]

    def test_a_store_that_refuses_a_delete_says_so(self, client, rdf_store):
        _listing(rdf_store)
        rdf_store.update_status = 500

        response = client.delete(f"/api/v1/store/datasets/{TABLE}")

        assert response.status_code == 502
        assert response.get_json() == {
            "error": "The store refused to delete (HTTP 500)."
        }


@pytest.fixture
def project_store():
    store = FakeRdfStore()
    store.base_url = PROJECT_BASE_URL
    store.repo = "projectRepo"
    return store


@pytest.fixture
def rdf_store(project_store):
    """The configured store, with a project's store answering beside it."""
    store = FakeRdfStore()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        store.install(mock)
        project_store.install(mock)
        yield store


def test_with_a_project_open_the_panel_works_on_the_projects_store(
    client, rdf_store, project_store
):
    mapping = dict(_MAPPING)
    mapping["endpoint"] = f"{PROJECT_BASE_URL}/repositories/projectRepo/statements"
    client.put("/api/v1/project", json={"mapping": mapping})
    _listing(project_store)
    rdf_store.calls.clear()

    client.delete(f"/api/v1/store/datasets/{TABLE}")

    assert _updates(project_store)
    assert not rdf_store.calls
