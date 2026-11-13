"""
The RDF workflow's golden path: a guard over everything the RDF flow writes.

A fixed CSV and JSON-LD map (``tests/fixtures/golden/rdf/``) go through
ingest, describe and annotate the way a user's browser drives them, with
the real triplifier and a real SPARQL engine behind the store's endpoints
(:class:`SparqlStore`). The graphs the flow leaves in the store are then
compared with the expected ones, as RDF: the order of the triples and the
names of blank nodes do not matter.

The other tests check that each step runs and that particular triples
appear; this one notices any change in what the flow produces, such as one
a change to the transform step (#36) or to describe (#121) brings about
without meaning to. When a change is meant to alter the output, write the
graphs it now produces, and review their diff like any other change:

    FLYOVER_UPDATE_GOLDEN=1 python -m pytest \\
        data_descriptor/tests/integration/test_integration_rdf_golden_path.py

The triplifier names the data after the machine it runs on
(``socket.gethostname()``), and the annotation queries expect
``data.local``: the stack sets that hostname (docker-compose.yml), and so
does this test.
"""

import os
import socket
from io import BytesIO
from pathlib import Path

import pytest
import responses
from rdflib import Graph
from rdflib.compare import graph_diff, isomorphic, to_canonical_graph

from tests.integration.support import SparqlStore

GOLDEN = Path(__file__).parent.parent / "fixtures" / "golden" / "rdf"
UPDATE = os.environ.get("FLYOVER_UPDATE_GOLDEN") == "1"

TABLE = "patients"
GRAPHS = {
    "data": f"http://data.local/{TABLE}/",
    "ontology": f"http://ontology.local/{TABLE}/",
    "annotation": f"http://annotation.local/{TABLE}/",
}
VARIABLES_STATE = "/api/v1/describe-variables-state"
DETAILS_STATE = "/api/v1/describe-variable-details-state"

# What a user enters on the describe pages: every column's type and
# description, then the categories' labels and the age's unit.
VARIABLES_FORM = {
    f"{TABLE}_id": "identifier",
    f"ncit_comment_{TABLE}_id": "Identifier",
    f"comment_{TABLE}_id": "patient number",
    f"{TABLE}_sex": "categorical",
    f"ncit_comment_{TABLE}_sex": "Biological sex",
    f"comment_{TABLE}_sex": "",
    f"{TABLE}_age": "continuous",
    f"ncit_comment_{TABLE}_age": "Age at diagnosis",
    f"comment_{TABLE}_age": "",
    f"{TABLE}_stage": "categorical",
    f"ncit_comment_{TABLE}_stage": "Tumour stage",
    f"comment_{TABLE}_stage": "not in the map",
}
DETAILS_FORM = {
    f'{TABLE}_sex_category_"M"': "Male",
    f'comment_{TABLE}_sex_category_"M"': "",
    f'count_{TABLE}_sex_category_"M"': "2",
    f'{TABLE}_sex_category_"F"': "Female",
    f'comment_{TABLE}_sex_category_"F"': "",
    f'count_{TABLE}_sex_category_"F"': "1",
    f"{TABLE}_age": "years",
}


@pytest.fixture
def rdf_store():
    """The store, answered by a real SPARQL engine (see SparqlStore)."""
    store = SparqlStore()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        store.install(mock)
        yield store


@pytest.fixture(autouse=True)
def data_local(monkeypatch):
    """The hostname the stack gives Flyover's container."""
    monkeypatch.setattr(socket, "gethostname", lambda: "data.local")


def _upload(client, route, field, path, **form):
    return client.post(
        route,
        data={field: (BytesIO(path.read_bytes()), path.name), **form},
        content_type="multipart/form-data",
    )


def _canonical(graph: Graph) -> str:
    """``graph`` as sorted N-Triples, its blank nodes named by their place in
    it, so the same graph always reads the same."""
    canonical = Graph()
    for triple in to_canonical_graph(graph):
        canonical.add(triple)
    lines = canonical.serialize(format="nt").splitlines()
    return "\n".join(sorted(line for line in lines if line.strip())) + "\n"


def _lines(graph: Graph, limit: int = 30) -> str:
    lines = _canonical(graph).splitlines()
    shown = "\n".join(f"    {line}" for line in lines[:limit])
    more = len(lines) - limit
    return shown + (f"\n    … and {more} more" if more > 0 else "")


def _difference(name: str, actual: Graph) -> str:
    """What differs between ``actual`` and the expected graph ``name``, or
    nothing. With FLYOVER_UPDATE_GOLDEN=1, ``actual`` becomes the expected
    graph."""
    path = GOLDEN / f"{name}.nt"
    if UPDATE:
        path.write_text(_canonical(actual), encoding="utf-8")
        return ""
    expected = Graph().parse(path, format="nt")
    if isomorphic(actual, expected):
        return ""
    _, missing, unexpected = graph_diff(
        to_canonical_graph(expected), to_canonical_graph(actual)
    )
    return (
        f"The {name} graph differs from {path.name}.\n"
        f"  Expected but missing ({len(missing)}):\n{_lines(missing)}\n"
        f"  Present but not expected ({len(unexpected)}):\n{_lines(unexpected)}"
    )


def test_the_rdf_flow_leaves_the_expected_graphs_in_the_store(
    client, session_cache, rdf_store, fake_curl
):
    # Ingest: the CSV is triplified, and both graphs go to the store.
    response = _upload(
        client, "/upload", "csvFile", GOLDEN / "patients.csv", fileType="CSV"
    )
    assert response.status_code == 302
    assert response.headers["Location"].startswith("/describe")
    rdf_store.load_uploads(fake_curl.uploads)

    # Describe: the map first (the describe page's upload), then the
    # variables page and its form, and the details page and its form.
    response = _upload(
        client, "/upload-semantic-map", "semanticMapFile", GOLDEN / "map.jsonld"
    )
    assert response.get_json()["success"] is True
    assert client.get(VARIABLES_STATE).get_json() == {
        "column_info": {TABLE: ["id", "sex", "age", "stage"]}
    }
    assert client.post("/units", data=VARIABLES_FORM).status_code == 302
    details = client.get(DETAILS_STATE).get_json()
    # The map's local terms preselect the labels of the store's categories.
    assert details["preselected_values"] == {
        f'{TABLE}_sex_category_"F"': "Female",
        f'{TABLE}_sex_category_"M"': "Male",
    }
    response = client.post("/end", data=DETAILS_FORM)
    assert response.headers["Location"].endswith("/annotate/review")

    # Annotate: every mapped variable, each write checked in the store.
    response = client.post("/start-annotation")
    assert response.get_json() == {
        "success": True,
        "message": "Annotated 3 variables across 1 databases",
    }
    assert all(entry["success"] for entry in session_cache.annotation_status.values())

    # The graphs the flow left, as RDF.
    differences = [
        _difference(name, rdf_store.named_graph(uri)) for name, uri in GRAPHS.items()
    ]
    if UPDATE:
        pytest.skip(f"Wrote the expected graphs to {GOLDEN}; review their diff.")
    assert not any(differences), "\n\n".join(filter(None, differences))
