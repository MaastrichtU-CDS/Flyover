"""
How much the RDF flow asks of the store, counted exactly.

Annotation is store time: for every variable Flyover sends the store a
handful of updates and checks, and the store does the work. How long that
takes depends on the machine; how many requests are sent does not. This
module counts them, on the golden path's table and map, with a real SPARQL
engine behind the store's endpoints (:class:`SparqlStore`), so the checks
get real answers.

The counts are pinned. They move only when the flow itself changes what it
asks of the store, and then that should be a decision: fewer requests is an
optimisation to keep, more is a cost someone chose to add. Update the
numbers in the same change, with the reason.

The timed counterpart, on a real store, is the benchmark in
``performance/`` (see its README).
"""

import json
import socket
from collections import Counter
from io import BytesIO
from pathlib import Path

import pytest
import responses

from tests.integration.support import SparqlStore

GOLDEN = Path(__file__).parent.parent / "fixtures" / "golden" / "rdf"

# What annotating the golden table (three mapped variables: an identifier,
# a category with two mapped values, a number with a unit) asks of the store:
# one SELECT for the tables it holds, then the updates that write the
# annotations and the ASK checks that each of them took effect.
ANNOTATION_OF_ONE_TABLE = Counter({"update": 11, "ask": 8, "select": 1})


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


def _map_for(table: str) -> bytes:
    """The golden map, naming ``table`` as its one table."""
    mapping = json.loads((GOLDEN / "map.jsonld").read_text(encoding="utf-8"))
    (database,) = mapping["databases"].values()
    ((_, entry),) = database["tables"].items()
    entry["@id"] = f"mapping:table/{table}"
    entry["sourceFile"] = table
    database["name"] = table
    database["tables"] = {table: entry}
    return json.dumps(mapping).encode("utf-8")


class Flow:
    """The golden path's requests, for a table of any name."""

    def __init__(self, client, rdf_store, fake_curl):
        self.client, self.store, self.curl = client, rdf_store, fake_curl
        self._loaded = 0

    def ingest(self, table: str) -> None:
        response = self.client.post(
            "/upload",
            data={
                "csvFile": (
                    BytesIO((GOLDEN / "patients.csv").read_bytes()),
                    f"{table}.csv",
                ),
                "fileType": "CSV",
            },
            content_type="multipart/form-data",
        )
        assert response.status_code == 302
        uploads = self.curl.uploads
        self.store.load_uploads(uploads[self._loaded :])
        self._loaded = len(uploads)

    def annotate(self, table: str) -> Counter:
        """Load the map for ``table`` and annotate it; what the annotation
        run itself sent to the store, by kind of request."""
        response = self.client.post(
            "/upload-semantic-map",
            data={"semanticMapFile": (BytesIO(_map_for(table)), "map.jsonld")},
            content_type="multipart/form-data",
        )
        assert response.get_json()["success"] is True
        before = len(self.store.calls)
        response = self.client.post("/start-annotation")
        assert response.get_json() == {
            "success": True,
            "message": "Annotated 3 variables across 1 databases",
        }
        return Counter(call.kind for call in self.store.calls[before:])


@pytest.fixture
def flow(client, rdf_store, fake_curl):
    return Flow(client, rdf_store, fake_curl)


def test_annotating_a_table_sends_the_store_a_fixed_number_of_requests(flow):
    flow.ingest("patients")

    assert flow.annotate("patients") == ANNOTATION_OF_ONE_TABLE


def test_a_table_already_in_the_store_adds_no_requests(flow):
    # Repeated intake: a second table is annotated in a store that already
    # holds an annotated one. The store has more to do; Flyover must not
    # ask more of it because of what is already there.
    flow.ingest("patients")
    flow.annotate("patients")
    flow.ingest("followup")

    assert flow.annotate("followup") == ANNOTATION_OF_ONE_TABLE


def test_annotating_the_same_table_again_sends_the_same_requests(flow):
    flow.ingest("patients")
    first = flow.annotate("patients")

    assert flow.annotate("patients") == first
