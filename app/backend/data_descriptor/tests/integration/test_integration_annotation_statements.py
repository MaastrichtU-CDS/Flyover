"""
What the annotation statements do, on a real SPARQL engine.

The golden path holds the flow's output to fixed graphs. The tests here hold
the statements that write it to what they are for, on small graphs made for
each case, with rdflib behind the store's endpoints (:class:`SparqlStore`):

- no statement sent while annotating asks the store for patterns that share
  nothing, which every store answers by pairing everything with everything;
- the clean-up removes the edges the schema places elsewhere, in the table's
  own annotation graph and nowhere else.

The data graph is written here in the triplifier's shape: a row
``…/data/<table>/<n>`` of class ``db:<table>`` has a column node
``…/<n>/<column>`` of class ``db:<table>.<column>``, whose cell
``…/<column>/value`` holds the value.
"""

from typing import Dict, Iterator, List, Sequence

import pytest
import responses
from rdflib import Graph, URIRef
from rdflib.plugins.sparql.algebra import translateUpdate
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.term import Variable

from tests.integration.support import SparqlStore

DB = "http://data.local/rdf/ontology/"
DBO = "http://um-cds/ontologies/databaseontology/"
SIO = "http://semanticscience.org/resource/"
PREFIXES = "\n".join(
    [
        f"PREFIX sio: <{SIO}>",
        "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>",
        "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>",
    ]
)


@pytest.fixture
def store():
    """The store, answered by a real SPARQL engine (see SparqlStore)."""
    store = SparqlStore()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        store.install(mock)
        yield store


@pytest.fixture
def endpoint(store):
    return f"{store.repository_url}/statements"


def _row(table: str, n: int) -> str:
    return f"http://data.local/rdf/data/{table}/{n}"


def _load_table(store: SparqlStore, table: str, columns: Dict[str, Sequence]) -> None:
    """Rows of ``table`` in its data graph, as the triplifier writes them;
    an empty value leaves the cell out, as the triplifier does."""
    rows = len(next(iter(columns.values())))
    lines = []
    for n in range(rows):
        row = _row(table, n)
        lines.append(f"<{row}> a <{DB}{table}> .")
        for column, values in columns.items():
            node = f"{row}/{column}"
            lines.append(f"<{row}> <{DBO}has_column> <{node}> .")
            lines.append(f"<{node}> a <{DB}{table}.{column}> .")
            if values[n] not in (None, ""):
                cell = f"{node}/value"
                lines.append(f"<{node}> <{DBO}has_cell> <{cell}> .")
                lines.append(f"<{cell}> a <{DBO}Cell> .")
                lines.append(f'<{cell}> <{DBO}has_value> "{values[n]}" .')
    store.dataset.update(
        f"INSERT DATA {{ GRAPH <http://data.local/{table}/> {{ {' '.join(lines)} }} }}"
    )


def _graph(store: SparqlStore, uri: str) -> Graph:
    return store.named_graph(uri)


# ---------------------------------------------------------------------------
# No statement pairs patterns that share nothing
# ---------------------------------------------------------------------------


def _where_patterns(update: str) -> Iterator[List[tuple]]:
    """The triple patterns of the WHERE clause of each operation of
    ``update`` (across its GRAPH blocks), as rdflib's algebra has them."""

    def triples(node):
        if isinstance(node, CompValue):
            if node.name == "BGP":
                yield from node.triples
                return
            for value in node.values():
                yield from triples(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                yield from triples(value)

    for operation in translateUpdate(parseUpdate(update)).algebra:
        where = operation.get("where")
        if where is not None:
            yield list(triples(where))


def _unrelated_groups(patterns: List[tuple]) -> List[List[tuple]]:
    """``patterns`` in groups that share a variable; more than one group
    means the store has to pair every answer of one with every answer of
    the other (a cross product)."""
    groups: List[List[tuple]] = []
    for pattern in patterns:
        names = {term for term in pattern if isinstance(term, Variable)}
        joined = [g for g in groups if names & _variables(g)]
        merged = [pattern] + [p for g in joined for p in g]
        groups = [g for g in groups if g not in joined] + [merged]
    return groups


def _variables(group: List[tuple]) -> set:
    return {term for pattern in group for term in pattern if isinstance(term, Variable)}


def test_the_guard_sees_patterns_that_share_nothing():
    """The check below finds a cross product where there is one."""
    (patterns,) = _where_patterns(
        "DELETE { ?row <http://p> ?node } "
        "WHERE { ?row a <http://t> . ?node a <http://c> . }"
    )
    assert len(_unrelated_groups(patterns)) == 2


def test_no_statement_sent_while_annotating_pairs_patterns_that_share_nothing(
    store, endpoint, tmp_path, monkeypatch
):
    """Every update annotation sends, the clean-up and the materialisation
    included, asks for patterns that are linked to each other. A statement
    whose patterns share nothing grows with the product of their answers on
    every store (assessment §3.3 and §16.4)."""
    from annotation_helper.src import miscellaneous

    monkeypatch.setattr(miscellaneous, "materialize", True)
    _load_table(
        store,
        "visits",
        {"id": ["V1", "V2", "V3"], "sex": ["M", "F", "M"], "age": ["40", "", "61"]},
    )

    results = miscellaneous.add_annotation(
        endpoint=endpoint,
        database="visits",
        prefixes=PREFIXES,
        annotation_data={
            "identifier": {
                "predicate": "sio:SIO_000673",
                "class": "ncit:C25364",
                "local_definition": "id",
            },
            # A class before the variable, and one after it: the class
            # after it is what the clean-up runs for.
            "biological_sex": {
                "predicate": "sio:SIO_000008",
                "class": "ncit:C28421",
                "local_definition": "sex",
                "schema_reconstruction": [
                    {
                        "type": "class",
                        "predicate": "sio:SIO_000235",
                        "class": "mesh:D000091569",
                        "class_label": "demographicClass",
                        "aesthetic_label": "Demographic",
                        "placement": "before",
                    },
                    {
                        "type": "class",
                        "predicate": "sio:SIO_000253",
                        "class": "ncit:C142529",
                        "class_label": "ehrClass",
                        "aesthetic_label": "EHR",
                        "placement": "after",
                    },
                ],
                "value_mapping": {
                    "terms": {
                        "male": {"local_term": "M", "target_class": "ncit:C20197"},
                        "female": {"local_term": "F", "target_class": "ncit:C16576"},
                    }
                },
            },
            "age_at_diagnosis": {
                "predicate": "sio:SIO_000008",
                "class": "ncit:C156420",
                "local_definition": "age",
                "schema_reconstruction": [
                    {
                        "type": "node",
                        "predicate": "sio:SIO_000221",
                        "class": "ncit:C29848",
                        "node_label": "years",
                        "aesthetic_label": "Years",
                    }
                ],
            },
        },
        path=str(tmp_path),
        save_query=False,
    )

    assert all(entry["success"] for entry in results.values())
    updates = store.updates
    assert any("DELETE" in update for update in updates)
    assert any("http://materialized.local/visits/" in update for update in updates)
    unrelated = {}
    for update in updates:
        for patterns in _where_patterns(update):
            groups = _unrelated_groups(patterns)
            if len(groups) > 1:
                unrelated[update] = groups
    assert not unrelated, "\n\n".join(
        f"{update}\n  groups: {groups}" for update, groups in unrelated.items()
    )


# ---------------------------------------------------------------------------
# The clean-up of an edge the schema places elsewhere
# ---------------------------------------------------------------------------


def _class_nodes(store: SparqlStore, table: str, rows: int) -> None:
    """What annotation leaves in ``table``'s annotation graph for a class
    that one variable places before itself and another after: per row a
    node of the class, the row's edge to it (which the variable that places
    it after itself has removed), and the edge from that variable's column
    node to it (which stays)."""
    annotation = f"http://annotation.local/{table}/"
    lines = []
    for n in range(rows):
        row = _row(table, n)
        node = f"{row}/ehr"
        lines += [
            f"<{row}> <{DBO}has_column> <{node}> .",
            f"<{node}> a <{DB}{table}.ehrClass> .",
            f"<{row}> <{SIO}SIO_000253> <{node}> .",
            f"<{row}/sex> <{SIO}SIO_000253> <{node}> .",
        ]
    store.dataset.update(
        f"INSERT DATA {{ GRAPH <{annotation}> {{ {' '.join(lines)} }} }}"
    )


def test_the_clean_up_removes_the_rows_edges_to_the_class_in_its_annotation_graph(
    store, endpoint
):
    from annotation_helper.src import miscellaneous

    for table in ("visits", "followups"):
        _load_table(store, table, {"sex": ["M", "F"]})
        _class_nodes(store, table, rows=2)
    followups_before = set(_graph(store, "http://annotation.local/followups/"))
    data_before = set(_graph(store, "http://data.local/visits/"))

    response, query = miscellaneous._remove_component(
        endpoint=endpoint,
        database_name="visits",
        prefixes=PREFIXES,
        local_variable="ehrClass",
        component_to_remove="sio:SIO_000253",
    )

    assert 200 <= response.status_code < 300
    visits = _graph(store, "http://annotation.local/visits/")
    edge = URIRef(f"{SIO}SIO_000253")
    for n in range(2):
        row, node = URIRef(_row("visits", n)), URIRef(f"{_row('visits', n)}/ehr")
        # The row's edge to the class node is gone ...
        assert (row, edge, node) not in visits
        # ... the variable's own edge to it stays, and so does the node.
        assert (URIRef(f"{_row('visits', n)}/sex"), edge, node) in visits
        assert (row, URIRef(f"{DBO}has_column"), node) in visits
    # Another table's annotation graph and the data are not touched.
    assert set(_graph(store, "http://annotation.local/followups/")) == followups_before
    assert set(_graph(store, "http://data.local/visits/")) == data_before
