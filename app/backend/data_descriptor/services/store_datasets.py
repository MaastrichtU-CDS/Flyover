"""
The datasets in the RDF store Flyover works on: listing, exporting and
deleting them.

A dataset is one ingested table. Flyover keeps it in named graphs:

- ``http://data.local/<name>/``: its rows and cells;
- ``http://ontology.local/<name>/``: its columns, their places in the source
  and the describe answers;
- ``http://annotation.local/<name>/``: its annotations;
- ``http://materialized.local/<name>/``: inferences materialised from them;

and its rows' links to other datasets' rows in the shared
``http://relationships.local/``. Deleting a dataset removes all of that, as
if it was never ingested. "Every dataset" means every graph Flyover writes,
never anything else the repository holds.

Exports are TriG, Turtle with the graphs kept, so a store can import a file
back into the graphs Flyover reads. The requests go straight to the store's
RDF4J-style endpoints (GraphDB, RDF4J and the QLever adapter all serve
them), so a refusal surfaces with its status instead of passing unnoticed.
"""

import csv
import io
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Sequence

import requests

try:
    from ..utils.rdf_store_url import build_repository_endpoint
except ImportError:
    from utils.rdf_store_url import build_repository_endpoint  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

DATA = "http://data.local/"
ONTOLOGY = "http://ontology.local/"
ANNOTATION = "http://annotation.local/"
MATERIALISED = "http://materialized.local/"
RELATIONSHIPS = "http://relationships.local/"

FLYOVER_GRAPHS = (DATA, ONTOLOGY, ANNOTATION, MATERIALISED, RELATIONSHIPS)
ANNOTATION_GRAPHS = (ANNOTATION, MATERIALISED)

# A dataset's name as the ingest makes it (from a file or table name). It
# goes into graph IRIs, so anything an IRI cannot hold is refused.
_NAME = re.compile(r'^[^\s<>"{}|^`\\/]+$')

UNREACHABLE = "Currently, the store cannot be reached by Flyover."

ROWS_QUERY = """
PREFIX dbo: <http://um-cds/ontologies/databaseontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?db (COUNT(DISTINCT ?row) AS ?rows) WHERE {
    ?class dbo:table ?db ;
           rdfs:subClassOf dbo:TableRow .
    OPTIONAL { ?row a ?class . }
}
GROUP BY ?db
"""

# The describe answers are literals; annotations are classes.
DESCRIBED_QUERY = """
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT ?g WHERE {
    GRAPH ?g { ?column owl:equivalentClass ?answer . }
    FILTER(isLiteral(?answer) && STRSTARTS(STR(?g), "http://ontology.local/"))
}
"""

# The links whose either end is a node of the dataset's data graph. Run
# before the data graph is dropped: it is what tells the links apart.
LINKS_FILTER = """
    GRAPH <{relationships}> {{ ?s ?p ?o . }}
    FILTER (
        EXISTS {{ GRAPH <{data}> {{ ?s ?anyP ?anyO . }} }}
        || EXISTS {{ GRAPH <{data}> {{ ?o ?anyP2 ?anyO2 . }} }}
    )
"""


class StoreDatasetError(Exception):
    """What went wrong, for the user, and the HTTP status to answer with."""

    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.message = message
        self.status = status


def valid_name(name: str) -> bool:
    return bool(isinstance(name, str) and _NAME.match(name))


def dataset_graphs(name: str) -> List[str]:
    """The graphs that hold one dataset (not its links)."""
    return [f"{prefix}{name}/" for prefix in (DATA, ONTOLOGY, ANNOTATION, MATERIALISED)]


def _graphs_query(prefixes: Sequence[str]) -> str:
    starts = " || ".join(f'STRSTARTS(STR(?g), "{prefix}")' for prefix in prefixes)
    return f"SELECT DISTINCT ?g WHERE {{ GRAPH ?g {{ ?s ?p ?o . }} FILTER({starts}) }}"


def _relabel_blank_nodes(line: str, label: str) -> str:
    """One N-Triples statement with its blank nodes renamed apart.

    Blank node labels hold for one document only: each graph is fetched on
    its own, so two graphs may reuse a label for different nodes. A TriG
    file is one document, so every graph's labels get a prefix of their own.
    """
    parts = line.split(None, 2)
    if len(parts) < 3:
        return line
    subject, predicate, rest = parts
    statement = rest.rstrip()
    if statement.endswith("."):
        statement = statement[:-1].rstrip()
    if subject.startswith("_:"):
        subject = f"_:{label}{subject[2:]}"
    if statement.startswith("_:"):
        statement = f"_:{label}{statement[2:]}"
    return f"{subject} {predicate} {statement} ."


def trig_block(graph: str, ntriples: str, label: str) -> str:
    """One graph of a TriG file, from its N-Triples; empty for no triples."""
    lines = [
        "    " + _relabel_blank_nodes(line.strip(), label)
        for line in ntriples.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines:
        return ""
    return f"<{graph}> {{\n" + "\n".join(lines) + "\n}\n"


class StoreDatasets:
    """The datasets of one store's repository."""

    def __init__(self, store_url: str, repo: str, timeout: float = 120):
        self.repo = repo
        self._query_url = build_repository_endpoint(store_url, repo)
        self._update_url = build_repository_endpoint(store_url, repo, "/statements")
        self._graph_url = build_repository_endpoint(
            store_url, repo, "/rdf-graphs/service"
        )
        self._timeout = timeout

    # -- what is there -----------------------------------------------------

    def list(self) -> List[Dict[str, Any]]:
        """Every dataset, by name: its rows and how far Flyover got with it."""
        described = {row[0] for row in self._select(DESCRIBED_QUERY)}
        annotated = set(self._graphs((ANNOTATION,)))
        datasets = [
            {
                "name": name,
                "rows": _count(rows),
                "described": f"{ONTOLOGY}{name}/" in described,
                "annotated": f"{ANNOTATION}{name}/" in annotated,
            }
            for name, rows in self._select(ROWS_QUERY)
            if name
        ]
        return sorted(datasets, key=lambda d: d["name"].casefold())

    def has(self, name: str) -> bool:
        return any(dataset["name"] == name for dataset in self.list())

    def _graphs(self, prefixes: Sequence[str]) -> List[str]:
        return sorted(row[0] for row in self._select(_graphs_query(prefixes)) if row)

    # -- exports -------------------------------------------------------------

    def export_dataset(self, name: str) -> str:
        """One dataset, its links included, as TriG."""
        blocks = [
            trig_block(graph, self._graph(graph), f"g{index}_")
            for index, graph in enumerate(dataset_graphs(name))
        ]
        links = self._construct(
            "CONSTRUCT { ?s ?p ?o } WHERE {"
            + LINKS_FILTER.format(relationships=RELATIONSHIPS, data=f"{DATA}{name}/")
            + "}"
        )
        blocks.append(trig_block(RELATIONSHIPS, links, "links_"))
        return self._trig(f"the dataset {name}", blocks)

    def export_all(self) -> str:
        """Every graph Flyover writes, as TriG."""
        return self._trig("every dataset", self._graph_blocks(FLYOVER_GRAPHS))

    def export_annotations(self) -> str:
        """Every annotation graph and materialised inference, as TriG."""
        return self._trig("the annotations", self._graph_blocks(ANNOTATION_GRAPHS))

    def _graph_blocks(self, prefixes: Sequence[str]) -> List[str]:
        return [
            trig_block(graph, self._graph(graph), f"g{index}_")
            for index, graph in enumerate(self._graphs(prefixes))
        ]

    def _trig(self, what: str, blocks: Iterable[str]) -> str:
        made = datetime.now(timezone.utc).isoformat(timespec="seconds")
        header = (
            f"# Flyover's export of {what} from the repository {self.repo}, {made}.\n"
            "# TriG: import it back into a store to restore the graphs.\n\n"
        )
        return header + "\n".join(block for block in blocks if block)

    # -- deletes -------------------------------------------------------------

    def delete_dataset(self, name: str) -> None:
        """Everything Flyover made for one dataset: its graphs and its links."""
        self._update(
            f"DELETE {{ GRAPH <{RELATIONSHIPS}> {{ ?s ?p ?o . }} }} WHERE {{"
            + LINKS_FILTER.format(relationships=RELATIONSHIPS, data=f"{DATA}{name}/")
            + "}"
        )
        for graph in dataset_graphs(name):
            self._update(f"DROP SILENT GRAPH <{graph}>")

    def delete_all(self) -> List[str]:
        """Every graph Flyover writes; anything else in the repository stays."""
        return self._drop(self._graphs(FLYOVER_GRAPHS))

    def delete_annotations(self) -> List[str]:
        """Every annotation graph and every materialised inference."""
        return self._drop(self._graphs(ANNOTATION_GRAPHS))

    def _drop(self, graphs: List[str]) -> List[str]:
        for graph in graphs:
            self._update(f"DROP SILENT GRAPH <{graph}>")
        return graphs

    # -- the store's endpoints ---------------------------------------------

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        try:
            return requests.request(method, url, timeout=self._timeout, **kwargs)
        except requests.RequestException as error:
            # The error repeats the URL, which may carry credentials.
            logger.info("The store request failed (%s)", type(error).__name__)
            raise StoreDatasetError(UNREACHABLE) from None

    def _refused(self, response: requests.Response, what: str) -> StoreDatasetError:
        return StoreDatasetError(
            f"The store refused to {what} (HTTP {response.status_code})."
        )

    def _select(self, query: str) -> List[List[str]]:
        response = self._request(
            "POST",
            self._query_url,
            data={"query": query},
            headers={"Accept": "text/csv"},
        )
        if response.status_code != 200:
            raise self._refused(response, "list the datasets")
        rows = list(csv.reader(io.StringIO(response.text)))
        return rows[1:]

    def _construct(self, query: str) -> str:
        response = self._request(
            "POST",
            self._query_url,
            data={"query": query},
            headers={"Accept": "application/n-triples"},
        )
        if response.status_code != 200:
            raise self._refused(response, "export the links")
        return response.text

    def _graph(self, graph: str) -> str:
        response = self._request(
            "GET",
            self._graph_url,
            params={"graph": graph},
            headers={"Accept": "application/n-triples"},
        )
        if response.status_code == 404:
            return ""
        if response.status_code != 200:
            raise self._refused(response, f"export {graph}")
        return response.text

    def _update(self, update: str) -> None:
        response = self._request(
            "POST",
            self._update_url,
            data={"update": update},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if response.status_code >= 300:
            raise self._refused(response, "delete")


def _count(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0
