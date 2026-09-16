"""
RDF4J/GraphDB-compatible REST adapter for QLever.

The Flyover triplifier talks to its RDF store exclusively through the
RDF4J/GraphDB REST API:

    * SPARQL query   GET/POST  /repositories/<repo>            (query=...)
    * SPARQL update  POST      /repositories/<repo>/statements (update=...)
    * Graph Store    GET/POST/PUT/DELETE
                               /repositories/<repo>/rdf-graphs/service?graph=G
    * init probes    GET       /repositories/<repo>/protocol
                     GET       /repositories/<repo>/size

QLever, however, only exposes a single plain SPARQL HTTP endpoint. This thin
adapter implements exactly the endpoints above and translates them onto
QLever, so the application behaves identically regardless of the backing
store.
"""

import logging
import os

import requests
from flask import Flask, Response, request
from rdflib import Graph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("qlever-adapter")

app = Flask(__name__)

QLEVER_ENDPOINT = os.environ.get("QLEVER_ENDPOINT", "http://qlever-engine:7019").rstrip(
    "/"
)
QLEVER_ACCESS_TOKEN = os.environ.get("QLEVER_ACCESS_TOKEN", "flyover")

# Number of triples sent per INSERT DATA request when loading graphs.
INSERT_BATCH_SIZE = int(os.environ.get("QLEVER_INSERT_BATCH_SIZE", "20000"))

# Map HTTP content types to rdflib parser format names.
CONTENT_TYPE_TO_FORMAT = {
    "text/turtle": "turtle",
    "application/x-turtle": "turtle",
    "application/n-triples": "nt",
    "text/plain": "nt",
    "application/rdf+xml": "xml",
    "application/xml": "xml",
    "text/n3": "n3",
    "application/n-quads": "nquads",
    "application/trig": "trig",
    "application/ld+json": "json-ld",
}


def _rdflib_format(content_type: str) -> str:
    """Resolve an rdflib parser format from a request Content-Type header."""
    if not content_type:
        return "turtle"
    base = content_type.split(";", 1)[0].strip().lower()
    return CONTENT_TYPE_TO_FORMAT.get(base, "turtle")


def _qlever_query(query: str, accept: str, timeout: int = 3600) -> Response:
    """Forward a SPARQL query to QLever, preserving the requested format."""
    try:
        resp = requests.post(
            QLEVER_ENDPOINT,
            data={"query": query},
            headers={
                "Accept": accept or "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=timeout,
        )
        return Response(
            resp.content,
            status=resp.status_code,
            content_type=resp.headers.get("Content-Type", "text/plain"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Query forwarding error: %s", exc)
        return Response(str(exc), status=502, content_type="text/plain")


def _qlever_update(update: str, timeout: int = 3600) -> Response:
    """Forward a SPARQL update to QLever using the configured access token."""
    try:
        # The access token must be supplied via the Authorization header only.
        # QLever rejects a URL-encoded POST that also carries query parameters
        # in the URL ("URL-encoded POST requests must not contain query
        # parameters in the URL"), so we must NOT pass it as a `params=...`
        # query argument here.
        resp = requests.post(
            QLEVER_ENDPOINT,
            data={"update": update},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {QLEVER_ACCESS_TOKEN}",
            },
            timeout=timeout,
        )
        if 200 <= resp.status_code < 300:
            # RDF4J answers updates with 204 No Content.
            return Response(status=204)
        logger.error("Update failed (%s): %s", resp.status_code, resp.text[:500])
        return Response(resp.text, status=resp.status_code, content_type="text/plain")
    except Exception as exc:  # noqa: BLE001
        logger.error("Update forwarding error: %s", exc)
        return Response(str(exc), status=502, content_type="text/plain")


def _insert_graph(graph: Graph, graph_uri: str) -> Response:
    """Insert all triples of `graph` into the named graph via SPARQL UPDATE."""
    triples = list(graph)
    if not triples:
        return Response(status=204)

    # Build N-Triples lines (valid inside a SPARQL INSERT DATA block) and send
    # them in batches to keep individual requests manageable.
    batch: list[str] = []
    for s, p, o in triples:
        line = f"{s.n3()} {p.n3()} {o.n3()} ."
        batch.append(line)
        if len(batch) >= INSERT_BATCH_SIZE:
            resp = _flush_insert(batch, graph_uri)
            if resp.status_code >= 300:
                return resp
            batch = []

    if batch:
        resp = _flush_insert(batch, graph_uri)
        if resp.status_code >= 300:
            return resp

    return Response(status=204)


def _flush_insert(lines: list[str], graph_uri: str) -> Response:
    body = "\n".join(lines)
    if graph_uri:
        update = f"INSERT DATA {{ GRAPH <{graph_uri}> {{\n{body}\n}} }}"
    else:
        update = f"INSERT DATA {{\n{body}\n}}"
    return _qlever_update(update)


# ─── SPARQL query / update endpoints ────────────────────────────────────


@app.route("/repositories/<repo>", methods=["GET", "POST"])
def repository(repo: str) -> Response:
    """SPARQL query endpoint (RDF4J: GET/POST with `query`)."""
    query = request.values.get("query")
    # Some clients post an update to the base endpoint as well.
    update = request.values.get("update")
    if update:
        return _qlever_update(update)
    if not query:
        return Response("Missing 'query' parameter", status=400)
    return _qlever_query(query, request.headers.get("Accept", ""))


@app.route("/repositories/<repo>/statements", methods=["POST", "PUT", "DELETE"])
def statements(repo: str) -> Response:
    """SPARQL update endpoint (RDF4J: POST with `update`)."""
    update = request.values.get("update")
    if not update:
        # A direct RDF body POST to /statements would add to the default graph.
        ctype = request.headers.get("Content-Type", "")
        if request.data and "sparql-update" not in ctype:
            graph = Graph()
            graph.parse(data=request.data, format=_rdflib_format(ctype))
            return _insert_graph(graph, "")
        update = request.get_data(as_text=True)
    if not update:
        return Response("Missing 'update' parameter", status=400)
    return _qlever_update(update)


# ─── Graph Store Protocol endpoint ──────────────────────────────────────


@app.route(
    "/repositories/<repo>/rdf-graphs/service",
    methods=["GET", "POST", "PUT", "DELETE"],
)
def graph_store(repo: str) -> Response:
    """Graph Store Protocol for named graphs, mapped onto SPARQL UPDATE."""
    graph_uri = request.args.get("graph", "")

    if request.method == "GET":
        if graph_uri:
            query = (
                f"CONSTRUCT {{ ?s ?p ?o }} WHERE "
                f"{{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"
            )
        else:
            query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        accept = request.headers.get("Accept", "application/n-triples")
        return _qlever_query(query, accept)

    if request.method == "DELETE":
        if graph_uri:
            return _qlever_update(f"DROP SILENT GRAPH <{graph_uri}>")
        return _qlever_update("DROP SILENT DEFAULT")

    # POST (merge) or PUT (replace).
    ctype = request.headers.get("Content-Type", "")
    try:
        graph = Graph()
        graph.parse(data=request.get_data(), format=_rdflib_format(ctype))
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to parse uploaded graph: %s", exc)
        return Response(f"Could not parse RDF payload: {exc}", status=400)

    if request.method == "PUT" and graph_uri:
        # Replace semantics: clear the target graph first.
        clear = _qlever_update(f"DROP SILENT GRAPH <{graph_uri}>")
        if clear.status_code >= 300:
            return clear

    return _insert_graph(graph, graph_uri)


# ─── RDF4J init / health probes ─────────────────────────────────────────


@app.route("/repositories/<repo>/protocol", methods=["GET"])
def protocol(repo: str) -> Response:
    """RDF4J protocol version probe used by health checks."""
    return Response("12", status=200, content_type="text/plain")


@app.route("/repositories/<repo>/size", methods=["GET"])
def size(repo: str) -> Response:
    """Return the number of triples, mimicking RDF4J's `/size` endpoint."""
    query = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
    try:
        resp = requests.post(
            QLEVER_ENDPOINT,
            data={"query": query},
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
        )
        count = (
            resp.json()
            .get("results", {})
            .get("bindings", [{}])[0]
            .get("count", {})
            .get("value", "0")
        )
        return Response(str(count), status=200, content_type="text/plain")
    except Exception as exc:  # noqa: BLE001
        logger.error("Size probe error: %s", exc)
        return Response("0", status=200, content_type="text/plain")


@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
def health() -> Response:
    return Response("QLever adapter is running", status=200, content_type="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7200)
