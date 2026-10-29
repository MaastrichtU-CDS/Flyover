"""
Test doubles and app wiring for the cross-layer integration tests (issue #108).

Every integration test drives a real Flask app whose internal wiring is the
production wiring: controllers call services, services call the repository,
the query builder, the loaders and the validators. Nothing inside
``data_descriptor`` is mocked. Only the two process boundaries are faked, and
both are faked where the real thing would leave the process:

- the RDF store is served by :class:`FakeRdfStore`, which answers SPARQL
  requests at the ``requests`` library level through ``responses``. It is
  backend-agnostic: it speaks the SPARQL 1.1 protocol (query/update form
  fields, CSV and JSON result formats, the Graph Store Protocol) and is not
  coupled to GraphDB or RDF4J (see issue #101);
- the ``curl`` binary the ingest service shells out to for graph uploads is
  replaced by a script on ``PATH`` that records its arguments
  (:class:`FakeCurl`).

The triplifier package (``pythonTool``) is a real dependency and runs for
real: the ingest tests triplify a CSV into a temporary directory.

:func:`build_app` uses ``create_app()``, the factory the gunicorn entry point
calls, so the app under test has the production wiring; only the store URL
and the background jobs (recorded instead of spawned) differ.

The fixtures live in ``conftest.py``; this module holds the plain Python so
test modules can import it by name (``from tests.integration.support import
...``), which pytest does not support for ``conftest`` modules.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

import pytest
import requests
import responses
from flask import Flask

# Make `data_descriptor` modules importable as top-level packages, mirroring
# the existing test modules.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app_factory import AppSettings, SessionCache, create_app  # noqa: E402,F401

REPO_ROOT = Path(__file__).resolve().parents[5]
EXAMPLE_DATA_DIR = REPO_ROOT / "example_data"
VALIDATION_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "validation"


# ---------------------------------------------------------------------------
# Fake RDF store (HTTP boundary)
# ---------------------------------------------------------------------------


@dataclass
class StoreCall:
    """One HTTP request Flyover sent to the RDF store."""

    method: str
    url: str
    query: Optional[str] = None
    update: Optional[str] = None
    accept: str = ""
    graph: Optional[str] = None

    @property
    def kind(self) -> str:
        if self.update is not None:
            return "update"
        if self.graph is not None:
            return "graph"
        if self.query is not None:
            return "ask" if _is_ask(self.query) else "select"
        return "other"


def _is_ask(query: str) -> bool:
    """True for a SPARQL ASK query, ignoring leading comments and PREFIX lines."""
    body = re.sub(r"^\s*#.*$", "", query, flags=re.MULTILINE)
    body = re.sub(r"PREFIX\s+\S+\s+<[^>]*>", "", body, flags=re.IGNORECASE)
    return body.lstrip().upper().startswith("ASK")


def _csv(header: List[str], rows: List[List[Any]]) -> str:
    """SPARQL CSV results: RFC 4180 quoting, so values may hold commas/quotes."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        writer.writerow([str(v) for v in row])
    return buffer.getvalue()


class FakeRdfStore:
    """
    A SPARQL 1.1 endpoint faked at the ``requests`` level.

    It serves the queries the query builder, the session helpers, the share
    service and the annotation helper issue, from a small in-memory model:

    - ``tables``: table name -> column name -> list of cell values. This is
      what the triplifier would have produced from an ingested CSV.
    - ``ontology_graphs``: database name -> N-Triples content, served by the
      Graph Store Protocol endpoint for the share flow.

    Behaviour knobs for error paths:

    - ``outage``: an exception every request raises (store unreachable);
    - ``outage_after``: the store goes down after this many requests;
    - ``ask_answer``: a bool, or a callable ``query -> bool``;
    - ``update_status``: HTTP status for SPARQL UPDATE requests, or
      ``update_status_fn`` to decide per update text;
    - ``select_status`` / ``ask_status``: HTTP status for SELECT and ASK
      requests (the body is an HTML error page when not 200, like a real
      store would return); ``ask_status_queue`` answers the next ASKs
      with the listed statuses first (a briefly overloaded store).

    Every request is recorded in ``calls`` so tests can assert what crossed
    the boundary (``selects``, ``asks``, ``updates``, ``graph_downloads``).
    """

    base_url = "http://rdf-store.test:7200"
    repo = "userRepo"
    column_uri_base = "http://data.local/rdf/ontology"

    def __init__(self) -> None:
        self.tables: Dict[str, Dict[str, List[str]]] = {}
        self.ontology_graphs: Dict[str, str] = {}
        self.calls: List[StoreCall] = []
        self.outage: Optional[BaseException] = None
        self.outage_after: Optional[int] = None
        self.ask_answer: Any = True
        self.update_status = 204
        # Optional per-update status: ``update_text -> HTTP status`` (e.g.
        # refuse writes into one named graph only).
        self.update_status_fn: Optional[Callable[[str], int]] = None
        self.select_status = 200
        self.ask_status = 200
        # Statuses for the next ASK requests, consumed in order; then
        # ``ask_status`` applies. Models a store that is briefly overloaded.
        self.ask_status_queue: List[int] = []
        # Raw bodies for SELECTs whose text contains the key (first match
        # wins): lets a test hand Flyover a malformed or unexpected result.
        self.select_overrides: Dict[str, str] = {}
        self.data_graph_exists = True

    # -- model helpers -----------------------------------------------------

    def seed_table(self, table: str, columns: Dict[str, List[str]]) -> None:
        """Register a triplified table with its columns and cell values."""
        self.tables[table] = {col: list(values) for col, values in columns.items()}

    def seed_ontology_graph(self, database: str, ntriples: str) -> None:
        self.ontology_graphs[database] = ntriples

    def column_uri(self, table: str, column: str) -> str:
        return f"{self.column_uri_base}/{table}.{column}"

    @property
    def repository_url(self) -> str:
        return f"{self.base_url}/repositories/{self.repo}"

    @property
    def statements_url(self) -> str:
        return f"{self.repository_url}/statements"

    @property
    def graph_store_url(self) -> str:
        return f"{self.repository_url}/rdf-graphs/service"

    # -- recorded traffic --------------------------------------------------

    @property
    def selects(self) -> List[StoreCall]:
        return [c for c in self.calls if c.kind == "select"]

    @property
    def asks(self) -> List[StoreCall]:
        return [c for c in self.calls if c.kind == "ask"]

    @property
    def updates(self) -> List[str]:
        return [c.update for c in self.calls if c.kind == "update"]

    @property
    def graph_downloads(self) -> List[str]:
        return [c.graph for c in self.calls if c.kind == "graph"]

    # -- responses wiring --------------------------------------------------

    def install(self, mock: responses.RequestsMock) -> None:
        pattern = re.compile(
            re.escape(self.repository_url)
            + r"(/statements|/rdf-graphs/service)?(\?.*)?$"
        )
        mock.add_callback(responses.GET, pattern, callback=self._handle)
        mock.add_callback(responses.POST, pattern, callback=self._handle)

    def _handle(self, request):
        call = self._record(request)
        if self.outage is not None:
            raise self.outage
        if self.outage_after is not None and len(self.calls) > self.outage_after:
            raise requests.ConnectionError("fake store went down")
        if call.kind == "update":
            status = (
                self.update_status_fn(call.update)
                if self.update_status_fn is not None
                else self.update_status
            )
            return status, {}, ""
        if call.kind == "graph":
            return self._serve_graph(call.graph)
        if call.kind == "ask":
            status = (
                self.ask_status_queue.pop(0)
                if self.ask_status_queue
                else self.ask_status
            )
            if status != 200:
                return (
                    status,
                    {"Content-Type": "text/html"},
                    "<html><body>Fake store error</body></html>",
                )
            answer = (
                self.ask_answer(call.query)
                if callable(self.ask_answer)
                else self.ask_answer
            )
            if "http://data.local/" in call.query and "GRAPH ?g" in call.query:
                answer = self.data_graph_exists
            body = json.dumps({"head": {}, "boolean": bool(answer)})
            return (
                200,
                {"Content-Type": "application/sparql-results+json; charset=utf-8"},
                body,
            )
        if call.kind == "select":
            if self.select_status != 200:
                return (
                    self.select_status,
                    {"Content-Type": "text/html"},
                    "<html><body>Fake store error</body></html>",
                )
            return (
                200,
                {"Content-Type": "text/csv; charset=utf-8"},
                self._answer_select(call.query),
            )
        return 400, {"Content-Type": "text/plain"}, "fake store: unsupported request"

    def _record(self, request) -> StoreCall:
        parsed = urlparse(request.url)
        params = parse_qs(parsed.query)
        form: Dict[str, List[str]] = {}
        if request.body:
            body = request.body
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            form = parse_qs(body)
        call = StoreCall(
            method=request.method,
            url=request.url,
            query=(form.get("query") or params.get("query") or [None])[0],
            update=(form.get("update") or [None])[0],
            accept=request.headers.get("Accept", ""),
            graph=(
                unquote(params["graph"][0])
                if parsed.path.endswith("/rdf-graphs/service") and "graph" in params
                else None
            ),
        )
        self.calls.append(call)
        return call

    def _serve_graph(self, graph_uri: str):
        for database, content in self.ontology_graphs.items():
            # The empty database name is the legacy single ontology graph
            expected = (
                f"http://ontology.local/{database}/"
                if database
                else "http://ontology.local/"
            )
            if graph_uri == expected:
                return (
                    200,
                    {"Content-Type": "application/n-triples; charset=utf-8"},
                    content,
                )
        return 404, {"Content-Type": "text/plain"}, "graph not found"

    def _answer_select(self, query: str) -> str:
        """Answer the SELECT queries Flyover's layers build."""
        compact = " ".join(query.split())
        for needle, body in self.select_overrides.items():
            if needle in compact:
                return body

        # Column info / graph structure: ?uri dbo:column ?column
        if "?uri dbo:column ?column" in compact:
            rows = [
                [self.column_uri(table, column), column]
                for table, columns in self.tables.items()
                for column in columns
            ]
            return _csv(["uri", "column"], rows)

        # Database names: ?s dbo:table ?db
        if "dbo:table ?db" in compact:
            return _csv(["db"], [[table] for table in self.tables])

        # Categories: ?v dbo:column '<col>' ... GROUP BY (?value)
        if "GROUP BY (?value)" in compact:
            column = re.search(r"dbo:column '([^']*)'", compact).group(1)
            scope = re.search(r"LCASE\('([^']*)\.'\)", compact)
            tables = (
                [
                    t
                    for t in self.tables
                    if scope and t.lower() == scope.group(1).lower()
                ]
                if scope
                else list(self.tables)
            )
            counts: Counter = Counter()
            for table in tables:
                counts.update(self.tables[table].get(column, []))
            return _csv(
                ["value", "count"],
                [[value, count] for value, count in sorted(counts.items())],
            )

        # Column class URI: ?uri dbo:column '<col>' . FILTER(... '<table>.') LIMIT 1
        if "SELECT ?uri WHERE" in compact and "LIMIT 1" in compact:
            column = re.search(r"dbo:column '([^']*)'", compact).group(1)
            table = re.search(r"LCASE\('([^']*)\.'\)", compact).group(1)
            for name, columns in self.tables.items():
                if name.lower() == table.lower() and column in columns:
                    return _csv(["uri"], [[self.column_uri(name, column)]])
            return _csv(["uri"], [])

        # Ontology graphs: SELECT DISTINCT ?g ... "http://ontology.local/"
        if "SELECT DISTINCT ?g" in compact and "http://ontology.local/" in compact:
            return _csv(
                ["g"], [[f"http://ontology.local/{db}/"] for db in self.ontology_graphs]
            )

        raise AssertionError(f"FakeRdfStore cannot answer this SELECT:\n{query}")


# ---------------------------------------------------------------------------
# Fake curl (subprocess boundary used by the graph upload)
# ---------------------------------------------------------------------------

_FAKE_CURL_SCRIPT = """#!/bin/sh
# Test double for curl: record argv, one argument per line, then exit with
# the configured status. The real upload code only looks at the exit code.
for arg in "$@"; do
    printf '%s\\n' "$arg" >> "$FAKE_CURL_LOG"
done
printf '%s\\n' '--END-OF-INVOCATION--' >> "$FAKE_CURL_LOG"
if [ -n "$FAKE_CURL_SLEEP" ]; then sleep "$FAKE_CURL_SLEEP"; fi
if [ "$FAKE_CURL_FAIL_DATA_BINARY" = "1" ]; then
    for arg in "$@"; do
        if [ "$arg" = "--data-binary" ]; then exit 22; fi
    done
fi
exit "${FAKE_CURL_EXIT:-0}"
"""


@dataclass
class CurlUpload:
    url: str
    content_type: str
    file_path: str
    method: str  # "data-binary" or "streaming"


class FakeCurl:
    def __init__(self, log_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._log = log_path
        self._monkeypatch = monkeypatch

    def fail(self, exit_code: int = 22) -> None:
        """Make every curl invocation fail (22 is curl's HTTP-error status)."""
        self._monkeypatch.setenv("FAKE_CURL_EXIT", str(exit_code))

    def fail_data_binary_only(self) -> None:
        """Fail the --data-binary strategy; the streaming fallback succeeds."""
        self._monkeypatch.setenv("FAKE_CURL_FAIL_DATA_BINARY", "1")

    def hang(self, seconds: float) -> None:
        """Make every curl invocation take this long before exiting."""
        self._monkeypatch.setenv("FAKE_CURL_SLEEP", str(seconds))

    @property
    def invocations(self) -> List[List[str]]:
        if not self._log.exists():
            return []
        result, current = [], []
        for line in self._log.read_text().splitlines():
            if line == "--END-OF-INVOCATION--":
                result.append(current)
                current = []
            else:
                current.append(line)
        return result

    @property
    def uploads(self) -> List[CurlUpload]:
        uploads = []
        for argv in self.invocations:
            content_type = next(
                (a.split(": ", 1)[1] for a in argv if a.startswith("Content-Type: ")),
                "",
            )
            if "--data-binary" in argv:
                file_path = argv[argv.index("--data-binary") + 1].lstrip("@")
                method = "data-binary"
            else:
                file_path = argv[argv.index("-T") + 1]
                method = "streaming"
            uploads.append(CurlUpload(argv[-1], content_type, file_path, method))
        return uploads


# ---------------------------------------------------------------------------
# The production app, against the fake store
# ---------------------------------------------------------------------------


def build_app(work_dir: Path, store: FakeRdfStore) -> Flask:
    """Build the app ``create_app()`` builds, pointed at the fake store.

    ``work_dir`` replaces the ``/app/`` root the Docker image uses: the
    triplifier writes its output there and uploads are saved under it.
    """
    root_dir = str(work_dir / "root") + os.sep
    child_dir = "data_descriptor"
    upload_folder = work_dir / "uploads"
    os.makedirs(os.path.join(root_dir, child_dir), exist_ok=True)
    upload_folder.mkdir()
    settings = AppSettings(
        rdf_store_url=store.base_url,
        repo=store.repo,
        root_dir=root_dir,
        child_dir=child_dir,
        upload_folder=str(upload_folder),
    )

    # main spawns the PK/FK and cross-graph jobs as gevent greenlets that
    # sleep for seconds before running. The test app records the request
    # instead; the jobs' own wiring is exercised directly by the ingest tests.
    background_jobs: List[Any] = []
    app = create_app(
        settings,
        context={"start_background": background_jobs.append},
        spa_dir=str(work_dir / "no-spa-built"),
    )
    app.config["TESTING"] = True
    app.config["BACKGROUND_JOBS"] = background_jobs
    return app


# ---------------------------------------------------------------------------
# A small, valid JSON-LD mapping and the matching triplified table
# ---------------------------------------------------------------------------

TABLE = "patients"
"""The RDF store's name for the ingested table (the CSV file name)."""

MAPPING_CONTEXT = {
    "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/",
    "sio": "http://semanticscience.org/resource/",
    "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
    "mesh": "http://id.nlm.nih.gov/mesh/",
    "roo": "http://www.cancerdata.org/roo/",
    "schema": "schema/",
    "mapping": "mapping/",
    "variables": {"@id": "schema:hasVariable", "@container": "@index"},
    "schemaReconstruction": {
        "@id": "schema:hasSchemaReconstruction",
        "@container": "@list",
    },
    "valueMapping": {"@id": "schema:hasValueMapping"},
    "terms": {"@id": "schema:hasTerms", "@container": "@index"},
    "targetClass": {"@id": "schema:mapsToClass", "@type": "@id"},
    "databases": {"@id": "mapping:hasDatabase", "@container": "@index"},
    "tables": {"@id": "mapping:hasTable", "@container": "@index"},
    "columns": {"@id": "mapping:hasColumn", "@container": "@index"},
    "localMappings": {"@id": "mapping:hasLocalMappings", "@container": "@index"},
    "mapsTo": {"@id": "mapping:mapsToVariable", "@type": "@id"},
}

_MAPPING = {
    "@context": MAPPING_CONTEXT,
    "@id": "mapping:integration",
    "@type": "mapping:DataMapping",
    "name": "Integration mapping",
    "description": "Three variables over one triplified table",
    "version": "1.0.0",
    "created": "2026-10-28",
    "endpoint": f"{FakeRdfStore.base_url}/repositories/{FakeRdfStore.repo}/statements",
    "schema": {
        "@id": "schema:integration/v1",
        "@type": "schema:SemanticSchema",
        "name": "Integration schema",
        "version": "1.0.0",
        "prefixes": {
            "sio": "http://semanticscience.org/resource/",
            "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
            "mesh": "http://id.nlm.nih.gov/mesh/",
            "roo": "http://www.cancerdata.org/roo/",
        },
        "variables": {
            "identifier": {
                "@id": "schema:variable/identifier",
                "@type": "schema:IdentifierVariable",
                "dataType": "identifier",
                "predicate": "sio:SIO_000673",
                "class": "ncit:C25364",
            },
            "biological_sex": {
                "@id": "schema:variable/biological_sex",
                "@type": "schema:CategoricalVariable",
                "dataType": "categorical",
                "predicate": "sio:SIO_000008",
                "class": "ncit:C28421",
                "schemaReconstruction": [
                    {
                        "@type": "schema:ClassNode",
                        "predicate": "sio:SIO_000235",
                        "class": "mesh:D000091569",
                        "classLabel": "demographicClass",
                        "aestheticLabel": "Demographic",
                    }
                ],
                "valueMapping": {
                    "terms": {
                        "male": {"targetClass": "ncit:C20197"},
                        "female": {"targetClass": "ncit:C16576"},
                        "missing_or_unspecified": {"targetClass": "ncit:C54031"},
                    }
                },
            },
            "age_at_diagnosis": {
                "@id": "schema:variable/age_at_diagnosis",
                "@type": "schema:ContinuousVariable",
                "dataType": "continuous",
                "predicate": "sio:SIO_000008",
                "class": "ncit:C156420",
                "schemaReconstruction": [
                    {
                        "@type": "schema:UnitNode",
                        "predicate": "sio:SIO_000221",
                        "class": "ncit:C29848",
                        "nodeLabel": "years",
                        "aestheticLabel": "Years",
                    }
                ],
            },
        },
    },
    "databases": {
        "centre_x": {
            "@id": "mapping:database/centre_x",
            "@type": "mapping:Database",
            # The store names a database after the ingested CSV file; the
            # describe and annotate flows match the mapping on this name.
            "name": TABLE,
            "tables": {
                TABLE: {
                    "@id": f"mapping:table/{TABLE}",
                    "@type": "mapping:Table",
                    "sourceFile": TABLE,
                    "columns": {
                        "identifier": {
                            "mapsTo": "schema:variable/identifier",
                            "localColumn": "id",
                        },
                        "biological_sex": {
                            "mapsTo": "schema:variable/biological_sex",
                            "localColumn": "sex",
                            "localMappings": {
                                "male": "M",
                                "female": "F",
                                "missing_or_unspecified": "",
                            },
                        },
                        "age_at_diagnosis": {
                            "mapsTo": "schema:variable/age_at_diagnosis",
                            "localColumn": "age",
                        },
                    },
                }
            },
        }
    },
}

TABLE_COLUMNS: Dict[str, List[str]] = {
    "id": ["P1", "P2", "P3"],
    "sex": ["M", "F", "M"],
    "age": ["45", "60", "32"],
}
"""What the store holds after the matching CSV was ingested."""

CSV_BYTES = b"id,sex,age\nP1,M,45\nP2,F,60\nP3,M,32\n"
"""The CSV that produces TABLE_COLUMNS when triplified."""


def xlsx_bytes(sheets: Dict[str, List[List[str]]]) -> bytes:
    """Build a minimal .xlsx workbook (inline strings) without a spreadsheet library.

    ``sheets`` maps sheet name -> rows (first row is the header).
    """
    import zipfile
    from xml.sax.saxutils import escape

    def cell_ref(col: int, row: int) -> str:
        letters = ""
        col += 1
        while col:
            col, rem = divmod(col - 1, 26)
            letters = chr(65 + rem) + letters
        return f"{letters}{row + 1}"

    def sheet_xml(rows: List[List[str]]) -> str:
        body = []
        for r, row in enumerate(rows):
            cells = "".join(
                f'<c r="{cell_ref(c, r)}" t="inlineStr"><is><t>{escape(str(v))}</t></is></c>'
                for c, v in enumerate(row)
            )
            body.append(f'<row r="{r + 1}">{cells}</row>')
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"<sheetData>{''.join(body)}</sheetData></worksheet>"
        )

    names = list(sheets)
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        + "".join(
            f'<sheet name="{escape(name)}" sheetId="{i + 1}" r:id="rId{i + 1}"/>'
            for i, name in enumerate(names)
        )
        + "</sheets></workbook>"
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(
            f'<Relationship Id="rId{i + 1}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{i + 1}.xml"/>'
            for i in range(len(names))
        )
        + "</Relationships>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        + "".join(
            f'<Override PartName="/xl/worksheets/sheet{i + 1}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            for i in range(len(names))
        )
        + "</Types>"
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        for i, name in enumerate(names):
            archive.writestr(f"xl/worksheets/sheet{i + 1}.xml", sheet_xml(sheets[name]))
    return buffer.getvalue()


def load_validation_fixture(name: str) -> dict:
    return json.loads((VALIDATION_FIXTURES_DIR / name).read_text(encoding="utf-8"))
