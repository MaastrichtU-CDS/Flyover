"""
Test doubles and app wiring for the API layer (issue #109).

Every API test drives the app ``create_app()`` builds — the production
blueprints, SPA route and ``APP_CONTEXT`` wiring — through Flask's test
client, with every service behind the controllers replaced by an autospecced
double:

- the RDF store service and the suggestion service in ``APP_CONTEXT``;
- every helper in ``APP_CONTEXT`` (triplifier run, graph upload, background
  jobs, PostgreSQL ingest, semantic-map lookups, the name matcher), specced
  from the factory's own callables, so a signature change in the factory or
  a wrong call in a controller fails here;
- the service classes the controllers import directly (``IngestService``,
  ``DescribeService``, ``AnnotateService``, ``ShareService``).

What stays real is the HTTP layer: Flask and Werkzeug, the controllers, and
the request-boundary validation they run before a service is involved
(``MappingValidator``, ``JSONLDMapping`` parsing, the orphan-column clean-up
and the pure name matching it uses). The session cache is the real
``SessionCache``: a controller writing a misspelt attribute fails a test
instead of passing silently on a MagicMock.

The app runs with production settings (``TESTING`` off), so an unhandled
exception becomes the 500 a browser would get rather than an exception in
the test. Doubles answer with a neutral "nothing there yet" by default; each
test sets the answers its scenario needs.

The fixtures live in ``conftest.py``; this module holds the plain Python so
test modules (and the performance checks) can import it by name.
"""

from __future__ import annotations

import ast
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from unittest.mock import create_autospec

import polars as pl
from flask import Flask, Response, request
from werkzeug.exceptions import HTTPException

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app_factory import (  # noqa: E402
    AppSettings,
    SessionCache,
    build_app_context,
    create_app,
)
from controllers import (  # noqa: E402
    annotate_controller,
    describe_controller,
    ingest_controller,
    share_controller,
)
from repositories import RDFStoreRepository  # noqa: E402
from services import (  # noqa: E402
    AnnotateService,
    DescribeService,
    IngestService,
    RDFStoreService,
    ShareService,
)
from services.suggestions import SuggestionConfig, SuggestionService  # noqa: E402
from tests.fixtures.mappings import TABLE, valid_mapping  # noqa: E402,F401

API_TESTS_DIR = Path(__file__).parent

STORE_URL = "http://rdf-store.api-test:7200"
REPO = "userRepo"

# The APP_CONTEXT callables the factory wires; every one is replaced.
HELPER_NAMES = (
    "run_triplifier",
    "upload_func",
    "start_background",
    "handle_postgres",
    "has_semantic_map",
    "formulate_local_map",
    "get_table_names",
    "name_matcher",
    "get_semantic_map",
)

ROUTE_OUTCOMES = ("success", "failure")

# Methods Flask adds to every rule on its own.
AUTOMATIC_METHODS = frozenset({"HEAD", "OPTIONS"})


# ---------------------------------------------------------------------------
# Settings and a tiny built SPA
# ---------------------------------------------------------------------------


def api_settings(work_dir: Path) -> AppSettings:
    """Settings that keep every file the app writes inside ``work_dir``."""
    root = work_dir / "root"
    (root / "data_descriptor" / "static").mkdir(parents=True, exist_ok=True)
    return AppSettings(
        rdf_store_url=STORE_URL,
        repo=REPO,
        root_dir=str(root) + os.sep,
        child_dir="data_descriptor",
        upload_folder=str(work_dir / "uploads"),
    )


SPA_INDEX = "<!doctype html><html><body><div id=app>Flyover SPA</div></body></html>"
SPA_SCRIPT = "console.log('flyover');\n" * 200
OUTSIDE_SECRET = "this file lives next to the SPA directory, never inside it"


def build_spa(work_dir: Path) -> Path:
    """A built SPA (index plus one asset) with a file next to it, not in it."""
    spa = work_dir / "spa"
    (spa / "assets").mkdir(parents=True)
    (spa / "index.html").write_text(SPA_INDEX, encoding="utf-8")
    (spa / "assets" / "app.js").write_text(SPA_SCRIPT, encoding="utf-8")
    (work_dir / "secret.txt").write_text(OUTSIDE_SECRET, encoding="utf-8")
    return spa


# ---------------------------------------------------------------------------
# Service doubles
# ---------------------------------------------------------------------------

PARSED_FRAME = pl.DataFrame({"id": ["P1", "P2"], "sex": ["M", "F"]})


def idle_suggestion_state() -> Dict[str, Any]:
    return {
        "status": "idle",
        "progress": {"done": 0, "total": 0},
        "error": None,
        "records": {},
    }


def suggestion_status(config: SuggestionConfig) -> Dict[str, Any]:
    """What ``SuggestionService.status()`` reports with tier 1 active."""
    return {
        "compute": config.compute,
        "tiers": {
            1: {"state": "active"},
            2: {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"},
            3: {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"},
        },
        "threshold": config.threshold,
        "rules_version": "1.2.0",
        "prompt_export": {"state": "active", "chunk": 40, "min_value_count": 10},
    }


def semantic_map_response() -> Response:
    """A JSON-LD download as ``ShareService.download_semantic_map`` builds it."""
    return Response(
        '{"@type": "mapping:DataMapping"}',
        mimetype="application/ld+json",
        headers={"Content-Disposition": "attachment;filename=semantic_mapping.jsonld"},
    )


def ontology_response() -> Response:
    """An N-Triples download as ``ShareService.download_ontology`` builds it."""
    return Response(
        "<http://data.local/a> <http://data.local/b> <http://data.local/c> .\n",
        mimetype="application/n-triples",
        headers={"Content-Disposition": "attachment;filename=local_ontology.nt"},
    )


def make_store_double() -> Any:
    store = create_autospec(RDFStoreService, instance=True)
    # Instance attributes set in __init__ are invisible to autospec.
    store.rdf_store_url = STORE_URL
    store.repo = REPO
    store.repository = create_autospec(RDFStoreRepository, instance=True)
    store.repository.download_ontology.return_value = ("", 404)
    store.repository.execute_query.return_value = ""
    store.check_data_exists.return_value = False
    store.get_databases.return_value = []
    store.get_column_info_by_database.return_value = {}
    store.get_categories.return_value = None
    store.insert_equivalencies.return_value = True
    store.get_graph_structure.return_value = {"tables": [], "tableColumns": {}}
    return store


def make_suggestion_config(
    tiers: Iterable[int] = (1,), compute: str = "host", threshold: float = 0.8
) -> SuggestionConfig:
    config = SuggestionConfig()
    config.tiers = list(tiers)
    config.compute = compute
    config.threshold = threshold
    return config


def make_suggestion_double(config: Optional[SuggestionConfig] = None) -> Any:
    service = create_autospec(SuggestionService, instance=True)
    service.config = config or make_suggestion_config()
    service.status.return_value = suggestion_status(service.config)
    service.start.return_value = {"status": "started"}
    service.get_state.return_value = idle_suggestion_state()
    service.bump_priority.return_value = {"status": "ok", "moved": 0}
    service.build_prompt.return_value = {"prompts": [], "items": 0}
    service.ingest.return_value = {"status": "ok", "accepted": 0, "rejected": []}
    return service


def make_helper_doubles(reference: Dict[str, Any]) -> Dict[str, Any]:
    """Doubles of the factory's helpers, specced from the real callables."""
    helpers = {name: create_autospec(reference[name]) for name in HELPER_NAMES}
    helpers["run_triplifier"].return_value = (True, "Triplification completed")
    helpers["upload_func"].return_value = (True, ["Uploaded the graphs"])
    helpers["start_background"].return_value = []
    helpers["handle_postgres"].return_value = None
    helpers["has_semantic_map"].return_value = False
    helpers["formulate_local_map"].return_value = {}
    helpers["get_table_names"].return_value = []
    # Exact names by default; a test that needs the fuzzy matcher sets it.
    helpers["name_matcher"].side_effect = lambda map_db, target_db: (
        map_db == target_db
    )
    helpers["get_semantic_map"].return_value = (None, None, False)
    return helpers


def make_ingest_double() -> Any:
    ingest = create_autospec(IngestService)
    ingest.allowed_file.return_value = True
    ingest.validate_csv_files.return_value = (True, None)
    ingest.parse_csv_files.return_value = ([PARSED_FRAME], [TABLE], None)
    ingest.validate_excel_files.return_value = (True, None)
    ingest.parse_excel_files.return_value = ([PARSED_FRAME], [f"{TABLE}_Sheet1"], None)
    ingest.parse_pk_fk_data.return_value = None
    ingest.parse_cross_graph_data.return_value = None
    return ingest


def make_describe_double() -> Any:
    describe = create_autospec(DescribeService)
    describe.parse_form_data_for_database.return_value = {}
    describe.process_detailed_form_data.return_value = {}
    describe.get_preselected_values.return_value = {}
    return describe


def make_annotate_double() -> Any:
    annotate = create_autospec(AnnotateService)
    annotate.prepare_annotation_data.return_value = {}
    annotate.execute_annotation.return_value = ({}, None)
    annotate.get_verification_data.return_value = ([], [], {})
    annotate.verify_single_annotation.return_value = (True, True, "ASK {}", None)
    return annotate


def make_share_double() -> Any:
    share = create_autospec(ShareService)
    # A Response is single-use: build a fresh one for every request.
    # (Autospec already checks the call against the real signature.)
    share.download_semantic_map.side_effect = lambda *a, **k: semantic_map_response()
    share.download_ontology.side_effect = lambda *a, **k: ontology_response()
    share.generate_mock_data_from_semantic_map.return_value = {
        "success": True,
        "data": {f"centre_x.{TABLE}": [{"id": "ID_00001"}]},
        "metadata": {
            "generated_from": "semantic_map",
            "privacy_preserving": True,
            "row_count": 1,
            "tables_generated": 1,
        },
    }
    return share


@dataclass
class ApiDoubles:
    """Everything an API test can arrange: the state and every service."""

    settings: AppSettings
    session_cache: SessionCache
    store: Any
    suggestions: Any
    helpers: Dict[str, Any]
    ingest: Any
    describe: Any
    annotate: Any
    share: Any

    @classmethod
    def create(cls, settings: AppSettings) -> "ApiDoubles":
        session_cache = SessionCache(settings.repo)
        store = make_store_double()
        suggestions = make_suggestion_double()
        reference = build_app_context(
            settings,
            {
                "session_cache": session_cache,
                "rdf_store_service": store,
                "suggestion_service": suggestions,
            },
        )
        return cls(
            settings=settings,
            session_cache=session_cache,
            store=store,
            suggestions=suggestions,
            helpers=make_helper_doubles(reference),
            ingest=make_ingest_double(),
            describe=make_describe_double(),
            annotate=make_annotate_double(),
            share=make_share_double(),
        )

    def context(self) -> Dict[str, Any]:
        """The ``APP_CONTEXT`` overrides that put the doubles in place."""
        return {
            "session_cache": self.session_cache,
            "rdf_store_service": self.store,
            "suggestion_service": self.suggestions,
            **self.helpers,
        }

    def controller_patches(self) -> List[Tuple[Any, str, Any]]:
        """(module, attribute, double) for the service classes controllers import."""
        return [
            (ingest_controller, "IngestService", self.ingest),
            (describe_controller, "DescribeService", self.describe),
            (annotate_controller, "AnnotateService", self.annotate),
            (share_controller, "ShareService", self.share),
        ]

    def reset_state(self) -> None:
        """Back to a fresh session and unused doubles (between Hypothesis examples)."""
        # Clear first: services add attributes of their own (suggestion jobs).
        vars(self.session_cache).clear()
        self.session_cache.__init__(self.settings.repo)  # type: ignore[misc]
        for double in (
            self.store,
            self.suggestions,
            self.ingest,
            self.describe,
            self.annotate,
            self.share,
            *self.helpers.values(),
        ):
            double.reset_mock()


def build_api_app(
    doubles: ApiDoubles,
    spa_dir: Path,
    context_overrides: Optional[Dict[str, Any]] = None,
) -> Flask:
    """The production app with the doubles in place of every service."""
    context = {**doubles.context(), **(context_overrides or {})}
    return create_app(doubles.settings, context=context, spa_dir=str(spa_dir))


# ---------------------------------------------------------------------------
# Which routes a test requested
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecordedRequest:
    method: str
    path: str


@dataclass
class RequestLog:
    """The requests a test sent, and an app whose URL map resolves them."""

    requests: List[RecordedRequest] = field(default_factory=list)
    app: Optional[Flask] = None

    def attach(self, app: Flask) -> None:
        if self.app is None:
            self.app = app

        @app.before_request
        def _record() -> None:
            self.requests.append(RecordedRequest(request.method, request.path))


def rule_for(app: Flask, path: str, rule_text: str) -> bool:
    """True when ``path`` routes to the rule ``rule_text`` under any of its methods.

    A request with a method the rule does not allow (a 405 test) still counts
    as a request to that rule.
    """
    adapter = app.url_map.bind("localhost")
    for rule in app.url_map.iter_rules():
        if rule.rule != rule_text:
            continue
        for method in sorted(rule.methods or ()):
            try:
                matched, _ = adapter.match(path, method=method, return_rule=True)
            except HTTPException:
                continue
            if matched.rule == rule_text:
                return True
    return False


def unmet_route_marks(log: RequestLog, marks: Iterable[Any]) -> List[str]:
    """Problems with a test's ``route`` marks, given the requests it sent."""
    problems = []
    for mark in marks:
        if len(mark.args) != 3:
            problems.append(f"route mark needs (method, rule, outcome): {mark.args}")
            continue
        method, rule_text, outcome = mark.args
        if outcome not in ROUTE_OUTCOMES:
            problems.append(f"unknown outcome {outcome!r} in {mark.args}")
        if log.app is None or not any(
            sent.method == method and rule_for(log.app, sent.path, rule_text)
            for sent in log.requests
        ):
            problems.append(
                f"marked as a {outcome} test of {method} {rule_text}, "
                "but never sent that request"
            )
    return problems


def _is_route_mark(node: ast.AST) -> bool:
    """``pytest.mark.route(...)``, spelt out."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    owner = getattr(func, "value", None)
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "route"
        and isinstance(owner, ast.Attribute)
        and owner.attr == "mark"
        and isinstance(owner.value, ast.Name)
        and owner.value.id == "pytest"
    )


def _route_marks_in(tree: ast.AST) -> Iterable[ast.Call]:
    """Route marks applied as decorators or through ``pytest.param(marks=...)``."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield from (d for d in node.decorator_list if _is_route_mark(d))
        elif isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg != "marks":
                    continue
                value = keyword.value
                candidates = (
                    value.elts if isinstance(value, (ast.List, ast.Tuple)) else [value]
                )
                yield from (c for c in candidates if _is_route_mark(c))


def declared_route_marks(test_dir: Path = API_TESTS_DIR) -> Set[Tuple[str, ...]]:
    """Every route mark the API test modules apply, read from their source.

    Read from the source, not from collected items, so the inventory check
    sees every module even when only some tests are selected. A mark counts
    when written as ``pytest.mark.route(<method>, <rule>, <outcome>)`` with
    literal arguments, as a decorator or in ``pytest.param(..., marks=...)``.
    """
    found: Set[Tuple[str, ...]] = set()
    for path in sorted(test_dir.glob("test_api_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for call in _route_marks_in(tree):
            found.add(tuple(ast.literal_eval(arg) for arg in call.args))
    return found


def declared_methods(app: Flask) -> Dict[str, Set[str]]:
    """Rule text -> the methods it declares (without HEAD/OPTIONS)."""
    methods: Dict[str, Set[str]] = {}
    for rule in app.url_map.iter_rules():
        methods.setdefault(rule.rule, set()).update(
            set(rule.methods or ()) - AUTOMATIC_METHODS
        )
    return methods
