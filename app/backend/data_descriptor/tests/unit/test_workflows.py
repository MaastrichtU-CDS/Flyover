"""
Every backend test module says which workflow it tests (tests/workflows.py),
so ``pytest -m rdf`` runs all of the RDF workflow's tests and nothing else.
"""

from pathlib import Path

from ..workflows import RDF, SHARED, marker_for

TESTS = Path(__file__).parent.parent
LAYERS = ("unit", "integration", "api", "perf")


def _modules():
    return {path.name for layer in LAYERS for path in (TESTS / layer).glob("test_*.py")}


def test_every_test_module_is_the_rdf_workflows_or_shared():
    unplaced = sorted(_modules() - RDF - SHARED)

    assert (
        unplaced == []
    ), "Add each of these to RDF or SHARED in tests/workflows.py: " + ", ".join(
        unplaced
    )


def test_no_test_module_is_both():
    assert RDF & SHARED == set()


def test_an_rdf_module_gets_the_rdf_marker_and_a_shared_one_none():
    assert marker_for("test_query_builder.py") == "rdf"
    assert marker_for("test_api_factory.py") is None


def test_conftest_marks_by_it(request):
    # This module is shared: it carries its layer's marker, and no rdf.
    assert request.node.get_closest_marker("unit") is not None
    assert request.node.get_closest_marker("rdf") is None
