"""
Shared pytest fixtures for all tests (unit, integration, e2e).

The pythonTool stub lives in tests/unit/conftest.py so it only applies to
unit tests and does not bleed into integration or e2e tests.
"""

from pathlib import Path

import pytest
import polars as pl

# ---------------------------------------------------------------------------
# Test layers (issue #106): one marker per layer, applied by folder, so each
# layer can be selected with `-m` as well as by path or `-k`.
#   unit         tests/unit/          isolated modules, external deps stubbed
#   integration  tests/integration/   real internal wiring, RDF store faked at HTTP level
#   api          tests/api/           HTTP behaviour of every route, services mocked (issue #109)
#   perf         tests/perf/          performance checks of the HTTP layer
#   e2e          tests/e2e/           full Docker stack (issue #110)
# ---------------------------------------------------------------------------

_TESTS_DIR = Path(__file__).parent
_LAYER_MARKERS = {
    "unit": "isolated unit test with external dependencies stubbed",
    "integration": "cross-layer test with the RDF store faked at the HTTP boundary",
    "api": "HTTP-level functional test of the Flask routes with the services mocked",
    "perf": "performance check of the HTTP layer; runs with -m perf or on its own",
    "e2e": "end-to-end test against the full Docker stack",
}


def pytest_configure(config):
    for marker, description in _LAYER_MARKERS.items():
        config.addinivalue_line("markers", f"{marker}: {description}")
    config.addinivalue_line(
        "markers",
        "route(method, rule, outcome): the route an API test requests and whether "
        "it exercises the route's 'success' or its 'failure' path",
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        path = Path(str(item.fspath))
        for layer in _LAYER_MARKERS:
            if (_TESTS_DIR / layer) in path.parents:
                item.add_marker(getattr(pytest.mark, layer))
    _skip_perf_unless_selected(config, items)


def _skip_perf_unless_selected(config, items):
    """Performance checks run only when asked for.

    They run with ``-m perf``, or when every collected test is one (pytest
    pointed at ``tests/perf/``). A plain run of the whole tree skips them, so
    timing-sensitive checks never slow down or flake the functional layers.
    """
    perf_items = [item for item in items if item.get_closest_marker("perf")]
    if not perf_items or len(perf_items) == len(items):
        return
    if "perf" in (config.getoption("markexpr") or ""):
        return
    skip = pytest.mark.skip(
        reason="performance checks run on their own: pytest -m perf (or tests/perf/)"
    )
    for item in perf_items:
        item.add_marker(skip)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_dataframe():
    """A small Polars DataFrame for preprocessing/service tests."""
    return pl.DataFrame(
        {
            "patient_id": ["1", "2", "3"],
            "age": ["45", "60", "32"],
            "sex": ["M", "F", "M"],
        }
    )


@pytest.fixture
def sample_jsonld_mapping():
    """Minimal JSON-LD mapping dict used by annotation and service tests."""
    return {
        "@context": {
            "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/",
            "schema": "schema/",
            "mapping": "mapping/",
        },
        "@id": "https://flyover.example.org/mapping/test",
        "@type": "mapping:DataMapping",
        "name": "Test Mapping",
        "version": "1.0.0",
        "endpoint": "http://localhost:7200/repositories/test/statements",
        "schema": {
            "@id": "schema:test/v1",
            "@type": "schema:SemanticSchema",
            "prefixes": {
                "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
                "roo": "http://www.cancerdata.org/roo/",
                "sio": "http://semanticscience.org/resource/",
            },
            "variables": {
                "biological_sex": {
                    "@id": "schema:variable/biological_sex",
                    "@type": "schema:CategoricalVariable",
                    "dataType": "categorical",
                    "predicate": "roo:P100018",
                    "class": "ncit:C28421",
                    "valueMapping": {
                        "terms": {
                            "male": {"targetClass": "ncit:C20197"},
                            "female": {"targetClass": "ncit:C16576"},
                        }
                    },
                },
                "age": {
                    "@id": "schema:variable/age",
                    "@type": "schema:ContinuousVariable",
                    "dataType": "continuous",
                    "predicate": "roo:P100027",
                    "class": "ncit:C25150",
                },
                "identifier": {
                    "@id": "schema:variable/identifier",
                    "@type": "schema:IdentifierVariable",
                    "dataType": "identifier",
                    "predicate": "roo:P100061",
                    "class": "ncit:C25364",
                },
            },
        },
        "databases": {
            "test_db": {
                "@id": "mapping:database/test_db",
                "@type": "mapping:Database",
                "name": "Test Database",
                "tables": {
                    "patients": {
                        "@id": "mapping:table/patients",
                        "@type": "mapping:Table",
                        "sourceFile": "patients.csv",
                        "columns": {
                            "sex_col": {
                                "mapsTo": "schema:variable/biological_sex",
                                "localColumn": "sex",
                                "localMappings": {
                                    "male": "M",
                                    "female": "F",
                                },
                            },
                            "age_col": {
                                "mapsTo": "schema:variable/age",
                                "localColumn": "age",
                            },
                            "id_col": {
                                "mapsTo": "schema:variable/identifier",
                                "localColumn": "patient_id",
                            },
                        },
                    }
                },
            }
        },
    }


@pytest.fixture
def mock_rdf_response():
    """A minimal CSV response that mimics RDF store query output."""
    return "uri,column\nhttp://data.local/patients.csv,age\nhttp://data.local/patients.csv,sex\n"


@pytest.fixture
def legacy_mapping_data():
    """A legacy JSON mapping dict for convert_legacy_mapping tests."""
    return {
        "endpoint": "http://localhost:7200/repositories/test/statements",
        "database_name": "patients.csv",
        "prefixes": "PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>\nPREFIX roo: <http://www.cancerdata.org/roo/>",
        "variable_info": {
            "biological_sex": {
                "predicate": "roo:P100018",
                "class": "ncit:C28421",
                "local_definition": "sex",
                "data_type": "categorical",
                "value_mapping": {
                    "terms": {
                        "male": {"target_class": "ncit:C20197", "local_term": "M"},
                        "female": {"target_class": "ncit:C16576", "local_term": "F"},
                    }
                },
            },
            "age": {
                "predicate": "roo:P100027",
                "class": "ncit:C25150",
                "local_definition": "age",
                "data_type": "continuous",
            },
        },
    }
