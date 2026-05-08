"""
Shared pytest fixtures for all tests (unit, integration, e2e).

The pythonTool stub lives in tests/unit/conftest.py so it only applies to
unit tests and does not bleed into integration or e2e tests.
"""

import pytest
import polars as pl


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
