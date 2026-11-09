"""
A small, valid JSON-LD semantic map shared by the test layers.

Three variables (an identifier, a categorical and a continuous one) over one
table, ``patients``, of one database. The database name equals the table's
``sourceFile`` and the store's name for the ingested CSV, so every matcher in
the app (ingest, describe, annotate) pairs the mapping with that table.
"""

import copy
from typing import Any, Dict

TABLE = "patients"

MAPPING_CONTEXT: Dict[str, Any] = {
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

_VALID_MAPPING: Dict[str, Any] = {
    "@context": MAPPING_CONTEXT,
    "@id": "mapping:shared-fixture",
    "@type": "mapping:DataMapping",
    "name": "Shared fixture mapping",
    "description": "Three variables over one triplified table",
    "version": "1.0.0",
    "created": "2026-10-29",
    "endpoint": "http://rdf-store.test:7200/repositories/userRepo/statements",
    "schema": {
        "@id": "schema:shared-fixture/v1",
        "@type": "schema:SemanticSchema",
        "name": "Shared fixture schema",
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
            },
        },
    },
    "databases": {
        "centre_x": {
            "@id": "mapping:database/centre_x",
            "@type": "mapping:Database",
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


def valid_mapping() -> Dict[str, Any]:
    """A fresh deep copy of the shared valid mapping (safe to mutate)."""
    return copy.deepcopy(_VALID_MAPPING)
