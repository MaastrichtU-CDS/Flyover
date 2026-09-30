"""
Unit tests for the describe details-state endpoint.

The describe pages work on the semantic map in the browser's IndexedDB. The
details-state endpoint therefore accepts that map in a POST body and uses it
for the response only (details population and preselected values): the value
mappings the page's dropdowns offer come from the same map, and one browser's
map can never leak into the shared session state.
"""

import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask

from controllers import describe_bp
from loaders import JSONLDMapping

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# A minimal mapping that passes MappingValidator (the full @context and the
# mapping:DataMapping @type are required). Terms and local mappings vary per
# test through _mapping_with_terms().
_BASE_MAPPING = {
    "@context": {
        "@vocab": "https://github.com/MaastrichtU-CDS/Flyover/",
        "sio": "http://semanticscience.org/resource/",
        "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
        "schema": "schema/",
        "mapping": "mapping/",
        "variables": {"@id": "schema:hasVariable", "@container": "@index"},
        "valueMapping": {"@id": "schema:hasValueMapping"},
        "terms": {"@id": "schema:hasTerms", "@container": "@index"},
        "targetClass": {"@id": "schema:mapsToClass", "@type": "@id"},
        "databases": {"@id": "mapping:hasDatabase", "@container": "@index"},
        "tables": {"@id": "mapping:hasTable", "@container": "@index"},
        "columns": {"@id": "mapping:hasColumn", "@container": "@index"},
        "localMappings": {"@id": "mapping:hasLocalMappings", "@container": "@index"},
        "mapsTo": {"@id": "mapping:mapsToVariable", "@type": "@id"},
    },
    "@id": "mapping:test",
    "@type": "mapping:DataMapping",
    "schema": {
        "@id": "schema:root",
        "@type": "schema:SemanticSchema",
        "variables": {},
    },
    "databases": {
        "christie": {
            "@id": "mapping:database/christie",
            "@type": "mapping:Database",
            "name": "christie",
            "tables": {
                "data": {
                    "@id": "mapping:table/christie/data",
                    "@type": "mapping:Table",
                    "sourceFile": "christie",
                    "columns": {},
                }
            },
        }
    },
}


def _mapping_with_terms(term_key, local_term="M", with_stage=False):
    """A valid mapping whose biological_sex variable maps 'M' to `term_key`."""
    data = copy.deepcopy(_BASE_MAPPING)
    data["schema"]["variables"]["biological_sex"] = {
        "@type": "schema:CategoricalVariable",
        "dataType": "categorical",
        "predicate": "sio:has_sex",
        "class": "ncit:C28421",
        "valueMapping": {"terms": {term_key: {"targetClass": "ncit:C20197"}}},
    }
    data["databases"]["christie"]["tables"]["data"]["columns"]["sex"] = {
        "mapsTo": "schema:variable/biological_sex",
        "localColumn": "sex",
        "localMappings": {term_key: local_term},
    }
    if with_stage:
        data["schema"]["variables"]["t_stage"] = {
            "@type": "schema:CategoricalVariable",
            "dataType": "categorical",
            "predicate": "sio:has_stage",
            "class": "ncit:C48885",
            "valueMapping": {"terms": {"0": {"targetClass": "ncit:C28095"}}},
        }
        data["databases"]["christie"]["tables"]["data"]["columns"]["stage"] = {
            "mapsTo": "schema:variable/t_stage",
            "localColumn": "stage",
        }
    return data


class _Cache:
    """A stand-in session cache with plain dicts, so object identity and
    mutation are observable in the tests."""

    def __init__(self, mapping_data):
        self.databases = None
        self.descriptive_info = {}
        self.DescriptiveInfoDetails = {
            "christie": [{'Biological Sex (or "sex")': [{"value": "M"}]}]
        }
        self.jsonld_mapping = JSONLDMapping.from_dict(mapping_data)


def _make_app(session_cache):
    app = Flask(__name__)
    app.secret_key = "test-secret"
    rdf_store_service = MagicMock()
    rdf_store_service.get_databases.return_value = ["christie"]
    rdf_store_service.get_column_info_by_database.return_value = {
        "christie": ["sex", "stage"]
    }
    rdf_store_service.get_categories.return_value = None
    app.config["APP_CONTEXT"] = {
        "session_cache": session_cache,
        "rdf_store_service": rdf_store_service,
        "name_matcher": lambda map_db, database: True,
    }
    app.register_blueprint(describe_bp)
    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDescribeVariableDetailsState(unittest.TestCase):
    def test_get_uses_the_session_mapping_and_populates_the_session(self):
        """GET (no body) keeps the previous behaviour: the session's own
        mapping drives the response and the details population persists in
        the session."""
        session_map = _mapping_with_terms("male", with_stage=True)
        session_cache = _Cache(session_map)
        # get_categories returns a CSV, so the t_stage variable is appended.
        app = _make_app(session_cache)
        app.config["APP_CONTEXT"][
            "rdf_store_service"
        ].get_categories.return_value = "value\n0\n1"
        with app.test_client() as client:
            resp = client.get("/api/v1/describe-variable-details-state")
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
        # Preselected from the SESSION mapping's terms: male -> 'M' -> Male.
        self.assertEqual(
            data["preselected_values"].get('christie_sex_category_"M"'), "Male"
        )
        # The variable's value-mapping options come from the same mapping,
        # for browsers that have no map of their own.
        self.assertEqual(
            data["category_options"].get('Biological Sex (or "sex")'), ["Male"]
        )
        # The population persists in the session (today's behaviour).
        flat = json.dumps(session_cache.DescriptiveInfoDetails)
        self.assertIn("T Stage", flat)

    def test_post_body_mapping_renders_this_response_only(self):
        """A POST body mapping drives the response — preselected values come
        from the browser's map, not the session's — and the session's details
        state is left untouched."""
        session_cache = _Cache(_mapping_with_terms("male"))
        session_details_before = json.dumps(session_cache.DescriptiveInfoDetails)
        session_info_before = json.dumps(session_cache.descriptive_info)

        # The browser's map: 'M' maps to 'man' instead of 'male', and it
        # also knows a t_stage variable the session's map does not.
        body_map = _mapping_with_terms("man", with_stage=True)

        app = _make_app(session_cache)
        rdf = app.config["APP_CONTEXT"]["rdf_store_service"]
        rdf.get_categories.return_value = "value\n0\n1"
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/describe-variable-details-state",
                json={"mapping": body_map},
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()

        # Preselected from the BODY mapping's terms: man -> 'M' -> Man.
        self.assertEqual(
            data["preselected_values"].get('christie_sex_category_"M"'), "Man"
        )
        # The value-mapping options follow the body map too.
        self.assertEqual(
            data["category_options"].get('Biological Sex (or "sex")'), ["Man"]
        )
        # The body map's variable shows in the response...
        self.assertIn("T Stage", json.dumps(data["descriptive_info_details"]))
        # ...but never in the session's details state.
        self.assertEqual(
            json.dumps(session_cache.DescriptiveInfoDetails), session_details_before
        )
        self.assertEqual(
            json.dumps(session_cache.descriptive_info), session_info_before
        )

    def test_post_invalid_body_mapping_falls_back_to_the_session(self):
        """An unvalidated request body must never reach the response: a
        mapping that fails MappingValidator is dropped and the session's own
        mapping is used."""
        session_cache = _Cache(_mapping_with_terms("male"))
        invalid = _mapping_with_terms("man")
        invalid["@type"] = "mapping:NotAThing"
        app = _make_app(session_cache)
        with app.test_client() as client:
            resp = client.post(
                "/api/v1/describe-variable-details-state",
                json={"mapping": invalid},
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
        self.assertEqual(
            data["preselected_values"].get('christie_sex_category_"M"'), "Male"
        )

    def test_post_without_mapping_uses_the_session(self):
        """No body mapping (empty body) keeps the previous behaviour."""
        session_cache = _Cache(_mapping_with_terms("male"))
        app = _make_app(session_cache)
        with app.test_client() as client:
            resp = client.post("/api/v1/describe-variable-details-state", json={})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
        self.assertEqual(
            data["preselected_values"].get('christie_sex_category_"M"'), "Male"
        )


if __name__ == "__main__":
    unittest.main()
