"""
Unit tests for the JSON-LD loader (loaders/jsonld_loader.py) — validation,
error handling, and edge cases.

Tests cover:
- Valid JSON-LD files load correctly from both string and Path arguments
- Malformed JSON raises json.JSONDecodeError
- Valid JSON but structurally incomplete data produces graceful defaults
- File encoding edge cases (UTF-8 BOM — explicitly tests current behaviour)
- from_dict with minimal / missing / extra fields
- Accessor methods return None for absent keys
- save() round-trips the mapping without data loss (full to_dict comparison)
- find_database_key_for_rdf_store() handles .csv extension variants and
  sourceFile matching
- get_all_variable_keys() and has_variable()
- ColumnMapping.get_variable_key() with non-standard mapsTo values
"""

import codecs
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add data_descriptor to path so local imports resolve
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from loaders import (
    ColumnMapping,
    Database,
    JSONLDMapping,
    SchemaReconstructionNode,
    SchemaVariable,
    Table,
)

# ---------------------------------------------------------------------------
# Shared minimal valid mapping dict
# ---------------------------------------------------------------------------

_MINIMAL = {
    "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
    "@id": "mapping:test",
    "@type": "mapping:DataMapping",
    "name": "Loader Test Mapping",
    "description": "Test description",
    "version": "1.0.0",
    "created": "2025-01-15",
    "endpoint": "http://localhost:7200/repositories/test/statements",
    "schema": {
        "@id": "schema:test/v1",
        "@type": "schema:SemanticSchema",
        "name": "Test Schema",
        "version": "1.0.0",
        "prefixes": {"sio": "http://semanticscience.org/resource/"},
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
                    }
                },
            },
        },
    },
    "databases": {
        "db1": {
            "@id": "mapping:database/db1",
            "@type": "mapping:Database",
            "name": "patient_records",
            "description": "Patient database",
            "locale": "en_GB",
            "tables": {
                "patients": {
                    "@id": "mapping:table/patients",
                    "@type": "mapping:Table",
                    "sourceFile": "patients.csv",
                    "description": "Patient records",
                    "columns": {
                        "id_col": {
                            "mapsTo": "schema:variable/identifier",
                            "localColumn": "id",
                        },
                        "sex_col": {
                            "mapsTo": "schema:variable/biological_sex",
                            "localColumn": "sex",
                            "localMappings": {"male": "M", "female": "F"},
                        },
                    },
                }
            },
        }
    },
}


# ---------------------------------------------------------------------------
# from_file: valid file loading
# ---------------------------------------------------------------------------


class TestFromFileValidLoading(unittest.TestCase):
    """JSONLDMapping.from_file must load valid JSON-LD files correctly."""

    def _write_tmp(self, data: dict, encoding: str = "utf-8") -> str:
        """Write data as JSON to a temporary file and return its path."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False, encoding=encoding
        ) as fh:
            json.dump(data, fh, ensure_ascii=False)
            return fh.name

    def test_from_file_with_string_path(self):
        """from_file must accept a plain string path."""
        tmp = self._write_tmp(_MINIMAL)
        try:
            mapping = JSONLDMapping.from_file(tmp)
            self.assertEqual(mapping.name, "Loader Test Mapping")
        finally:
            os.unlink(tmp)

    def test_from_file_with_path_object(self):
        """from_file must accept a pathlib.Path object."""
        tmp = self._write_tmp(_MINIMAL)
        try:
            mapping = JSONLDMapping.from_file(Path(tmp))
            self.assertEqual(mapping.name, "Loader Test Mapping")
        finally:
            os.unlink(tmp)

    def test_from_file_populates_variables(self):
        """from_file must populate the variables dict correctly."""
        tmp = self._write_tmp(_MINIMAL)
        try:
            mapping = JSONLDMapping.from_file(tmp)
            self.assertIn("identifier", mapping.variables)
            self.assertIn("biological_sex", mapping.variables)
        finally:
            os.unlink(tmp)

    def test_from_file_populates_databases(self):
        """from_file must populate the databases dict correctly."""
        tmp = self._write_tmp(_MINIMAL)
        try:
            mapping = JSONLDMapping.from_file(tmp)
            self.assertIn("db1", mapping.databases)
        finally:
            os.unlink(tmp)

    def test_from_file_utf8_bom_raises_decode_error(self):
        """from_file with a UTF-8 BOM-encoded file must raise json.JSONDecodeError.

        The loader currently uses encoding='utf-8' (not 'utf-8-sig'), so the
        BOM byte-order mark is not stripped and causes a parse failure.
        """
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as raw_fh:
                tmp_path = raw_fh.name
                content = json.dumps(_MINIMAL, ensure_ascii=False)
                raw_fh.write(codecs.BOM_UTF8)
                raw_fh.write(content.encode("utf-8"))

            with self.assertRaises(json.JSONDecodeError):
                JSONLDMapping.from_file(tmp_path)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# from_file: error paths
# ---------------------------------------------------------------------------


class TestFromFileErrorPaths(unittest.TestCase):
    """from_file must raise appropriate exceptions for problematic inputs."""

    def test_nonexistent_file_raises_file_not_found(self):
        """A path to a non-existent file must raise FileNotFoundError."""
        missing = Path(tempfile.gettempdir()) / "nonexistent_loader_abc123.jsonld"
        with self.assertRaises(FileNotFoundError):
            JSONLDMapping.from_file(str(missing))

    def test_malformed_json_raises_decode_error(self):
        """A file containing malformed JSON must raise json.JSONDecodeError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False
        ) as fh:
            fh.write('{"invalid": json, }')
            tmp = fh.name
        try:
            with self.assertRaises(json.JSONDecodeError):
                JSONLDMapping.from_file(tmp)
        finally:
            os.unlink(tmp)

    def test_truncated_json_raises_decode_error(self):
        """A truncated JSON file must raise json.JSONDecodeError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False
        ) as fh:
            fh.write('{"name": "incomplete"')
            tmp = fh.name
        try:
            with self.assertRaises(json.JSONDecodeError):
                JSONLDMapping.from_file(tmp)
        finally:
            os.unlink(tmp)

    def test_empty_file_raises_decode_error(self):
        """An empty file must raise json.JSONDecodeError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False
        ) as fh:
            tmp = fh.name  # file is empty
        try:
            with self.assertRaises(json.JSONDecodeError):
                JSONLDMapping.from_file(tmp)
        finally:
            os.unlink(tmp)

    def test_json_array_root_is_rejected(self):
        """A valid JSON file whose root is an array (not an object) must be
        rejected with an error — non-object roots are not valid mappings."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False
        ) as fh:
            json.dump([{"key": "value"}], fh)
            tmp = fh.name
        try:
            with self.assertRaises(Exception):
                JSONLDMapping.from_file(tmp)
        finally:
            os.unlink(tmp)


# ---------------------------------------------------------------------------
# from_dict: minimal and missing fields
# ---------------------------------------------------------------------------


class TestFromDictMinimalFields(unittest.TestCase):
    """from_dict must handle missing optional fields gracefully."""

    def test_from_dict_with_only_required_fields(self):
        """from_dict must succeed with only required structural fields present."""
        bare = {
            "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
            "@type": "mapping:DataMapping",
            "name": "Bare Mapping",
            "schema": {
                "@type": "schema:SemanticSchema",
                "variables": {
                    "id": {
                        "@type": "schema:IdentifierVariable",
                        "dataType": "identifier",
                        "predicate": "sio:SIO_000673",
                        "class": "ncit:C25364",
                    }
                },
            },
            "databases": {
                "db1": {
                    "@type": "mapping:Database",
                    "tables": {
                        "t1": {
                            "@type": "mapping:Table",
                            "columns": {
                                "id_col": {
                                    "mapsTo": "schema:variable/id",
                                    "localColumn": "id",
                                }
                            },
                        }
                    },
                }
            },
        }
        mapping = JSONLDMapping.from_dict(bare)
        self.assertEqual(mapping.name, "Bare Mapping")
        self.assertIn("id", mapping.variables)

    def test_from_dict_missing_schema_produces_empty_variables(self):
        """from_dict without a schema key must produce an empty variables dict."""
        data = {
            "@context": {"@vocab": "https://example.org/"},
            "name": "No Schema",
            "databases": {},
        }
        mapping = JSONLDMapping.from_dict(data)
        self.assertEqual(mapping.variables, {})

    def test_from_dict_missing_databases_produces_empty_databases(self):
        """from_dict without a databases key must produce an empty databases dict."""
        data = {
            "@context": {"@vocab": "https://example.org/"},
            "name": "No Databases",
            "schema": {"@type": "schema:SemanticSchema", "variables": {}},
        }
        mapping = JSONLDMapping.from_dict(data)
        self.assertEqual(mapping.databases, {})

    def test_from_dict_defaults_name_to_empty_string(self):
        """from_dict without a name key must default to an empty string."""
        mapping = JSONLDMapping.from_dict({})
        self.assertEqual(mapping.name, "")

    def test_from_dict_preserves_raw_data(self):
        """from_dict must store the original dict in raw_data unchanged."""
        data = copy.deepcopy(_MINIMAL)
        mapping = JSONLDMapping.from_dict(data)
        self.assertEqual(mapping.raw_data, data)

    def test_from_dict_with_extra_fields_does_not_raise(self):
        """from_dict must not raise when extra fields are present in the dict."""
        data = copy.deepcopy(_MINIMAL)
        data["unknownField"] = "some extra value"
        data["databases"]["db1"]["unknownDbField"] = 99
        try:
            JSONLDMapping.from_dict(data)
        except Exception as exc:  # noqa: BLE001
            self.fail(f"from_dict raised with extra fields: {exc}")


# ---------------------------------------------------------------------------
# Accessor methods
# ---------------------------------------------------------------------------


class TestAccessorMethods(unittest.TestCase):
    """Accessor methods must return the correct data or None for absent keys."""

    def setUp(self):
        """Create a mapping instance for all accessor tests."""
        self.mapping = JSONLDMapping.from_dict(_MINIMAL)

    def test_get_variable_returns_correct_type(self):
        """get_variable must return a SchemaVariable for a known key."""
        var = self.mapping.get_variable("biological_sex")
        self.assertIsInstance(var, SchemaVariable)
        self.assertEqual(var.data_type, "categorical")

    def test_get_variable_returns_none_for_unknown_key(self):
        """get_variable must return None for an unknown variable key."""
        self.assertIsNone(self.mapping.get_variable("nonexistent"))

    def test_get_database_returns_correct_type(self):
        """get_database must return a Database for a known key."""
        db = self.mapping.get_database("db1")
        self.assertIsInstance(db, Database)

    def test_get_database_returns_none_for_unknown_key(self):
        """get_database must return None for an unknown database key."""
        self.assertIsNone(self.mapping.get_database("ghost_db"))

    def test_get_all_tables_returns_all_table_objects(self):
        """get_all_tables must return a list of all Table objects."""
        tables = self.mapping.get_all_tables()
        self.assertEqual(len(tables), 1)
        self.assertIsInstance(tables[0], Table)

    def test_get_column_for_variable_returns_column_mapping(self):
        """get_column_for_variable must return a ColumnMapping for a known variable."""
        col = self.mapping.get_column_for_variable("biological_sex")
        self.assertIsInstance(col, ColumnMapping)
        self.assertEqual(col.local_column, "sex")

    def test_get_column_for_variable_returns_none_for_unknown(self):
        """get_column_for_variable must return None when the variable is absent."""
        self.assertIsNone(self.mapping.get_column_for_variable("not_a_variable"))

    def test_get_local_term_returns_correct_value(self):
        """get_local_term must return the local term for a known schema term."""
        self.assertEqual(self.mapping.get_local_term("biological_sex", "male"), "M")

    def test_get_local_term_returns_none_for_unknown_term(self):
        """get_local_term must return None when the schema term is not mapped."""
        self.assertIsNone(self.mapping.get_local_term("biological_sex", "unknown_term"))

    def test_get_target_class_returns_correct_value(self):
        """get_target_class must return the ontology URI for a known schema term."""
        self.assertEqual(
            self.mapping.get_target_class("biological_sex", "male"), "ncit:C20197"
        )

    def test_get_target_class_returns_none_for_unknown_variable(self):
        """get_target_class must return None when the variable does not exist."""
        self.assertIsNone(self.mapping.get_target_class("no_var", "male"))

    def test_get_local_column_returns_correct_name(self):
        """get_local_column must return the local column name for a known variable."""
        self.assertEqual(self.mapping.get_local_column("identifier"), "id")

    def test_get_local_column_returns_none_for_unknown_variable(self):
        """get_local_column must return None when the variable is not mapped."""
        self.assertIsNone(self.mapping.get_local_column("no_such_variable"))

    def test_get_all_variable_keys_returns_all_keys(self):
        """get_all_variable_keys must return a list containing all variable keys."""
        keys = self.mapping.get_all_variable_keys()
        self.assertIn("identifier", keys)
        self.assertIn("biological_sex", keys)

    def test_has_variable_returns_true_for_existing(self):
        """has_variable must return True for a variable that exists."""
        self.assertTrue(self.mapping.has_variable("identifier"))

    def test_has_variable_returns_false_for_absent(self):
        """has_variable must return False for a variable that does not exist."""
        self.assertFalse(self.mapping.has_variable("missing_var"))

    def test_get_first_database_name_returns_name(self):
        """get_first_database_name must return the name of the first database."""
        self.assertEqual(self.mapping.get_first_database_name(), "patient_records")

    def test_get_first_database_name_returns_none_when_empty(self):
        """get_first_database_name must return None when there are no databases."""
        mapping = JSONLDMapping.from_dict({})
        self.assertIsNone(mapping.get_first_database_name())


# ---------------------------------------------------------------------------
# find_database_key_for_rdf_store
# ---------------------------------------------------------------------------


class TestFindDatabaseKeyForRdfStore(unittest.TestCase):
    """find_database_key_for_rdf_store must match by name or sourceFile."""

    def setUp(self):
        """Create a mapping instance.

        The fixture uses distinct values:
        - database name = 'patient_records'
        - table sourceFile = 'patients.csv'
        """
        self.mapping = JSONLDMapping.from_dict(_MINIMAL)

    def test_matches_by_database_name(self):
        """Must find a database when the RDF store name matches the database name."""
        result = self.mapping.find_database_key_for_rdf_store("patient_records")
        self.assertEqual(result, "db1")

    def test_matches_without_csv_extension(self):
        """Must match when the RDF store name omits the .csv extension."""
        result = self.mapping.find_database_key_for_rdf_store("patients")
        self.assertEqual(result, "db1")

    def test_matches_by_source_file(self):
        """Must find a database when the RDF store name matches a table sourceFile
        (not the database name)."""
        result = self.mapping.find_database_key_for_rdf_store("patients.csv")
        self.assertEqual(result, "db1")

    def test_returns_none_for_unknown_name(self):
        """Must return None when the RDF store name does not match any database."""
        result = self.mapping.find_database_key_for_rdf_store("unknown_db")
        self.assertIsNone(result)

    def test_returns_none_when_no_databases(self):
        """Must return None when the mapping has no databases."""
        mapping = JSONLDMapping.from_dict({})
        result = mapping.find_database_key_for_rdf_store("any_db")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# save() and round-trip
# ---------------------------------------------------------------------------


class TestSaveAndRoundTrip(unittest.TestCase):
    """save() must write valid JSON-LD that can be loaded back without data loss."""

    def test_save_creates_valid_json_file(self):
        """save() must produce a file that parses as valid JSON."""
        mapping = JSONLDMapping.from_dict(_MINIMAL)
        with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as fh:
            tmp = fh.name
        try:
            mapping.save(tmp)
            with open(tmp, encoding="utf-8") as fh:
                data = json.load(fh)
            self.assertIn("schema", data)
            self.assertIn("databases", data)
        finally:
            os.unlink(tmp)

    def test_save_and_reload_full_round_trip(self):
        """Reloading a saved mapping must preserve all data (full to_dict comparison)."""
        mapping = JSONLDMapping.from_dict(_MINIMAL)
        with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as fh:
            tmp = fh.name
        try:
            mapping.save(tmp)
            reloaded = JSONLDMapping.from_file(tmp)
            self.assertEqual(reloaded.to_dict(), mapping.to_dict())
        finally:
            os.unlink(tmp)

    def test_save_uses_ensure_ascii_false(self):
        """save() must write Unicode characters as-is, not as escaped sequences."""
        data = copy.deepcopy(_MINIMAL)
        data["name"] = "Données Cliniques — Ünïcödé"
        mapping = JSONLDMapping.from_dict(data)
        with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as fh:
            tmp = fh.name
        try:
            mapping.save(tmp)
            raw_content = Path(tmp).read_text(encoding="utf-8")
            # Unicode characters must appear verbatim, not as \uXXXX sequences
            self.assertIn("Données", raw_content)
        finally:
            os.unlink(tmp)


# ---------------------------------------------------------------------------
# ColumnMapping edge cases
# ---------------------------------------------------------------------------


class TestColumnMappingEdgeCases(unittest.TestCase):
    """Edge cases for ColumnMapping.from_dict and get_variable_key."""

    def test_get_variable_key_returns_none_for_empty_maps_to(self):
        """get_variable_key must return None when maps_to is empty."""
        col = ColumnMapping(key="x", maps_to="", local_column="col")
        self.assertIsNone(col.get_variable_key())

    def test_get_variable_key_returns_none_for_wrong_prefix(self):
        """get_variable_key must return None when maps_to has the wrong prefix."""
        col = ColumnMapping(
            key="x", maps_to="mapping:variable/test", local_column="col"
        )
        self.assertIsNone(col.get_variable_key())

    def test_from_dict_local_column_as_list_uses_first_element(self):
        """When localColumn is a list, from_dict must use the first element."""
        data = {"mapsTo": "schema:variable/id", "localColumn": ["id_col", "alt_col"]}
        col = ColumnMapping.from_dict("id", data)
        self.assertEqual(col.local_column, "id_col")

    def test_from_dict_empty_local_column_list_falls_back_to_empty_string(self):
        """When localColumn is an empty list, from_dict must use an empty string."""
        data = {"mapsTo": "schema:variable/id", "localColumn": []}
        col = ColumnMapping.from_dict("id", data)
        self.assertEqual(col.local_column, "")

    def test_local_mappings_with_null_value_are_preserved(self):
        """localMappings with null values must be preserved in the dataclass."""
        data = {
            "mapsTo": "schema:variable/sex",
            "localColumn": "sex",
            "localMappings": {"missing_or_unspecified": None},
        }
        col = ColumnMapping.from_dict("sex", data)
        self.assertIsNone(col.local_mappings["missing_or_unspecified"])


# ---------------------------------------------------------------------------
# SchemaVariable edge cases
# ---------------------------------------------------------------------------


class TestSchemaVariableEdgeCases(unittest.TestCase):
    """Edge cases for SchemaVariable.from_dict."""

    def test_variable_without_value_mapping_has_empty_value_mappings(self):
        """A variable with no valueMapping must have an empty value_mappings dict."""
        data = {
            "@type": "schema:ContinuousVariable",
            "dataType": "continuous",
            "predicate": "sio:SIO_000008",
            "class": "ncit:C156420",
        }
        var = SchemaVariable.from_dict("age", data)
        self.assertEqual(var.value_mappings, {})

    def test_variable_with_value_mapping_terms_missing_target_class_is_skipped(self):
        """A term in valueMapping that lacks targetClass must be silently skipped."""
        data = {
            "@type": "schema:CategoricalVariable",
            "dataType": "categorical",
            "predicate": "sio:SIO_000008",
            "class": "ncit:C28421",
            "valueMapping": {
                "terms": {
                    "valid": {"targetClass": "ncit:C20197"},
                    "invalid": {"noTargetClass": "something"},
                }
            },
        }
        var = SchemaVariable.from_dict("test_var", data)
        self.assertIn("valid", var.value_mappings)
        self.assertNotIn("invalid", var.value_mappings)

    def test_variable_defaults_id_from_key_when_absent(self):
        """When @id is absent, the var_id must be derived from the variable key."""
        data = {
            "@type": "schema:ContinuousVariable",
            "dataType": "continuous",
            "predicate": "sio:SIO_000008",
            "class": "ncit:C156420",
        }
        var = SchemaVariable.from_dict("my_var", data)
        self.assertIn("my_var", var.var_id)

    def test_schema_reconstruction_nodes_are_parsed(self):
        """schemaReconstruction list must be parsed into SchemaReconstructionNode objects."""
        data = {
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
                },
                {
                    "@type": "schema:UnitNode",
                    "predicate": "sio:SIO_000221",
                    "class": "ncit:C29848",
                    "nodeLabel": "years",
                },
            ],
        }
        var = SchemaVariable.from_dict("age", data)
        self.assertEqual(len(var.schema_reconstruction), 2)
        self.assertIsInstance(var.schema_reconstruction[0], SchemaReconstructionNode)


# ---------------------------------------------------------------------------
# get_prefixes_string
# ---------------------------------------------------------------------------


class TestGetPrefixesString(unittest.TestCase):
    """get_prefixes_string must produce well-formed SPARQL PREFIX declarations."""

    def setUp(self):
        """Create a mapping instance."""
        self.mapping = JSONLDMapping.from_dict(_MINIMAL)

    def test_returns_prefix_declarations(self):
        """Each prefix must appear as a PREFIX line in the output string."""
        prefixes_str = self.mapping.get_prefixes_string()
        self.assertIn("PREFIX sio:", prefixes_str)

    def test_each_prefix_line_contains_angle_bracket_uri(self):
        """Each PREFIX line must contain the URI in angle brackets."""
        prefixes_str = self.mapping.get_prefixes_string()
        self.assertIn("<http://semanticscience.org/resource/>", prefixes_str)

    def test_empty_prefixes_returns_empty_or_blank_string(self):
        """An empty prefixes dict must return an empty (or whitespace-only) string."""
        mapping = JSONLDMapping.from_dict({})
        result = mapping.get_prefixes_string()
        self.assertEqual(result.strip(), "")


if __name__ == "__main__":
    unittest.main()
