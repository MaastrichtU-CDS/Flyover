"""
Unit tests for the JSONLDMapping loader class.

Tests cover:
- Loading from dictionary and file
- Schema variable parsing
- Database and table parsing
- Column mapping parsing
- Local term and target class retrieval
- Legacy format conversion
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from loaders import (
    SchemaReconstructionNode,
    SchemaVariable,
    ColumnMapping,
    Table,
    Database,
    JSONLDMapping,
)


class TestSchemaReconstructionNode(unittest.TestCase):
    """Test the SchemaReconstructionNode dataclass."""

    def test_from_dict_class_node(self):
        """Test creating a class node from dictionary."""
        data = {
            "@type": "schema:ClassNode",
            "predicate": "sio:SIO_000235",
            "class": "mesh:D000091569",
            "classLabel": "demographicClass",
            "aestheticLabel": "Demographic",
        }
        node = SchemaReconstructionNode.from_dict(data)
        self.assertEqual(node.node_type, "schema:ClassNode")
        self.assertEqual(node.predicate, "sio:SIO_000235")
        self.assertEqual(node.class_uri, "mesh:D000091569")
        self.assertEqual(node.class_label, "demographicClass")

    def test_from_dict_unit_node(self):
        """Test creating a unit node from dictionary."""
        data = {
            "@type": "schema:UnitNode",
            "predicate": "sio:SIO_000221",
            "class": "ncit:C29848",
            "nodeLabel": "years",
            "aestheticLabel": "Years",
        }
        node = SchemaReconstructionNode.from_dict(data)
        self.assertEqual(node.node_type, "schema:UnitNode")
        self.assertEqual(node.node_label, "years")

    def test_to_dict(self):
        """Test converting node back to dictionary."""
        node = SchemaReconstructionNode(
            node_type="schema:ClassNode",
            predicate="sio:SIO_000235",
            class_uri="mesh:D000091569",
            class_label="testLabel",
        )
        result = node.to_dict()
        self.assertEqual(result["@type"], "schema:ClassNode")
        self.assertEqual(result["predicate"], "sio:SIO_000235")
        self.assertEqual(result["classLabel"], "testLabel")


class TestSchemaVariable(unittest.TestCase):
    """Test the SchemaVariable dataclass."""

    def test_from_dict_simple(self):
        """Test creating a simple variable from dictionary."""
        data = {
            "@id": "schema:variable/identifier",
            "@type": "schema:IdentifierVariable",
            "dataType": "identifier",
            "predicate": "sio:SIO_000673",
            "class": "ncit:C25364",
        }
        var = SchemaVariable.from_dict("identifier", data)
        self.assertEqual(var.key, "identifier")
        self.assertEqual(var.var_type, "schema:IdentifierVariable")
        self.assertEqual(var.data_type, "identifier")
        self.assertEqual(var.predicate, "sio:SIO_000673")

    def test_from_dict_with_value_mapping(self):
        """Test creating a variable with value mapping."""
        data = {
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
        }
        var = SchemaVariable.from_dict("biological_sex", data)
        self.assertEqual(len(var.value_mappings), 2)
        self.assertEqual(var.value_mappings["male"], "ncit:C20197")
        self.assertEqual(var.value_mappings["female"], "ncit:C16576")

    def test_from_dict_with_schema_reconstruction(self):
        """Test creating a variable with schema reconstruction."""
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
                }
            ],
        }
        var = SchemaVariable.from_dict("test_var", data)
        self.assertEqual(len(var.schema_reconstruction), 1)
        self.assertEqual(var.schema_reconstruction[0].class_uri, "mesh:D000091569")


class TestColumnMapping(unittest.TestCase):
    """Test the ColumnMapping dataclass."""

    def test_from_dict_simple(self):
        """Test creating a simple column mapping."""
        data = {"mapsTo": "schema:variable/identifier", "localColumn": "id"}
        col = ColumnMapping.from_dict("identifier", data)
        self.assertEqual(col.maps_to, "schema:variable/identifier")
        self.assertEqual(col.local_column, "id")

    def test_from_dict_with_local_mappings(self):
        """Test creating a column with local mappings."""
        data = {
            "mapsTo": "schema:variable/biological_sex",
            "localColumn": "sex",
            "localMappings": {"male": "M", "female": "F"},
        }
        col = ColumnMapping.from_dict("biological_sex", data)
        self.assertEqual(col.local_mappings["male"], "M")
        self.assertEqual(col.local_mappings["female"], "F")

    def test_get_variable_key(self):
        """Test extracting variable key from mapsTo."""
        col = ColumnMapping(
            key="test", maps_to="schema:variable/biological_sex", local_column="sex"
        )
        self.assertEqual(col.get_variable_key(), "biological_sex")


class TestJSONLDMapping(unittest.TestCase):
    """Test the JSONLDMapping class."""

    def setUp(self):
        """Set up test mapping data."""
        self.mapping_data = {
            "@context": {"@vocab": "https://github.com/MaastrichtU-CDS/Flyover/"},
            "@id": "mapping:test",
            "@type": "mapping:DataMapping",
            "name": "Test Mapping",
            "description": "Test description",
            "version": "1.0.0",
            "created": "2025-01-01",
            "endpoint": "http://localhost:7200/repositories/test",
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
                "test_db": {
                    "@id": "mapping:database/test_db",
                    "@type": "mapping:Database",
                    "name": "Test Database",
                    "description": "Test database description",
                    "tables": {
                        "test_table": {
                            "@id": "mapping:table/test_table",
                            "@type": "mapping:Table",
                            "sourceFile": "test.csv",
                            "description": "Test table",
                            "columns": {
                                "identifier": {
                                    "mapsTo": "schema:variable/identifier",
                                    "localColumn": "id",
                                },
                                "biological_sex": {
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

    def test_from_dict(self):
        """Test creating mapping from dictionary."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        self.assertEqual(mapping.name, "Test Mapping")
        self.assertEqual(mapping.version, "1.0.0")
        self.assertEqual(len(mapping.variables), 2)
        self.assertEqual(len(mapping.databases), 1)

    def test_get_variable(self):
        """Test getting a variable by key."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        var = mapping.get_variable("biological_sex")
        self.assertIsNotNone(var)
        self.assertEqual(var.data_type, "categorical")

    def test_get_database(self):
        """Test getting a database by key."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        db = mapping.get_database("test_db")
        self.assertIsNotNone(db)
        self.assertEqual(db.name, "Test Database")

    def test_get_all_tables(self):
        """Test getting all tables."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        tables = mapping.get_all_tables()
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0].key, "test_table")

    def test_get_column_for_variable(self):
        """Test getting column mapping for a variable."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        col = mapping.get_column_for_variable("biological_sex")
        self.assertIsNotNone(col)
        self.assertEqual(col.local_column, "sex")

    def test_get_local_term(self):
        """Test getting local term for a schema term."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        local_term = mapping.get_local_term("biological_sex", "male")
        self.assertEqual(local_term, "M")

    def test_get_target_class(self):
        """Test getting target class for a schema term."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        target = mapping.get_target_class("biological_sex", "male")
        self.assertEqual(target, "ncit:C20197")

    def test_get_local_column(self):
        """Test getting local column name for a variable."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        local_col = mapping.get_local_column("biological_sex")
        self.assertEqual(local_col, "sex")

    def test_from_file(self):
        """Test loading from file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonld", delete=False) as f:
            json.dump(self.mapping_data, f)
            temp_path = f.name

        try:
            mapping = JSONLDMapping.from_file(temp_path)
            self.assertEqual(mapping.name, "Test Mapping")
        finally:
            os.unlink(temp_path)

    def test_save(self):
        """Test saving to file."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonld", delete=False) as f:
            temp_path = f.name

        try:
            mapping.save(temp_path)

            # Verify saved file
            with open(temp_path, "r") as f:
                saved_data = json.load(f)

            self.assertEqual(saved_data["name"], "Test Mapping")
            self.assertIn("schema", saved_data)
            self.assertIn("databases", saved_data)
        finally:
            os.unlink(temp_path)


    def test_to_legacy_format(self):
        """Test converting JSON-LD mapping to legacy format."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        legacy = mapping.to_legacy_format()

        # Check basic structure
        self.assertIn("variable_info", legacy)
        self.assertIn("database_name", legacy)
        self.assertIn("prefixes", legacy)
        self.assertIn("endpoint", legacy)

        # Check variable_info contents
        self.assertIn("biological_sex", legacy["variable_info"])
        var_info = legacy["variable_info"]["biological_sex"]

        self.assertEqual(var_info["predicate"], "sio:SIO_000008")
        self.assertEqual(var_info["class"], "ncit:C28421")
        self.assertEqual(var_info["local_definition"], "sex")
        self.assertEqual(var_info["data_type"], "categorical")

        # Check value mapping conversion
        self.assertIn("value_mapping", var_info)
        self.assertIn("terms", var_info["value_mapping"])
        self.assertIn("male", var_info["value_mapping"]["terms"])
        self.assertEqual(
            var_info["value_mapping"]["terms"]["male"]["target_class"], "ncit:C20197"
        )
        self.assertEqual(var_info["value_mapping"]["terms"]["male"]["local_term"], "M")

    def test_get_prefixes_string(self):
        """Test getting prefixes as SPARQL PREFIX string."""
        mapping = JSONLDMapping.from_dict(self.mapping_data)
        prefixes_str = mapping.get_prefixes_string()

        self.assertIn("PREFIX sio:", prefixes_str)
        self.assertIn("http://semanticscience.org/resource/", prefixes_str)


class TestJSONLDMappingWithRealFiles(unittest.TestCase):
    """Test JSONLDMapping with actual example files."""

    def setUp(self):
        """Set up file paths."""
        self.example_dir = Path(__file__).parent.parent.parent.parent / "example_data"

    def test_load_centre_a_mapping(self):
        """Test loading Centre A English mapping."""
        file_path = self.example_dir / "centre_a_english" / "mapping_centre_a.jsonld"
        if file_path.exists():
            mapping = JSONLDMapping.from_file(file_path)
            self.assertEqual(mapping.name, "Centre A - English Hospital Mapping")
            self.assertEqual(len(mapping.variables), 11)

            # Test accessor methods
            var = mapping.get_variable("biological_sex")
            self.assertIsNotNone(var)
            self.assertEqual(var.data_type, "categorical")

            # Test local term retrieval
            local_term = mapping.get_local_term("biological_sex", "male")
            self.assertEqual(local_term, "male")

            # Test target class retrieval
            target = mapping.get_target_class("biological_sex", "male")
            self.assertEqual(target, "ncit:C20197")

    def test_load_centre_b_mapping(self):
        """Test loading Centre B Dutch mapping."""
        file_path = self.example_dir / "centre_b_dutch" / "mapping_centre_b.jsonld"
        if file_path.exists():
            mapping = JSONLDMapping.from_file(file_path)

            # Test Dutch local mappings
            local_term = mapping.get_local_term("biological_sex", "male")
            self.assertEqual(local_term, "man")

            local_term = mapping.get_local_term("biological_sex", "female")
            self.assertEqual(local_term, "vrouw")

            # Test locale field
            db = mapping.get_database("centre_b_ehr")
            self.assertIsNotNone(db)
            self.assertEqual(db.locale, "nl_NL")

    def test_locale_field(self):
        """Test that locale field is properly loaded and saved."""
        file_path = self.example_dir / "centre_a_english" / "mapping_centre_a.jsonld"
        if file_path.exists():
            mapping = JSONLDMapping.from_file(file_path)

            # Test locale field in Centre A
            db = mapping.get_database("centre_a_ehr")
            self.assertIsNotNone(db)
            self.assertEqual(db.locale, "en_GB")

            # Test that locale is preserved in to_dict
            mapping_dict = mapping.to_dict()
            self.assertEqual(
                mapping_dict["databases"]["centre_a_ehr"]["locale"], "en_GB"
            )


if __name__ == "__main__":
    unittest.main()
