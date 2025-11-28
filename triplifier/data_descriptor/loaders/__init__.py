"""
Flyover JSON-LD Mapping Loader Module

This module provides dataclasses and loaders for working with JSON-LD semantic
mapping files. It provides a clean interface for accessing schema variables,
database configurations, and local mappings.

Classes:
    SchemaReconstructionNode: A node in the schema reconstruction chain.
    SchemaVariable: A variable definition in the semantic schema.
    ColumnMapping: A column mapping linking local data to schema variables.
    Table: A table containing column mappings.
    Database: A database containing tables.
    JSONLDMapping: Complete JSON-LD mapping with schema and databases.

Usage:
    from data_descriptor.loaders import JSONLDMapping

    mapping = JSONLDMapping.from_file('mapping.jsonld')
    var = mapping.get_variable('biological_sex')
    local_term = mapping.get_local_term('biological_sex', 'male')
"""

from .jsonld_loader import (
    SchemaReconstructionNode,
    SchemaVariable,
    ColumnMapping,
    Table,
    Database,
    JSONLDMapping,
)

__all__ = [
    "SchemaReconstructionNode",
    "SchemaVariable",
    "ColumnMapping",
    "Table",
    "Database",
    "JSONLDMapping",
]
