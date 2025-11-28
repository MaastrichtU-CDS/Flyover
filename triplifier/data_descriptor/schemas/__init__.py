"""
Flyover JSON-LD Mapping Schemas

This module contains JSON Schema definitions for validating
Flyover semantic mapping files.
"""

from pathlib import Path

SCHEMA_DIR = Path(__file__).parent
MAPPING_SCHEMA_PATH = SCHEMA_DIR / "mapping_schema.json"

__all__ = ["SCHEMA_DIR", "MAPPING_SCHEMA_PATH"]
