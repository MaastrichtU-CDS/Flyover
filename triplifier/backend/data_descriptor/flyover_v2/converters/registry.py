"""Allowlisted converter discovery using standard Python entry points."""

from __future__ import annotations

import importlib.metadata
import os

from ..errors import NotFoundError
from .base import Converter
from .mapped_csv import MappedCsvConverter
from .omop import OmopConverter
from .rdf import RdfConverter


class ConverterRegistry:
    def __init__(self) -> None:
        builtins: list[Converter] = [OmopConverter(), MappedCsvConverter(), RdfConverter()]
        self._converters = {converter.manifest().id: converter for converter in builtins}
        allowlist = {
            item.strip()
            for item in os.getenv("FLYOVER_CONVERTER_ALLOWLIST", "").split(",")
            if item.strip()
        }
        if allowlist:
            for entry in importlib.metadata.entry_points(group="flyover.converters"):
                if entry.name not in allowlist:
                    continue
                converter = entry.load()()
                self._converters[converter.manifest().id] = converter

    def list(self) -> list[dict]:
        return [converter.manifest().to_dict() for converter in self._converters.values()]

    def get(self, converter_id: str) -> Converter:
        if converter_id not in self._converters:
            raise NotFoundError("Converter")
        return self._converters[converter_id]
