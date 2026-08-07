"""Small, explicit converter interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ConverterManifest:
    id: str
    version: str
    label: str
    input_kinds: tuple[str, ...]
    output_kind: str
    capabilities: tuple[str, ...] = ()
    configuration_schema: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        return {
            "id": value["id"],
            "version": value["version"],
            "label": value["label"],
            "inputKinds": list(value["input_kinds"]),
            "outputKind": value["output_kind"],
            "capabilities": list(value["capabilities"]),
            "configurationSchema": value["configuration_schema"],
        }


@dataclass
class ProjectContext:
    project_id: str
    project_dir: Path
    source: dict[str, Any]
    requirement: dict[str, Any]
    mapping: dict[str, Any]


class Converter(ABC):
    @abstractmethod
    def manifest(self) -> ConverterManifest:
        raise NotImplementedError

    @abstractmethod
    def validate(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def convert(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError
