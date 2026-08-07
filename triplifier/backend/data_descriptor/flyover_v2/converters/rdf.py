"""Mapped-only RDF artifacts using the retained Python Triplifier adapter."""

from __future__ import annotations

from pathlib import Path
import uuid
from typing import Any

from ..errors import V2Error
from ..files import sha256_file
from ..profiling import read_source
from .base import Converter, ConverterManifest, ProjectContext
from .mapped_csv import MappedCsvConverter


def _validate_link_cycles(mapping: dict[str, Any]) -> None:
    edges: dict[str, set[str]] = {}
    for link in mapping.get("crossGraphLinks", []):
        source, target = link.get("fromTable"), link.get("toTable")
        if source and target:
            edges.setdefault(str(source), set()).add(str(target))
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise V2Error("cross_graph_cycle", "Cross-graph links contain a cycle", 422)
        if node in visited:
            return
        visiting.add(node)
        for target in edges.get(node, set()):
            visit(target)
        visiting.remove(node)
        visited.add(node)

    for node in edges:
        visit(node)


class RdfConverter(Converter):
    def manifest(self) -> ConverterManifest:
        return ConverterManifest(
            id="rdf", version="2.0.0", label="RDF (mapped attributes)",
            input_kinds=("csv",), output_kind="text/turtle",
            capabilities=("wide", "long", "mapped-columns-only", "cycle-validation"),
            configuration_schema={
                "type": "object",
                "properties": {"baseUri": {"type": "string", "format": "uri"}},
            },
        )

    def validate(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        _validate_link_cycles(context.mapping)
        mapped = MappedCsvConverter().validate(context, target)
        return {"valid": mapped["valid"], "issues": mapped["issues"], "mappedAttributesOnly": True}

    def convert(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        self.validate(context, target)
        mapped_report = MappedCsvConverter().convert(context, target)
        mapped_path = Path(mapped_report["artifacts"][0]["path"])
        frame = read_source(mapped_path)
        try:
            from data_descriptor.services.conversion_service import PythonTriplifierIntegration

            success, message, outputs = PythonTriplifierIntegration(
                str(context.project_dir), "."
            ).run_triplifier_csv([frame], ["mapped"], base_uri=target.get("baseUri"))
        except Exception as exc:
            raise V2Error("rdf_conversion_failed", "The retained Triplifier adapter failed", 422) from exc
        if not success or not outputs:
            raise V2Error("rdf_conversion_failed", str(message), 422)
        artifacts: list[dict[str, Any]] = []
        output = outputs[0]
        export_id = uuid.uuid4().hex
        for kind, source_key, filename in (
            ("rdf-data", "data_file", f"data-{export_id}.ttl"),
            ("rdf-ontology", "ontology_file", f"ontology-{export_id}.owl"),
        ):
            source = Path(output[source_key])
            destination = context.project_dir / "artifacts" / filename
            source.replace(destination)
            artifacts.append({"kind": kind, "path": str(destination), "sha256": sha256_file(destination)})
        mapped_path.unlink(missing_ok=True)
        return {"success": True, "message": message, "artifacts": artifacts}
