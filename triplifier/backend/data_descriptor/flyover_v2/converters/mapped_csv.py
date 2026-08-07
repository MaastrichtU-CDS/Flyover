"""Mapped CSV export: only selected columns and canonical terms."""

from __future__ import annotations

import csv
import uuid
from pathlib import Path
from typing import Any

from ..contracts import first_table, variable_key
from ..files import sha256_file
from ..profiling import read_source
from .base import Converter, ConverterManifest, ProjectContext


class MappedCsvConverter(Converter):
    def manifest(self) -> ConverterManifest:
        return ConverterManifest(
            id="mapped-csv",
            version="1.0.0",
            label="Mapped CSV",
            input_kinds=("csv",),
            output_kind="text/csv",
            capabilities=("wide", "long", "mapped-columns-only"),
        )

    def validate(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        _, table = first_table(context.mapping)
        return {"valid": bool(table.get("columns")), "issues": [] if table.get("columns") else ["No columns are mapped"]}

    def convert(self, context: ProjectContext, target: dict[str, Any]) -> dict[str, Any]:
        frame = read_source(context.source["path"])
        _, table = first_table(context.mapping)
        layout = table.get("layout", "wide")
        output_path = context.project_dir / "artifacts" / f"mapped-{uuid.uuid4().hex}.csv"
        rows: list[dict[str, Any]] = []
        columns = list(table.get("columns", {}).values())
        for source_row in frame.iter_rows(named=True):
            output: dict[str, Any] = {}
            for mapping in columns:
                when = mapping.get("when")
                if when and str(source_row.get(when["column"])) != str(when.get("equals")):
                    continue
                key = variable_key(mapping["mapsTo"])
                value = source_row.get(mapping["localColumn"])
                for canonical, local in mapping.get("localMappings", {}).items():
                    candidates = local if isinstance(local, list) else [local]
                    if value in candidates:
                        value = canonical
                        break
                output[key] = value
            if output or layout == "wide":
                rows.append(output)
        fieldnames = sorted({key for row in rows for key in row})
        with output_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return {
            "success": True,
            "rowCount": len(rows),
            "artifacts": [{"kind": "mapped-csv", "path": str(output_path), "sha256": sha256_file(output_path)}],
        }
