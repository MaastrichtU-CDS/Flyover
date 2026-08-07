"""Minimal administrator-installed converter package example."""

from data_descriptor.flyover_v2.converters.base import Converter, ConverterManifest, ProjectContext


class ExampleConverter(Converter):
    def manifest(self) -> ConverterManifest:
        return ConverterManifest(
            id="example", version="1.0.0", label="Example",
            input_kinds=("csv",), output_kind="application/octet-stream",
        )

    def validate(self, context: ProjectContext, target: dict) -> dict:
        return {"valid": True}

    def convert(self, context: ProjectContext, target: dict) -> dict:
        return {"success": True, "artifacts": []}


# pyproject.toml in the external package:
# [project.entry-points."flyover.converters"]
# example = "example_package:ExampleConverter"
#
# The administrator additionally sets:
# FLYOVER_CONVERTER_ALLOWLIST=example
