"""Typed public documents; runtime invariants live in :mod:`contracts`."""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict


VariableDefinition = TypedDict(
    "VariableDefinition",
    {
        "class": str,
        "constraints": NotRequired[list[dict[str, Any]]],
        "valueMapping": NotRequired[dict[str, Any]],
    },
)


class EventBinding(TypedDict):
    domain: Literal["Observation", "Measurement", "ConditionOccurrence"]
    valueMode: Literal["number", "concept", "string", "presence"]
    dateVariable: str
    typeConceptId: NotRequired[int]
    typeUri: NotRequired[str]
    unitUri: NotRequired[str]
    conceptId: NotRequired[int]


class ColumnMapping(TypedDict):
    mapsTo: str
    localColumn: str
    localMappings: NotRequired[dict[str, str | list[str] | None]]
    ignoredValues: NotRequired[list[str]]
    when: NotRequired[dict[str, str]]


class ApiErrorDocument(TypedDict):
    code: str
    message: str
    fieldErrors: dict[str, str]
    details: dict[str, Any]
