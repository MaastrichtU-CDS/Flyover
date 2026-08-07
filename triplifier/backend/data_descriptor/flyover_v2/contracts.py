"""Versioned JSON-LD and mapping contracts used by the v2 workflow."""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .errors import V2Error

OMOP_DOMAINS = {"Person", "Observation", "Measurement", "ConditionOccurrence"}
VALUE_MODES = {"number", "concept", "string", "presence"}


def _require(condition: bool, field: str, message: str) -> None:
    if not condition:
        raise V2Error("invalid_contract", "The document is not a valid Flyover v2 contract", 422, {field: message})


@lru_cache(maxsize=1)
def _requirement_validator() -> Draft202012Validator:
    with (Path(__file__).parent / "schemas" / "requirement-2.0.schema.json").open(encoding="utf-8") as stream:
        return Draft202012Validator(json.load(stream))


def canonical_requirement(value: dict[str, Any]) -> dict[str, Any]:
    """Validate a requirement document and return a safe, versioned copy."""
    _require(isinstance(value, dict), "$", "Expected a JSON object")
    result = copy.deepcopy(value)
    result.setdefault("formatVersion", "2.0")
    errors = sorted(_requirement_validator().iter_errors(result), key=lambda error: list(error.path))
    if errors:
        error = errors[0]
        path = ".".join(str(item) for item in error.path) or "$"
        raise V2Error("invalid_contract", "The document is not a valid Flyover v2 contract", 422, {path: error.message})
    _require(result["formatVersion"] == "2.0", "formatVersion", "Only format version 2.0 is supported")
    _require(isinstance(result.get("@context"), dict), "@context", "A JSON-LD context is required")
    schema = result.get("schema")
    _require(isinstance(schema, dict), "schema", "A semantic schema is required")
    variables = schema.get("variables")
    _require(isinstance(variables, dict) and bool(variables), "schema.variables", "At least one variable is required")
    for key, variable in variables.items():
        _require(
            isinstance(key, str) and bool(key) and key[0].isalpha()
            and all(character.isalnum() or character in "_-" for character in key),
            f"schema.variables.{key}", "Variable keys must start with a letter and contain letters, numbers, _ or -",
        )
        _require(isinstance(variable, dict), f"schema.variables.{key}", "Expected an object")
        _require(bool(variable.get("class")), f"schema.variables.{key}.class", "A terminology class is required")
        constraints = variable.get("constraints", [])
        _require(isinstance(constraints, list), f"schema.variables.{key}.constraints", "Expected an array")

    targets = result.get("targets", {})
    _require(isinstance(targets, dict), "targets", "Expected an object")
    if "omop" in targets:
        validate_omop_target(targets["omop"], variables)
    return result


def validate_omop_target(target: Any, variables: dict[str, Any]) -> None:
    _require(isinstance(target, dict), "targets.omop", "Expected an object")
    _require(target.get("cdmVersion") == "5.4", "targets.omop.cdmVersion", "OMOP CDM 5.4 is required")
    person = target.get("person")
    _require(isinstance(person, dict), "targets.omop.person", "Person bindings are required")
    source_id = person.get("sourceId")
    _require(source_id in variables, "targets.omop.person.sourceId", "Must reference a schema variable")
    _require(
        person.get("birthDate") in variables or person.get("birthYear") in variables,
        "targets.omop.person",
        "A birthDate or birthYear variable is required",
    )
    bindings = target.get("variables", {})
    _require(isinstance(bindings, dict), "targets.omop.variables", "Expected an object")
    for key, binding in bindings.items():
        path = f"targets.omop.variables.{key}"
        _require(key in variables, path, "Binding references an unknown variable")
        _require(isinstance(binding, dict), path, "Expected an object")
        _require(binding.get("domain") in OMOP_DOMAINS - {"Person"}, f"{path}.domain", "Unsupported OMOP domain")
        _require(binding.get("valueMode") in VALUE_MODES, f"{path}.valueMode", "Unsupported value mode")
        date_variable = binding.get("dateVariable")
        _require(date_variable in variables, f"{path}.dateVariable", "An event date variable is required")
        _require(
            bool(binding.get("typeConceptId") or binding.get("typeUri")),
            f"{path}.typeConceptId",
            "A typeConceptId or typeUri is required by OMOP",
        )


def validate_local_mapping(requirement: dict[str, Any], mapping: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(mapping)
    result["formatVersion"] = "2.0"
    databases = result.get("databases")
    _require(isinstance(databases, dict) and databases, "databases", "A local database mapping is required")
    variable_keys = set(requirement["schema"]["variables"])
    seen: set[tuple[str, str | None]] = set()
    for database_key, database in databases.items():
        tables = database.get("tables", {}) if isinstance(database, dict) else {}
        _require(bool(tables), f"databases.{database_key}.tables", "At least one table is required")
        for table_key, table in tables.items():
            path = f"databases.{database_key}.tables.{table_key}"
            layout = table.get("layout", "wide")
            _require(layout in {"wide", "long"}, f"{path}.layout", "Use wide or long")
            roles = table.get("roles", {})
            _require(bool(roles.get("subject")), f"{path}.roles.subject", "A subject column is required")
            if layout == "long":
                for role in ("eventType", "eventValue", "eventDate"):
                    _require(bool(roles.get(role)), f"{path}.roles.{role}", f"The {role} column is required")
            columns = table.get("columns", {})
            _require(isinstance(columns, dict), f"{path}.columns", "Expected an object")
            for mapping_key, column in columns.items():
                column_path = f"{path}.columns.{mapping_key}"
                _require(isinstance(column, dict), column_path, "Expected an object")
                maps_to = str(column.get("mapsTo", ""))
                variable_key = maps_to.rsplit("/", 1)[-1]
                _require(variable_key in variable_keys, f"{column_path}.mapsTo", "Unknown schema variable")
                _require(bool(column.get("localColumn")), f"{column_path}.localColumn", "A source column is required")
                when = column.get("when")
                if when is not None:
                    _require(layout == "long", f"{column_path}.when", "Filters are only valid for long tables")
                    _require(
                        isinstance(when, dict) and bool(when.get("column")) and "equals" in when,
                        f"{column_path}.when",
                        "Use {column, equals}",
                    )
                ignored = column.get("ignoredValues", [])
                _require(
                    isinstance(ignored, list) and all(isinstance(value, str) for value in ignored),
                    f"{column_path}.ignoredValues", "Expected an array of local values",
                )
                signature = (variable_key, None if when is None else str(when.get("equals")))
                _require(signature not in seen, column_path, "Duplicate variable/filter mapping")
                seen.add(signature)
    return result


def validate_omop_mapping(
    requirement: dict[str, Any], mapping: dict[str, Any], profile: dict[str, Any]
) -> None:
    """Apply conversion-readiness checks that depend on source statistics."""
    _, table = first_table(mapping)
    by_variable: dict[str, list[dict[str, Any]]] = {}
    for column in table.get("columns", {}).values():
        by_variable.setdefault(variable_key(column["mapsTo"]), []).append(column)
    omop = requirement.get("targets", {}).get("omop")
    _require(isinstance(omop, dict), "targets.omop", "An OMOP target extension is required")
    person = omop["person"]
    roles = table.get("roles", {})
    if person["sourceId"] not in by_variable:
        _require(bool(roles.get("subject")), "targets.omop.person.sourceId", "Map a source identifier or subject role")
    for field in ("birthDate", "birthYear"):
        if person.get(field):
            _require(person[field] in by_variable, f"targets.omop.person.{field}", "This person field is not mapped")
    for variable, binding in omop.get("variables", {}).items():
        path = f"targets.omop.variables.{variable}"
        _require(variable in by_variable, path, "This event variable is not mapped")
        _require(
            binding["dateVariable"] in by_variable or (table.get("layout") == "long" and bool(roles.get("eventDate"))),
            f"{path}.dateVariable", "No mapped event date source is available",
        )
        for column in by_variable[variable]:
            source_profile = profile.get("columns", {}).get(column["localColumn"], {})
            if column.get("when"):
                source_profile = profile.get("conditionalValueProfiles", {}).get(str(column["when"]["equals"]), source_profile)
            if binding["valueMode"] == "number":
                _require(
                    source_profile.get("nonNumericCount", 0) == 0,
                    f"{path}.valueMode", "The mapped source contains non-numeric values",
                )
            if binding["valueMode"] == "concept" and source_profile.get("valueFrequencies") is not None:
                represented = {
                    str(item) for local in column.get("localMappings", {}).values()
                    for item in (local if isinstance(local, list) else [local]) if item is not None
                }
                ignored = {str(item) for item in column.get("ignoredValues", [])}
                present = {str(item["value"]) for item in source_profile["valueFrequencies"]}
                missing = sorted(present - represented - ignored)
                _require(not missing, f"{path}.categories", f"Unmapped local values: {', '.join(missing[:10])}")
            if binding["valueMode"] == "concept" and source_profile.get("frequenciesSuppressed"):
                _require(
                    False, f"{path}.categories",
                    "The source is too high-cardinality for safe categorical mapping",
                )


def first_table(mapping: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    for database in mapping.get("databases", {}).values():
        for key, table in database.get("tables", {}).items():
            return key, table
    raise V2Error("mapping_missing", "No mapped table is available", 422)


def variable_key(maps_to: str) -> str:
    return maps_to.rsplit("/", 1)[-1]


def expand_term(requirement: dict[str, Any], term: str) -> str:
    if term.startswith(("http://", "https://")):
        return term
    if ":" not in term:
        return term
    prefix, suffix = term.split(":", 1)
    prefixes = {
        **{k: v for k, v in requirement.get("@context", {}).items() if isinstance(v, str)},
        **requirement.get("schema", {}).get("prefixes", {}),
    }
    return f"{prefixes[prefix]}{suffix}" if prefix in prefixes else term
