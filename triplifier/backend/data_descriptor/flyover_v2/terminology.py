"""Deterministic terminology URI to OMOP standard concept resolution."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from .errors import V2Error

DEFAULT_NAMESPACES = {
    "http://snomed.info/id/": "SNOMED",
    "http://loinc.org/rdf/": "LOINC",
    "http://purl.bioontology.org/ontology/LNC/": "LOINC",
    "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#": "NCIt",
    "http://unitsofmeasure.org/": "UCUM",
    "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender/": "Gender",
}

DOMAIN_NAMES = {
    "ConditionOccurrence": "Condition",
    "Measurement": "Measurement",
    "Observation": "Observation",
    "Person": "Gender",
}


@dataclass(frozen=True)
class ParsedTerm:
    uri: str
    vocabulary_id: str
    concept_code: str


class NamespaceRegistry:
    def __init__(self, additional: dict[str, str] | None = None):
        self.namespaces = {**DEFAULT_NAMESPACES, **(additional or {})}

    def parse(self, uri: str) -> ParsedTerm:
        for namespace in sorted(self.namespaces, key=len, reverse=True):
            if uri.startswith(namespace):
                code = uri[len(namespace) :].strip("/#")
                if code:
                    return ParsedTerm(uri, self.namespaces[namespace], code)
        raise V2Error(
            "unknown_terminology_namespace",
            "No OMOP vocabulary mapping is configured for a terminology URI",
            422,
            details={"uri": uri},
        )


def connection_parameters(target: dict[str, Any]) -> dict[str, Any]:
    allowed = {"host", "port", "dbname", "database", "user", "password", "sslmode"}
    parameters = {key: value for key, value in target.items() if key in allowed and value not in (None, "")}
    if "database" in parameters:
        parameters["dbname"] = parameters.pop("database")
    parameters.setdefault("port", 5432)
    parameters.setdefault("sslmode", "require")
    if parameters["sslmode"] in {"disable", "allow", "prefer"} and os.getenv("FLYOVER_ALLOW_INSECURE_POSTGRES") != "true":
        raise V2Error("insecure_postgres", "PostgreSQL TLS is required by this installation", 422)
    return parameters


def safe_schema(target: dict[str, Any]) -> str:
    schema = str(target.get("schema", "public"))
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise V2Error("invalid_schema", "Invalid PostgreSQL schema name", 422)
    return schema


class OmopTerminologyResolver:
    def __init__(self, connection: Any, schema: str, registry: NamespaceRegistry | None = None):
        self.connection = connection
        self.schema = schema
        self.registry = registry or NamespaceRegistry()

    def resolve(self, uri: str, domain: str | None = None) -> dict[str, Any]:
        parsed = self.registry.parse(uri)
        expected_domain = DOMAIN_NAMES.get(domain or "", domain)
        query = f"""
            SELECT concept_id, concept_name, domain_id, vocabulary_id, concept_code,
                   standard_concept, invalid_reason
            FROM {self.schema}.concept
            WHERE vocabulary_id = %s AND concept_code = %s AND invalid_reason IS NULL
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (parsed.vocabulary_id, parsed.concept_code))
            source_rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
        sources = [dict(zip(columns, row)) for row in source_rows]
        candidates: list[dict[str, Any]] = []
        for source in sources:
            if source["standard_concept"] == "S":
                candidates.append(source)
                continue
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT target.concept_id, target.concept_name, target.domain_id,
                           target.vocabulary_id, target.concept_code,
                           target.standard_concept, target.invalid_reason
                    FROM {self.schema}.concept_relationship relationship
                    JOIN {self.schema}.concept target
                      ON target.concept_id = relationship.concept_id_2
                    WHERE relationship.concept_id_1 = %s
                      AND relationship.relationship_id = 'Maps to'
                      AND relationship.invalid_reason IS NULL
                      AND target.invalid_reason IS NULL
                      AND target.standard_concept = 'S'
                    """,
                    (source["concept_id"],),
                )
                mapped_columns = [description[0] for description in cursor.description]
                candidates.extend(dict(zip(mapped_columns, row)) for row in cursor.fetchall())
        if expected_domain:
            candidates = [candidate for candidate in candidates if candidate["domain_id"] == expected_domain]
        unique = {candidate["concept_id"]: candidate for candidate in candidates}
        status = "resolved" if len(unique) == 1 else "missing" if not unique else "ambiguous"
        return {
            "uri": uri,
            "status": status,
            "sourceConcepts": sources,
            "candidates": list(unique.values()),
            "conceptId": next(iter(unique)) if len(unique) == 1 else None,
        }
