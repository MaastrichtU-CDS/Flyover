"""Real PostgreSQL transaction and terminology smoke tests for OMOP 5.4."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
import pytest

from data_descriptor.flyover_v2.contracts import canonical_requirement, validate_local_mapping
from data_descriptor.flyover_v2.converters.base import ProjectContext
from data_descriptor.flyover_v2.converters.omop import OmopConverter
from data_descriptor.flyover_v2.errors import V2Error
from data_descriptor.flyover_v2.profiling import profile_csv
from data_descriptor.flyover_v2.terminology import NamespaceRegistry, OmopTerminologyResolver

pytestmark = pytest.mark.skipif(not os.getenv("FLYOVER_TEST_OMOP_HOST"), reason="OMOP PostgreSQL fixture is not configured")


def target() -> dict:
    return {
        "host": os.environ["FLYOVER_TEST_OMOP_HOST"],
        "port": int(os.getenv("FLYOVER_TEST_OMOP_PORT", "5432")),
        "database": os.getenv("FLYOVER_TEST_OMOP_DATABASE", "flyover"),
        "user": os.getenv("FLYOVER_TEST_OMOP_USER", "flyover"),
        "password": os.getenv("FLYOVER_TEST_OMOP_PASSWORD", "flyover"),
        "schema": "flyover_test", "sslmode": "disable",
    }


def connect():
    value = target().copy(); value.pop("schema")
    return psycopg2.connect(**value)


@pytest.fixture(autouse=True)
def omop_schema():
    fixture = Path(__file__).parents[1] / "fixtures" / "omop54_minimal.sql"
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(fixture.read_text(encoding="utf-8"))
    yield


def context(tmp_path: Path) -> ProjectContext:
    requirement = canonical_requirement({
        "@context": {
            "loinc": "http://loinc.org/rdf/",
            "gender": "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender/",
        }, "formatVersion": "2.0",
        "schema": {"variables": {
            "identifier": {"class": "loinc:id"}, "birth_date": {"class": "loinc:birth"},
            "biological_sex": {
                "class": "gender:AdministrativeGender",
                "valueMapping": {"terms": {
                    "male": {"targetClass": "gender:M"},
                    "female": {"targetClass": "gender:F"},
                }},
            },
            "date": {"class": "loinc:date"}, "weight": {"class": "loinc:29463-7"},
        }},
        "targets": {"omop": {"cdmVersion": "5.4", "person": {
            "sourceId": "identifier", "birthDate": "birth_date",
            "sexAtBirth": "biological_sex", "genderConceptIds": {"female": 8532},
        }, "variables": {"weight": {
            "domain": "Measurement", "valueMode": "number", "dateVariable": "date",
            "conceptId": 3025315, "typeConceptId": 32817, "unitConceptId": 9529,
        }}}},
    })
    mapping = validate_local_mapping(requirement, {"databases": {"source": {"tables": {"source": {
        "layout": "wide", "roles": {"subject": "id"}, "columns": {
            "id": {"mapsTo": "schema:variable/identifier", "localColumn": "id"},
            "born": {"mapsTo": "schema:variable/birth_date", "localColumn": "born"},
            "gender": {
                "mapsTo": "schema:variable/biological_sex", "localColumn": "gender",
                "localMappings": {"female": "F", "male": "M"},
            },
            "date": {"mapsTo": "schema:variable/date", "localColumn": "measured"},
            "weight": {"mapsTo": "schema:variable/weight", "localColumn": "weight"},
        },
    }}}}})
    source = tmp_path / "source.csv"
    source.write_text(
        "id,born,gender,measured,weight\np1,1980-02-03,F,2024-01-02,72.5\n",
        encoding="utf-8",
    )
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps(profile_csv(source, "wide", {"subject": "id"})), encoding="utf-8")
    return ProjectContext("test", tmp_path, {"path": str(source), "profile_path": str(profile)}, requirement, mapping)


def test_atomic_csv_to_omop_and_failure_rollback(tmp_path, monkeypatch):
    monkeypatch.setenv("FLYOVER_ALLOW_INSECURE_POSTGRES", "true")
    converter = OmopConverter()
    assert converter.validate(context(tmp_path), target())["ready"] is True
    report = converter.convert(context(tmp_path), target())
    assert report["inserted"] == {"person": 1, "observation": 0, "measurement": 1, "condition_occurrence": 0}
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT person_source_value, gender_concept_id, gender_source_value FROM flyover_test.person")
        assert cursor.fetchone() == ("p1", 8532, "female")
        cursor.execute("TRUNCATE flyover_test.measurement, flyover_test.person")
        cursor.execute("CREATE FUNCTION flyover_test.reject_measurement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$")
        cursor.execute("CREATE TRIGGER reject BEFORE INSERT ON flyover_test.measurement FOR EACH ROW EXECUTE FUNCTION flyover_test.reject_measurement()")
    with pytest.raises(V2Error, match="rolled back"):
        converter.convert(context(tmp_path), target())
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT (SELECT count(*) FROM flyover_test.person), (SELECT count(*) FROM flyover_test.measurement)")
        assert cursor.fetchone() == (0, 0)


def test_exact_nonstandard_ambiguous_invalid_and_wrong_domain_resolution():
    registry = NamespaceRegistry({"https://example.org/": "Example"})
    with connect() as connection:
        resolver = OmopTerminologyResolver(connection, "flyover_test", registry)
        assert resolver.resolve("https://example.org/source-weight", "Measurement")["conceptId"] == 3025315
        assert resolver.resolve("https://example.org/ambiguous", "Measurement")["status"] == "ambiguous"
        assert resolver.resolve("https://example.org/wrong-domain", "Measurement")["status"] == "missing"
        assert resolver.resolve("https://example.org/invalid", "Measurement")["status"] == "missing"
        assert resolver.resolve("https://example.org/absent", "Measurement")["status"] == "missing"
        assert resolver.resolve(
            "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender/F", "Person"
        )["conceptId"] == 8532
