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
        assert resolver.resolve("http://loinc.org/rdf/21905-5", "Observation")["conceptId"] == 910001
        assert resolver.resolve("http://snomed.info/id/1228889001", "Observation")["conceptId"] == 910011


def _resolved_example_requirement(fixture_directory: Path) -> dict:
    requirement = canonical_requirement(json.loads(
        (fixture_directory / "omop-wide-requirement.jsonld").read_text(encoding="utf-8")
    ))
    person = requirement["targets"]["omop"]["person"]
    person["genderConceptIds"] = {"male": 8507, "female": 8532}
    bindings = requirement["targets"]["omop"]["variables"]
    bindings["weight"].update({"conceptId": 3025315, "unitConceptId": 9529})
    bindings["clinical_t"].update({
        "conceptId": 910001,
        "termConceptIds": {"cT1": 910011, "cT2": 910012, "cT3": 910013},
    })
    bindings["clinical_n"].update({
        "conceptId": 910002,
        "termConceptIds": {"cN0": 910021, "cN1": 910022},
    })
    bindings["clinical_m"].update({
        "conceptId": 910003,
        "termConceptIds": {"cM0": 910031, "cM1": 910032},
    })
    return requirement


def _mapped(variable: str, local_column: str, **extra) -> dict:
    return {
        "mapsTo": f"schema:variable/{variable}",
        "localColumn": local_column,
        **extra,
    }


def test_committed_wide_example_writes_gender_weight_and_clinical_tnm(tmp_path, monkeypatch):
    """Keep the documented wide example executable against the minimal OMOP target."""
    monkeypatch.setenv("FLYOVER_ALLOW_INSECURE_POSTGRES", "true")
    repository = Path(__file__).parents[5]
    fixture_directory = repository / "docs" / "v2" / "fixtures"
    requirement = _resolved_example_requirement(fixture_directory)

    mapping = validate_local_mapping(requirement, {"databases": {"source": {"tables": {"source": {
        "layout": "wide",
        "roles": {"subject": "person_id"},
        "columns": {
            "identifier": _mapped("identifier", "person_id"),
            "birth_date": _mapped("birth_date", "birth_date"),
            "gender": _mapped(
                "biological_sex", "gender", localMappings={"female": "F", "male": "M"}
            ),
            "measurement_date": _mapped("measurement_date", "measurement_date"),
            "weight": _mapped("weight", "weight_kg"),
            "tnm_date": _mapped("tnm_date", "tnm_date"),
            "clinical_t": _mapped(
                "clinical_t", "clinical_t",
                localMappings={"cT1": "T1", "cT2": "T2", "cT3": "T3"},
            ),
            "clinical_n": _mapped(
                "clinical_n", "clinical_n", localMappings={"cN0": "N0", "cN1": "N1"},
            ),
            "clinical_m": _mapped(
                "clinical_m", "clinical_m", localMappings={"cM0": "M0", "cM1": "M1"},
            ),
        },
    }}}}})
    source = fixture_directory / "omop-wide-source.csv"
    profile_path = tmp_path / "example-profile.json"
    profile_path.write_text(
        json.dumps(profile_csv(source, "wide", {"subject": "person_id"})), encoding="utf-8"
    )
    project_context = ProjectContext(
        "documented-example", tmp_path,
        {"path": str(source), "profile_path": str(profile_path)},
        requirement, mapping,
    )

    converter = OmopConverter()
    assert converter.validate(project_context, target())["rowCounts"] == {
        "person": 5, "observation": 15, "measurement": 5, "condition_occurrence": 0,
    }
    assert converter.convert(project_context, target())["inserted"] == {
        "person": 5, "observation": 15, "measurement": 5, "condition_occurrence": 0,
    }
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT gender_source_value, gender_concept_id FROM flyover_test.person ORDER BY person_id"
        )
        assert cursor.fetchall() == [
            ("female", 8532), ("male", 8507), ("female", 8532),
            ("male", 8507), ("female", 8532),
        ]
        cursor.execute(
            "SELECT observation_source_value, value_source_value, observation_concept_id, "
            "value_as_concept_id FROM flyover_test.observation ORDER BY observation_id"
        )
        assert cursor.fetchall() == [
            ("clinical_t", "cT1", 910001, 910011),
            ("clinical_n", "cN0", 910002, 910021),
            ("clinical_m", "cM0", 910003, 910031),
            ("clinical_t", "cT2", 910001, 910012),
            ("clinical_n", "cN1", 910002, 910022),
            ("clinical_m", "cM0", 910003, 910031),
            ("clinical_t", "cT3", 910001, 910013),
            ("clinical_n", "cN1", 910002, 910022),
            ("clinical_m", "cM1", 910003, 910032),
            ("clinical_t", "cT1", 910001, 910011),
            ("clinical_n", "cN0", 910002, 910021),
            ("clinical_m", "cM0", 910003, 910031),
            ("clinical_t", "cT2", 910001, 910012),
            ("clinical_n", "cN0", 910002, 910021),
            ("clinical_m", "cM0", 910003, 910031),
        ]


def test_committed_long_example_writes_conditional_clinical_tnm(tmp_path, monkeypatch):
    """Prove discriminator filters normalize to the same OMOP TNM representation."""
    monkeypatch.setenv("FLYOVER_ALLOW_INSECURE_POSTGRES", "true")
    repository = Path(__file__).parents[5]
    fixture_directory = repository / "docs" / "v2" / "fixtures"
    requirement = _resolved_example_requirement(fixture_directory)

    def event_filter(value: str) -> dict:
        return {"column": "event_type", "equals": value}

    mapping = validate_local_mapping(requirement, {"databases": {"source": {"tables": {"source": {
        "layout": "long",
        "roles": {
            "subject": "person_id", "eventType": "event_type",
            "eventValue": "event_value", "eventDate": "event_date",
        },
        "columns": {
            "identifier": _mapped("identifier", "person_id"),
            "birth_date": _mapped("birth_date", "birth_date"),
            "gender": _mapped(
                "biological_sex", "gender", localMappings={"female": "F", "male": "M"}
            ),
            "weight": _mapped("weight", "event_value", when=event_filter("body_weight")),
            "clinical_t": _mapped(
                "clinical_t", "event_value", when=event_filter("clinical_t"),
                localMappings={"cT1": "T1", "cT2": "T2", "cT3": "T3"},
            ),
            "clinical_n": _mapped(
                "clinical_n", "event_value", when=event_filter("clinical_n"),
                localMappings={"cN0": "N0", "cN1": "N1"},
            ),
            "clinical_m": _mapped(
                "clinical_m", "event_value", when=event_filter("clinical_m"),
                localMappings={"cM0": "M0", "cM1": "M1"},
            ),
        },
    }}}}})
    source = fixture_directory / "omop-long-source.csv"
    profile_path = tmp_path / "long-example-profile.json"
    roles = {
        "subject": "person_id", "eventType": "event_type",
        "eventValue": "event_value", "eventDate": "event_date",
    }
    profile_path.write_text(
        json.dumps(profile_csv(source, "long", roles)), encoding="utf-8"
    )
    project_context = ProjectContext(
        "documented-long-example", tmp_path,
        {"path": str(source), "profile_path": str(profile_path)},
        requirement, mapping,
    )

    converter = OmopConverter()
    assert converter.validate(project_context, target())["rowCounts"] == {
        "person": 5, "observation": 15, "measurement": 6, "condition_occurrence": 0,
    }
    assert converter.convert(project_context, target())["inserted"] == {
        "person": 5, "observation": 15, "measurement": 6, "condition_occurrence": 0,
    }
