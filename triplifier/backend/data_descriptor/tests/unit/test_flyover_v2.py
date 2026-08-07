"""Focused unit tests for v2's persistent and conversion-critical boundaries."""

from __future__ import annotations

import json
import io

import pytest
from flask import Flask

from data_descriptor.flyover_v2.api import create_api_blueprint
from data_descriptor.flyover_v2.contracts import canonical_requirement, validate_local_mapping
from data_descriptor.flyover_v2.converters.base import ProjectContext
from data_descriptor.flyover_v2.converters.omop import OmopBatchBuilder
from data_descriptor.flyover_v2.converters.rdf import _validate_link_cycles
from data_descriptor.flyover_v2.constraints import validate_value
from data_descriptor.flyover_v2.database import ProjectStore
from data_descriptor.flyover_v2.errors import ConflictError, V2Error
from data_descriptor.flyover_v2.profiling import profile_csv
from data_descriptor.flyover_v2.files import ProjectFiles
from data_descriptor.flyover_v2.publication import export_healthdcat
from data_descriptor.flyover_v2.terminology import NamespaceRegistry


def requirement() -> dict:
    return canonical_requirement({
        "@context": {"loinc": "http://loinc.org/rdf/"},
        "formatVersion": "2.0",
        "schema": {"variables": {
            "identifier": {"class": "loinc:100"},
            "birth_date": {"class": "loinc:101"},
            "measurement_date": {"class": "loinc:102"},
            "weight": {"class": "loinc:29463-7"},
        }},
        "targets": {"omop": {
            "cdmVersion": "5.4",
            "person": {"sourceId": "identifier", "birthDate": "birth_date"},
            "variables": {"weight": {
                "domain": "Measurement", "valueMode": "number",
                "dateVariable": "measurement_date", "conceptId": 3025315,
                "typeConceptId": 32817, "unitConceptId": 9529,
            }},
        }},
    })


def wide_mapping() -> dict:
    columns = {
        key: {"mapsTo": f"schema:variable/{key}", "localColumn": column}
        for key, column in {
            "identifier": "id", "birth_date": "born",
            "measurement_date": "measured", "weight": "weight_kg",
        }.items()
    }
    return {"databases": {"source": {"tables": {"source": {
        "layout": "wide", "roles": {"subject": "id"}, "columns": columns,
    }}}}}


def context(tmp_path, csv_text: str, mapping: dict, layout: str, roles: dict) -> ProjectContext:
    source = tmp_path / "source.csv"
    source.write_text(csv_text, encoding="utf-8")
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(profile_csv(source, layout, roles)), encoding="utf-8")
    return ProjectContext(
        project_id="test", project_dir=tmp_path,
        source={"path": str(source), "profile_path": str(profile_path)},
        requirement=requirement(), mapping=validate_local_mapping(requirement(), mapping),
    )


def test_projects_survive_store_restart_and_reject_stale_edit(tmp_path):
    first = ProjectStore(tmp_path)
    project = first.create_project("Cohort A")
    first.save_snapshot(project["id"], "mapping", {"revision": 1}, expected_revision=0)

    restarted = ProjectStore(tmp_path)
    assert restarted.get_project(project["id"])["mappingRevision"] == 1
    with pytest.raises(ConflictError):
        restarted.save_snapshot(project["id"], "mapping", {"revision": 2}, expected_revision=0)


def test_profile_suppresses_high_cardinality_but_keeps_discriminator(tmp_path):
    source = tmp_path / "long.csv"
    source.write_text("id,type,value,date\n" + "\n".join(f"p{i},weight,{i},2024-01-01" for i in range(10)), encoding="utf-8")
    profile = profile_csv(source, "long", {"subject": "id", "eventType": "type", "eventValue": "value", "eventDate": "date"})
    assert profile["columns"]["id"]["frequenciesSuppressed"] is True
    assert profile["columns"]["type"]["valueFrequencies"] == [{"value": "weight", "count": 10}]
    assert profile["conditionalValueProfiles"]["weight"]["numeric"]["median"] == 4.5


def test_wide_csv_builds_person_and_measurement_batches(tmp_path):
    ctx = context(
        tmp_path, "id,born,measured,weight_kg\np1,1980-02-03,2024-01-02,72.5\n",
        wide_mapping(), "wide", {"subject": "id"},
    )
    batches = OmopBatchBuilder(ctx).build()
    assert batches["person"][0]["year_of_birth"] == 1980
    assert batches["measurement"][0]["value_as_number"] == 72.5
    assert batches["measurement"][0]["measurement_concept_id"] == 3025315


def test_long_csv_uses_when_filter_and_event_date_role(tmp_path):
    mapping = {"databases": {"source": {"tables": {"source": {
        "layout": "long",
        "roles": {"subject": "id", "eventType": "kind", "eventValue": "value", "eventDate": "date"},
        "columns": {
            "id": {"mapsTo": "schema:variable/identifier", "localColumn": "id"},
            "born": {"mapsTo": "schema:variable/birth_date", "localColumn": "born"},
            "weight": {"mapsTo": "schema:variable/weight", "localColumn": "value", "when": {"column": "kind", "equals": "weight"}},
        },
    }}}}}
    ctx = context(
        tmp_path,
        "id,born,kind,value,date\np1,1980-02-03,weight,70,2024-01-01\np1,1980-02-03,other,ignored,2024-01-02\n",
        mapping, "long", {"subject": "id", "eventType": "kind", "eventValue": "value", "eventDate": "date"},
    )
    batches = OmopBatchBuilder(ctx).build()
    assert len(batches["person"]) == 1
    assert len(batches["measurement"]) == 1
    assert str(batches["measurement"][0]["measurement_date"]) == "2024-01-01"


def test_long_csv_rejects_inconsistent_person_attributes(tmp_path):
    mapping = {"databases": {"source": {"tables": {"source": {
        "layout": "long",
        "roles": {"subject": "id", "eventType": "kind", "eventValue": "value", "eventDate": "date"},
        "columns": {
            "id": {"mapsTo": "schema:variable/identifier", "localColumn": "id"},
            "born": {"mapsTo": "schema:variable/birth_date", "localColumn": "born"},
            "weight": {"mapsTo": "schema:variable/weight", "localColumn": "value", "when": {"column": "kind", "equals": "weight"}},
        },
    }}}}}
    ctx = context(
        tmp_path,
        "id,born,kind,value,date\np1,1980-02-03,weight,70,2024-01-01\np1,1981-02-03,weight,71,2024-01-02\n",
        mapping, "long", {"subject": "id", "eventType": "kind", "eventValue": "value", "eventDate": "date"},
    )
    with pytest.raises(V2Error, match="Repeated person attributes"):
        OmopBatchBuilder(ctx).build()


def test_namespace_registry_is_exact_and_extensible():
    parsed = NamespaceRegistry({"https://example.org/codes/": "Example"}).parse("https://example.org/codes/A-12")
    assert (parsed.vocabulary_id, parsed.concept_code) == ("Example", "A-12")
    with pytest.raises(V2Error, match="No OMOP vocabulary"):
        NamespaceRegistry().parse("https://unknown.example/code")


def test_api_project_resume_and_optimistic_mapping_lock(tmp_path):
    app = Flask(__name__)
    app.register_blueprint(create_api_blueprint(tmp_path))
    client = app.test_client()

    response = client.post("/api/v2/projects", json={"name": "Restartable study"})
    assert response.status_code == 201
    project_id = response.json["id"]
    assert client.put(f"/api/v2/projects/{project_id}/requirement", json=requirement()).status_code == 201
    source = client.post(
        f"/api/v2/projects/{project_id}/source",
        data={
            "file": (io.BytesIO(b"id,born,measured,weight_kg\np1,1980-02-03,2024-01-02,72.5\n"), "cohort.csv"),
            "layout": "wide", "roles": json.dumps({"subject": "id"}),
        },
        content_type="multipart/form-data",
    )
    assert source.status_code == 201
    assert source.json["profile"]["columns"]["weight_kg"]["inferredType"] == "numeric"

    first = client.put(f"/api/v2/projects/{project_id}/mapping", json=wide_mapping())
    assert first.status_code == 201
    missing_match = client.put(f"/api/v2/projects/{project_id}/mapping", json=wide_mapping())
    assert missing_match.status_code == 428
    assert set(missing_match.json) == {"code", "message", "fieldErrors", "details"}
    stale = client.put(
        f"/api/v2/projects/{project_id}/mapping", json=wide_mapping(), headers={"If-Match": "0"}
    )
    assert stale.status_code == 409
    assert stale.json["details"]["currentRevision"] == 1
    current = client.get(f"/api/v2/projects/{project_id}").json
    assert current["readiness"] == {
        "requirement": True, "source": True, "mapping": True, "omopPreflight": False,
    }


def test_constraints_and_cross_graph_cycles_are_blocking():
    with pytest.raises(V2Error, match="Source values do not satisfy"):
        validate_value("weight", "not-a-number", [{"kind": "range", "minimum": 0}], 2)
    assert validate_value("weight", "-1", [{"kind": "range", "minimum": 0, "severity": "warning"}], 2)[0]["severity"] == "warning"
    with pytest.raises(V2Error, match="contain a cycle"):
        _validate_link_cycles({"crossGraphLinks": [
            {"fromTable": "a", "toTable": "b"}, {"fromTable": "b", "toTable": "a"},
        ]})


def test_healthdcat_release_7_export_is_local_and_access_aware(tmp_path):
    files = ProjectFiles(tmp_path)
    artifacts = export_healthdcat(files, "12345678-1234-1234-1234-123456789abc", {
        "datasetUri": "https://example.org/dataset/1", "title": "Cohort",
        "description": "Research cohort", "publisherUri": "https://example.org/organisation",
        "contactName": "Data Office", "contactEmail": "data@example.org",
        "accessLevel": "public", "accessUrl": "https://example.org/data",
        "licenseUri": "https://creativecommons.org/licenses/by/4.0/",
        "recordCount": 10, "confirmRecordCount": True,
    })
    turtle = next(item for item in artifacts if item["kind"] == "healthdcat-turtle")
    text = open(turtle["path"], encoding="utf-8").read()
    assert "releases/release-7" in text
    assert "10 records" in text
    assert "dcat:accessURL" in text
