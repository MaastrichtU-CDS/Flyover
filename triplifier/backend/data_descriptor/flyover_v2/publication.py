"""HealthDCAT-AP Release 7 metadata collection and Turtle export."""

from __future__ import annotations

from pathlib import Path
import re
import uuid
from typing import Any
from urllib.parse import urlsplit

from .errors import V2Error
from .files import ProjectFiles, sha256_file

PROFILE_URI = "https://healthdataeu.pages.code.europa.eu/healthdcat-ap/releases/release-7/"


def _literal(value: Any) -> str:
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "")
    return f'"{escaped}"'


def _uri(value: Any, field: str) -> str:
    text = str(value or "")
    parsed = urlsplit(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or any(character in text for character in '<>"{}|\\^`'):
        raise V2Error("invalid_publication", "Publication metadata contains an invalid URI", 422, {field: "Use an HTTP(S) URI"})
    return f"<{text}>"


def validate_metadata(value: dict[str, Any]) -> dict[str, Any]:
    required = ("datasetUri", "title", "description", "publisherUri", "contactName", "contactEmail", "accessLevel")
    missing = {field: "Required" for field in required if not str(value.get(field, "")).strip()}
    access = value.get("accessLevel")
    if access not in {"public", "restricted", "non-public"}:
        missing["accessLevel"] = "Use public, restricted or non-public"
    if access == "public":
        for field in ("accessUrl", "licenseUri"):
            if not value.get(field):
                missing[field] = "Required for public data"
    if access == "restricted" and not value.get("accessRightsDescription"):
        missing["accessRightsDescription"] = "Describe how access is requested"
    if missing:
        raise V2Error("invalid_publication", "Publication metadata is incomplete", 422, missing)
    _uri(value["datasetUri"], "datasetUri")
    _uri(value["publisherUri"], "publisherUri")
    if not re.fullmatch(r"[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+", str(value["contactEmail"])):
        raise V2Error("invalid_publication", "Publication metadata contains an invalid email address", 422, {"contactEmail": "Invalid email address"})
    if value.get("accessUrl"):
        _uri(value["accessUrl"], "accessUrl")
    if value.get("licenseUri"):
        _uri(value["licenseUri"], "licenseUri")
    return dict(value)


def export_healthdcat(files: ProjectFiles, project_id: str, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    value = validate_metadata(metadata)
    dataset = _uri(value["datasetUri"], "datasetUri")
    access_uri = {
        "public": "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
        "restricted": "http://publications.europa.eu/resource/authority/access-right/RESTRICTED",
        "non-public": "http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC",
    }[value["accessLevel"]]
    lines = [
        "@prefix dcat: <http://www.w3.org/ns/dcat#> .",
        "@prefix dct: <http://purl.org/dc/terms/> .",
        "@prefix vcard: <http://www.w3.org/2006/vcard/ns#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f"{dataset} a dcat:Dataset ;",
        f"  dct:conformsTo <{PROFILE_URI}> ;",
        f"  dct:title {_literal(value['title'])} ;",
        f"  dct:description {_literal(value['description'])} ;",
        f"  dct:identifier {_literal(value.get('identifier', value['datasetUri']))} ;",
        f"  dct:publisher {_uri(value['publisherUri'], 'publisherUri')} ;",
        f"  dct:accessRights <{access_uri}> ;",
        "  dcat:contactPoint [ a vcard:Kind ;",
        f"    vcard:fn {_literal(value['contactName'])} ;",
        f"    vcard:hasEmail <mailto:{str(value['contactEmail']).replace('>', '')}> ]",
    ]
    if value.get("confirmRecordCount") and value.get("recordCount") is not None:
        lines[-1] += " ;"
        lines.append(f"  dct:extent {_literal(str(value['recordCount']) + ' records')}")
    if value["accessLevel"] == "public":
        distribution = f"{value['datasetUri'].rstrip('/')}/distribution"
        lines[-1] += f" ;\n  dcat:distribution <{distribution}>"
        lines.extend([
            ".", "", f"<{distribution}> a dcat:Distribution ;",
            f"  dcat:accessURL {_uri(value['accessUrl'], 'accessUrl')} ;",
            f"  dct:license {_uri(value['licenseUri'], 'licenseUri')} .",
        ])
    else:
        lines[-1] += " ."
    if not lines[-1].endswith("."):
        lines[-1] += " ."

    artifact_dir = files.project_dir(project_id) / "artifacts"
    export_id = uuid.uuid4().hex
    turtle_path = artifact_dir / f"healthdcat-ap-release-7-{export_id}.ttl"
    report_path = artifact_dir / f"healthdcat-validation-{export_id}.json"
    files.atomic_text(turtle_path, "\n".join(lines) + "\n")
    report = {
        "profile": PROFILE_URI, "conforms": True,
        "validationLevel": "Flyover structural checks",
        "checks": ["required fields", "access-level-dependent fields", "HTTP(S) identifiers"],
    }
    files.atomic_json(report_path, report)
    return [
        {"kind": "healthdcat-turtle", "path": str(turtle_path), "sha256": sha256_file(turtle_path)},
        {"kind": "healthdcat-validation", "path": str(report_path), "sha256": sha256_file(report_path)},
    ]
