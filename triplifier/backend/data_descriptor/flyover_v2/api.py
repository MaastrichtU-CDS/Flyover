"""Flask JSON API for the independent Flyover v2 workflow."""

from __future__ import annotations

import ipaddress
import json
import shutil
import socket
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from flask import Blueprint, Response, jsonify, request, send_file

from .contracts import canonical_requirement, expand_term, validate_local_mapping
from .converters.registry import ConverterRegistry
from .database import ProjectStore
from .errors import NotFoundError, V2Error
from .files import ProjectFiles, sha256_file
from .jobs import JobRunner
from .profiling import profile_csv
from .project_service import project_context
from .publication import export_healthdcat, validate_metadata
from .terminology import OmopTerminologyResolver, connection_parameters, safe_schema

MAX_REMOTE_REQUIREMENT_BYTES = 2 * 1024 * 1024


def _json_object() -> dict[str, Any]:
    value = request.get_json(silent=True)
    if not isinstance(value, dict):
        raise V2Error("invalid_json", "A JSON object is required", 400)
    return value


def _if_match(required: bool) -> int | None:
    value = request.headers.get("If-Match")
    if not value:
        if required:
            raise V2Error("if_match_required", "If-Match is required for an existing revision", 428)
        return None
    value = value.strip().strip('W/').strip('"')
    try:
        return int(value)
    except ValueError as exc:
        raise V2Error("invalid_if_match", "If-Match must contain a revision number", 400) from exc


def _public_project(store: ProjectStore, project_id: str) -> dict[str, Any]:
    project = store.get_project(project_id)
    try:
        preflight = store.latest_snapshot(project_id, "omop-preflight")[1]
        has_preflight = (
            preflight.get("requirementRevision") == project["requirementRevision"]
            and preflight.get("mappingRevision") == project["mappingRevision"]
            and preflight.get("sourceSha256") == store.source(project_id)["sha256"]
        )
    except NotFoundError:
        has_preflight = False
    project["readiness"] = {
        "requirement": project["requirementRevision"] > 0,
        "source": project["hasSource"],
        "mapping": project["mappingRevision"] > 0,
        "omopPreflight": has_preflight,
    }
    return project


def _validate_remote_host(url: str) -> None:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise V2Error("unsafe_requirement_url", "Requirement imports must use a public HTTPS URL", 422)
    try:
        addresses = socket.getaddrinfo(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise V2Error("requirement_url_failed", "The requirement host could not be resolved", 422) from exc
    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        if not ip.is_global:
            raise V2Error("unsafe_requirement_url", "Private and local requirement hosts are prohibited", 422)


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> Any:
        _validate_remote_host(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _remote_requirement(url: str) -> dict[str, Any]:
    _validate_remote_host(url)
    try:
        with urllib.request.build_opener(_SafeRedirect()).open(url, timeout=10) as response:
            if response.headers.get_content_type() not in {"application/json", "application/ld+json", "text/plain"}:
                raise V2Error("invalid_requirement_type", "The URL did not return JSON", 422)
            content = response.read(MAX_REMOTE_REQUIREMENT_BYTES + 1)
    except V2Error:
        raise
    except (urllib.error.URLError, TimeoutError) as exc:
        raise V2Error("requirement_url_failed", "The requirement URL could not be downloaded", 422) from exc
    if len(content) > MAX_REMOTE_REQUIREMENT_BYTES:
        raise V2Error("requirement_too_large", "The remote requirement exceeds 2 MB", 413)
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V2Error("invalid_json", "The downloaded requirement is not valid JSON", 422) from exc
    return canonical_requirement(value)


def _terminology_requests(requirement: dict[str, Any]) -> list[dict[str, str]]:
    schema = requirement["schema"]
    omop = requirement["targets"]["omop"]
    result: list[dict[str, str]] = []
    for variable, binding in omop.get("variables", {}).items():
        definition = schema["variables"][variable]
        result.append({
            "key": f"variable:{variable}", "uri": expand_term(requirement, definition["class"]),
            "domain": binding["domain"],
        })
        if binding.get("typeUri"):
            result.append({"key": f"type:{variable}", "uri": expand_term(requirement, binding["typeUri"]), "domain": ""})
        if binding.get("unitUri"):
            result.append({"key": f"unit:{variable}", "uri": expand_term(requirement, binding["unitUri"]), "domain": "Unit"})
        for term, definition in definition.get("valueMapping", {}).get("terms", {}).items():
            result.append({
                "key": f"term:{variable}:{term}",
                "uri": expand_term(requirement, definition["targetClass"]),
                "domain": binding["domain"],
            })
    sex_variable = omop.get("person", {}).get("sexAtBirth")
    if sex_variable:
        for term, definition in schema["variables"][sex_variable].get("valueMapping", {}).get("terms", {}).items():
            result.append({
                "key": f"person-gender:{term}",
                "uri": expand_term(requirement, definition["targetClass"]), "domain": "Person",
            })
    return result


def create_api_blueprint(data_root: str | Path) -> Blueprint:
    root = Path(data_root).resolve()
    store = ProjectStore(root)
    store.interrupt_running_jobs()
    files = ProjectFiles(root)
    registry = ConverterRegistry()
    runner = JobRunner(root, store)
    api = Blueprint("flyover_v2_api", __name__, url_prefix="/api/v2")

    @api.errorhandler(V2Error)
    def known_error(error: V2Error) -> tuple[Response, int]:
        return jsonify(error.response()), error.status

    @api.errorhandler(413)
    def upload_too_large(_: Any) -> tuple[Response, int]:
        error = V2Error("upload_too_large", "The uploaded file exceeds the configured limit", 413)
        return jsonify(error.response()), 413

    @api.errorhandler(Exception)
    def unexpected_error(_: Exception) -> tuple[Response, int]:
        error = V2Error("internal_error", "The request could not be completed", 500)
        return jsonify(error.response()), 500

    @api.get("/projects")
    def projects_list() -> Response:
        return jsonify([_public_project(store, project["id"]) for project in store.list_projects()])

    @api.post("/projects")
    def projects_create() -> tuple[Response, int]:
        name = str(_json_object().get("name", "")).strip()
        if not name:
            raise V2Error("invalid_project", "A project name is required", 422, {"name": "Required"})
        project = store.create_project(name)
        files.project_dir(project["id"])
        return jsonify(_public_project(store, project["id"])), 201

    @api.get("/projects/<project_id>")
    def projects_get(project_id: str) -> Response:
        return jsonify(_public_project(store, project_id))

    @api.delete("/projects/<project_id>")
    def projects_delete(project_id: str) -> tuple[str, int]:
        store.get_project(project_id)
        project_dir = files.project_dir(project_id)
        store.delete_project(project_id)
        shutil.rmtree(project_dir)
        return "", 204

    @api.put("/projects/<project_id>/requirement")
    def requirement_put(project_id: str) -> tuple[Response, int]:
        current = store.get_project(project_id)["requirementRevision"]
        if request.is_json:
            body = _json_object()
            requirement = _remote_requirement(str(body["url"])) if set(body) == {"url"} else canonical_requirement(body)
        elif "file" in request.files:
            try:
                requirement = canonical_requirement(json.load(request.files["file"].stream))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise V2Error("invalid_json", "The requirement file is not valid JSON", 422) from exc
        else:
            raise V2Error("requirement_missing", "Upload JSON or provide an HTTPS URL", 400)
        revision = store.save_snapshot(project_id, "requirement", requirement, _if_match(current > 0))
        path = files.project_dir(project_id) / "mappings" / f"requirement-r{revision}.jsonld"
        files.atomic_json(path, requirement)
        return jsonify({"revision": revision, "requirement": requirement}), 201

    @api.get("/projects/<project_id>/requirement")
    def requirement_get(project_id: str) -> Response:
        revision, requirement = store.latest_snapshot(project_id, "requirement")
        response = jsonify({"revision": revision, "requirement": requirement})
        response.headers["ETag"] = f'"{revision}"'
        return response

    @api.post("/projects/<project_id>/source")
    def source_post(project_id: str) -> tuple[Response, int]:
        store.get_project(project_id)
        upload = request.files.get("file")
        if not upload or not upload.filename:
            raise V2Error("source_missing", "A CSV source file is required", 400)
        layout = request.form.get("layout", "wide")
        if layout not in {"wide", "long"}:
            raise V2Error("invalid_layout", "Use wide or long", 422)
        try:
            roles = json.loads(request.form.get("roles", "{}"))
        except json.JSONDecodeError as exc:
            raise V2Error("invalid_roles", "Roles must be a JSON object", 422) from exc
        if not isinstance(roles, dict) or not roles.get("subject"):
            raise V2Error("invalid_roles", "A subject column role is required", 422)
        path = files.save_upload(project_id, upload.filename, upload.stream)
        try:
            profile = profile_csv(path, layout, roles)
        except Exception:
            path.unlink(missing_ok=True)
            raise
        profile_path = files.project_dir(project_id) / "profiles" / "source.json"
        files.atomic_json(profile_path, profile)
        source = store.save_source(project_id, {
            "filename": path.name, "path": str(path), "sha256": sha256_file(path),
            "layout": layout, "roles": roles, "profile_path": str(profile_path),
        })
        public_source = {
            key: source[key] for key in ("id", "filename", "sha256", "layout", "roles", "created_at")
        }
        return jsonify({"source": public_source, "profile": profile}), 201

    @api.get("/projects/<project_id>/profile")
    def profile_get(project_id: str) -> Response:
        source = store.source(project_id)
        if not source.get("profile_path"):
            raise NotFoundError("Profile")
        with Path(source["profile_path"]).open(encoding="utf-8") as stream:
            return jsonify(json.load(stream))

    @api.put("/projects/<project_id>/mapping")
    def mapping_put(project_id: str) -> tuple[Response, int]:
        requirement = store.latest_snapshot(project_id, "requirement")[1]
        current = store.get_project(project_id)["mappingRevision"]
        mapping = validate_local_mapping(requirement, _json_object())
        revision = store.save_snapshot(project_id, "mapping", mapping, _if_match(current > 0))
        files.atomic_json(files.project_dir(project_id) / "mappings" / f"mapping-r{revision}.jsonld", mapping)
        return jsonify({"revision": revision, "mapping": mapping}), 201

    @api.get("/projects/<project_id>/mapping")
    def mapping_get(project_id: str) -> Response:
        revision, mapping = store.latest_snapshot(project_id, "mapping")
        response = jsonify({"revision": revision, "mapping": mapping})
        response.headers["ETag"] = f'"{revision}"'
        return response

    @api.get("/converters")
    def converters_list() -> Response:
        return jsonify(registry.list())

    @api.post("/projects/<project_id>/terminology/resolve")
    def terminology_resolve(project_id: str) -> Response:
        body = _json_object()
        requirement = store.latest_snapshot(project_id, "requirement")[1]
        if "omop" not in requirement.get("targets", {}):
            raise V2Error("omop_target_missing", "The requirement has no OMOP target extension", 422)
        try:
            with psycopg2.connect(**connection_parameters(body)) as connection:
                resolver = OmopTerminologyResolver(connection, safe_schema(body))
                resolutions = [
                    {"key": item["key"], **resolver.resolve(item["uri"], item["domain"] or None)}
                    for item in _terminology_requests(requirement)
                ]
        except V2Error:
            raise
        except psycopg2.Error as exc:
            raise V2Error("postgres_connection_failed", "Could not query the OMOP vocabulary", 422) from exc
        return jsonify({"resolutions": resolutions, "ready": all(item["status"] == "resolved" for item in resolutions)})

    @api.put("/projects/<project_id>/terminology/overrides")
    def terminology_overrides(project_id: str) -> tuple[Response, int]:
        body = _json_object()
        overrides = body.get("overrides")
        target = body.get("target")
        if not isinstance(overrides, dict):
            raise V2Error("invalid_overrides", "Overrides must be an object", 422)
        if not isinstance(target, dict):
            raise V2Error("invalid_target", "Transient OMOP connection settings are required", 422)
        requirement = store.latest_snapshot(project_id, "requirement")[1]
        expected_domains = {item["key"]: item["domain"] for item in _terminology_requests(requirement)}
        sanitized: dict[str, Any] = {"overrides": {}}
        try:
            with psycopg2.connect(**connection_parameters(target)) as connection, connection.cursor() as cursor:
                for key, value in overrides.items():
                    if key not in expected_domains or not isinstance(value, dict) or not isinstance(value.get("conceptId"), int) or value["conceptId"] <= 0:
                        raise V2Error("invalid_override", "Each known terminology key needs a positive conceptId", 422)
                    cursor.execute(
                        f"SELECT concept_id, domain_id FROM {safe_schema(target)}.concept "
                        "WHERE concept_id = %s AND standard_concept = 'S' AND invalid_reason IS NULL",
                        (value["conceptId"],),
                    )
                    row = cursor.fetchone()
                    domain = expected_domains[key]
                    from .terminology import DOMAIN_NAMES
                    expected = DOMAIN_NAMES.get(domain, domain)
                    if not row or (expected and row[1] != expected):
                        raise V2Error("invalid_override", "The selected concept is invalid, non-standard, or in the wrong domain", 422, details={"key": key})
                    sanitized["overrides"][key] = {
                        "conceptId": value["conceptId"], "uri": value.get("uri"),
                        "provenance": "manual", "selectedAt": value.get("selectedAt"),
                    }
        except V2Error:
            raise
        except psycopg2.Error as exc:
            raise V2Error("postgres_connection_failed", "Could not verify terminology overrides", 422) from exc
        revision = store.save_snapshot(project_id, "terminology", sanitized)
        return jsonify({"revision": revision, **sanitized}), 201

    @api.post("/projects/<project_id>/omop/preflight")
    def omop_preflight(project_id: str) -> Response:
        context = project_context(store, files, project_id)
        target = _json_object()
        report = registry.get("omop-5.4").validate(context, target)
        store.save_snapshot(project_id, "omop-preflight", {
            "completedAt": datetime.now(timezone.utc).isoformat(),
            "requirementRevision": store.get_project(project_id)["requirementRevision"],
            "mappingRevision": store.get_project(project_id)["mappingRevision"],
            "sourceSha256": store.source(project_id)["sha256"],
            "target": {
                key: target.get(key)
                for key in ("host", "port", "database", "dbname", "user", "schema", "sslmode")
                if target.get(key) is not None
            },
            "report": report,
        })
        return jsonify(report)

    @api.post("/projects/<project_id>/omop/connection-test")
    def omop_connection_test(project_id: str) -> Response:
        store.get_project(project_id)
        target = _json_object()
        try:
            with psycopg2.connect(**connection_parameters(target)) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT current_setting('server_version_num')::integer")
                version = int(cursor.fetchone()[0])
        except V2Error:
            raise
        except psycopg2.Error as exc:
            raise V2Error("postgres_connection_failed", "Could not connect to PostgreSQL", 422) from exc
        return jsonify({
            "connected": True, "postgresVersionNumber": version,
            "postgres14OrNewer": version >= 140000,
        })

    @api.post("/projects/<project_id>/conversions")
    def conversion_submit(project_id: str) -> tuple[Response, int]:
        body = _json_object()
        converter_id = str(body.get("converterId", ""))
        registry.get(converter_id)
        target = body.get("target", {})
        if not isinstance(target, dict):
            raise V2Error("invalid_target", "Converter target configuration must be an object", 422)
        project_context(store, files, project_id)
        return jsonify(runner.submit(project_id, converter_id, target)), 202

    @api.get("/projects/<project_id>/jobs/<job_id>")
    def job_get(project_id: str, job_id: str) -> Response:
        return jsonify(store.get_job(project_id, job_id))

    @api.delete("/projects/<project_id>/jobs/<job_id>")
    def job_cancel(project_id: str, job_id: str) -> Response:
        return jsonify(runner.cancel(project_id, job_id))

    @api.get("/projects/<project_id>/artifacts/<artifact_id>")
    def artifact_get(project_id: str, artifact_id: str) -> Response:
        artifact = store.get_artifact(project_id, artifact_id)
        path = Path(artifact["path"]).resolve()
        project_dir = files.project_dir(project_id)
        if project_dir not in path.parents:
            raise V2Error("unsafe_artifact", "Artifact path escaped the project directory", 500)
        return send_file(path, as_attachment=True, download_name=path.name)

    @api.get("/projects/<project_id>/publication")
    def publication_get(project_id: str) -> Response:
        revision, metadata = store.latest_snapshot(project_id, "publication")
        response = jsonify({"revision": revision, "metadata": metadata})
        response.headers["ETag"] = f'"{revision}"'
        return response

    @api.put("/projects/<project_id>/publication")
    def publication_put(project_id: str) -> tuple[Response, int]:
        try:
            current = store.latest_snapshot(project_id, "publication")[0]
        except NotFoundError:
            current = 0
        metadata = validate_metadata(_json_object())
        revision = store.save_snapshot(project_id, "publication", metadata, _if_match(current > 0))
        return jsonify({"revision": revision, "metadata": metadata}), 201

    @api.post("/projects/<project_id>/publication/export")
    def publication_export(project_id: str) -> tuple[Response, int]:
        metadata = store.latest_snapshot(project_id, "publication")[1]
        public_artifacts = []
        for artifact in export_healthdcat(files, project_id, metadata):
            path = artifact.pop("path")
            saved = store.save_artifact(project_id, None, artifact["kind"], path, artifact["sha256"])
            public_artifacts.append({
                **artifact, "id": saved["id"],
                "downloadUrl": f"/api/v2/projects/{project_id}/artifacts/{saved['id']}",
            })
        return jsonify({"profile": "HealthDCAT-AP Release 7", "artifacts": public_artifacts}), 201

    return api
