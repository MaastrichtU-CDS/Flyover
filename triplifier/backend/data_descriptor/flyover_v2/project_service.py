"""Assembly of a converter context from persistent project state."""

from __future__ import annotations

import copy
from typing import Any

from .converters.base import ProjectContext
from .database import ProjectStore
from .files import ProjectFiles


def _optional_snapshot(store: ProjectStore, project_id: str, kind: str) -> dict[str, Any]:
    try:
        return store.latest_snapshot(project_id, kind)[1]
    except Exception as exc:
        # Avoid hiding storage failures: only the known not-found contract is optional.
        if getattr(exc, "code", None) == "not_found":
            return {}
        raise


def apply_terminology_overrides(
    requirement: dict[str, Any], overrides: dict[str, Any]
) -> dict[str, Any]:
    result = copy.deepcopy(requirement)
    omop = result.get("targets", {}).get("omop", {})
    for key, resolution in overrides.get("overrides", {}).items():
        concept_id = resolution.get("conceptId")
        parts = key.split(":")
        if concept_id is None:
            continue
        if parts[0] == "variable" and len(parts) == 2:
            omop["variables"][parts[1]]["conceptId"] = concept_id
        elif parts[0] == "type" and len(parts) == 2:
            omop["variables"][parts[1]]["typeConceptId"] = concept_id
        elif parts[0] == "unit" and len(parts) == 2:
            omop["variables"][parts[1]]["unitConceptId"] = concept_id
        elif parts[0] == "term" and len(parts) >= 3:
            variable, term = parts[1], ":".join(parts[2:])
            omop["variables"][variable].setdefault("termConceptIds", {})[term] = concept_id
        elif parts[0] == "person-gender" and len(parts) >= 2:
            term = ":".join(parts[1:])
            omop["person"].setdefault("genderConceptIds", {})[term] = concept_id
    return result


def project_context(store: ProjectStore, files: ProjectFiles, project_id: str) -> ProjectContext:
    requirement = store.latest_snapshot(project_id, "requirement")[1]
    mapping = store.latest_snapshot(project_id, "mapping")[1]
    overrides = _optional_snapshot(store, project_id, "terminology")
    return ProjectContext(
        project_id=project_id,
        project_dir=files.project_dir(project_id),
        source=store.source(project_id),
        requirement=apply_terminology_overrides(requirement, overrides),
        mapping=mapping,
    )
