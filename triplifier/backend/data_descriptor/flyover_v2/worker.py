"""Child-process entry point for a single converter job."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from .converters.registry import ConverterRegistry
from .database import ProjectStore
from .errors import V2Error
from .files import ProjectFiles
from .project_service import project_context


def main() -> int:
    if len(sys.argv) != 5:
        return 2
    data_root, project_id, job_id, converter_id = sys.argv[1:]
    target = json.load(sys.stdin)
    store = ProjectStore(data_root)
    files = ProjectFiles(data_root)
    store.update_job(job_id, status="running", progress=5)
    try:
        context = project_context(store, files, project_id)
        report = ConverterRegistry().get(converter_id).convert(context, target)
        for artifact in report.get("artifacts", []):
            artifact_path = artifact.pop("path")
            saved = store.save_artifact(
                project_id, job_id, artifact["kind"], artifact_path, artifact["sha256"]
            )
            artifact["id"] = saved["id"]
            artifact["downloadUrl"] = f"/api/v2/projects/{project_id}/artifacts/{saved['id']}"
        store.update_job(job_id, status="completed", progress=100, report=report)
        return 0
    except V2Error as exc:
        store.update_job(job_id, status="failed", progress=0, report=exc.response())
        return 1
    except Exception:
        # The report is deliberately generic: credentials and row values must never leak.
        store.update_job(
            job_id, status="failed", progress=0,
            report={"code": "conversion_failed", "message": "The converter stopped unexpectedly"},
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
