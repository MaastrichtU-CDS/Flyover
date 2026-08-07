"""Supervise converters in child processes without persisting connection secrets."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from .database import ProjectStore
from .errors import ConflictError


class JobRunner:
    def __init__(self, data_root: Path, store: ProjectStore):
        self.data_root = data_root
        self.store = store
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.lock = threading.Lock()

    def submit(self, project_id: str, converter_id: str, target: dict[str, Any]) -> dict[str, Any]:
        job = self.store.create_job(project_id, converter_id)
        command = [
            sys.executable, "-m", "data_descriptor.flyover_v2.worker",
            str(self.data_root), project_id, job["id"], converter_id,
        ]
        process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True,
        )
        assert process.stdin is not None
        process.stdin.write(json.dumps(target).encode("utf-8"))
        process.stdin.close()
        with self.lock:
            self.processes[job["id"]] = process
        threading.Thread(target=self._reap, args=(project_id, job["id"], process), daemon=True).start()
        return job

    def _reap(self, project_id: str, job_id: str, process: subprocess.Popen[bytes]) -> None:
        process.wait()
        with self.lock:
            self.processes.pop(job_id, None)
        job = self.store.get_job(project_id, job_id)
        if job["status"] in {"queued", "running", "cancelling"}:
            self.store.update_job(
                job_id, status="retryable", progress=0,
                report={"code": "worker_interrupted", "message": "The converter worker stopped; submit the job again"},
            )

    def cancel(self, project_id: str, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(project_id, job_id)
        if job["status"] not in {"queued", "running"}:
            raise ConflictError("Only queued or running jobs can be cancelled")
        with self.lock:
            process = self.processes.get(job_id)
        if process is None:
            self.store.update_job(job_id, status="retryable", report={"message": "Worker is no longer available"})
        else:
            self.store.update_job(job_id, status="cancelling")
            process.terminate()
            self.store.update_job(job_id, status="cancelled", progress=0)
        return self.store.get_job(project_id, job_id)
