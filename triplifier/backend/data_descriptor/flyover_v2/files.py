"""Project-local file storage with atomic writes and safe paths."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any, BinaryIO

from .errors import V2Error

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(name: str) -> str:
    cleaned = SAFE_NAME.sub("_", Path(name).name).strip("._")
    if not cleaned:
        raise V2Error("invalid_filename", "The uploaded filename is not usable")
    return cleaned[:180]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ProjectFiles:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def project_dir(self, project_id: str) -> Path:
        if not re.fullmatch(r"[0-9a-f-]{36}", project_id):
            raise V2Error("invalid_project_id", "Invalid project identifier")
        path = (self.root / project_id).resolve()
        if self.root not in path.parents:
            raise V2Error("unsafe_path", "Project path escaped the data directory")
        path.mkdir(parents=True, exist_ok=True)
        for child in ("sources", "profiles", "mappings", "jobs", "artifacts"):
            (path / child).mkdir(exist_ok=True)
        return path

    def atomic_json(self, path: Path, value: Any) -> None:
        self.atomic_text(path, json.dumps(value, indent=2, ensure_ascii=False))

    def atomic_text(self, path: Path, value: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".flyover-", dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def save_upload(self, project_id: str, filename: str, stream: BinaryIO) -> Path:
        target = self.project_dir(project_id) / "sources" / f"{uuid.uuid4().hex[:12]}-{safe_filename(filename)}"
        descriptor, temporary = tempfile.mkstemp(prefix=".upload-", dir=target.parent)
        try:
            with os.fdopen(descriptor, "wb") as output:
                while chunk := stream.read(1024 * 1024):
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return target
