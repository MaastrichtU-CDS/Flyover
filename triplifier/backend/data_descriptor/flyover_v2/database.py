"""SQLite persistence for resumable Flyover v2 projects."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy import event
from alembic import command
from alembic.config import Config

from .errors import ConflictError, NotFoundError

metadata = MetaData()

projects = Table(
    "projects",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("name", String(200), nullable=False),
    Column("created_at", String(40), nullable=False),
    Column("updated_at", String(40), nullable=False),
    Column("revision", Integer, nullable=False, default=0),
)

snapshots = Table(
    "snapshots",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("project_id", ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    Column("kind", String(30), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("content", Text, nullable=False),
    Column("created_at", String(40), nullable=False),
)

sources = Table(
    "sources",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("project_id", ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    Column("filename", String(200), nullable=False),
    Column("path", Text, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("layout", String(10), nullable=False),
    Column("roles", Text, nullable=False),
    Column("profile_path", Text),
    Column("created_at", String(40), nullable=False),
)

jobs = Table(
    "jobs",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("project_id", ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    Column("converter", String(100), nullable=False),
    Column("status", String(30), nullable=False),
    Column("progress", Integer, nullable=False, default=0),
    Column("report", Text),
    Column("created_at", String(40), nullable=False),
    Column("updated_at", String(40), nullable=False),
)

artifacts = Table(
    "artifacts",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("project_id", ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    Column("job_id", ForeignKey("jobs.id", ondelete="CASCADE")),
    Column("kind", String(80), nullable=False),
    Column("path", Text, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("created_at", String(40), nullable=False),
)

audit_events = Table(
    "audit_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("project_id", ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
    Column("event", String(100), nullable=False),
    Column("details", Text, nullable=False),
    Column("created_at", String(40), nullable=False),
)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProjectStore:
    def __init__(self, data_root: str | Path):
        root = Path(data_root)
        root.mkdir(parents=True, exist_ok=True)
        self.data_root = root
        database_url = f"sqlite:///{root / 'flyover-v2.sqlite3'}"
        self.engine = create_engine(database_url)
        event.listen(self.engine, "connect", lambda connection, _: connection.execute("PRAGMA foreign_keys=ON"))
        migration_root = Path(__file__).parent / "migrations"
        config = Config(str(migration_root / "alembic.ini"))
        config.set_main_option("script_location", str(migration_root))
        config.set_main_option("sqlalchemy.url", database_url)
        command.upgrade(config, "head")

    @staticmethod
    def _project(row: Any) -> dict[str, Any]:
        return dict(row._mapping)

    def list_projects(self) -> list[dict[str, Any]]:
        with self.engine.begin() as connection:
            rows = connection.execute(select(projects).order_by(projects.c.updated_at.desc()))
            return [self._project(row) for row in rows]

    def create_project(self, name: str) -> dict[str, Any]:
        if not name.strip():
            raise ValueError("Project name is required")
        project_id = str(uuid.uuid4())
        timestamp = now()
        value = {
            "id": project_id,
            "name": name.strip(),
            "created_at": timestamp,
            "updated_at": timestamp,
            "revision": 0,
        }
        with self.engine.begin() as connection:
            connection.execute(insert(projects).values(**value))
            self._audit(connection, project_id, "project.created", {"name": value["name"]})
        return value

    def get_project(self, project_id: str) -> dict[str, Any]:
        with self.engine.begin() as connection:
            row = connection.execute(select(projects).where(projects.c.id == project_id)).first()
            if not row:
                raise NotFoundError("Project")
            result = self._project(row)
            result["requirementRevision"] = self.latest_revision(connection, project_id, "requirement")
            result["mappingRevision"] = self.latest_revision(connection, project_id, "mapping")
            result["hasSource"] = connection.execute(
                select(sources.c.id).where(sources.c.project_id == project_id).limit(1)
            ).first() is not None
            return result

    def delete_project(self, project_id: str) -> None:
        self.get_project(project_id)
        with self.engine.begin() as connection:
            connection.execute(delete(projects).where(projects.c.id == project_id))

    def save_snapshot(
        self,
        project_id: str,
        kind: str,
        content: dict[str, Any],
        expected_revision: int | None = None,
    ) -> int:
        self.get_project(project_id)
        with self.engine.begin() as connection:
            current = self.latest_revision(connection, project_id, kind)
            if expected_revision is not None and expected_revision != current:
                raise ConflictError(
                    f"The {kind} changed since it was loaded",
                    {"expectedRevision": expected_revision, "currentRevision": current},
                )
            revision = current + 1
            timestamp = now()
            connection.execute(
                insert(snapshots).values(
                    project_id=project_id,
                    kind=kind,
                    revision=revision,
                    content=json.dumps(content, ensure_ascii=False),
                    created_at=timestamp,
                )
            )
            connection.execute(
                update(projects)
                .where(projects.c.id == project_id)
                .values(updated_at=timestamp, revision=projects.c.revision + 1)
            )
            self._audit(connection, project_id, f"{kind}.saved", {"revision": revision})
            return revision

    def latest_snapshot(self, project_id: str, kind: str) -> tuple[int, dict[str, Any]]:
        self.get_project(project_id)
        with self.engine.begin() as connection:
            row = connection.execute(
                select(snapshots)
                .where(snapshots.c.project_id == project_id, snapshots.c.kind == kind)
                .order_by(snapshots.c.revision.desc())
                .limit(1)
            ).first()
            if not row:
                raise NotFoundError(kind.capitalize())
            return row.revision, json.loads(row.content)

    @staticmethod
    def latest_revision(connection: Any, project_id: str, kind: str) -> int:
        row = connection.execute(
            select(snapshots.c.revision)
            .where(snapshots.c.project_id == project_id, snapshots.c.kind == kind)
            .order_by(snapshots.c.revision.desc())
            .limit(1)
        ).first()
        return int(row.revision) if row else 0

    def save_source(self, project_id: str, value: dict[str, Any]) -> dict[str, Any]:
        self.get_project(project_id)
        source_id = str(uuid.uuid4())
        record = {
            "id": source_id,
            "project_id": project_id,
            "created_at": now(),
            **value,
            "roles": json.dumps(value.get("roles", {})),
        }
        with self.engine.begin() as connection:
            connection.execute(delete(sources).where(sources.c.project_id == project_id))
            connection.execute(insert(sources).values(**record))
            connection.execute(update(projects).where(projects.c.id == project_id).values(updated_at=now()))
            self._audit(connection, project_id, "source.saved", {"filename": value["filename"]})
        record["roles"] = value.get("roles", {})
        return record

    def source(self, project_id: str) -> dict[str, Any]:
        self.get_project(project_id)
        with self.engine.begin() as connection:
            row = connection.execute(
                select(sources).where(sources.c.project_id == project_id).limit(1)
            ).first()
            if not row:
                raise NotFoundError("Source")
            result = dict(row._mapping)
            result["roles"] = json.loads(result["roles"])
            return result

    def update_profile(self, source_id: str, profile_path: str) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                update(sources).where(sources.c.id == source_id).values(profile_path=profile_path)
            )

    def create_job(self, project_id: str, converter: str) -> dict[str, Any]:
        self.get_project(project_id)
        timestamp = now()
        value = {
            "id": str(uuid.uuid4()),
            "project_id": project_id,
            "converter": converter,
            "status": "queued",
            "progress": 0,
            "report": None,
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        with self.engine.begin() as connection:
            connection.execute(insert(jobs).values(**value))
        return value

    def update_job(self, job_id: str, **values: Any) -> None:
        if "report" in values and not isinstance(values["report"], str):
            values["report"] = json.dumps(values["report"])
        values["updated_at"] = now()
        with self.engine.begin() as connection:
            connection.execute(update(jobs).where(jobs.c.id == job_id).values(**values))

    def interrupt_running_jobs(self) -> None:
        """A restarted server cannot retain credentials, so old jobs become retryable."""
        with self.engine.begin() as connection:
            connection.execute(
                update(jobs)
                .where(jobs.c.status.in_(["queued", "running", "cancelling"]))
                .values(status="retryable", updated_at=now())
            )

    def get_job(self, project_id: str, job_id: str) -> dict[str, Any]:
        with self.engine.begin() as connection:
            row = connection.execute(
                select(jobs).where(jobs.c.id == job_id, jobs.c.project_id == project_id)
            ).first()
            if not row:
                raise NotFoundError("Job")
            result = dict(row._mapping)
            if result["report"]:
                result["report"] = json.loads(result["report"])
            return result

    def save_artifact(
        self, project_id: str, job_id: str | None, kind: str, path: str, checksum: str
    ) -> dict[str, Any]:
        value = {
            "id": str(uuid.uuid4()), "project_id": project_id, "job_id": job_id,
            "kind": kind, "path": path, "sha256": checksum, "created_at": now(),
        }
        with self.engine.begin() as connection:
            connection.execute(insert(artifacts).values(**value))
            self._audit(connection, project_id, "artifact.created", {"kind": kind})
        return value

    def get_artifact(self, project_id: str, artifact_id: str) -> dict[str, Any]:
        with self.engine.begin() as connection:
            row = connection.execute(
                select(artifacts).where(
                    artifacts.c.id == artifact_id, artifacts.c.project_id == project_id
                )
            ).first()
            if not row:
                raise NotFoundError("Artifact")
            return dict(row._mapping)

    @staticmethod
    def _audit(connection: Any, project_id: str, event: str, details: dict[str, Any]) -> None:
        connection.execute(
            insert(audit_events).values(
                project_id=project_id,
                event=event,
                details=json.dumps(details),
                created_at=now(),
            )
        )
