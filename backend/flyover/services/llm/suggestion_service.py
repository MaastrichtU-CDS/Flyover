"""Job orchestration for LLM mapping suggestions.

Runs chunked Ollama matching as gevent background jobs so suggestions are
computed before and while the user works through the describe forms. Two
job phases exist: "variables" (CSV columns to semantic variables) and
"values" (local categorical values to semantic value-mapping terms).

Jobs are stored on the session cache (``session_cache.llm_jobs``) and are
polled by the frontend; chunk results become visible as soon as each chunk
completes. All entry points fail soft — LLM trouble must never break the
describe workflow.
"""

import hashlib
import json
import logging
import re
from typing import Any

import gevent

from services.llm.base import LLMProvider
from services.llm.config import LLMConfig

logger = logging.getLogger(__name__)

VARIABLES_PHASE = "variables"
VALUES_PHASE = "values"

_MAX_CONSECUTIVE_CHUNK_FAILURES = 3
_VALUE_CHUNK_MAX_ITEMS = 15

_DISPLAY_NAME_RE = re.compile(r'^(?P<global>.*) \(or "(?P<local>.*)"\)$')


class SuggestionJob:
    """State of one suggestion job, polled by the frontend.

    Attributes:
        phase: VARIABLES_PHASE or VALUES_PHASE.
        status: idle | pulling_model | running | done | failed | unavailable.
        generation: Incremented when the job is superseded; stale workers
            compare against it and exit without writing.
        fingerprint: Hash of the job inputs, for idempotent starts.
        pending: Ordered chunk descriptors still to process. Reordering this
            list is the priority mechanism.
        results: Suggestion entries by database (shape depends on phase).
        used_matches: Per-database matches already handed out, for
            cross-chunk first-wins deduplication in the variables phase.
        blueprints: Per (database, column) chunk templates used to rebuild
            single-item chunks for retries.
    """

    def __init__(self, phase: str, fingerprint: str):
        self.phase = phase
        self.fingerprint = fingerprint
        self.status = "idle"
        self.generation = 0
        self.pending: list[dict] = []
        self.results: dict[str, dict] = {}
        self.chunks_total = 0
        self.chunks_done = 0
        self.error: dict | None = None
        self.used_matches: dict[str, set] = {}
        self.blueprints: dict[tuple[str, str], dict] = {}
        self.model_in_use: str | None = None
        self.worker = None

    def to_public_dict(self) -> dict:
        """Return the JSON-safe snapshot served to the polling frontend."""
        suggestions: dict[str, dict] = {}
        for database, entries in self.results.items():
            suggestions[database] = {
                key: {k: v for k, v in entry.items() if not k.startswith("_")}
                for key, entry in entries.items()
            }
        return {
            "status": self.status,
            "model": self.model_in_use,
            "progress": {
                "chunks_done": self.chunks_done,
                "chunks_total": self.chunks_total,
            },
            "error": self.error,
            "suggestions": suggestions,
        }


def _preselected_for_database(
    mapping: Any, database: str, all_databases: list[str]
) -> tuple[set, set]:
    """Return the (columns, variable keys) already mapped for a store database.

    Mirrors the frontend's preselection rule: a mapping table applies to the
    store database whose name matches its sourceFile (with the shared
    delimited-containment fallback); the mapping database name is only used
    when the sourceFile matches no store database at all. Columns and
    variables that will be preselected in the UI are excluded from the LLM
    job — suggesting them again would fight the mapping.
    """
    from services.rdf_store_service import RDFStoreService

    match = RDFStoreService.graph_database_find_name_match
    columns: set = set()
    variables: set = set()
    for db in getattr(mapping, "databases", {}).values():
        for table in db.tables.values():
            source = getattr(table, "source_file", "")
            if source and any(match(source, d) for d in all_databases):
                applies = match(source, database)
            else:
                applies = match(db.name, database)
            if not applies:
                continue
            for column in table.columns.values():
                if column.local_column:
                    columns.add(column.local_column)
                variable_key = column.get_variable_key()
                if variable_key:
                    variables.add(variable_key)
    return columns, variables


def _unavailable_job(phase: str, reason: str) -> SuggestionJob:
    """Build a job representing 'nothing to suggest', with a reason."""
    job = SuggestionJob(phase, fingerprint="")
    job.status = "unavailable"
    job.error = {"kind": reason, "message": None}
    return job


class LLMSuggestionService:
    """Orchestrates chunked LLM matching jobs on the session cache."""

    def __init__(
        self,
        config: LLMConfig,
        client: LLMProvider,
        spawn: Any = None,
    ):
        """Create the service.

        Args:
            config: Feature configuration.
            client: Ollama client to run matching requests through.
            spawn: Callable used to launch workers; defaults to
                ``gevent.spawn``. Injectable so tests can run synchronously.
        """
        self.config = config
        self.client = client
        self._spawn = spawn or gevent.spawn

    # ------------------------------------------------------------------
    # Job starts
    # ------------------------------------------------------------------

    def start_variable_job(
        self, session_cache: Any, rdf_store_service: Any, force: bool = False
    ) -> dict:
        """Start (or reuse) the column-to-variable suggestion job.

        Args:
            session_cache: The application session cache.
            rdf_store_service: Service used to list columns per database.
            force: Rebuild the job even if inputs are unchanged.

        Returns:
            Status dict, e.g. ``{"status": "started"}``.
        """
        if not self.config.enabled:
            return {"status": "disabled"}
        jobs = self._jobs(session_cache)

        mapping = session_cache.jsonld_mapping
        if mapping is None:
            jobs[VARIABLES_PHASE] = _unavailable_job(
                VARIABLES_PHASE, "no_semantic_map"
            )
            return {"status": "unavailable", "reason": "no_semantic_map"}

        columns_by_db = rdf_store_service.get_column_info_by_database() or {}
        variable_keys = mapping.get_all_variable_keys()
        if not columns_by_db or not variable_keys:
            reason = "no_data" if not columns_by_db else "no_semantic_map"
            jobs[VARIABLES_PHASE] = _unavailable_job(VARIABLES_PHASE, reason)
            return {"status": "unavailable", "reason": reason}

        fingerprint = _fingerprint(
            {"columns": columns_by_db, "variables": sorted(variable_keys)},
            f"{self.config.provider}:{self.config.model}",
        )
        reuse = self._reusable_status(jobs.get(VARIABLES_PHASE), fingerprint, force)
        if reuse:
            return reuse

        job = SuggestionJob(VARIABLES_PHASE, fingerprint)
        for database, columns in columns_by_db.items():
            preselected_columns, preselected_variables = _preselected_for_database(
                mapping, database, list(columns_by_db)
            )
            candidates = [k for k in variable_keys if k not in preselected_variables]
            remaining = [c for c in columns if c not in preselected_columns]
            if not candidates or not remaining:
                continue

            job.results[database] = {c: {"status": "pending"} for c in remaining}
            for column in remaining:
                job.blueprints[(database, column)] = {
                    "database": database,
                    "items": [column],
                    "candidates": candidates,
                }
            for start in range(0, len(remaining), self.config.chunk_size):
                job.pending.append(
                    {
                        "database": database,
                        "items": remaining[start : start + self.config.chunk_size],
                        "candidates": candidates,
                    }
                )

        return self._launch(jobs, VARIABLES_PHASE, job)

    def start_value_job(self, session_cache: Any, force: bool = False) -> dict:
        """Start (or reuse) the categorical value-mapping suggestion job.

        Inputs are taken from ``session_cache.DescriptiveInfoDetails`` as
        built by the /units handler, so this is callable the moment that
        form is submitted.

        Args:
            session_cache: The application session cache.
            force: Rebuild the job even if inputs are unchanged.

        Returns:
            Status dict, e.g. ``{"status": "started"}``.
        """
        if not self.config.enabled:
            return {"status": "disabled"}
        jobs = self._jobs(session_cache)

        mapping = session_cache.jsonld_mapping
        details = session_cache.DescriptiveInfoDetails
        if mapping is None or not details:
            reason = "no_semantic_map" if mapping is None else "no_data"
            jobs[VALUES_PHASE] = _unavailable_job(VALUES_PHASE, reason)
            return {"status": "unavailable", "reason": reason}

        variable_lookup = {
            key.replace("_", " ").lower(): key
            for key in mapping.get_all_variable_keys()
        }

        targets = []
        for database, entries in details.items():
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                for display_name, rows in entry.items():
                    target = self._value_target(
                        mapping, variable_lookup, database, display_name, rows
                    )
                    if target:
                        targets.append(target)

        if not targets:
            jobs[VALUES_PHASE] = _unavailable_job(VALUES_PHASE, "nothing_to_suggest")
            return {"status": "unavailable", "reason": "nothing_to_suggest"}

        fingerprint = _fingerprint(
            {
                "targets": [
                    (t["database"], t["local_column"], t["items"]) for t in targets
                ]
            },
            f"{self.config.provider}:{self.config.model}",
        )
        reuse = self._reusable_status(jobs.get(VALUES_PHASE), fingerprint, force)
        if reuse:
            return reuse

        job = SuggestionJob(VALUES_PHASE, fingerprint)
        for target in targets:
            database, local_column = target["database"], target["local_column"]
            entry = {
                "status": "pending",
                "variable_key": target["variable_key"],
                "values": {},
                "_chunks_left": 0,
            }
            job.results.setdefault(database, {})[local_column] = entry
            job.blueprints[(database, local_column)] = target
            items = target["items"]
            for start in range(0, len(items), _VALUE_CHUNK_MAX_ITEMS):
                job.pending.append(
                    {
                        "database": database,
                        "local_column": local_column,
                        "items": items[start : start + _VALUE_CHUNK_MAX_ITEMS],
                        "candidates": target["candidates"],
                    }
                )
                entry["_chunks_left"] += 1

        return self._launch(jobs, VALUES_PHASE, job)

    # ------------------------------------------------------------------
    # Polling and priority
    # ------------------------------------------------------------------

    def get_state(self, session_cache: Any, phase: str) -> dict:
        """Return the polling snapshot for a phase (idle if no job)."""
        job = self._jobs(session_cache).get(phase)
        if job is None:
            return {
                "status": "idle",
                "model": None,
                "progress": {"chunks_done": 0, "chunks_total": 0},
                "error": None,
                "suggestions": {},
            }
        return job.to_public_dict()

    def bump_priority(
        self,
        session_cache: Any,
        phase: str,
        database: str,
        columns: list[str],
        retry: bool = False,
    ) -> dict:
        """Move chunks for the given columns to the front of the queue.

        Args:
            session_cache: The application session cache.
            phase: VARIABLES_PHASE or VALUES_PHASE.
            database: Database the columns belong to.
            columns: Column names (variables phase) or local variable names
                (values phase) to prioritise.
            retry: Re-enqueue fresh single-item chunks for already-processed
                columns, discarding their previous results.

        Returns:
            ``{"status": "ok", "moved": n}`` or ``{"status": "no_job"}``.
        """
        job = self._jobs(session_cache).get(phase)
        if job is None or job.status in ("unavailable",):
            return {"status": "no_job"}

        wanted = set(columns)
        moved = 0

        if retry:
            for column in columns:
                blueprint = job.blueprints.get((database, column))
                if blueprint is None:
                    continue
                self._discard_result(job, phase, database, column)
                if phase == VARIABLES_PHASE:
                    chunk = dict(blueprint)
                else:
                    chunk = {
                        "database": database,
                        "local_column": column,
                        "items": blueprint["items"],
                        "candidates": blueprint["candidates"],
                    }
                    job.results[database][column]["_chunks_left"] = 1
                job.pending.insert(0, chunk)
                job.chunks_total += 1
                moved += 1
        else:

            def _is_wanted(chunk: dict) -> bool:
                if chunk["database"] != database:
                    return False
                if phase == VALUES_PHASE:
                    return chunk["local_column"] in wanted
                return any(item in wanted for item in chunk["items"])

            front = [c for c in job.pending if _is_wanted(c)]
            back = [c for c in job.pending if not _is_wanted(c)]
            job.pending = front + back
            moved = len(front)

        if job.pending and job.status == "done":
            job.status = "running"
            job.worker = self._spawn(self._run_worker, job)

        return {"status": "ok", "moved": moved}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _jobs(session_cache: Any) -> dict:
        """Return the jobs dict on the session cache, creating it if needed."""
        if getattr(session_cache, "llm_jobs", None) is None:
            session_cache.llm_jobs = {VARIABLES_PHASE: None, VALUES_PHASE: None}
        return session_cache.llm_jobs

    @staticmethod
    def _reusable_status(
        existing: SuggestionJob | None, fingerprint: str, force: bool
    ) -> dict | None:
        """Return an 'already_*' status if the existing job still applies."""
        if force or existing is None or existing.fingerprint != fingerprint:
            if existing is not None:
                existing.generation += 1
            return None
        if existing.status in ("pulling_model", "running"):
            return {"status": "already_running"}
        if existing.status == "done":
            return {"status": "already_done"}
        return None

    def _launch(self, jobs: dict, phase: str, job: SuggestionJob) -> dict:
        """Store a built job and spawn its worker."""
        if not job.pending:
            jobs[phase] = _unavailable_job(phase, "nothing_to_suggest")
            return {"status": "unavailable", "reason": "nothing_to_suggest"}
        job.chunks_total = len(job.pending)
        jobs[phase] = job
        job.status = "pulling_model"
        job.worker = self._spawn(self._run_worker, job)
        return {"status": "started"}

    def _value_target(
        self,
        mapping: Any,
        variable_lookup: dict,
        database: str,
        display_name: str,
        rows: list,
    ) -> dict | None:
        """Build a values-phase target from one DescriptiveInfoDetails entry."""
        parsed = _DISPLAY_NAME_RE.match(display_name)
        if not parsed:
            return None
        global_name = parsed.group("global").strip()
        local_column = parsed.group("local")
        if global_name.lower() == "missing description":
            return None

        variable_key = variable_lookup.get(global_name.lower())
        if not variable_key:
            return None
        variable = mapping.get_variable(variable_key)
        if not variable or not variable.value_mappings:
            return None

        seen = set()
        items = []
        for row in rows:
            value = str(row.get("value", "")).strip()
            if value and value not in seen:
                seen.add(value)
                items.append(value)
        if not items:
            return None

        return {
            "database": database,
            "local_column": local_column,
            "variable_key": variable_key,
            "items": items,
            "candidates": list(variable.value_mappings.keys()),
        }

    def _discard_result(
        self, job: SuggestionJob, phase: str, database: str, column: str
    ) -> None:
        """Reset a column's result before a retry, releasing held matches."""
        entry = job.results.get(database, {}).get(column)
        if entry is None:
            return
        if phase == VARIABLES_PHASE:
            held = entry.get("variable_key")
            if held:
                job.used_matches.get(database, set()).discard(held)
            job.results[database][column] = {"status": "pending"}
        else:
            entry["status"] = "pending"
            entry["values"] = {}

    def _run_worker(self, job: SuggestionJob) -> None:
        """Process a job's chunk queue until empty, failed, or superseded."""
        generation = job.generation
        try:
            model = self.client.ensure_ready()
        except Exception as exc:
            if job.generation == generation:
                job.status = "failed"
                job.error = {"kind": "llm_unavailable", "message": str(exc)}
            logger.warning("LLM suggestion job failed to obtain a model: %s", exc)
            return

        if job.generation != generation:
            return
        job.model_in_use = model
        job.status = "running"
        consecutive_failures = 0

        while job.generation == generation and job.pending:
            chunk = job.pending.pop(0)
            try:
                pairs = self.client.match_equivalents(
                    chunk["items"], chunk["candidates"]
                )
            except Exception as exc:
                if job.generation != generation:
                    return
                consecutive_failures += 1
                logger.warning(
                    "LLM chunk failed (%s consecutive): %s", consecutive_failures, exc
                )
                self._record_chunk_error(job, chunk)
                job.chunks_done += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_CHUNK_FAILURES:
                    job.status = "failed"
                    job.error = {"kind": "chunk_failures", "message": str(exc)}
                    return
                continue

            if job.generation != generation:
                return
            consecutive_failures = 0
            self._record_chunk_results(job, chunk, pairs)
            job.chunks_done += 1

        if job.generation == generation:
            job.status = "done"

    def _record_chunk_results(
        self, job: SuggestionJob, chunk: dict, pairs: list[dict]
    ) -> None:
        """Write one chunk's sanitised pairs into the job results."""
        database = chunk["database"]
        if job.phase == VARIABLES_PHASE:
            used = job.used_matches.setdefault(database, set())
            for pair in pairs:
                match = pair["match"]
                reason = pair["reason"]
                confidence = pair["confidence"]
                if match is not None and match in used:
                    reason = (
                        f"{reason} | Note: candidate already suggested for "
                        "another column."
                    ).strip()
                    match = None
                    confidence = 0.0
                elif match is not None:
                    used.add(match)
                job.results[database][pair["item"]] = {
                    "status": "done",
                    "variable_key": match,
                    "confidence": confidence,
                    "reason": reason,
                }
        else:
            entry = job.results[database][chunk["local_column"]]
            for pair in pairs:
                entry["values"][pair["item"]] = {
                    "term_key": pair["match"],
                    "confidence": pair["confidence"],
                    "reason": pair["reason"],
                }
            entry["_chunks_left"] -= 1
            if entry["_chunks_left"] <= 0:
                entry["status"] = "done"

    def _record_chunk_error(self, job: SuggestionJob, chunk: dict) -> None:
        """Mark all of a failed chunk's items as errored."""
        database = chunk["database"]
        if job.phase == VARIABLES_PHASE:
            for item in chunk["items"]:
                job.results[database][item] = {"status": "error"}
        else:
            entry = job.results[database][chunk["local_column"]]
            entry["_chunks_left"] -= 1
            if entry["_chunks_left"] <= 0:
                entry["status"] = "error"


def _fingerprint(inputs: dict, model: str) -> str:
    """Hash job inputs + model for idempotent job starts."""
    payload = json.dumps({"inputs": inputs, "model": model}, sort_keys=True)
    return hashlib.sha1(payload.encode()).hexdigest()
