"""
Prompt export and paste-back: the copy-prompt / paste-answer round trip.

A user whose site cannot run a model next to Flyover generates a prompt
here, pastes it into an LLM they may use, and pastes the answer back;
the answer becomes ordinary ``pasted_llm`` suggestion records in the
phase's job. The prompt itself is composed by :mod:`.prompt_export` and
the pasted text is parsed by :mod:`.pasted_answer`; this module is the
service side: request validation, the gate every pasted record passes
(unknown items rejected, invalid keys nulled, already-mapped items
skipped), the merge into the job and the re-apply after a rebuild.

The methods are a mixin of :class:`services.suggestions.SuggestionService`
so the service class stays in one place while this feature lives in its
own module. The mixin relies on the host for ``_jobs``, ``get_state`` and
``_downgrade_conflicts``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Optional

from services.rdf_store_service import RDFStoreService

from .contract import sanitise_pairs
from .jobs import (
    PHASES,
    VALUES_PHASE,
    VARIABLES_PHASE,
    SuggestionJob,
    _merge_records,
    _parse_category_values,
)
from .pasted_answer import AnswerParseError, parse_answer_text, records_from_answer
from .prompt_export import (
    DEFAULT_PASTED_CONFIDENCE,
    PASTED_SOURCE,
    PASTED_TIER,
    PromptExport,
    local_mappings,
    mapped_columns,
)

logger = logging.getLogger(__name__)


class SuggestionRequestError(ValueError):
    """A prompt/ingest request the service cannot serve; ``kind`` is stable.

    Raised for caller mistakes (unknown database, no semantic map, no JSON
    in the pasted text). The controller maps it to a 400 with ``kind`` and
    the readable ``message``.
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message


def _replace_record(prev: dict, record: dict) -> dict:
    """``record`` takes the key; ``prev`` is kept as an alternative.

    Used when the user dismissed ``prev`` and pasted an answer: unlike
    :func:`_merge_records` the confidences do not decide. ``prev`` only
    becomes an alternative when it proposes a different non-null match
    (an abstain or the same proposal is noise), and ``prev``'s own
    alternatives are carried over minus any duplicate of the new match.
    """
    winner = dict(record)
    alts = [
        alt
        for alt in prev.get("alternatives", [])
        if alt.get("match") and alt.get("match") != winner.get("match")
    ]
    prev_copy = {k: v for k, v in prev.items() if k != "alternatives"}
    if prev_copy.get("match") and prev_copy["match"] != winner.get("match"):
        alts.insert(0, prev_copy)
    if alts:
        winner["alternatives"] = alts + [
            alt
            for alt in winner.get("alternatives", [])
            if alt.get("match") and alt not in alts
        ]
    return winner


class PasteRoundTripMixin:
    """Prompt export and paste-back methods of ``SuggestionService``."""

    def build_prompt(
        self,
        phase: str,
        session_cache: Any,
        rdf_store_service: Any,
        *,
        database: str,
        mapping: Any = None,
        mapping_data: Optional[dict] = None,
        chunk: Optional[int] = None,
        include: Optional[list[str]] = None,
    ) -> dict:
        """Compose the copy-prompt payload for one database and phase.

        Works whether or not any tier is enabled: the prompt needs the
        semantic map, the store's column names and (values phase) distinct
        values, and (when a job exists) the job's records as hints. The
        variables phase shares no distinct values at all; in the values
        phase a column whose values look like free text is held back
        unless named in ``include``. Raises
        :class:`SuggestionRequestError` for an unknown phase or database
        or a missing semantic map.
        """
        if phase not in PHASES:
            raise SuggestionRequestError("unknown_phase", f"unknown phase '{phase}'")
        if mapping is None:
            mapping = getattr(session_cache, "jsonld_mapping", None)
        if mapping is None:
            raise SuggestionRequestError(
                "no_semantic_map", "Upload a semantic map before generating a prompt."
            )
        columns = self._columns_of(rdf_store_service, database)
        records = self.get_state(session_cache, phase).get("records", {})

        def distinct_values(column: str) -> list[str]:
            try:
                return _parse_category_values(
                    rdf_store_service.get_categories(column, database)
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "get_categories(%s, %s) failed: %s", column, database, exc
                )
                return []

        export = PromptExport(
            phase,
            database,
            mapping,
            columns=columns,
            distinct_values=distinct_values if rdf_store_service is not None else None,
            records=records,
            mapping_data=mapping_data,
            chunk_size=chunk,
            include=include,
            name_match=RDFStoreService.graph_database_find_name_match,
        )
        return export.build()

    def ingest(
        self,
        phase: str,
        session_cache: Any,
        rdf_store_service: Any,
        *,
        database: str,
        answer: Any = None,
        records: Optional[list] = None,
        mapping: Any = None,
        source: str = PASTED_SOURCE,
        dismissed: Optional[list] = None,
    ) -> dict:
        """Merge a pasted LLM answer into the phase's job as suggestions.

        ``answer`` is the raw pasted text (or already-parsed JSON); the
        alternative ``records`` is the flat record list. Every record goes
        through the same gate as a tier producer's output: unknown items
        are rejected, matches that are not exact schema keys are nulled
        (reason prefixed ``[invalid key from LLM]``), a variable already
        mapped in the JSON-LD may not be reused, items that are already
        mapped are skipped, confidence is clamped and duplicates keep the
        highest confidence. Records are stamped ``source``/tier 3 and
        merged by the cascade rule (highest confidence wins). Nothing is
        written to the JSON-LD and the browser's review marks are never
        touched; the public fingerprint changes so stale marks expire per
        key.

        ``dismissed`` lists the job keys (``<db>_<column>`` /
        ``<db>_<column>_<value>``) whose current suggestion the user
        dismissed in the browser. A dismissal judges one suggestion, not
        the field: pasting an answer is the user's explicit request for a
        new one, so for those keys a pasted record with a match replaces
        the dismissed record outright instead of competing with it on
        confidence (the dismissed record is kept as an alternative). Those
        keys come back as ``reopened`` so the browser can drop the marks.
        Fields the JSON-LD already maps are skipped before this applies.

        Returns ``{accepted, nulled, rejected, skipped, reopened,
        messages, job}``.
        """
        if phase not in PHASES:
            raise SuggestionRequestError("unknown_phase", f"unknown phase '{phase}'")
        if source not in (PASTED_SOURCE, "manual"):
            raise SuggestionRequestError("bad_source", f"unsupported source '{source}'")
        if mapping is None:
            mapping = getattr(session_cache, "jsonld_mapping", None)
        if mapping is None:
            raise SuggestionRequestError(
                "no_semantic_map", "Upload a semantic map before importing an answer."
            )
        columns = self._columns_of(rdf_store_service, database)

        if answer is not None:
            try:
                data = parse_answer_text(answer)
            except AnswerParseError as exc:
                raise SuggestionRequestError("bad_answer", str(exc)) from exc
        else:
            data = records or []
        try:
            raw = records_from_answer(
                phase, data, default_confidence=DEFAULT_PASTED_CONFIDENCE
            )
        except ValueError as exc:  # pragma: no cover - phase checked above
            raise SuggestionRequestError("bad_answer", str(exc)) from exc
        if not raw:
            raise SuggestionRequestError(
                "bad_answer",
                "The pasted answer holds no column entries (no mapsTo/localColumn"
                + (" with localMappings" if phase == VALUES_PHASE else "")
                + ").",
            )

        name_match = RDFStoreService.graph_database_find_name_match
        mapped = mapped_columns(mapping, database, name_match)
        messages: list[str] = []
        counts = {"accepted": 0, "nulled": 0, "rejected": 0, "skipped": 0}

        if phase == VARIABLES_PHASE:
            groups = self._gate_variables(
                raw, columns, mapped, mapping, messages, counts
            )
        else:
            groups = self._gate_values(
                raw,
                columns,
                mapped,
                mapping,
                database,
                rdf_store_service,
                name_match,
                messages,
                counts,
            )

        # Sanitise per group exactly like a producer's output; only the
        # answered items are passed, so no abstain filler is generated.
        sanitised: dict[str, dict] = {}
        for group in groups:
            items = [r["item"] for r in group["records"]]
            for record in sanitise_pairs(
                group["records"],
                items=items,
                valid_targets=group["targets"],
                source=source,
                tier=PASTED_TIER,
            ):
                record.update(group["location"](record["item"]))
                record["database"] = database
                sanitised[group["key_for"](record["item"])] = record
        dismissed_keys = {str(k) for k in (dismissed or [])}
        reopened: list[str] = []
        for key, record in sanitised.items():
            if record["match"] is not None:
                counts["accepted"] += 1
                full_key = f"{database}_{key}"
                if full_key in dismissed_keys:
                    record["reopens"] = True
                    reopened.append(full_key)
            else:
                counts["nulled"] += 1

        job = self._merge_ingested(session_cache, phase, sanitised)
        return {
            **counts,
            "reopened": reopened,
            "messages": messages,
            "job": job.to_public_dict(),
        }

    # -- ingest helpers -------------------------------------------------------

    @staticmethod
    def _columns_of(rdf_store_service: Any, database: str) -> list[str]:
        columns_by_db: dict = {}
        if rdf_store_service is not None:
            try:
                columns_by_db = rdf_store_service.get_column_info_by_database() or {}
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("get_column_info_by_database failed: %s", exc)
        if not database or database not in columns_by_db:
            known = ", ".join(sorted(columns_by_db)) or "none"
            raise SuggestionRequestError(
                "unknown_database",
                f"unknown database '{database}' (known: {known})",
            )
        return list(columns_by_db[database] or [])

    @staticmethod
    def _gate_variables(raw, columns, mapped, mapping, messages, counts) -> list[dict]:
        column_set = set(columns)
        targets = set(mapping.get_all_variable_keys())
        taken = {var: col for col, var in mapped.items()}
        kept: list[dict] = []
        for record in raw:
            item = record["item"]
            if item not in column_set:
                counts["rejected"] += 1
                messages.append(f"'{item}' is not a column of this database; ignored.")
                continue
            if item in mapped:
                counts["skipped"] += 1
                messages.append(
                    f"'{item}' is already mapped to {mapped[item]}; left unchanged."
                )
                continue
            match = record.get("match")
            if match is not None and match in taken:
                record = {
                    **record,
                    "match": None,
                    "confidence": 0.0,
                    "reason": (
                        f"[already mapped] {match} is mapped to column "
                        f"'{taken[match]}' in this database"
                        + (f" | {record['reason']}" if record.get("reason") else "")
                    ),
                }
            elif match is not None and match not in targets:
                record = {
                    **record,
                    "match": None,
                    "confidence": 0.0,
                    "reason": (
                        f"[invalid key from LLM] '{match}' is not a schema variable"
                        + (f" | {record['reason']}" if record.get("reason") else "")
                    ),
                }
            kept.append(record)
        if not kept:
            return []
        return [
            {
                "records": kept,
                "targets": targets,
                "key_for": lambda item: item,
                "location": lambda item: {"column": item},
            }
        ]

    @staticmethod
    def _gate_values(
        raw,
        columns,
        mapped,
        mapping,
        database,
        rdf_store_service,
        name_match,
        messages,
        counts,
    ) -> list[dict]:
        column_set = set(columns)
        by_column: dict[str, list[dict]] = {}
        for record in raw:
            column = record.get("column")
            if not column:
                counts["rejected"] += 1
                messages.append(
                    f"value '{record['item']}' names no column (localColumn); ignored."
                )
                continue
            if column not in column_set:
                counts["rejected"] += 1
                messages.append(
                    f"'{column}' is not a column of this database; ignored."
                )
                continue
            if column not in mapped:
                counts["rejected"] += 1
                messages.append(
                    f"'{column}' is not mapped to a variable yet; map it on the "
                    "variables page first."
                )
                continue
            by_column.setdefault(column, []).append(record)

        groups: list[dict] = []
        for column, records in by_column.items():
            var_key = mapped[column]
            variable = mapping.get_variable(var_key)
            terms = set((getattr(variable, "value_mappings", None) or {}).keys())
            if not terms:
                counts["rejected"] += len(records)
                messages.append(
                    f"{var_key} ('{column}') has no terms to map values to."
                )
                continue
            try:
                distinct = set(
                    _parse_category_values(
                        rdf_store_service.get_categories(column, database)
                    )
                )
            except Exception:  # pragma: no cover - defensive
                distinct = set()
            already = {
                v
                for vals in local_mappings(
                    mapping, database, column, name_match
                ).values()
                for v in vals
            }
            kept: list[dict] = []
            for record in records:
                item = record["item"]
                if distinct and item not in distinct:
                    counts["rejected"] += 1
                    messages.append(f"'{item}' is not a value of '{column}'; ignored.")
                    continue
                if item in already:
                    counts["skipped"] += 1
                    messages.append(
                        f"'{column}' = '{item}' is already mapped; left unchanged."
                    )
                    continue
                match = record.get("match")
                if match is not None and match not in terms:
                    record = {
                        **record,
                        "match": None,
                        "confidence": 0.0,
                        "reason": (
                            f"[invalid key from LLM] '{match}' is not a term of {var_key}"
                            + (f" | {record['reason']}" if record.get("reason") else "")
                        ),
                    }
                kept.append(record)
            if kept:
                groups.append(
                    {
                        "records": kept,
                        "targets": terms,
                        "key_for": lambda item, c=column: f"{c}_{item}",
                        "location": lambda item, c=column: {"column": c, "value": item},
                    }
                )
        return groups

    def _merge_ingested(
        self, session_cache: Any, phase: str, sanitised: dict
    ) -> "SuggestionJob":
        """Store the records on the session and merge them into the job."""
        store = self._ingested(session_cache)[phase]
        for key, record in sanitised.items():
            store[key] = record
        jobs = self._jobs(session_cache)
        job = jobs.get(phase)
        if job is None or job.status in ("unavailable", "failed"):
            job = SuggestionJob(phase, job.fingerprint if job else "")
            job.status = "done"
            jobs[phase] = job
        self._apply_ingested(job, store, sanitised)
        return job

    def _reapply_ingested(self, session_cache: Any, job: "SuggestionJob") -> None:
        store = self._ingested(session_cache).get(job.phase) or {}
        if store:
            self._apply_ingested(job, store, store)

    def _apply_ingested(self, job: "SuggestionJob", store: dict, records: dict) -> None:
        # The job keys are database-prefixed; the ingest keys are built
        # relative to the database, so prefix them here.
        for key, record in records.items():
            record = dict(record)
            record["status"] = "done"
            full_key = f"{record['database']}_{key}" if record.get("database") else key
            prev = job.records.get(full_key)
            if prev is None:
                job.records[full_key] = record
            elif record.get("reopens"):
                # The user dismissed ``prev`` and asked an LLM instead: the
                # paste takes the field whatever the confidences, and the
                # dismissed candidate stays one click away. The flag lives
                # on the stored record, so a rebuild repeats this.
                job.records[full_key] = _replace_record(prev, record)
            else:
                merged, _loser = _merge_records(prev, record)
                job.records[full_key] = merged
        if job.phase == VARIABLES_PHASE:
            databases = {r.get("database") for r in records.values()}
            for database in databases:
                best = {
                    r["column"]: r
                    for r in job.records.values()
                    if r.get("database") == database and r.get("column")
                }
                self._downgrade_conflicts(best)
        job.progress = {
            "done": len(job.records),
            "total": max(job.progress.get("total", 0), len(job.records)),
        }
        job.ingested_fingerprint = hashlib.sha256(
            json.dumps(
                sorted((k, r.get("match")) for k, r in store.items()), default=str
            ).encode()
        ).hexdigest()[:16]

    @staticmethod
    def _ingested(session_cache: Any) -> dict:
        if not isinstance(getattr(session_cache, "suggestion_ingested", None), dict):
            session_cache.suggestion_ingested = {p: {} for p in PHASES}
        return session_cache.suggestion_ingested
