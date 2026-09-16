"""
Suggestion service: job lifecycle, cascade, merge, and env configuration.

This package is the backend home of tiered mapping suggestions. The service
runs enabled tier producers in cascade order (1 -> 2 -> 3), escalates only
items whose best record so far is an abstain or below threshold, merges per
key by highest confidence (ties broken by lower tier), and never overwrites
user marks. It is the rule-based, provider-free core cherry-picked from the
LLM branch's ``suggestion_service.py`` with the provider dependency dropped.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Optional

from .contract import sanitise_pairs
from .tiers import SuggestionContext
from .tiers.rules import _iter_columns, load_rules, tier1_producers

logger = logging.getLogger(__name__)

VARIABLES_PHASE = "variables"
VALUES_PHASE = "values"
PHASES = (VARIABLES_PHASE, VALUES_PHASE)

DEFAULT_THRESHOLD = 0.8
DEFAULT_MARGIN = 0.05
DEFAULT_COMPUTE = "host"


def _env_tiers() -> list[int]:
    """Parse ``FLYOVER_SUGGESTION_TIERS`` into a sorted list of tier numbers.

    Empty/unset disables the feature (returns ``[]``). Malformed entries are
    dropped; unknown tiers are ignored.
    """
    raw = os.getenv("FLYOVER_SUGGESTION_TIERS", "1")
    if raw is None or raw.strip() == "":
        return []
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            n = int(part)
        except ValueError:
            continue
        if n in (1, 2, 3) and n not in out:
            out.append(n)
    return sorted(out)


def _env_compute() -> str:
    return os.getenv("FLYOVER_SUGGESTION_COMPUTE", DEFAULT_COMPUTE)


def _env_threshold() -> float:
    try:
        return float(os.getenv("FLYOVER_SUGGESTION_THRESHOLD", str(DEFAULT_THRESHOLD)))
    except (TypeError, ValueError):
        return DEFAULT_THRESHOLD


class SuggestionConfig:
    """Effective feature configuration parsed from the environment."""

    def __init__(self) -> None:
        self.tiers: list[int] = _env_tiers()
        self.compute: str = _env_compute()
        self.threshold: float = _env_threshold()
        self.margin: float = DEFAULT_MARGIN

    @property
    def enabled(self) -> bool:
        return bool(self.tiers)

    def tier_state(self, tier: int) -> dict:
        """Return ``{state, reason}`` for one tier for the ``/status`` endpoint."""
        if tier in self.tiers:
            return {"state": "active"}
        if not self.tiers:
            return {"state": "inactive", "reason": "disabled by FLYOVER_SUGGESTION_TIERS"}
        if tier == 2:
            return {"state": "inactive", "reason": "not enabled in FLYOVER_SUGGESTION_TIERS"}
        if tier == 3:
            return {"state": "inactive", "reason": "not enabled in FLYOVER_SUGGESTION_TIERS"}
        return {"state": "inactive", "reason": "unknown tier"}


def _fingerprint(payload: dict) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def _merge_records(a: dict, b: dict) -> tuple[dict, Optional[dict]]:
    """Merge two records for the same item; return (winner, loser_or_None).

    Highest confidence wins; ties go to the lower tier (cheaper). The loser is
    kept (without ``alternatives``) so the UI can offer it in a popover.
    """
    if a.get("confidence") == b.get("confidence"):
        if a.get("tier", 99) <= b.get("tier", 99):
            winner, loser = a, b
        else:
            winner, loser = b, a
    elif a.get("confidence", 0.0) > b.get("confidence", 0.0):
        winner, loser = a, b
    else:
        winner, loser = b, a

    loser_copy = {k: v for k, v in loser.items() if k != "alternatives"}
    alts = list(winner.get("alternatives", []))
    alts.append(loser_copy)
    winner = dict(winner)
    winner["alternatives"] = alts
    return winner, loser_copy


class SuggestionJob:
    """State of one suggestion job, polled by the frontend.

    Records are stored as ``item -> record dict`` keyed by the composite
    phase key (``${db}_${column}`` or ``${db}_${column}_${value}``).
    """

    def __init__(self, phase: str, fingerprint: str) -> None:
        self.phase = phase
        self.fingerprint = fingerprint
        self.status = "pending"
        self.records: dict[str, dict] = {}
        self.progress = {"done": 0, "total": 0}
        self.error: Optional[dict] = None

    def to_public_dict(self) -> dict:
        return {
            "status": self.status,
            "progress": self.progress,
            "error": self.error,
            "records": dict(self.records),
        }


class SuggestionService:
    """Orchestrate tier producers into a single job per phase.

    The service is constructed once per app and reads env vars for which
    tiers to run. For tier 1 it runs synchronously (milliseconds); later
    tiers may add gevent workers but the public shape stays identical.
    """

    def __init__(self, config: Optional[SuggestionConfig] = None) -> None:
        self.config = config or SuggestionConfig()
        self._rules = load_rules() if self.config.enabled else None

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return the ``/status`` payload."""
        tiers = {}
        for tier in (1, 2, 3):
            tiers[tier] = self.config.tier_state(tier)
        return {
            "compute": self.config.compute,
            "tiers": tiers,
            "threshold": self.config.threshold,
            "rules_version": (self._rules or {}).get("version") if self._rules else None,
        }

    # ------------------------------------------------------------------
    # Job starts
    # ------------------------------------------------------------------

    def start(
        self,
        phase: str,
        session_cache: Any,
        rdf_store_service: Any,
        force: bool = False,
    ) -> dict:
        """Build (or reuse) the job for ``phase`` and run the enabled tiers.

        Returns a status dict (``{"status": ...}``) mirroring the LLM branch.
        """
        if phase not in PHASES:
            return {"status": "error", "reason": "unknown_phase"}
        if not self.config.enabled:
            return {"status": "disabled"}

        jobs = self._jobs(session_cache)
        existing = jobs.get(phase)

        mapping = getattr(session_cache, "jsonld_mapping", None)
        if mapping is None:
            job = SuggestionJob(phase, "")
            job.status = "unavailable"
            job.error = {"kind": "no_semantic_map"}
            jobs[phase] = job
            return {"status": "unavailable", "reason": "no_semantic_map"}

        if phase == VARIABLES_PHASE:
            payload = self._build_variables_payload(mapping, rdf_store_service)
        else:
            payload = self._build_values_payload(
                mapping, session_cache, rdf_store_service
            )

        if not payload["items"]:
            job = SuggestionJob(phase, "")
            job.status = "unavailable"
            job.error = {"kind": "nothing_to_suggest"}
            jobs[phase] = job
            return {"status": "unavailable", "reason": "nothing_to_suggest"}

        fp = _fingerprint(self._fingerprint_payload(phase, payload, mapping))
        if not force and existing is not None and existing.fingerprint == fp:
            if existing.status == "done":
                return {"status": "already_done"}
            if existing.status in ("running", "pending"):
                return {"status": "already_running"}

        job = SuggestionJob(phase, fp)
        job.progress = {"done": 0, "total": len(payload["items"])}
        jobs[phase] = job

        # Only tier 1 is wired; tiers 2/3 will add their producers here.
        producers = self._producers_for_enabled_tiers()
        if not producers:
            job.status = "unavailable"
            job.error = {"kind": "no_tiers_enabled"}
            return {"status": "unavailable", "reason": "no_tiers_enabled"}

        try:
            self._run_cascade(job, producers, payload)
            job.status = "done"
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Suggestion job failed: %s", exc)
            job.status = "failed"
            job.error = {"kind": "job_failed", "message": str(exc)}
        return {"status": "started"}

    @staticmethod
    def _fingerprint_payload(phase: str, payload: dict, mapping: Any) -> dict:
        """Build a JSON-serialisable subset of the payload for fingerprinting."""
        if phase == VARIABLES_PHASE:
            return {
                "phase": phase,
                "items": sorted(payload["items"]),
                "variables": sorted(
                    mapping.get_all_variable_keys() if mapping else []
                ),
            }
        groups = payload.get("groups", [])
        return {
            "phase": phase,
            "groups": [
                {
                    "database": g["key_for"]("").rsplit("_", 1)[0],
                    "items": g["items"],
                    "terms": sorted(
                        {t for terms in g["schema_slice"].values() for t in terms}
                    ),
                }
                for g in groups
            ],
        }

    def _producers_for_enabled_tiers(self) -> list:
        producers: list = []
        if 1 in self.config.tiers:
            producers.extend(tier1_producers())
        # Tiers 2/3 are reserved for later issues; they register here.
        return producers

    def _run_cascade(self, job: SuggestionJob, producers: list, payload: dict) -> None:
        """Run producers in cascade order, escalating abstains/low-confidence.

        The variables phase runs one group (all columns against all
        variables). The values phase runs one group per (database,
        local_column) because the same value string can map to different
        terms depending on its column's variable.
        """
        phase = job.phase
        groups = payload.get("groups")
        if groups is None:
            groups = [{
                "items": payload["items"],
                "schema_slice": payload["schema_slice"],
                "key_for": payload["key_for"],
            }]

        for group in groups:
            self._run_group(job, producers, payload, group)
        job.progress = {"done": len(job.records), "total": sum(
            len(g["items"]) for g in groups
        )}

    def _run_group(
        self,
        job: SuggestionJob,
        producers: list,
        payload: dict,
        group: dict,
    ) -> None:
        phase = job.phase
        items = group["items"]
        schema_slice = group["schema_slice"]
        key_for = group["key_for"]
        if phase == VARIABLES_PHASE:
            targets = set(schema_slice.get("*", []))
        else:
            targets = _all_targets(schema_slice)

        best: dict[str, dict] = {item: None for item in items}
        for producer in producers:
            tier = getattr(producer, "tier", 1)
            source = getattr(producer, "source", "manual")
            to_run = [
                item for item in items
                if best.get(item) is None
                or (best[item].get("confidence", 0.0) < self.config.threshold)
            ]
            if not to_run:
                continue
            ctx = SuggestionContext(
                phase=phase,
                mapping=payload["mapping"],
                described_database=payload.get("described_database"),
                threshold=self.config.threshold,
                margin=self.config.margin,
                rules=self._rules,
                column_values=payload.get("column_values", {}),
                value_targets=payload.get("value_targets", {}),
            )
            raw = producer.run(list(to_run), schema_slice, ctx)
            sanitised = sanitise_pairs(
                raw,
                items=to_run,
                valid_targets=targets,
                source=source,
                tier=tier,
            )
            for record in sanitised:
                item = record["item"]
                prev = best.get(item)
                if prev is None:
                    best[item] = record
                else:
                    merged, _loser = _merge_records(prev, record)
                    best[item] = merged

        if phase == VARIABLES_PHASE:
            self._downgrade_conflicts(best)

        for item, record in best.items():
            key = key_for(item)
            if record is None:
                job.records[key] = {
                    "item": item,
                    "match": None,
                    "confidence": 0.0,
                    "reason": "No suggestion produced.",
                    "source": "manual",
                    "tier": 1,
                    "status": "done",
                }
            else:
                record["status"] = "done"
                job.records[key] = record

    @staticmethod
    def _downgrade_conflicts(best: dict) -> None:
        """Downgrade variables-phase duplicate matches to alternatives."""
        by_match: dict[str, list[str]] = {}
        for item, record in best.items():
            if record is None:
                continue
            match = record.get("match")
            if match:
                by_match.setdefault(match, []).append(item)
        for match, items in by_match.items():
            if len(items) <= 1:
                continue
            for item in items:
                record = best[item]
                record["match"] = None
                record["confidence"] = 0.0
                record["reason"] = f"conflict: {len(items)} columns mapped to {match}"
                record["status"] = "done"

    # ------------------------------------------------------------------
    # Payload builders
    # ------------------------------------------------------------------

    def _build_variables_payload(
        self, mapping: Any, rdf_store_service: Any
    ) -> dict:
        columns_by_db = {}
        if rdf_store_service is not None:
            try:
                columns_by_db = rdf_store_service.get_column_info_by_database() or {}
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("get_column_info_by_database failed: %s", exc)
        variable_keys = list(mapping.get_all_variable_keys()) if mapping else []
        items: list[str] = []
        for cols in columns_by_db.values():
            for col in cols or []:
                if col not in items:
                    items.append(col)

        described_db = next(iter(columns_by_db), None)

        def key_for(item: str) -> str:
            return f"{described_db}_{item}" if described_db else item

        return {
            "items": items,
            "schema_slice": {"*": variable_keys},
            "mapping": mapping,
            "described_database": described_db,
            "column_values": self._collect_column_values(
                mapping, rdf_store_service, columns_by_db
            ),
            "groups": [{
                "items": items,
                "schema_slice": {"*": variable_keys},
                "key_for": key_for,
            }],
            "key_for": key_for,
        }

    def _collect_column_values(
        self, mapping: Any, rdf_store_service: Any, columns_by_db: dict
    ) -> dict:
        """Gather distinct categorical values per database -> column."""
        out: dict[str, dict[str, list[str]]] = {}
        if rdf_store_service is None:
            return out
        for db, cols in columns_by_db.items():
            out[db] = {}
            for col in cols or []:
                try:
                    cats = rdf_store_service.get_categories(col, db)
                except Exception:  # pragma: no cover - defensive
                    cats = None
                values: list[str] = []
                if cats:
                    try:
                        import polars as pl
                        from io import StringIO

                        df = pl.read_csv(
                            StringIO(cats),
                            separator=",",
                            infer_schema_length=0,
                            null_values=[],
                            try_parse_dates=False,
                        )
                        for row in df.to_dicts():
                            v = row.get("value")
                            if v is not None and str(v) not in values:
                                values.append(str(v))
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.debug("categories parse failed for %s: %s", col, exc)
                out[db][col] = values
        return out

    def _build_values_payload(
        self, mapping: Any, session_cache: Any, rdf_store_service: Any = None
    ) -> dict:
        """Build the values-phase payload.

        Primary source: ``DescriptiveInfoDetails`` on the session cache
        (populated by the describe controller when the user submits the
        variables form).  Fallback when that is empty (e.g. when the
        database-name match fails and ``_populate_details_from_jsonld``
        bails out): build groups directly from the mapping + RDF store
        by querying ``get_categories`` for each categorical column.
        """
        details = getattr(session_cache, "DescriptiveInfoDetails", None) or {}
        variable_lookup = {}
        if mapping is not None:
            for key in mapping.get_all_variable_keys():
                variable_lookup[key.replace("_", " ").lower()] = key

        all_items: list[str] = []
        groups: list[dict] = []
        value_targets: dict = {}
        import re as _re

        display_re = _re.compile(r'^(?P<global>.*) \(or "(?P<local>.*)"\)$')

        for database, entries in details.items():
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                for display_name, rows in entry.items():
                    parsed = display_re.match(display_name)
                    if not parsed:
                        continue
                    global_name = parsed.group("global").strip()
                    local_column = parsed.group("local")
                    if global_name.lower() == "missing description":
                        continue
                    variable_key = variable_lookup.get(global_name.lower())
                    if not variable_key:
                        continue
                    variable = mapping.get_variable(variable_key) if mapping else None
                    if not variable or not variable.value_mappings:
                        continue
                    terms = list(variable.value_mappings.keys())
                    seen: set[str] = set()
                    group_items: list[str] = []
                    for row in rows or []:
                        value = str((row or {}).get("value", "")).strip()
                        if not value or value in seen:
                            continue
                        seen.add(value)
                        group_items.append(value)
                        all_items.append(value)
                        value_targets.setdefault(database, {}).setdefault(
                            variable_key, []
                        ).append(value)
                    if not group_items:
                        continue

                    def make_key_for(db=database, col=local_column):
                        def key_for(value: str) -> str:
                            return f"{db}_{col}_{value}"
                        return key_for

                    groups.append({
                        "items": group_items,
                        "schema_slice": {value: terms for value in group_items},
                        "key_for": make_key_for(),
                    })

        described_db = next(iter(details), None)

        # Fallback: when DescriptiveInfoDetails is empty (e.g. when
        # _populate_details_from_jsonld bailed out on a database-name
        # mismatch), build value groups directly from the mapping + RDF
        # store by querying get_categories for each categorical column.
        if not groups and mapping is not None and rdf_store_service is not None:
            groups, all_items, value_targets, described_db = self._build_values_fallback(
                mapping, rdf_store_service
            )

        schema_slice: dict[str, list[str]] = {}
        for group in groups:
            schema_slice.update(group["schema_slice"])
        return {
            "items": all_items,
            "schema_slice": schema_slice,
            "mapping": mapping,
            "described_database": described_db,
            "value_targets": value_targets,
            "groups": groups,
            "key_for": lambda item: f"{described_db}_{item}" if described_db else item,
        }

    @staticmethod
    def _build_values_fallback(
        mapping: Any, rdf_store_service: Any
    ) -> tuple[list[dict], list[str], dict, Optional[str]]:
        """Build value groups when DescriptiveInfoDetails is empty.

        Walks the mapping's columns, finds categorical variables with
        value mappings, and queries the RDF store for distinct values.
        """
        import io as _io

        columns_by_db: dict[str, list[str]] = {}
        try:
            columns_by_db = rdf_store_service.get_column_info_by_database() or {}
        except Exception:
            pass

        described_db = next(iter(columns_by_db), None)
        groups: list[dict] = []
        all_items: list[str] = []
        value_targets: dict = {}

        for db, cols in columns_by_db.items():
            for col in cols or []:
                # Find the variable key for this column from the mapping.
                var_key = None
                for _db_obj, column in _iter_columns(mapping):
                    if column.local_column == col:
                        var_key = column.get_variable_key()
                        break
                if not var_key:
                    continue
                variable = mapping.get_variable(var_key)
                if not variable or not variable.value_mappings:
                    continue
                terms = list(variable.value_mappings.keys())
                if not terms:
                    continue
                # Query the RDF store for distinct values.
                try:
                    cat_result = rdf_store_service.get_categories(col, db)
                    if not cat_result:
                        continue
                    import polars as pl
                    df = pl.read_csv(
                        _io.StringIO(cat_result),
                        separator=",",
                        infer_schema_length=0,
                        null_values=[],
                        try_parse_dates=False,
                    )
                    rows = df.to_dicts()
                except Exception:
                    continue
                seen: set[str] = set()
                group_items: list[str] = []
                for row in rows:
                    value = str((row or {}).get("value", "")).strip()
                    if not value or value in seen:
                        continue
                    seen.add(value)
                    group_items.append(value)
                    all_items.append(value)
                    value_targets.setdefault(db, {}).setdefault(var_key, []).append(value)
                if not group_items:
                    continue

                def make_key_for(database=db, column=col):
                    def key_for(value: str) -> str:
                        return f"{database}_{column}_{value}"
                    return key_for

                groups.append({
                    "items": group_items,
                    "schema_slice": {v: terms for v in group_items},
                    "key_for": make_key_for(),
                })

        return groups, all_items, value_targets, described_db

    # ------------------------------------------------------------------
    # Polling / priority
    # ------------------------------------------------------------------

    def get_state(self, session_cache: Any, phase: str) -> dict:
        job = self._jobs(session_cache).get(phase)
        if job is None:
            return {
                "status": "idle",
                "progress": {"done": 0, "total": 0},
                "error": None,
                "records": {},
            }
        return job.to_public_dict()

    def bump_priority(
        self,
        session_cache: Any,
        phase: str,
        items: list[str],
    ) -> dict:
        """Tier 1 is synchronous; priority is a no-op kept for API parity."""
        job = self._jobs(session_cache).get(phase)
        if job is None:
            return {"status": "no_job"}
        return {"status": "ok", "moved": 0}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _jobs(session_cache: Any) -> dict:
        if getattr(session_cache, "suggestion_jobs", None) is None:
            session_cache.suggestion_jobs = {p: None for p in PHASES}
        return session_cache.suggestion_jobs


def _all_targets(schema_slice: dict[str, list[str]]) -> set[str]:
    targets: set[str] = set()
    for terms in schema_slice.values():
        if isinstance(terms, list):
            targets.update(terms)
    return targets
