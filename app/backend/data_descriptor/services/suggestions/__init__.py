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
import io
import json
import logging
import os
import re
from typing import Any, Optional

from services.rdf_store_service import RDFStoreService

from .contract import sanitise_pairs
from .pasted_answer import AnswerParseError, parse_answer_text, records_from_answer
from .prompt_export import (
    DEFAULT_PASTED_CONFIDENCE,
    PASTED_SOURCE,
    PASTED_TIER,
    PromptExport,
    chunk_size_from_env,
    local_mappings,
    mapped_columns,
)
from .tiers import SuggestionContext
from .tiers.rules import (
    _iter_columns,
    load_rules,
    tier1_producers,
    value_based_variable_suggestions_enabled,
)

logger = logging.getLogger(__name__)

VARIABLES_PHASE = "variables"
VALUES_PHASE = "values"
PHASES = (VARIABLES_PHASE, VALUES_PHASE)

DEFAULT_THRESHOLD = 0.8
DEFAULT_MARGIN = 0.05
DEFAULT_COMPUTE = "host"

# Tiers enabled by FLYOVER_SUGGESTION_TIERS but not implemented yet map to
# the issue file that will add their producer; /status must never claim
# them active.
_UNIMPLEMENTED_TIER_ISSUES = {2: "issue 3", 3: "issue 4"}


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

    def tier_state(self, tier: int, producer_tiers: Optional[set] = None) -> dict:
        """Return ``{state, reason}`` for one tier for the ``/status`` endpoint.

        The flag only says the user *asked* for a tier; the state is derived
        from the registered producers, so a tier enabled in
        ``FLYOVER_SUGGESTION_TIERS`` without an implementation reports
        ``inactive`` with a "not implemented yet" reason instead of
        claiming to be active.
        """
        if tier not in (1, 2, 3):
            return {"state": "inactive", "reason": "unknown tier"}
        if tier not in self.tiers:
            return {
                "state": "inactive",
                "reason": "disabled by FLYOVER_SUGGESTION_TIERS",
            }
        if producer_tiers is not None and tier not in producer_tiers:
            issue = _UNIMPLEMENTED_TIER_ISSUES.get(tier, "a later issue")
            return {"state": "inactive", "reason": f"not implemented yet ({issue})"}
        return {"state": "active"}


def _fingerprint(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def _parse_category_values(categories_csv: Any) -> list[str]:
    """Parse the RDF store's get_categories CSV into distinct value strings.

    Shared by the column-values collection (variables phase) and the
    values-phase fallback so both parse categories exactly the same way.
    """
    if not categories_csv:
        return []
    try:
        import polars as pl

        df = pl.read_csv(
            io.StringIO(categories_csv),
            separator=",",
            infer_schema_length=0,
            null_values=[],
            try_parse_dates=False,
        )
    except Exception:  # pragma: no cover - defensive
        return []
    values: list[str] = []
    for row in df.to_dicts():
        v = row.get("value")
        if v is not None and str(v) not in values:
            values.append(str(v))
    return values


def _alias_memory_hash(mapping: Any) -> str:
    """Stable hash of every remembered (database, column, variable) pair.

    A site's own review results must not be reused when any other site's
    mappings change, so the fingerprint covers the alias memory as a
    whole, not just this phase's item list.
    """
    pairs = sorted(
        (
            (
                db.name or "",
                str(column.local_column or ""),
                column.get_variable_key() or "",
            )
            for db, column in _iter_columns(mapping)
        )
    )
    return hashlib.sha256(json.dumps(pairs).encode()).hexdigest()[:16]


def _merge_records(a: dict, b: dict) -> tuple[dict, Optional[dict]]:
    """Merge two records for the same item; return (winner, loser_or_None).

    Highest confidence wins; ties go to the lower tier (cheaper). When
    both records abstain, the reason that mentions the margin wins: the
    string matcher's abstain names the scores it saw, the earlier tiers'
    "no hit" reasons do not — the plan's "reason mentions margin"
    criterion must hold end-to-end.

    Only a losing record with a non-null ``match`` different from the
    winner's is kept in ``alternatives``: abstains and duplicates of the
    winner are noise, not choices for the user.
    """
    both_abstain = a.get("match") is None and b.get("match") is None
    if (
        both_abstain
        and "margin" in str(b.get("reason", ""))
        and "margin" not in str(a.get("reason", ""))
    ):
        winner, loser = b, a
    elif both_abstain:
        winner, loser = a, b
    elif a.get("confidence") == b.get("confidence"):
        if a.get("tier", 99) <= b.get("tier", 99):
            winner, loser = a, b
        else:
            winner, loser = b, a
    elif a.get("confidence", 0.0) > b.get("confidence", 0.0):
        winner, loser = a, b
    else:
        winner, loser = b, a

    loser_copy = {k: v for k, v in loser.items() if k != "alternatives"}
    winner = dict(winner)
    if loser_copy.get("match") and loser_copy["match"] != winner.get("match"):
        alts = list(winner.get("alternatives", []))
        alts.append(loser_copy)
        winner["alternatives"] = alts
    return winner, loser_copy


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
        # Hash of the pasted records merged into this job. Kept apart from
        # ``fingerprint`` (which decides whether ``start`` may reuse the
        # job) so an ingest never forces a rebuild, while the public
        # fingerprint still changes so the browser expires stale marks
        # per key (decision D3).
        self.ingested_fingerprint: Optional[str] = None

    @property
    def public_fingerprint(self) -> str:
        if self.ingested_fingerprint:
            return f"{self.fingerprint}+{self.ingested_fingerprint}"
        return self.fingerprint

    def to_public_dict(self) -> dict:
        return {
            "status": self.status,
            "fingerprint": self.public_fingerprint,
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
        producers = self._producers_for_enabled_tiers()
        producer_tiers = {getattr(p, "tier", 1) for p in producers}
        tiers = {
            tier: self.config.tier_state(tier, producer_tiers) for tier in (1, 2, 3)
        }
        return {
            "compute": self.config.compute,
            "tiers": tiers,
            "threshold": self.config.threshold,
            "rules_version": (
                (self._rules or {}).get("version") if self._rules else None
            ),
            # The copy-prompt / paste-answer round trip needs no model and
            # no flag: it is available whenever the service is. The chunk
            # default is the env's clamped value so the panel can offer the
            # site's chosen default (FLYOVER_SUGGESTION_PROMPT_CHUNK).
            "prompt_export": {
                "state": "active",
                "chunk": chunk_size_from_env(),
            },
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
        mapping: Any = None,
    ) -> dict:
        """Build (or reuse) the job for ``phase`` and run the enabled tiers.

        Returns a status dict (``{"status": ...}``) mirroring the LLM branch.

        ``mapping`` is a validated job-local mapping: the browser's current
        semantic map, which the describe pages work on (and, in the values
        phase, the user's latest variable selections). It is used for this
        job only and never written back to ``session_cache.jsonld_mapping``.
        Without it the session's own mapping is used.
        """
        if phase not in PHASES:
            return {"status": "error", "reason": "unknown_phase"}
        if not self.config.enabled:
            return {"status": "disabled"}

        jobs = self._jobs(session_cache)
        existing = jobs.get(phase)

        if mapping is None:
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
        # Pasted records outlive a rebuild (page reload, forced re-run):
        # they were the user's explicit action, not a cache.
        self._reapply_ingested(session_cache, job)
        return {"status": "started"}

    def _fingerprint_payload(self, phase: str, payload: dict, mapping: Any) -> dict:
        """Build a JSON-serialisable subset of the payload for fingerprinting.

        Includes the rules version and a hash of the alias memory (every
        remembered column/value -> variable pair): a rules bump or any
        other site's mappings changing must expire a cached job, not just
        the item list.
        """
        base = {
            "phase": phase,
            "rules_version": (self._rules or {}).get("version"),
            "alias_memory": _alias_memory_hash(mapping),
        }
        if phase == VARIABLES_PHASE:
            return {
                **base,
                "items": sorted(payload["items"]),
                "variables": sorted(mapping.get_all_variable_keys() if mapping else []),
            }
        groups = payload.get("groups", [])
        return {
            **base,
            "groups": [
                {
                    "database": g["database"],
                    "column": g["column"],
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
            groups = [
                {
                    "items": payload["items"],
                    "schema_slice": payload["schema_slice"],
                    "key_for": payload["key_for"],
                }
            ]

        for group in groups:
            self._run_group(job, producers, payload, group)
        job.progress = {
            "done": len(job.records),
            "total": sum(len(g["items"]) for g in groups),
        }

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

        # Values-phase column context: a value-set rule only applies when the
        # whole column (minus missing codes) falls inside one of the rule's
        # sets. A values group is exactly one column, so its own items are
        # that column's distinct values. This must stay per group: the same
        # value string ("1") appears in many columns with different coding
        # schemes, and a payload-wide map would let whichever column came
        # first decide for all of them.
        item_column_values = (
            {value: list(items) for value in items} if phase == VALUES_PHASE else {}
        )

        best: dict[str, dict] = {item: None for item in items}
        for producer in producers:
            tier = getattr(producer, "tier", 1)
            source = getattr(producer, "source", "manual")
            to_run = [
                item
                for item in items
                if best.get(item) is None
                or (best[item].get("confidence", 0.0) < self.config.threshold)
            ]
            if not to_run:
                continue
            ctx = SuggestionContext(
                phase=phase,
                mapping=payload["mapping"],
                described_database=group.get("described_database")
                or payload.get("described_database"),
                threshold=self.config.threshold,
                margin=self.config.margin,
                rules=self._rules,
                column_values=payload.get("column_values", {}),
                value_targets=payload.get("value_targets", {}),
                item_column_values=item_column_values,
                database_name_match=RDFStoreService.graph_database_find_name_match,
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
                record = {
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
            record.update(self._record_location_fields(group, item, phase))
            job.records[key] = record

    @staticmethod
    def _record_location_fields(group: dict, item: str, phase: str) -> dict:
        """Explicit location fields for one record.

        The frontend must never have to recover the database or column by
        splitting the composite key: database names can themselves contain
        underscores (``nki`` vs ``nki_prospective``), which makes prefix
        matching ambiguous. Variables-phase records carry ``database`` and
        ``column``; values-phase records additionally carry ``value`` (the
        item itself).
        """
        fields: dict[str, str] = {}
        database = group.get("described_database") or group.get("database")
        if database:
            fields["database"] = database
        if phase == VARIABLES_PHASE:
            fields["column"] = item
        else:
            column = group.get("column")
            if column:
                fields["column"] = column
                fields["value"] = item
        return fields

    @staticmethod
    def _downgrade_conflicts(best: dict) -> None:
        """Resolve variables-phase duplicate matches, keeping the winner.

        Within one database each schema variable may be chosen by one column
        only. When several columns are suggested the same variable, the
        highest-confidence record keeps its match (ties go to the first
        column in item order); the losers are nulled with a reason naming
        the winning column (decision D2). Each loser keeps the contested
        variable in its ``alternatives``, with the confidence, source and
        tier it had, so the UI can still offer it: if the user decides the
        winner is wrong, the loser's candidate is one click away.
        """
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
            winner = items[0]
            for item in items[1:]:
                if best[item].get("confidence", 0.0) > best[winner].get(
                    "confidence", 0.0
                ):
                    winner = item
            for item in items:
                if item == winner:
                    continue
                record = best[item]
                contested = {
                    k: record[k]
                    for k in ("match", "confidence", "reason", "source", "tier")
                    if k in record
                }
                record["alternatives"] = [contested] + [
                    alt
                    for alt in record.get("alternatives", [])
                    if alt.get("match") != match
                ]
                record["match"] = None
                record["confidence"] = 0.0
                record["reason"] = (
                    f"conflict: column '{winner}' is a stronger candidate "
                    f"for {match}"
                )
                record["status"] = "done"

    # ------------------------------------------------------------------
    # Payload builders
    # ------------------------------------------------------------------

    def _build_variables_payload(self, mapping: Any, rdf_store_service: Any) -> dict:
        columns_by_db = {}
        if rdf_store_service is not None:
            try:
                columns_by_db = rdf_store_service.get_column_info_by_database() or {}
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("get_column_info_by_database failed: %s", exc)
        variable_keys = list(mapping.get_all_variable_keys()) if mapping else []
        all_items: list[str] = []
        for cols in columns_by_db.values():
            for col in cols or []:
                if col not in all_items:
                    all_items.append(col)

        described_db = next(iter(columns_by_db), None)

        # One group per database so each group's keys are prefixed with the
        # correct database name and the alias matcher's leave-one-site-out
        # excludes the right database per group.
        groups: list[dict] = []
        for db_name, cols in columns_by_db.items():
            db_items = [c for c in cols or [] if c in all_items]
            if not db_items:
                continue

            def make_key_for(db=db_name):
                def key_for(item: str) -> str:
                    return f"{db}_{item}"

                return key_for

            groups.append(
                {
                    "items": db_items,
                    "schema_slice": {"*": variable_keys},
                    "key_for": make_key_for(),
                    "described_database": db_name,
                }
            )

        # Fallback: if no groups were built (no databases), use a single
        # group with all items and no database prefix.
        if not groups:

            def key_for_fallback(item: str) -> str:
                return item

            groups = [
                {
                    "items": all_items,
                    "schema_slice": {"*": variable_keys},
                    "key_for": key_for_fallback,
                }
            ]

        return {
            "items": all_items,
            "schema_slice": {"*": variable_keys},
            "mapping": mapping,
            "described_database": described_db,
            # Collecting distinct values costs one RDF-store query per
            # column; only pay it while value-based variable suggestions
            # are enabled (see tiers/rules.value_based_variable_suggestions_enabled).
            "column_values": (
                self._collect_column_values(mapping, rdf_store_service, columns_by_db)
                if value_based_variable_suggestions_enabled()
                else {}
            ),
            "groups": groups,
            "key_for": groups[0]["key_for"] if groups else (lambda item: item),
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
                out[db][col] = _parse_category_values(cats)
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
        display_re = re.compile(r'^(?P<global>.*) \(or "(?P<local>.*)"\)$')

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

                    groups.append(
                        {
                            "items": group_items,
                            "schema_slice": {value: terms for value in group_items},
                            "key_for": make_key_for(),
                            "database": database,
                            "column": local_column,
                            # Leave-one-site-out must exclude THIS database,
                            # not whichever database the payload happens to
                            # name first (the value groups used to rely on
                            # the payload-level fallback and leaked the
                            # first database into its own alias memory).
                            "described_database": database,
                        }
                    )

        described_db = next(iter(details), None)

        # Fallback: when DescriptiveInfoDetails is empty (e.g. when
        # _populate_details_from_jsonld bailed out on a database-name
        # mismatch), build value groups directly from the mapping + RDF
        # store by querying get_categories for each categorical column.
        if not groups and mapping is not None and rdf_store_service is not None:
            groups, all_items, value_targets, described_db = (
                self._build_values_fallback(mapping, rdf_store_service)
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

        Walks the RDF store's columns, finds each column's variable in
        the mapping by (database, local column), and queries the store
        for the column's distinct values.
        """
        columns_by_db: dict[str, list[str]] = {}
        try:
            columns_by_db = rdf_store_service.get_column_info_by_database() or {}
        except Exception:
            pass

        described_db = next(iter(columns_by_db), None)
        groups: list[dict] = []
        all_items: list[str] = []
        value_targets: dict = {}

        # Index the mapping's columns by local name once: each store column
        # then only checks the few mapping columns sharing its name instead
        # of scanning every column of every database. The database check
        # stays a name_match call (it tolerates a ".csv" suffix), so the
        # index is keyed by column, not by (database, column).
        name_match = RDFStoreService.graph_database_find_name_match
        columns_by_local: dict[str, list[tuple[str, str]]] = {}
        for mapping_db, column in _iter_columns(mapping):
            columns_by_local.setdefault(str(column.local_column or ""), []).append(
                (mapping_db.name or "", column.get_variable_key() or "")
            )

        for db, cols in columns_by_db.items():
            for col in cols or []:
                # Find the variable key for this column of THIS database;
                # a same-named column in another database may map to an
                # entirely different variable.
                var_key = next(
                    (
                        vk
                        for db_name, vk in columns_by_local.get(col, ())
                        if name_match(db_name, db)
                    ),
                    None,
                )
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
                except Exception:
                    continue
                # _parse_category_values already dedupes, order-preserving.
                group_items = [v for v in _parse_category_values(cat_result) if v]
                if not group_items:
                    continue
                all_items.extend(group_items)
                value_targets.setdefault(db, {}).setdefault(var_key, []).extend(
                    group_items
                )

                def make_key_for(database=db, column=col):
                    def key_for(value: str) -> str:
                        return f"{database}_{column}_{value}"

                    return key_for

                groups.append(
                    {
                        "items": group_items,
                        "schema_slice": {v: terms for v in group_items},
                        "key_for": make_key_for(),
                        "database": db,
                        "column": col,
                        # Leave-one-site-out must exclude THIS database.
                        "described_database": db,
                    }
                )

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
    # Prompt export / paste-back (issue 2)
    # ------------------------------------------------------------------

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
    ) -> dict:
        """Compose the copy-prompt payload for one database and phase.

        Works whether or not any tier is enabled: the prompt needs the
        semantic map, the store's column names and (values phase) distinct
        values, and (when a job exists) the job's records as hints. The
        variables phase shares no distinct values at all. Raises
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
