"""
Shared building blocks of a suggestion job.

The phase names, the job state the frontend polls, the fingerprint that
decides whether a job may be reused, the store's category CSV parser and
the record merge rule live here so that both the cascade
(:mod:`services.suggestions`) and the paste round trip
(:mod:`.roundtrip`) can use them without importing each other.
"""

from __future__ import annotations

import hashlib
import io
import json
from typing import Any, Optional

VARIABLES_PHASE = "variables"
VALUES_PHASE = "values"
PHASES = (VARIABLES_PHASE, VALUES_PHASE)


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


def _parse_category_counts(categories_csv: Any) -> dict[str, int]:
    """Parse the RDF store's get_categories CSV into ``value -> row count``.

    Same CSV as :func:`_parse_category_values` (``value,count``); a value
    whose count does not parse counts as 0.
    """
    if not categories_csv:
        return {}
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
        return {}
    counts: dict[str, int] = {}
    for row in df.to_dicts():
        v = row.get("value")
        if v is None:
            continue
        try:
            n = int(float(row.get("count") or 0))
        except (TypeError, ValueError):
            n = 0
        counts[str(v)] = counts.get(str(v), 0) + n
    return counts


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
        # per key (decision D3 in docs/mapping-suggestions/README.md).
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
