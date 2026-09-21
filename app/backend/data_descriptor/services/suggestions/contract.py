"""
Suggestion record contract and server-side validation.

This module is the single source of truth for the shape of a suggestion
record. Every tier producer (rules, embedding, llm) and every ingest path
produces records that conform to ``SUGGESTION_RECORD_SCHEMA`` and is run
through :func:`sanitise_pairs` before they reach the job snapshot. That
guarantees the README invariants: exact keys only, confidence clamped,
reason non-empty, source/tier/status present.

The schema is cherry-picked from the LLM branch's ``matching.py``
``MATCH_OUTPUT_SCHEMA`` and extended with ``source``, ``tier`` and
``status`` so the same contract serves every tier.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from jsonschema import ValidationError, validate

logger = logging.getLogger(__name__)

# Sources produced by the tiers; ``manual``/``pasted_llm`` are reserved for
# later issues but defined here so the enum is stable from day one.
SOURCES = ("alias", "value_regex", "string", "embedding", "llm", "pasted_llm", "manual")
STATUSES = ("pending", "running", "done", "failed", "unavailable")

# JSON Schema for a single suggestion record. Used by the contract tests and
# by ``sanitise_pairs`` to validate every producer's output.
SUGGESTION_RECORD_SCHEMA = {
    "type": "object",
    "required": ["item", "match", "confidence", "reason", "source", "tier", "status"],
    "additionalProperties": True,
    "properties": {
        "item": {"type": "string"},
        "match": {"type": ["string", "null"]},
        "confidence": {"type": "number"},
        "reason": {"type": "string"},
        "source": {"type": "string", "enum": list(SOURCES)},
        "tier": {"type": "integer", "enum": [1, 2, 3]},
        "status": {"type": "string", "enum": list(STATUSES)},
        "alternatives": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["match", "confidence", "source", "tier"],
                "additionalProperties": True,
                "properties": {
                    "match": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                    "source": {"type": "string", "enum": list(SOURCES)},
                    "tier": {"type": "integer", "enum": [1, 2, 3]},
                    "reason": {"type": "string"},
                },
            },
        },
    },
}


@dataclass
class SuggestionRecord:
    """One suggestion about one local item (column or distinct value).

    Attributes match the README contract; ``alternatives`` holds the losing
    records from the cascade merge so the UI can offer them in a popover.
    """

    item: str
    match: Optional[str]
    confidence: float
    reason: str
    source: str
    tier: int
    status: str = "done"
    alternatives: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {
            "item": self.item,
            "match": self.match,
            "confidence": self.confidence,
            "reason": self.reason,
            "source": self.source,
            "tier": self.tier,
            "status": self.status,
        }
        if self.alternatives:
            d["alternatives"] = self.alternatives
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "SuggestionRecord":
        return cls(
            item=data["item"],
            match=data.get("match"),
            confidence=float(data.get("confidence", 0.0)),
            reason=data.get("reason", ""),
            source=data.get("source", "manual"),
            tier=int(data.get("tier", 1)),
            status=data.get("status", "done"),
            alternatives=list(data.get("alternatives", []) or []),
        )


def _clamp_confidence(value: Any) -> float:
    """Clamp a confidence value to [0, 1], treating garbage as 0."""
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, confidence))


def sanitise_pairs(
    pairs: Iterable[dict],
    items: Iterable[str],
    valid_targets: Iterable[str],
    *,
    source: str,
    tier: int,
    status: str = "done",
) -> list[dict]:
    """Validate and normalise raw producer output into contract records.

    This is the generic server-side validator every producer (and the
    ``/ingest`` path) passes through. It:

    - clamps ``confidence`` to ``[0, 1]``;
    - nulls ``match`` values that are not exact members of ``valid_targets``
      (hallucination guard), annotating ``reason`` with what was rejected;
    - drops pairs for items not in ``items``;
    - fills in ``source``/``tier``/``status`` and a non-empty ``reason``;
    - returns one record per item in ``items`` order (missing items get a
      null abstain record).

    Args:
        pairs: Raw records from a producer. Each must carry ``item``; the
            other fields are optional and defaulted.
        items: The local items being mapped, in the desired output order.
        valid_targets: The exact keys the ``match`` is allowed to take.
        source: The source label to stamp on every record.
        tier: The tier number to stamp on every record.
        status: The status to stamp on every record.

    Returns:
        A list of record dicts (``SuggestionRecord.to_dict`` shape), one per
        item in ``items`` order.
    """
    item_set = set(items)
    target_set = set(valid_targets)
    by_item: dict[str, dict] = {}

    for pair in pairs or []:
        item = pair.get("item")
        if item is None or item not in item_set or item in by_item:
            continue

        match = pair.get("match")
        reason = str(pair.get("reason") or "").strip()
        confidence = _clamp_confidence(pair.get("confidence", 0.0))

        if match is not None and match not in target_set:
            proposed = str(match)
            reason = (f"{reason} | Note: '{proposed}' is not a valid target.").strip(
                " |"
            )
            match = None
            confidence = 0.0

        if not reason:
            reason = (
                "Matched to this target."
                if match is not None
                else "No confident match found."
            )

        by_item[item] = {
            "item": item,
            "match": match,
            "confidence": confidence,
            "reason": reason,
            "source": source,
            "tier": tier,
            "status": status,
        }

    return [
        by_item.get(
            item,
            {
                "item": item,
                "match": None,
                "confidence": 0.0,
                "reason": "No suggestion produced for this item.",
                "source": source,
                "tier": tier,
                "status": status,
            },
        )
        for item in items
    ]


def validate_record(record: dict) -> None:
    """Validate a single record dict against the contract schema.

    Raises ``jsonschema.ValidationError`` on violations; used by tests.
    """
    validate(record, SUGGESTION_RECORD_SCHEMA)


def is_valid_record(record: dict) -> bool:
    """Return True when ``record`` conforms to the contract schema."""
    try:
        validate_record(record)
        return True
    except ValidationError:
        return False
