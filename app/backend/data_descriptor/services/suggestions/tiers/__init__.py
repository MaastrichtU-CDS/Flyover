"""
Tier producer protocol for mapping suggestions.

Every tier (rule-based, embedding, LLM) implements the same protocol so the
:class:`~services.suggestions.SuggestionService` can run them in cascade order
without knowing how any tier works. A producer receives the items to map, the
schema slice (the set of exact target keys valid for this phase), and a shared
context, and returns a list of raw record dicts that :func:`sanitise_pairs`
will validate.

The protocol is deliberately minimal; the context object carries everything a
producer might need (the loaded JSON-LD mapping, distinct values, thresholds,
loaded rules, the database currently being described for leave-one-site-out
alias memory, and an optional site dictionary hook reserved for a follow-up
issue).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable


@dataclass
class SuggestionContext:
    """Shared inputs handed to every tier producer for one job run.

    Attributes:
        phase: "variables" or "values".
        mapping: The loaded ``JSONLDMapping`` (may be None if absent).
        described_database: The store database currently being described; alias
            memory excludes this database so a site never suggests from itself.
        threshold: Confidence below which an item escalates to the next tier.
        margin: Minimum top1-top2 score gap required to avoid abstaining.
        rules: Parsed ``suggestion_rules.json`` (versioned, loaded once).
        column_values: ``database -> column -> [distinct values]`` for the
            variables phase value-type regexes.
        value_targets: For the values phase, ``database -> local_column ->
            (variable_key, [distinct values])`` describing which variable each
            value maps into and the terms available for it.
        dictionary: Optional site-provided ``{column: label}`` dictionary hook;
            reserved for a follow-up issue, unused by tier 1.
    """

    phase: str
    mapping: Any = None
    described_database: Optional[str] = None
    threshold: float = 0.8
    margin: float = 0.05
    rules: Optional[dict] = None
    column_values: dict = field(default_factory=dict)
    value_targets: dict = field(default_factory=dict)
    dictionary: Optional[dict] = None


@runtime_checkable
class TierProducer(Protocol):
    """Produce raw suggestion records for the items a tier is asked to run on.

    Implementations return one record dict per item (records with ``match:
    None`` mean the tier abstained). The service runs the records through
    ``sanitise_pairs`` and the cascade merge; producers never write to the
    JSON-LD.
    """

    tier: int
    source: str

    def run(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
    ) -> list[dict]:
        """Return raw records for ``items``.

        Args:
            items: Local item labels to map (column name or distinct value),
                in the order the caller wants them back.
            schema_slice: Per-item candidate target keys. For the variables
                phase this is ``{"*": [variable keys]}``; for the values phase
                it is ``{item: [term keys of the column's mapped variable]}``.
            ctx: Shared :class:`SuggestionContext`.

        Returns:
            A list of raw record dicts (``item``, ``match``, ``confidence``,
            ``reason`` at minimum); the service stamps source/tier/status.
        """
        ...
