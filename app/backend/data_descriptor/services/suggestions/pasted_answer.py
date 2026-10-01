"""
Tolerant parsing of a pasted LLM answer into raw suggestion records.

The prompt (:mod:`.prompt_export`) asks for a JSON-LD ``databases``
section, but the text a user pastes back comes from any model and any
client: wrapped in code fences, preceded by "Here is the mapping:",
followed by caveats, sometimes with the ``databases`` wrapper dropped or
the whole document echoed. :func:`parse_answer_text` finds the JSON, and
:func:`extract_column_entries` finds the column entries wherever they are
nested, so a reply only has to be *recognisable* to be usable. The flat
``[{"item", "match", "confidence", "reason"}]`` form from the design
document is accepted too.

Nothing here validates keys against the schema: that is
``SuggestionService.ingest``, which runs the raw records through
``contract.sanitise_pairs`` like every other producer.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional

FENCE_RE = re.compile(r"```[a-zA-Z0-9_+-]*\s*\n(.*?)```", re.DOTALL)
VARIABLE_PREFIX = "schema:variable/"


class AnswerParseError(ValueError):
    """The pasted text holds no usable JSON; the message is user-facing."""


def _try_load(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


def _bracket_slice(text: str, open_ch: str, close_ch: str) -> Optional[str]:
    """Largest ``open_ch``...``close_ch`` span of ``text`` (outermost)."""
    start = text.find(open_ch)
    end = text.rfind(close_ch)
    if start == -1 or end == -1 or end <= start:
        return None
    return text[start : end + 1]


def _strip_trailing_commas(text: str) -> str:
    """Remove ``,`` before ``}``/``]`` — a common small-model slip."""
    return re.sub(r",(\s*[}\]])", r"\1", text)


def parse_answer_text(text: Any) -> Any:
    """Return the JSON value inside ``text`` or raise :class:`AnswerParseError`.

    Tries, in order: the whole text; each fenced code block; the outermost
    ``{...}`` span; the outermost ``[...]`` span; and each of those again
    with trailing commas removed.
    """
    if text is None:
        raise AnswerParseError("Nothing was pasted.")
    if not isinstance(text, str):
        # Already-parsed JSON from a client that sent an object.
        return text
    stripped = text.strip()
    if not stripped:
        raise AnswerParseError("Nothing was pasted.")

    candidates: list[str] = [stripped]
    candidates.extend(block.strip() for block in FENCE_RE.findall(stripped))
    # Outermost object or array, whichever opens first: a record array
    # holds objects, so the object span must not win over the array's.
    spans = []
    for open_ch, close_ch in (("{", "}"), ("[", "]")):
        span = _bracket_slice(stripped, open_ch, close_ch)
        if span:
            spans.append((stripped.find(open_ch), span))
    candidates.extend(span for _, span in sorted(spans))
    seen: set[str] = set()
    last_error: Optional[str] = None
    for candidate in candidates:
        for variant in (candidate, _strip_trailing_commas(candidate)):
            if variant in seen:
                continue
            seen.add(variant)
            try:
                return json.loads(variant)
            except ValueError as exc:
                last_error = str(exc)
    detail = f" ({last_error})" if last_error else ""
    raise AnswerParseError(
        "Could not find valid JSON in the pasted text. Paste the LLM's answer "
        f"including the opening and closing braces{detail}."
    )


# ---------------------------------------------------------------------------
# Column entries
# ---------------------------------------------------------------------------

_ENTRY_FIELDS = ("mapsTo", "localColumn", "localMappings")


def _looks_like_entry(obj: Any) -> bool:
    return isinstance(obj, dict) and any(field in obj for field in _ENTRY_FIELDS)


def extract_column_entries(data: Any) -> list[tuple[Optional[str], dict]]:
    """Every JSON-LD column entry in ``data`` as ``(entry_key, entry)``.

    Walks the value depth-first; a dict carrying ``mapsTo``, ``localColumn``
    or ``localMappings`` is an entry and its key in the parent object is
    returned with it (None for list members). Entries are not descended
    into, so a nested ``localMappings`` object is never mistaken for one.
    """
    found: list[tuple[Optional[str], dict]] = []

    def walk(node: Any, key: Optional[str]) -> None:
        if _looks_like_entry(node):
            found.append((key, node))
            return
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, str(k))
        elif isinstance(node, list):
            for v in node:
                walk(v, None)

    walk(data, None)
    return found


def is_flat_records(data: Any) -> bool:
    """The design document's ``[{item, match, ...}]`` form."""
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        data = data["records"]
    return (
        isinstance(data, list)
        and bool(data)
        and all(isinstance(r, dict) and "item" in r for r in data)
    )


def _variable_key(entry: dict, entry_key: Optional[str]) -> Optional[str]:
    maps_to = entry.get("mapsTo")
    if isinstance(maps_to, str) and maps_to.strip():
        maps_to = maps_to.strip()
        if maps_to.startswith(VARIABLE_PREFIX):
            return maps_to[len(VARIABLE_PREFIX) :] or None
        return maps_to.split("/")[-1] or None
    variable = entry.get("variable")
    if isinstance(variable, str) and variable.strip():
        return variable.strip()
    return entry_key or None


def _local_column(entry: dict) -> Optional[str]:
    local = entry.get("localColumn")
    if isinstance(local, list):
        local = local[0] if local else None
    if local is None:
        return None
    local = str(local)
    return local if local != "" else None


def _confidence(value: Any, default: float) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _reason(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def records_from_answer(
    phase: str, data: Any, *, default_confidence: float
) -> list[dict]:
    """Turn a parsed answer into raw records for ``sanitise_pairs``.

    Variables phase records: ``{item: column, match: variable_key,
    confidence, reason}``. Values phase records additionally carry
    ``column`` (the local column the value belongs to) and ``variable``
    (the key the entry claimed), so the service can pick that column's
    valid targets.

    Records for the same item keep the first occurrence; the service's
    sanitiser drops later duplicates, so dedupe-by-highest-confidence is
    done here where the raw values are still at hand.
    """
    if phase not in ("variables", "values"):
        raise ValueError(f"unknown phase '{phase}'")

    if is_flat_records(data):
        raw = data["records"] if isinstance(data, dict) else data
        records = [
            {
                "item": str(r.get("item")),
                "match": r.get("match") if r.get("match") is not None else None,
                "confidence": _confidence(r.get("confidence"), default_confidence),
                "reason": _reason(r.get("reason")),
                **({"column": str(r["column"])} if r.get("column") is not None else {}),
            }
            for r in raw
            if r.get("item") is not None
        ]
        return _dedupe(records, phase)

    entries = extract_column_entries(data)
    records: list[dict] = []
    for entry_key, entry in entries:
        variable = _variable_key(entry, entry_key)
        column = _local_column(entry)
        if phase == "variables":
            if column is None:
                continue
            records.append(
                {
                    "item": column,
                    "match": variable,
                    "confidence": _confidence(
                        entry.get("confidence"), default_confidence
                    ),
                    "reason": _reason(entry.get("reason")),
                }
            )
            continue

        local_mappings = entry.get("localMappings")
        if not isinstance(local_mappings, dict):
            continue
        notes = entry.get("valueNotes")
        notes = notes if isinstance(notes, dict) else {}
        column_confidence = _confidence(entry.get("confidence"), default_confidence)
        column_reason = _reason(entry.get("reason"))
        for term, values in local_mappings.items():
            if values is None:
                continue
            if not isinstance(values, list):
                values = [values]
            for value in values:
                if value is None:
                    continue
                value = str(value)
                note = notes.get(value)
                note = note if isinstance(note, dict) else {}
                records.append(
                    {
                        "item": value,
                        "match": str(term) if term is not None else None,
                        "confidence": _confidence(
                            note.get("confidence"), column_confidence
                        ),
                        "reason": _reason(note.get("reason")) or column_reason,
                        "column": column,
                        "variable": variable,
                    }
                )
    return _dedupe(records, phase)


def _dedupe(records: list[dict], phase: str) -> list[dict]:
    """Keep the highest-confidence record per item (per column for values)."""
    best: dict[tuple, dict] = {}
    order: list[tuple] = []
    for record in records:
        key = (
            (record.get("column"), record["item"])
            if phase == "values"
            else (record["item"],)
        )
        if key not in best:
            best[key] = record
            order.append(key)
        elif record["confidence"] > best[key]["confidence"]:
            best[key] = record
    return [best[k] for k in order]
