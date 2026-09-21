"""
Tier 1 rule-based mapping suggestions.

Three deterministic matchers, run in cascade order alias -> value_regex ->
string. Each matcher implements the :class:`TierProducer` protocol and
returns raw records; the :class:`~services.suggestions.SuggestionService`
runs them through ``sanitise_pairs`` and the cascade merge.

Matchers:

(a) :class:`AliasMatcher` (``source: alias``) — walks every
    ``databases.<db>.tables.<t>.columns.<c>`` of the loaded JSON-LD and
    collects ``normalise(localColumn) -> mapsTo`` pairs, excluding the
    database currently being described (leave-one-site-out). Same for
    values: ``normalise(localValue) -> term`` per variable from
    ``localMappings``. Exact normalised hits score 1.0; near hits use
    Jaro-Winkler similarity and are scaled by 0.9.

(b) :class:`ValueRegexMatcher` (``source: value_regex``) — fires when the
    full distinct-value set (minus missing codes) is covered by one of the
    pattern sets in ``suggestion_rules.json``. In the variables phase it
    suggests variables whose predicates match; in the values phase it
    suggests the matching term. Abstains when more than ``margin`` candidates
    tie.

(c) :class:`StringMatcher` (``source: string``) — normalises both sides
    (lowercase, split snake/camel/digits, drop stopwords, expand
    abbreviations) and scores Jaro-Winkler on the joined tokens plus Jaccard
    on the token sets against variable keys and labels. Confidence is the
    best score; abstains when ``top1 - top2 < margin``.

A small pure-Python Jaro-Winkler is included so tier 1 stays dependency-free
and runs on the air-gapped 4-core/8 GB target.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Optional

from services.rdf_store_service import RDFStoreService

from . import SuggestionContext

logger = logging.getLogger(__name__)

SOURCE_ALIAS = "alias"
SOURCE_VALUE_REGEX = "value_regex"
SOURCE_STRING = "string"

TIER = 1

_RESOURCES_DIR = Path(__file__).resolve().parent.parent.parent.parent / "resources"
_RULES_PATH = _RESOURCES_DIR / "suggestion_rules.json"

_RULES_CACHE: Optional[dict] = None


def load_rules(path: Optional[str] = None) -> dict:
    """Load and cache ``suggestion_rules.json`` once per process.

    The resource is versioned (``version`` field); callers may force a path
    for tests. Returns the parsed dict.
    """
    global _RULES_CACHE
    if path is not None:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    if _RULES_CACHE is None:
        with open(_RULES_PATH, "r", encoding="utf-8") as fh:
            _RULES_CACHE = json.load(fh)
    return _RULES_CACHE


# ---------------------------------------------------------------------------
# Normalisation helpers (shared by alias and string matchers)
# ---------------------------------------------------------------------------

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Za-z])(?=[0-9])|[_\-.]")


def normalise_label(text: str) -> str:
    """Lowercase, split snake/camel/digit boundaries, single spaces."""
    if text is None:
        return ""
    cleaned = _CAMEL_BOUNDARY.sub(" ", str(text)).lower()
    return re.sub(r"\s+", " ", cleaned).strip()


def tokenise(text: str, rules: Optional[dict] = None) -> list[str]:
    """Tokenise a label into normalised, stopword-free, expanded tokens."""
    abbreviations = (rules or {}).get("abbreviations", {}) or {}
    stopwords = set((rules or {}).get("stopwords", []) or [])
    raw = [t for t in normalise_label(text).split(" ") if t]
    out: list[str] = []
    for tok in raw:
        if tok in stopwords:
            continue
        expanded = abbreviations.get(tok)
        out.append(expanded if expanded else tok)
    return out


def _variable_label(variable: Any) -> str:
    """Best human label for a SchemaVariable; falls back to its key."""
    if variable is None:
        return ""
    key = getattr(variable, "key", "") or ""
    # SchemaVariable stores data_type/predicate/class but no display label;
    # the key (snake_case) is the canonical label surface for string matching.
    return key.replace("_", " ")


# ---------------------------------------------------------------------------
# Pure-Python Jaro-Winkler (dependency-free for the air-gapped target)
# ---------------------------------------------------------------------------


def jaro_winkler(a: str, b: str, prefix_weight: float = 0.1) -> float:
    """Return Jaro-Winkler similarity in [0, 1]."""
    a = a or ""
    b = b or ""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0

    match_distance = max(len(a), len(b)) // 2 - 1
    if match_distance < 0:
        match_distance = 0
    a_matches = [False] * len(a)
    b_matches = [False] * len(b)
    matches = 0

    for i, ch in enumerate(a):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len(b))
        for j in range(start, end):
            if not b_matches[j] and ch == b[j]:
                a_matches[i] = True
                b_matches[j] = True
                matches += 1
                break

    if matches == 0:
        return 0.0

    transpositions = 0
    k = 0
    for i in range(len(a)):
        if not a_matches[i]:
            continue
        while not b_matches[k]:
            k += 1
        if a[i] != b[k]:
            transpositions += 1
        k += 1
    transpositions //= 2

    jaro = (
        matches / len(a) + matches / len(b) + (matches - transpositions) / matches
    ) / 3.0

    prefix = 0
    for i in range(min(4, len(a), len(b))):
        if a[i] == b[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * prefix_weight * (1.0 - jaro)


def jaccard(a: set[str], b: set[str]) -> float:
    """Token-set Jaccard similarity in [0, 1]."""
    if not a and not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


# ---------------------------------------------------------------------------
# Alias memory
# ---------------------------------------------------------------------------


def _iter_columns(mapping: Any):
    """Yield ``(database, column)`` for every column in the JSON-LD mapping."""
    if mapping is None:
        return
    for db in getattr(mapping, "databases", {}).values():
        for table in db.tables.values():
            for column in table.columns.values():
                yield db, column


def build_alias_memory(mapping: Any, described_database: Optional[str]) -> dict:
    """Build ``normalise(label) -> target`` memory, excluding ``described_database``.

    For the variables phase the target is a variable key; for the values
    phase callers pass a pre-built ``localValue -> term`` memory via
    :func:`build_value_alias_memory`.
    """
    memory: dict[str, tuple[str, str]] = {}
    for db, column in _iter_columns(mapping):
        if described_database and RDFStoreService.graph_database_find_name_match(
            db.name, described_database
        ):
            continue
        local = column.local_column
        if not local:
            continue
        key = normalise_label(str(local))
        var_key = column.get_variable_key()
        if not var_key or not key:
            continue
        memory.setdefault(key, (var_key, db.name or ""))
    return memory


def build_value_alias_memory(mapping: Any, described_database: Optional[str]) -> dict:
    """Build ``normalise(value) -> (term, db)`` memory from localMappings."""
    memory: dict[str, tuple[str, str]] = {}
    for db, column in _iter_columns(mapping):
        if described_database and RDFStoreService.graph_database_find_name_match(
            db.name, described_database
        ):
            continue
        var_key = column.get_variable_key()
        if not var_key:
            continue
        for term, values in (column.local_mappings or {}).items():
            if values is None:
                continue
            if not isinstance(values, list):
                values = [values]
            for value in values:
                if value is None:
                    continue
                key = normalise_label(str(value))
                if key:
                    memory.setdefault(key, (str(term), db.name or ""))
    return memory


# ---------------------------------------------------------------------------
# (a) Alias matcher
# ---------------------------------------------------------------------------


class AliasMatcher:
    """Alias memory matcher (``source: alias``)."""

    tier = TIER
    source = SOURCE_ALIAS

    def __init__(self, similarity_floor: float = 0.95):
        self.similarity_floor = similarity_floor

    def run(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
    ) -> list[dict]:
        phase = ctx.phase
        targets = set(schema_slice.get("*", []))
        if phase == "values":
            memory = build_value_alias_memory(ctx.mapping, ctx.described_database)
        else:
            memory = build_alias_memory(ctx.mapping, ctx.described_database)

        records: list[dict] = []
        for item in items:
            norm = normalise_label(item)
            if not norm:
                records.append(
                    {
                        "item": item,
                        "match": None,
                        "confidence": 0.0,
                        "reason": "Empty label.",
                    }
                )
                continue

            exact = memory.get(norm)
            best: Optional[tuple[str, float, str]] = None
            if exact:
                target, source_db = exact
                if not targets or target in targets:
                    best = (target, 1.0, source_db)

            if best is None:
                # Fuzzy near-hit on the alias keys.
                for key, (target, source_db) in memory.items():
                    if not targets or target in targets:
                        sim = jaro_winkler(norm, key)
                        if sim >= self.similarity_floor:
                            if best is None or sim > best[1]:
                                best = (target, sim, source_db)

            if best is not None:
                target, sim, source_db = best
                confidence = 1.0 if sim >= 1.0 else round(0.9 * sim, 4)
                reason = (
                    f"Alias: '{item}' matches column/value from database "
                    f"'{source_db}' mapped to this {('variable' if phase == 'variables' else 'term')}."
                )
                records.append(
                    {
                        "item": item,
                        "match": target,
                        "confidence": confidence,
                        "reason": reason,
                    }
                )
            else:
                records.append(
                    {
                        "item": item,
                        "match": None,
                        "confidence": 0.0,
                        "reason": "No alias memory hit.",
                    }
                )
        return records


# ---------------------------------------------------------------------------
# (b) Value-type regex matcher
# ---------------------------------------------------------------------------


def _missing_codes(rules: dict) -> set[str]:
    return {str(c).lower() for c in (rules.get("missing_codes") or [])}


def _strip_missing(values: list[str], rules: dict) -> list[str]:
    missing = _missing_codes(rules)
    return [v for v in values if str(v).strip().lower() not in missing]


def _value_set_matches(
    values: list[str], candidate_sets: list[list[str]]
) -> Optional[list[str]]:
    """Return the matched canonical set when ``values`` equals one of the sets."""
    norm = {str(v).strip().lower() for v in values}
    for cand in candidate_sets:
        if norm == {str(c).strip().lower() for c in cand}:
            return cand
    return None


def _value_in_any_set(
    value: str, candidate_sets: list[list[str]]
) -> Optional[list[str]]:
    """Return the matched canonical set when ``value`` is a member of one of the sets."""
    norm = str(value).strip().lower()
    for cand in candidate_sets:
        if norm in {str(c).strip().lower() for c in cand}:
            return cand
    return None


def _all_match_patterns(values: list[str], patterns: list[str]) -> bool:
    compiled = [re.compile(p) for p in patterns]
    return all(any(p.match(str(v)) for p in compiled) for v in values)


def _all_match_range(values: list[str], rng: dict) -> bool:
    min_v = rng.get("min")
    max_v = rng.get("max")
    if max_v == "current":
        max_v = date.today().year
    for v in values:
        try:
            year = int(str(v))
        except ValueError:
            return False
        if min_v is not None and year < min_v:
            return False
        if max_v is not None and year > max_v:
            return False
    return True


class ValueRegexMatcher:
    """Value-type regex matcher (``source: value_regex``)."""

    tier = TIER
    source = SOURCE_VALUE_REGEX

    def run(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
    ) -> list[dict]:
        rules = ctx.rules or load_rules()
        records: list[dict] = []

        if ctx.phase == "variables":
            records = self._run_variables(items, schema_slice, ctx, rules)
        else:
            records = self._run_values(items, schema_slice, ctx, rules)
        return records

    def _run_variables(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
        rules: dict,
    ) -> list[dict]:
        targets = schema_slice.get("*", [])
        variable_keys = {v for v in targets}
        column_values = ctx.column_values or {}
        out: list[dict] = []

        for item in items:
            # Find this column's distinct values across databases.
            distinct: list[str] = []
            for cols in column_values.values():
                if item in cols:
                    distinct = cols[item]
                    break
            values = _strip_missing(distinct, rules)
            match = self._match_variable_rule(values, rules, variable_keys)
            if match is None:
                out.append(
                    {
                        "item": item,
                        "match": None,
                        "confidence": 0.0,
                        "reason": "No value-type pattern matched.",
                    }
                )
            else:
                match["item"] = item
                out.append(match)
        return out

    def _match_variable_rule(
        self,
        values: list[str],
        rules: dict,
        variable_keys: set[str],
    ) -> Optional[dict]:
        if not values:
            return None
        for rule in rules.get("value_regexes", []):
            matched = False
            if "value_sets" in rule:
                matched = _value_set_matches(values, rule["value_sets"]) is not None
            elif "patterns" in rule:
                if _all_match_patterns(values, rule["patterns"]):
                    if "range" in rule and not _all_match_range(values, rule["range"]):
                        matched = False
                    else:
                        matched = True
            if not matched:
                continue

            preds = rule.get("variable_predicates", [])
            contains = rule.get("variable_predicates_contains", [])
            candidates = [
                k
                for k in variable_keys
                if any(p in k for p in preds) or any(c in k for c in contains)
            ]
            if not candidates:
                continue
            if len(candidates) > 1:
                # Abstain: pattern matches but multiple variables qualify.
                return {
                    "item": "",  # filled by caller
                    "match": None,
                    "confidence": 0.0,
                    "reason": f"value pattern matches {len(candidates)} variables",
                }
            return {
                "item": "",
                "match": candidates[0],
                "confidence": 0.9,
                "reason": f"Value pattern '{rule.get('name')}' matched the distinct values.",
            }
        return None

    def _run_values(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
        rules: dict,
    ) -> list[dict]:
        out: list[dict] = []
        for item in items:
            terms = schema_slice.get(item, [])
            record = self._match_value_term(item, terms, rules)
            out.append(record)
        return out

    def _match_value_term(self, item: str, terms: list[str], rules: dict) -> dict:
        """Map a single value to its term using positional correspondence.

        Value-sets within a rule are ordered equivalence classes across
        languages/codings (e.g. ``["ja","nee"]`` and ``["yes","no"]``). The
        value's position in its matched set determines which term it maps to:
        the element at the same position in any set that is also an exact
        schema term is the suggestion.
        """
        value = item
        norm_value = str(value).strip().lower()
        for rule in rules.get("value_regexes", []):
            if "value_sets" not in rule:
                continue
            # Find the position of value in any value-set.
            position: Optional[int] = None
            for vs in rule["value_sets"]:
                norm_vs = [str(c).strip().lower() for c in vs]
                try:
                    position = norm_vs.index(norm_value)
                    break
                except ValueError:
                    continue
            if position is None:
                continue
            # At the same position in every set, look for a schema term.
            term_preds = rule.get("term_predicates", [])
            terms_lower = {t.lower(): t for t in terms}
            candidates: list[str] = []
            seen: set[str] = set()
            for vs in rule["value_sets"]:
                if position >= len(vs):
                    continue
                elem = str(vs[position]).strip().lower()
                term = terms_lower.get(elem)
                if term and term not in seen:
                    if not term_preds or any(p in term.lower() for p in term_preds):
                        candidates.append(term)
                        seen.add(term)
            if not candidates:
                continue
            if len(candidates) == 1:
                return {
                    "item": item,
                    "match": candidates[0],
                    "confidence": 0.9,
                    "reason": f"Value '{value}' matched the {rule.get('name')} pattern.",
                }
            return {
                "item": item,
                "match": None,
                "confidence": 0.0,
                "reason": f"value pattern matches {len(candidates)} terms",
            }
        return {
            "item": item,
            "match": None,
            "confidence": 0.0,
            "reason": "No value-type pattern matched.",
        }


# ---------------------------------------------------------------------------
# (c) Normalised token similarity matcher
# ---------------------------------------------------------------------------


def _score_string(item_tokens: list[str], candidate_label: str, rules: dict) -> float:
    """Best of Jaro-Winkler on joined tokens and Jaccard on token sets."""
    cand_tokens = tokenise(candidate_label, rules)
    if not item_tokens or not cand_tokens:
        return 0.0
    joined_item = " ".join(item_tokens)
    joined_cand = " ".join(cand_tokens)
    jw = jaro_winkler(joined_item, joined_cand)
    jac = jaccard(set(item_tokens), set(cand_tokens))
    return max(jw, jac)


class StringMatcher:
    """Normalised token similarity matcher (``source: string``)."""

    tier = TIER
    source = SOURCE_STRING

    def __init__(self, margin: Optional[float] = None):
        self.margin = margin

    def run(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
    ) -> list[dict]:
        rules = ctx.rules or load_rules()
        margin = self.margin if self.margin is not None else ctx.margin
        # In the variables phase the schema_slice has a single "*" key whose
        # value is the list of all variable keys. In the values phase each
        # value maps to its own list of terms — there is no "*" key, so we
        # collect all terms across the slice.
        targets = schema_slice.get("*", [])
        if not targets:
            targets = set()
            for terms in schema_slice.values():
                if isinstance(terms, list):
                    targets.update(terms)
            targets = list(targets)

        # Pre-compute candidate labels (key + label surface).
        mapping = ctx.mapping
        candidates: list[tuple[str, str]] = []
        for key in targets:
            label = key.replace("_", " ")
            if mapping is not None:
                variable = mapping.get_variable(key)
                vlabel = _variable_label(variable)
                if vlabel:
                    label = vlabel
            candidates.append((key, label))

        out: list[dict] = []
        for item in items:
            item_tokens = tokenise(item, rules)
            if not item_tokens:
                out.append(
                    {
                        "item": item,
                        "match": None,
                        "confidence": 0.0,
                        "reason": "Empty label after normalisation.",
                    }
                )
                continue

            scored: list[tuple[str, float]] = []
            for key, label in candidates:
                score = _score_string(item_tokens, label, rules)
                scored.append((key, score))
            scored.sort(key=lambda x: x[1], reverse=True)

            top1 = scored[0][1] if scored else 0.0
            top2 = scored[1][1] if len(scored) > 1 else 0.0
            if top1 - top2 < margin:
                out.append(
                    {
                        "item": item,
                        "match": None,
                        "confidence": round(top1, 4),
                        "reason": (
                            f"Top candidates too close (top1={top1:.3f}, "
                            f"top2={top2:.3f}, margin={margin}); abstaining."
                        ),
                    }
                )
                continue

            best_key, best_score = scored[0]
            confidence = round(best_score, 4)
            out.append(
                {
                    "item": item,
                    "match": best_key,
                    "confidence": confidence,
                    "reason": f"String similarity to '{best_key.replace('_', ' ')}' ({confidence:.2f}).",
                }
            )
        return out


# ---------------------------------------------------------------------------
# Cascade entry point
# ---------------------------------------------------------------------------


def tier1_producers() -> list:
    """Return the three tier-1 matchers in cascade order."""
    return [AliasMatcher(), ValueRegexMatcher(), StringMatcher()]
