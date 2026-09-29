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
    Jaro-Winkler similarity and are scaled by 0.9, but only when the two
    labels do not differ merely in digits and no other alias key with a
    different target is within ``margin`` of the best hit — ``surv1``
    must never fuzzy-match another site's ``surv7``. A label that maps
    to different targets at different sites abstains instead of letting
    the first site win silently.

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
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Callable, Optional

from . import SuggestionContext

logger = logging.getLogger(__name__)

SOURCE_ALIAS = "alias"
SOURCE_VALUE_REGEX = "value_regex"
SOURCE_STRING = "string"

TIER = 1

# Near-hit floor for fuzzy alias matches. Jaro-Winkler('morf', 'morph') is
# 0.848, so the floor must sit below that for the headline near-miss example
# to fire; the resulting confidence (0.9 x similarity) stays modest so the
# item can still escalate to a later tier.
ALIAS_SIMILARITY_FLOOR = 0.84

# Value-based variable recommendations: in the variables phase, check a
# column's distinct values against the value_regexes in
# suggestion_rules.json to suggest which schema variable the column maps
# to. The matcher works (its tests run against it) but collecting the
# distinct values costs one RDF-store query per column on every job start,
# which is too expensive for the air-gapped target. The feature will be
# re-enabled once categorical columns can be detected without querying the
# store. Read the flag through the accessor below (never import it by
# value) so tests can flip it with a patch and a future toggle cannot
# diverge from what the matcher sees.
VALUE_BASED_VARIABLE_SUGGESTIONS = False


def value_based_variable_suggestions_enabled() -> bool:
    """Call-time view of VALUE_BASED_VARIABLE_SUGGESTIONS."""
    return VALUE_BASED_VARIABLE_SUGGESTIONS


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


def jaro_winkler(
    a: str, b: str, prefix_weight: float = 0.1, min_score: float = 0.0
) -> float:
    """Return Jaro-Winkler similarity in [0, 1].

    ``min_score`` is a hot-loop escape hatch: when the matches seen so far
    prove the final score cannot reach it, the function bails out and
    returns 0.0 early instead of finishing the O(len^2) scan. Callers that
    only compare the result against their floor (the alias matcher) pass
    the floor; without ``min_score`` the result is exact.

    The match scan uses ``str.find`` (C speed) instead of a per-character
    Python loop over the match window: on the target profile the fuzzy
    pass over thousands of remembered labels dominated the job runtime.
    """
    a = a or ""
    b = b or ""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0

    la, lb = len(a), len(b)
    match_distance = max(la, lb) // 2 - 1
    if match_distance < 0:
        match_distance = 0
    a_matches = bytearray(la)
    b_matches = bytearray(lb)
    matches = 0
    max_prefix_boost = 4 * prefix_weight
    shares_first_char = a[0] == b[0]

    for i, ch in enumerate(a):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, lb)
        j = b.find(ch, start, end)
        while j != -1 and b_matches[j]:
            j = b.find(ch, j + 1, end)
        if j != -1:
            a_matches[i] = 1
            b_matches[j] = 1
            matches += 1
        elif min_score:
            # Even if every remaining character of a matched, could the
            # final score reach the floor?
            m_max = matches + la - i - 1
            if m_max > lb:
                m_max = lb
            jaro_max = (m_max / la + m_max / lb + 1.0) / 3.0
            if jaro_max < 1.0 and shares_first_char:
                jaro_max = jaro_max + max_prefix_boost * (1.0 - jaro_max)
            if jaro_max < min_score:
                return 0.0

    if matches == 0:
        return 0.0

    transpositions = 0
    k = 0
    for i in range(la):
        if not a_matches[i]:
            continue
        while not b_matches[k]:
            k += 1
        if a[i] != b[k]:
            transpositions += 1
        k += 1
    transpositions //= 2

    jaro = (matches / la + matches / lb + (matches - transpositions) / matches) / 3.0

    prefix = 0
    for i in range(min(4, la, lb)):
        if a[i] == b[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * prefix_weight * (1.0 - jaro)


def jaro_winkler_upper_bound(
    a: str, b: str, a_counts: Counter, b_counts: Counter
) -> float:
    """Cheap sound upper bound on :func:`jaro_winkler` for ``a`` vs ``b``.

    The number of matching positions is bounded by the multiset
    intersection of the two strings' characters, which bounds Jaro, and
    the Winkler prefix boost adds at most ``0.4 * (1 - jaro)``. Both
    Counter arguments are pre-computed by the caller; the bound costs a
    pass over the distinct characters instead of the O(len^2) match
    scan, so matchers can skip candidates that cannot reach their floor
    or the running top-2 without computing the real similarity.
    """
    la, lb = len(a), len(b)
    if not la or not lb:
        return 0.0
    m = sum(min(n, b_counts.get(c, 0)) for c, n in a_counts.items())
    jaro_bound = (m / la + m / lb + 1.0) / 3.0
    if jaro_bound >= 1.0:
        return 1.0
    return min(1.0, 0.4 + 0.6 * jaro_bound)


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


class AliasMemory(dict):
    """``normalise(label) -> (target, source_db)`` memory for the alias matcher.

    A plain dict, plus a ``conflicts`` side-table: when the same normalised
    label maps to different targets at different sites the label is
    recorded there (with both targets) and the matcher must abstain for it
    instead of letting whichever site was iterated first win silently.
    """

    def __init__(self) -> None:
        super().__init__()
        self.conflicts: dict[str, tuple[str, str]] = {}

    def add(self, label: str, target: str, source_db: str) -> None:
        existing = self.get(label)
        if existing is None:
            super().__setitem__(label, (target, source_db))
            return
        if existing[0] != target:
            self.conflicts[label] = (existing[0], target)


def _default_name_match(mapping_db_name: str, described_database: str) -> bool:
    """Fallback database-name match when no matcher is injected.

    Mirrors the semantics of RDFStoreService.graph_database_find_name_match
    for the common case: an unnamed database matches anything, otherwise
    the names must be equal.
    """
    if not mapping_db_name:
        return True
    return mapping_db_name == described_database


def build_alias_memory(
    mapping: Any,
    described_database: Optional[str],
    name_match: Optional[Callable[[str, str], bool]] = None,
) -> AliasMemory:
    """Build ``normalise(label) -> target`` memory, excluding ``described_database``.

    For the variables phase the target is a variable key; for the values
    phase callers pass a pre-built ``localValue -> term`` memory via
    :func:`build_value_alias_memory`.
    """
    memory = AliasMemory()
    match = name_match or _default_name_match
    for db, column in _iter_columns(mapping):
        if described_database and match(db.name, described_database):
            continue
        local = column.local_column
        if not local:
            continue
        key = normalise_label(str(local))
        var_key = column.get_variable_key()
        if not var_key or not key:
            continue
        memory.add(key, var_key, db.name or "")
    return memory


def build_value_alias_memory(
    mapping: Any,
    described_database: Optional[str],
    name_match: Optional[Callable[[str, str], bool]] = None,
) -> AliasMemory:
    """Build ``normalise(value) -> (term, db)`` memory from localMappings."""
    memory = AliasMemory()
    match = name_match or _default_name_match
    for db, column in _iter_columns(mapping):
        if described_database and match(db.name, described_database):
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
                    memory.add(key, str(term), db.name or "")
    return memory


# ---------------------------------------------------------------------------
# (a) Alias matcher
# ---------------------------------------------------------------------------


def _differs_only_in_digits(a: str, b: str) -> bool:
    """True when two normalised labels differ only in their digits.

    ``surv1`` vs ``surv7`` is the README's own warning case: Jaro-Winkler
    rates the pair 0.92, but the digits are exactly what distinguishes
    them, so a fuzzy hit between them must never fire.
    """
    a_letters = re.sub(r"\d", "", a)
    b_letters = re.sub(r"\d", "", b)
    return bool(a_letters.strip()) and a_letters == b_letters


class AliasMatcher:
    """Alias memory matcher (``source: alias``)."""

    tier = TIER
    source = SOURCE_ALIAS

    def __init__(
        self, similarity_floor: Optional[float] = None, margin: Optional[float] = None
    ):
        self.similarity_floor = (
            ALIAS_SIMILARITY_FLOOR if similarity_floor is None else similarity_floor
        )
        self.margin = margin

    def run(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
    ) -> list[dict]:
        phase = ctx.phase
        margin = self.margin if self.margin is not None else ctx.margin
        targets = set(schema_slice.get("*", []))
        if phase == "values":
            memory = build_value_alias_memory(
                ctx.mapping, ctx.described_database, ctx.database_name_match
            )
            kind = "value"
            kind_target = "term"
        else:
            memory = build_alias_memory(
                ctx.mapping, ctx.described_database, ctx.database_name_match
            )
            kind = "column"
            kind_target = "variable"

        # Fuzzy-scan accelerators, computed once per run:
        # - keys bucketed by first character: the similarity floor (0.84)
        #   is out of reach in practice without a shared first character,
        #   so the scan only touches the item's own bucket;
        # - character counts per key for the cheap sound upper bound that
        #   replaces the O(len^2) similarity for hopeless candidates.
        by_first_char: dict[str, list[str]] = {}
        for key in memory:
            if key:
                by_first_char.setdefault(key[0], []).append(key)
        key_counts = {key: Counter(key) for key in memory}

        def _no_hit(item: str, reason: str = "No alias memory hit.") -> dict:
            return {"item": item, "match": None, "confidence": 0.0, "reason": reason}

        records: list[dict] = []
        for item in items:
            norm = normalise_label(item)
            if not norm:
                records.append(_no_hit(item, "Empty label."))
                continue

            if norm in memory.conflicts:
                # The label maps to different targets at different sites;
                # whichever site won would be arbitrary, so abstain and
                # name both candidates.
                target_a, target_b = memory.conflicts[norm]
                records.append(
                    _no_hit(
                        item,
                        f"Alias conflict: '{item}' is mapped to both "
                        f"'{target_a}' and '{target_b}' at other sites; "
                        "cannot choose between them.",
                    )
                )
                continue

            exact = memory.get(norm)
            best: Optional[tuple[str, float, str]] = None
            if exact:
                target, source_db = exact
                if not targets or target in targets:
                    best = (target, 1.0, source_db)

            if best is None:
                best, abstain_reason = self._fuzzy_best(
                    norm,
                    Counter(norm),
                    key_counts,
                    by_first_char,
                    memory,
                    targets,
                    margin,
                )
                if best is None:
                    records.append(_no_hit(item, abstain_reason))
                    continue

            target, sim, source_db = best
            confidence = 1.0 if sim >= 1.0 else round(0.9 * sim, 4)
            reason = (
                f"Alias: {kind} '{item}' in database '{source_db}' "
                f"is mapped to this {kind_target}."
            )
            records.append(
                {
                    "item": item,
                    "match": target,
                    "confidence": confidence,
                    "reason": reason,
                }
            )
        return records

    def _fuzzy_best(
        self,
        norm: str,
        norm_counts: Counter,
        key_counts: dict[str, Counter],
        by_first_char: dict[str, list[str]],
        memory: AliasMemory,
        targets: set[str],
        margin: float,
    ) -> tuple[Optional[tuple[str, float, str]], Optional[str]]:
        """Best fuzzy near-hit for ``norm`` over the alias keys.

        Returns ``(best, None)`` on success or ``(None, reason)`` when the
        matcher must abstain. Rejects candidates whose label differs from
        ``norm`` only in digits, and abstains when the two best candidates
        with *distinct* targets are closer than ``margin`` (the sibling
        guard): a near miss against one remembered column is a hint, a
        coin flip between two remembered columns is not.
        """
        candidates: list[tuple[str, float, str]] = []
        # The bucket already guarantees a shared first character.
        for key in by_first_char.get(norm[0], ()):
            target, source_db = memory[key]
            if key in memory.conflicts:
                continue
            if targets and target not in targets:
                continue
            if _differs_only_in_digits(norm, key):
                continue
            # Cheap blocks before the O(len^2) similarity. With thousands
            # of remembered labels these cut the real comparisons to a
            # small fraction:
            # - sound: a candidate whose Jaro-Winkler upper bound (from
            #   the multiset char overlap) cannot reach the floor is
            #   never a hit;
            # - heuristic: the floor sits at 0.84, which in practice
            #   needs a length gap under half the longer label.
            if abs(len(norm) - len(key)) > max(len(norm), len(key)) // 2:
                continue
            if (
                jaro_winkler_upper_bound(norm, key, norm_counts, key_counts[key])
                < self.similarity_floor
            ):
                continue
            sim = jaro_winkler(norm, key, min_score=self.similarity_floor)
            if sim >= self.similarity_floor:
                candidates.append((target, sim, source_db))
        if not candidates:
            return None, None

        candidates.sort(key=lambda c: c[1], reverse=True)
        best = candidates[0]
        for target, sim, _source_db in candidates[1:]:
            if target == best[0]:
                continue
            if best[1] - sim < margin:
                # Distinct targets within the margin: abstain. The margin
                # wording matches the string matcher's so the cascade merge
                # keeps it as the most informative reason.
                return None, (
                    f"Top alias candidates too close (top1={best[1]:.3f}, "
                    f"top2={sim:.3f}, margin={margin}); abstaining."
                )
            break
        return best, None


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


def _column_inside_one_set(values: list[str], candidate_sets: list[list[str]]) -> bool:
    """True when every value falls inside ONE of the candidate sets.

    A value-set rule describes a whole column's coding scheme, not single
    values: a column holding ``{ja, y}`` must not get a yes-suggestion for
    ``ja`` just because ``ja`` is in the Dutch yes/no set.
    """
    if not values:
        return False
    norm = {str(v).strip().lower() for v in values}
    for cand in candidate_sets:
        if norm <= {str(c).strip().lower() for c in cand}:
            return True
    return False


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

        if ctx.phase == "variables":
            if not value_based_variable_suggestions_enabled():
                return [
                    {
                        "item": item,
                        "match": None,
                        "confidence": 0.0,
                        "reason": "Value-based variable suggestions are disabled.",
                    }
                    for item in items
                ]
            return self._run_variables(items, schema_slice, ctx, rules)
        return self._run_values(items, schema_slice, ctx, rules)

    def _run_variables(
        self,
        items: list[str],
        schema_slice: dict[str, list[str]],
        ctx: SuggestionContext,
        rules: dict,
    ) -> list[dict]:
        targets = schema_slice.get("*", [])
        variable_keys = {v for v in targets}
        out: list[dict] = []

        for item in items:
            # Only the described database's values are relevant; a column
            # with the same name in another database may hold entirely
            # different values.
            distinct = (
                (ctx.column_values or {}).get(ctx.described_database, {}).get(item, [])
            )
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
            record = self._match_value_term(item, terms, rules, ctx)
            out.append(record)
        return out

    def _match_value_term(
        self, item: str, terms: list[str], rules: dict, ctx: SuggestionContext
    ) -> dict:
        """Map a single value to its term using positional correspondence.

        Value-sets within a rule are ordered equivalence classes across
        languages/codings (e.g. ``["ja","nee"]`` and ``["yes","no"]``). The
        value's position in its matched set determines which term it maps to:
        the element at the same position in any set that is also an exact
        schema term is the suggestion.

        A value-set rule only applies when every non-missing distinct value
        of the column the value belongs to falls inside one of the rule's
        sets — a rule describes a coding scheme, not individual values.
        Missing codes are handled by their own rule first: they map to the
        term that names a missing/unknown/unspecified category.
        """
        value = item
        norm_value = str(value).strip().lower()

        missing_record = self._match_missing_code(value, terms, rules)
        if missing_record is not None:
            return missing_record

        # All non-missing distinct values of the column this value belongs
        # to. Without column context (e.g. matcher used standalone) the
        # value alone decides, matching the pre-existing behaviour.
        column_values = (ctx.item_column_values or {}).get(value)
        if column_values is None:
            column_values = [value]
        column_values = _strip_missing(column_values, rules)

        for rule in rules.get("value_regexes", []):
            if "value_sets" not in rule:
                continue
            if not _column_inside_one_set(column_values, rule["value_sets"]):
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

    @staticmethod
    def _match_missing_code(
        value: str, terms: list[str], rules: dict
    ) -> Optional[dict]:
        """Suggest the missing/unknown term for a value that is a missing code.

        Returns None when the value is not a missing code (so the caller
        falls through to the value-set rules), a record when it is. The
        target term is chosen by the rule file's ``missing_code_rule``
        ``term_predicates``; more than one matching term abstains.
        """
        if str(value).strip().lower() not in _missing_codes(rules):
            return None
        rule = rules.get("missing_code_rule") or {}
        preds = rule.get("term_predicates") or ["missing", "unknown", "unspecified"]
        candidates: list[str] = []
        seen: set[str] = set()
        for term in terms:
            if term in seen:
                continue
            if any(p in term.lower() for p in preds):
                candidates.append(term)
                seen.add(term)
        if not candidates:
            return None
        if len(candidates) == 1:
            return {
                "item": value,
                "match": candidates[0],
                "confidence": 0.9,
                "reason": f"Value '{value}' is a missing code.",
            }
        return {
            "item": value,
            "match": None,
            "confidence": 0.0,
            "reason": f"missing code matches {len(candidates)} terms",
        }


# ---------------------------------------------------------------------------
# (c) Normalised token similarity matcher
# ---------------------------------------------------------------------------


def _score_tokens(item_tokens: list[str], cand_tokens: list[str]) -> float:
    """Best of Jaro-Winkler on joined tokens and Jaccard on token sets.

    Both token lists are pre-computed by the caller: tokenising every
    candidate label for every item used to dominate the runtime on
    large schemas (600 columns x every variable key re-tokenised the
    candidate side on each of the 600 passes).
    """
    if not item_tokens or not cand_tokens:
        return 0.0
    jw = jaro_winkler(" ".join(item_tokens), " ".join(cand_tokens))
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

        # Pre-compute candidate labels AND their tokens once: scoring one
        # item must not re-tokenise every candidate (see _score_tokens).
        mapping = ctx.mapping
        candidates: list[tuple[str, list[str]]] = []
        for key in targets:
            label = key.replace("_", " ")
            if mapping is not None:
                variable = mapping.get_variable(key)
                vlabel = _variable_label(variable)
                if vlabel:
                    label = vlabel
            candidates.append((key, tokenise(label, rules)))

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
            for key, cand_tokens in candidates:
                score = _score_tokens(item_tokens, cand_tokens)
                scored.append((key, score))
            scored.sort(key=lambda x: x[1], reverse=True)

            top1 = scored[0][1] if scored else 0.0
            top2 = scored[1][1] if len(scored) > 1 else 0.0
            if top1 - top2 < margin:
                out.append(
                    {
                        "item": item,
                        "match": None,
                        # Abstains always carry confidence 0 so they never
                        # beat a real match in the cascade merge and always
                        # escalate to the next tier; the raw scores live in
                        # the reason text.
                        "confidence": 0.0,
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
