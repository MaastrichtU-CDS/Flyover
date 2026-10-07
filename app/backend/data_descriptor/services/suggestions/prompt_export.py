"""
Prompt export for the LLM paste-back round trip.

Many sites cannot run a model next to Flyover but do have an LLM they may
use elsewhere (an institutional ChatGPT/Copilot licence, an internal Ollama
box). This module composes a self-contained prompt per database and phase
that such a user can paste into any LLM client, and whose answer
:mod:`.pasted_answer` turns back into ordinary suggestion records.

The answer format is deliberately the JSON-LD ``databases`` section Flyover
itself writes when a column or value is mapped (``mapsTo`` /
``localColumn`` / ``localMappings``), so the reply is a valid slice of the
semantic map that merges with the entries already there. The prompt only
asks about items that still need a decision and shows the existing
mappings as context, never as work to redo.

Provider-agnostic on purpose: plain text, no system prompt, no JSON mode,
no tool use, a worked answer skeleton, and chunking for small context
windows. Each chunk repeats the schema slice and the existing mappings so
it can be sent on its own.

Privacy: the prompt never contains data rows. The variables phase shares
only the column names — no distinct values at all; if a user wants an
LLM to reason over the values, the values-phase prompt is the place. In
the values phase the distinct values of a categorical column are the
very things being mapped; a mapped column is categorical by definition
(it may hold string values, but it is never treated as free text), so
every mapped column with terms is asked.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

PASTED_SOURCE = "pasted_llm"
PASTED_TIER = 3

# Items per prompt when the caller does not choose (env override below).
DEFAULT_CHUNK = 40
MIN_CHUNK = 5
MAX_CHUNK = 1000

# Confidence stamped on a pasted record that carries none of its own. Kept
# under the default 0.8 threshold so the variables page shows it as a hint
# to accept rather than a pre-filled answer.
DEFAULT_PASTED_CONFIDENCE = 0.7

ValueSource = Callable[[str], list[str]]
NameMatch = Callable[[str, str], bool]


def chunk_size_from_env() -> int:
    """``FLYOVER_SUGGESTION_PROMPT_CHUNK`` clamped to a sane range."""
    raw = os.getenv("FLYOVER_SUGGESTION_PROMPT_CHUNK", str(DEFAULT_CHUNK))
    try:
        return clamp_chunk(int(raw))
    except (TypeError, ValueError):
        return DEFAULT_CHUNK


def clamp_chunk(value: Any) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return DEFAULT_CHUNK
    return max(MIN_CHUNK, min(MAX_CHUNK, n))


def default_name_match(map_db_name: str, target_db: str) -> bool:
    """Same rule as ``jsonld.graphDatabaseFindNameMatch`` in the frontend."""
    if not map_db_name:
        return True
    if map_db_name == target_db:
        return True
    a = map_db_name[:-4] if map_db_name.endswith(".csv") else map_db_name
    b = target_db[:-4] if target_db and target_db.endswith(".csv") else target_db
    return a == b


# ---------------------------------------------------------------------------
# JSON-LD side: what is already mapped for the described database
# ---------------------------------------------------------------------------


def find_database(
    mapping: Any, database: str, name_match: NameMatch = default_name_match
):
    """Return ``(db_key, db, table_key, table)`` of the mapping entry for ``database``.

    The first table of the first database whose ``name`` or any table's
    ``sourceFile`` matches wins, mirroring ``updateMappingFromForm``. When
    the mapping does not know the database yet, the keys default to the
    snake-cased database name and ``data`` (what the frontend creates on
    first mapping), and ``db``/``table`` are None.
    """
    for db_key, db in (getattr(mapping, "databases", None) or {}).items():
        tables = getattr(db, "tables", None) or {}
        table_key = next(iter(tables), None)
        for t_key, table in tables.items():
            if name_match(getattr(table, "source_file", "") or "", database):
                return db_key, db, t_key, table
        if name_match(getattr(db, "name", "") or "", database):
            return db_key, db, table_key, tables.get(table_key) if table_key else None
    return database.lower().replace(" ", "_"), None, "data", None


def mapped_columns(
    mapping: Any, database: str, name_match: NameMatch = default_name_match
) -> dict[str, str]:
    """``local column -> variable key`` for the described database."""
    _, db, _, _ = find_database(mapping, database, name_match)
    out: dict[str, str] = {}
    if db is None:
        return out
    for table in (getattr(db, "tables", None) or {}).values():
        for column in (getattr(table, "columns", None) or {}).values():
            local = getattr(column, "local_column", None)
            var_key = (
                column.get_variable_key()
                if hasattr(column, "get_variable_key")
                else None
            )
            if local and var_key and local not in out:
                out[str(local)] = var_key
    return out


def local_mappings(
    mapping: Any,
    database: str,
    column_name: str,
    name_match: NameMatch = default_name_match,
) -> dict[str, list[str]]:
    """``term key -> [local values]`` already recorded for one column."""
    _, db, _, _ = find_database(mapping, database, name_match)
    if db is None:
        return {}
    for table in (getattr(db, "tables", None) or {}).values():
        for column in (getattr(table, "columns", None) or {}).values():
            if str(getattr(column, "local_column", "") or "") != column_name:
                continue
            raw = getattr(column, "local_mappings", None) or {}
            out: dict[str, list[str]] = {}
            for term, values in raw.items():
                if values is None:
                    continue
                if not isinstance(values, list):
                    values = [values]
                out[str(term)] = [str(v) for v in values if v is not None]
            return out
    return {}


def existing_section(
    mapping: Any,
    database: str,
    phase: str,
    name_match: NameMatch = default_name_match,
) -> dict:
    """The ``columns`` object Flyover already holds for ``database``.

    Only the fields the answer is allowed to use are shown: ``mapsTo`` and
    ``localColumn`` (plus ``localMappings`` in the values phase).
    """
    _, db, _, _ = find_database(mapping, database, name_match)
    columns: dict[str, dict] = {}
    if db is None:
        return columns
    for table in (getattr(db, "tables", None) or {}).values():
        for key, column in (getattr(table, "columns", None) or {}).items():
            entry = {
                "mapsTo": getattr(column, "maps_to", ""),
                "localColumn": getattr(column, "local_column", None) or None,
            }
            if phase == "values":
                lm = getattr(column, "local_mappings", None) or {}
                if lm:
                    entry["localMappings"] = lm
            columns[key] = entry
    return columns


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Hints from the tier-1 job
# ---------------------------------------------------------------------------


def hint_for(records: Optional[dict], key: str) -> Optional[str]:
    """One-line hint from the existing job record for ``key``, or None."""
    record = (records or {}).get(key)
    if not record:
        return None
    match = record.get("match")
    if not match:
        return "no candidate"
    confidence = record.get("confidence")
    try:
        pct = f"{float(confidence):.2f}"
    except (TypeError, ValueError):
        pct = "?"
    return f"{match} ({pct}, {record.get('source', 'rule')})"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _label(key: str) -> str:
    return key.replace("_", " ").strip().capitalize()


def _variable_meta(mapping_data: Optional[dict], key: str) -> dict:
    variables = ((mapping_data or {}).get("schema") or {}).get("variables") or {}
    return variables.get(key) or {}


def _render_variable_line(
    mapping: Any, mapping_data: Optional[dict], key: str, with_terms: bool
) -> str:
    var = mapping.get_variable(key)
    meta = _variable_meta(mapping_data, key)
    label = meta.get("label") or meta.get("name") or _label(key)
    data_type = getattr(var, "data_type", None) or meta.get("dataType") or ""
    line = f'- {key}: "{label}"'
    if data_type:
        line += f" ({data_type})"
    description = meta.get("description")
    if description:
        desc = str(description).strip().replace("\n", " ")
        if len(desc) > 160:
            desc = desc[:157] + "..."
        line += f" — {desc}"
    terms = getattr(var, "value_mappings", None) or {}
    if with_terms and terms:
        rendered = ", ".join(
            f"{term} ({target})" if target else term for term, target in terms.items()
        )
        line += f"\n    terms: {rendered}"
    return line


def _json(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


def _header(phase: str, database: str) -> str:
    what = "columns" if phase == "variables" else "values"
    return (
        "You are helping a data steward describe a local dataset with Flyover, a tool "
        "that maps local data to a shared semantic schema. Flyover records mappings as "
        "a JSON-LD document: `schema.variables` defines the allowed target variables "
        f"(and, for categorical variables, their terms), and `databases` records which "
        f"local column (and which local value) maps to which variable (and term).\n\n"
        f'Your task: for the local database "{database}", propose mappings for the '
        f"{what} listed in section 3 that are still unmapped. Everything you need is in "
        "this message; do not assume anything about the data beyond it.\n\n"
        "The answer may only use keys from the lists below, character-for-character. "
        "A person will review every proposal in Flyover; nothing is saved automatically."
    )


def _privacy_note(phase: str) -> str:
    """What the prompt carries, worded per phase.

    The user must see exactly what leaves the browser (the issue's
    privacy goal): the variables phase shares no values at all, the
    values phase shares the distinct values being mapped.
    """
    if phase == "variables":
        return (
            "This prompt contains variable keys and labels and the local "
            "column names. It contains no values and no data rows."
        )
    return (
        "This prompt contains variable keys, their term keys, the local column "
        "names and the distinct values of the mapped categorical columns. "
        "It contains no data rows."
    )


class PromptExport:
    """Builder for one prompt export request (one database, one phase)."""

    def __init__(
        self,
        phase: str,
        database: str,
        mapping: Any,
        *,
        columns: list[str],
        distinct_values: Optional[ValueSource] = None,
        records: Optional[dict] = None,
        mapping_data: Optional[dict] = None,
        chunk_size: Optional[int] = None,
        name_match: NameMatch = default_name_match,
    ) -> None:
        if phase not in ("variables", "values"):
            raise ValueError(f"unknown phase '{phase}'")
        self.phase = phase
        self.database = database
        self.mapping = mapping
        self.mapping_data = mapping_data
        self.columns = list(columns or [])
        self.distinct_values = distinct_values
        self.records = records or {}
        self.chunk_size = (
            clamp_chunk(chunk_size) if chunk_size else chunk_size_from_env()
        )
        self.name_match = name_match
        self.db_key, _, self.table_key, _ = find_database(mapping, database, name_match)
        self.mapped = mapped_columns(mapping, database, name_match)
        self._value_cache: dict[str, Optional[list[str]]] = {}

    # -- data access -------------------------------------------------------

    def _values(self, column: str) -> Optional[list[str]]:
        if column in self._value_cache:
            return self._value_cache[column]
        values: Optional[list[str]] = None
        if self.distinct_values is not None:
            try:
                values = [str(v) for v in (self.distinct_values(column) or [])]
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("distinct values for %s failed: %s", column, exc)
                values = None
        self._value_cache[column] = values
        return values

    # -- item collection ---------------------------------------------------

    def variable_items(self) -> list[dict]:
        """Unmapped columns of the database, with hint.

        Names only: the variables phase shares no distinct values (the
        values-phase prompt is where values are mapped), so building the
        prompt costs no per-column store query either.
        """
        items = []
        for column in self.columns:
            if column in self.mapped:
                continue
            items.append(
                {
                    "column": column,
                    "key": f"{self.database}_{column}",
                    "hint": hint_for(self.records, f"{self.database}_{column}"),
                }
            )
        return items

    def value_groups(self) -> list[dict]:
        """Per mapped categorical column: the values still to map.

        A mapped column's variable defines its terms, so a column here is
        categorical by definition; it may hold string values, but it is
        never treated as free text — every mapped column with terms is
        asked (there is no free-text exclusion to configure).
        """
        groups: list[dict] = []
        for column in self.columns:
            var_key = self.mapped.get(column)
            if not var_key:
                continue
            variable = self.mapping.get_variable(var_key)
            terms = list((getattr(variable, "value_mappings", None) or {}).keys())
            if not terms:
                continue
            values = self._values(column)
            if not values:
                continue
            already = local_mappings(
                self.mapping, self.database, column, self.name_match
            )
            mapped_values = {v for vals in already.values() for v in vals}
            todo = [v for v in values if v != "" and v not in mapped_values]
            if not todo:
                continue
            groups.append(
                {
                    "column": column,
                    "variable": var_key,
                    "terms": terms,
                    "already": already,
                    "values": [
                        {
                            "value": v,
                            "key": f"{self.database}_{column}_{v}",
                            "hint": hint_for(
                                self.records, f"{self.database}_{column}_{v}"
                            ),
                        }
                        for v in todo
                    ],
                }
            )
        return groups

    # -- chunking ------------------------------------------------------------

    @staticmethod
    def _chunk_variables(items: list[dict], size: int) -> list[list[dict]]:
        return [items[i : i + size] for i in range(0, len(items), size)] or [[]]

    @staticmethod
    def _chunk_values(groups: list[dict], size: int) -> list[list[dict]]:
        """Whole columns per chunk, at most ``size`` values per chunk.

        A column with more values than ``size`` gets a chunk of its own
        rather than being split: the LLM needs a column's full value set to
        map its codes consistently.
        """
        chunks: list[list[dict]] = []
        current: list[dict] = []
        count = 0
        for group in groups:
            n = len(group["values"])
            if current and count + n > size:
                chunks.append(current)
                current, count = [], 0
            current.append(group)
            count += n
        if current:
            chunks.append(current)
        return chunks or [[]]

    # -- rendering -----------------------------------------------------------

    def _schema_section(self, variable_keys: list[str], with_terms: bool) -> str:
        lines = [
            _render_variable_line(self.mapping, self.mapping_data, k, with_terms)
            for k in variable_keys
        ]
        return "\n".join(lines) if lines else "(none)"

    def _existing_section_text(self) -> str:
        columns = existing_section(
            self.mapping, self.database, self.phase, self.name_match
        )
        if not columns:
            return "(nothing mapped yet)"
        return _json({"columns": columns})

    def _skeleton(self, entry: dict) -> str:
        return _json(
            {
                "databases": {
                    self.db_key: {
                        "tables": {
                            self.table_key: {"columns": {"<variable_key>": entry}}
                        }
                    }
                }
            }
        )

    def _render_variables_prompt(self, items: list[dict], part: tuple[int, int]) -> str:
        taken = set(self.mapped.values())
        free_keys = [k for k in self.mapping.get_all_variable_keys() if k not in taken]
        lines = [_header("variables", self.database)]
        if part[1] > 1:
            lines.append(
                f"(Part {part[0]} of {part[1]}: this part is complete on its own; "
                "answer it independently of the other parts.)"
            )
        lines.append(
            "\n## 1. Schema variables — the ONLY allowed targets (use these EXACT keys)\n"
            + self._schema_section(free_keys, with_terms=False)
        )
        lines.append(
            f'\n## 2. Database "{self.database}": columns already mapped (context only — '
            "do not repeat, do not reuse their variables)\n"
            + self._existing_section_text()
        )
        item_lines = []
        for item in items:
            line = f"- {item['column']}"
            if item["hint"]:
                line += f"    hint: {item['hint']}"
            item_lines.append(line)
        lines.append(
            "\n## 3. Local columns still to map (map these; names are exact)\n"
            + ("\n".join(item_lines) if item_lines else "(none)")
        )
        lines.append(
            "\nHints are Flyover's current suggestions (confidence 0–1, source: the "
            "rule-based matcher or an earlier pasted answer); confirm or overrule "
            "them, they are not authoritative."
        )
        entry = {
            "mapsTo": "schema:variable/<variable_key>",
            "localColumn": "<local column name exactly as in section 3>",
            "confidence": "<number between 0 and 1, optional>",
            "reason": "<one short sentence, optional>",
        }
        lines.append(
            "\n## 4. Answer format\n"
            "Answer with ONLY one JSON object and no text before or after it, in exactly "
            "this Flyover JSON-LD form (one entry per column you map):\n\n"
            + self._skeleton(entry)
            + "\n\nRules:\n"
            '- The entry key and "mapsTo" use a variable key from section 1 EXACTLY '
            "(character-for-character). Never invent a variable and never use a label.\n"
            '- "localColumn" is a column name from section 3 EXACTLY as written.\n'
            "- Each variable key may be used for at most one column, and the variables "
            "in section 2 are already taken.\n"
            "- Leave out columns you cannot map; a missing column is better than a guess.\n"
            '- "confidence" and "reason" are optional; Flyover shows them to the reviewer '
            "and drops them before the JSON-LD is saved.\n"
            "- Do not repeat the entries of section 2; Flyover merges your entries into them."
        )
        return "\n".join(lines)

    def _render_values_prompt(self, groups: list[dict], part: tuple[int, int]) -> str:
        variable_keys: list[str] = []
        for g in groups:
            if g["variable"] not in variable_keys:
                variable_keys.append(g["variable"])
        lines = [_header("values", self.database)]
        if part[1] > 1:
            lines.append(
                f"(Part {part[0]} of {part[1]}: this part is complete on its own; "
                "answer it independently of the other parts.)"
            )
        lines.append(
            "\n## 1. Schema variables and their terms — the ONLY allowed targets "
            "(use these EXACT term keys; the code in brackets is the ontology class)\n"
            + self._schema_section(variable_keys, with_terms=True)
        )
        lines.append(
            f'\n## 2. Database "{self.database}": existing mappings (context only — '
            "do not repeat)\n" + self._existing_section_text()
        )
        group_lines = []
        for g in groups:
            group_lines.append(
                f'Column "{g["column"]}" → variable {g["variable"]} '
                f"(terms: {', '.join(g['terms'])})"
            )
            if g["already"]:
                pairs = "; ".join(
                    f'"{v}" → {term}'
                    for term, vals in g["already"].items()
                    for v in vals
                )
                group_lines.append(f"  already mapped: {pairs}")
            group_lines.append("  values to map:")
            for v in g["values"]:
                line = f'  - "{v["value"]}"'
                if v["hint"]:
                    line += f"    hint: {v['hint']}"
                group_lines.append(line)
        lines.append(
            "\n## 3. Local values still to map (per column; values are exact strings)\n"
            + ("\n".join(group_lines) if group_lines else "(none)")
        )
        lines.append(
            "\nHints are Flyover's current suggestions (confidence 0–1, source: the "
            "rule-based matcher or an earlier pasted answer); confirm or overrule "
            "them, they are not authoritative."
        )
        entry = {
            "mapsTo": "schema:variable/<variable_key>",
            "localColumn": "<column name exactly as in section 3>",
            "localMappings": {"<term_key>": ["<local value>", "<local value>"]},
            "valueNotes": {
                "<local value>": {
                    "confidence": "<number between 0 and 1, optional>",
                    "reason": "<one short sentence, optional>",
                }
            },
        }
        lines.append(
            "\n## 4. Answer format\n"
            "Answer with ONLY one JSON object and no text before or after it, in exactly "
            "this Flyover JSON-LD form (one entry per column from section 3):\n\n"
            + self._skeleton(entry)
            + "\n\nRules:\n"
            '- The entry key and "mapsTo" use the column\'s variable key from section 3; '
            '"localColumn" is the column name EXACTLY as written.\n'
            '- Each "localMappings" key is a term key of THAT variable from section 1 '
            "EXACTLY (character-for-character); never invent a term.\n"
            "- Local values are the strings from section 3 EXACTLY as written (keep "
            'them as strings, e.g. "1" not 1); each value under at most one term.\n'
            "- Leave out values you cannot map; a missing value is better than a guess.\n"
            '- "valueNotes" is optional; Flyover shows confidence and reason to the '
            "reviewer and drops them before the JSON-LD is saved.\n"
            "- Do not repeat the mappings of section 2; Flyover merges yours into them."
        )
        return "\n".join(lines)

    # -- public --------------------------------------------------------------

    def build(self) -> dict:
        """Compose the prompt(s) and the metadata the UI shows."""
        contains = [
            "variable keys",
            "variable labels",
            "column names",
            "existing mappings",
        ]
        if self.phase == "variables":
            items = self.variable_items()
            item_count = len(items)
            parts = self._chunk_variables(items, self.chunk_size)
            chunks = [
                {
                    "index": i + 1,
                    "items": [it["column"] for it in part],
                    "item_count": len(part),
                    "prompt": self._render_variables_prompt(part, (i + 1, len(parts))),
                }
                for i, part in enumerate(parts)
            ]
        else:
            groups = self.value_groups()
            item_count = sum(len(g["values"]) for g in groups)
            contains.append("term keys")
            contains.append("distinct values of the categorical columns being mapped")
            parts = self._chunk_values(groups, self.chunk_size)
            chunks = [
                {
                    "index": i + 1,
                    "items": [g["column"] for g in part],
                    "item_count": sum(len(g["values"]) for g in part),
                    "prompt": self._render_values_prompt(part, (i + 1, len(parts))),
                }
                for i, part in enumerate(parts)
            ]
        if item_count == 0:
            chunks = []
        return {
            "phase": self.phase,
            "database": self.database,
            "prompt": chunks[0]["prompt"] if chunks else "",
            "chunks": chunks,
            "item_count": item_count,
            "chunk_hint": self.chunk_size,
            "contains": contains,
            "privacy": _privacy_note(self.phase),
            "already_mapped": len(self.mapped),
            "answer_schema": answer_schema(self.phase),
        }


# ---------------------------------------------------------------------------
# Answer schema (what /ingest accepts)
# ---------------------------------------------------------------------------


def answer_schema(phase: str) -> dict:
    """JSON Schema of the JSON-LD answer the prompt asks for.

    ``/ingest`` is more tolerant than this (it also finds column entries
    nested anywhere and accepts the flat ``[{item, match, ...}]`` form);
    the schema documents the canonical shape for the UI and the tests.
    """
    entry: dict = {
        "type": "object",
        "required": ["mapsTo", "localColumn"],
        "properties": {
            "mapsTo": {"type": "string", "pattern": "^schema:variable/.+$"},
            "localColumn": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "reason": {"type": "string"},
        },
        "additionalProperties": True,
    }
    if phase == "values":
        entry["required"] = ["mapsTo", "localColumn", "localMappings"]
        entry["properties"]["localMappings"] = {
            "type": "object",
            "additionalProperties": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "array", "items": {"type": "string"}},
                ]
            },
        }
        entry["properties"]["valueNotes"] = {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "reason": {"type": "string"},
                },
            },
        }
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": f"Flyover pasted LLM answer ({phase})",
        "type": "object",
        "required": ["databases"],
        "properties": {
            "databases": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "tables": {
                            "type": "object",
                            "additionalProperties": {
                                "type": "object",
                                "properties": {
                                    "columns": {
                                        "type": "object",
                                        "additionalProperties": entry,
                                    }
                                },
                            },
                        }
                    },
                },
            }
        },
    }
