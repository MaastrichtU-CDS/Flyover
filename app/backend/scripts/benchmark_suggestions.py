#!/usr/bin/env python3
"""
Benchmark script for tier-1 rule-based mapping suggestions.

Protocol (WS6 of the tier-1 remediation):

- Leave one SITE out: all AYA branches are loaded into ONE pool and a
  branch counts as a site. When a site is evaluated, ALL of its
  databases are hidden from the alias memory (NKI prospective and
  retrospective together), so a site never suggests from itself and
  single-database sites still get alias memory from the others.
- The run goes through SuggestionService with a fake session cache and
  RDF store — the same code path the app uses — so sanitise_pairs, the
  cascade merge, conflict handling, and the values-phase column guards
  all apply.
- Metrics per site and pooled, with EXACT counts: recall@1, recall@3
  (from alternatives), abstain rate, false-accept rate at the threshold
  (confident but wrong), precision of accepted matches, a per-source
  breakdown, a values-phase run over localMappings, and wall-clock time.
- Optional --sweep runs the variables phase over a grid of thresholds
  (0.60-0.95) and margins (0.02-0.15) and reports the best combination,
  to inform DEFAULT_THRESHOLD / DEFAULT_MARGIN.

Usage:
    python app/backend/scripts/benchmark_suggestions.py \
        --cache-dir /tmp/aya-cache \
        --out docs/mapping-suggestions/benchmark-results.md

Fetches each branch's JSON-LD from GitHub on demand and caches it under
--cache-dir (or $TMPDIR/aya-benchmark when omitted). Nothing large is
committed.

Ground truth comes from each branch's ``databases`` section: ``mapsTo``
is the correct variable key per column, and ``localMappings`` are the
correct value-to-term mappings.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Optional
from urllib.request import urlopen

# Make the backend package importable when run from the repo root.
_BACKEND = Path(__file__).resolve().parent.parent / "data_descriptor"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# pythonTool is an internal package present only in the full deployment;
# the services package imports it transitively. Stub it exactly like
# tests/unit/conftest.py does so the benchmark can run anywhere.
if "pythonTool" not in sys.modules:
    from unittest.mock import MagicMock

    _python_tool_mock = MagicMock()
    _python_tool_mock.main_app.run_triplifier = MagicMock(return_value=(True, "ok", []))
    sys.modules.setdefault("pythonTool", _python_tool_mock)
    sys.modules.setdefault("pythonTool.main_app", _python_tool_mock.main_app)

from loaders import JSONLDMapping

from services.suggestions import (
    DEFAULT_MARGIN,
    DEFAULT_THRESHOLD,
    SuggestionConfig,
    SuggestionService,
    VALUES_PHASE,
    VARIABLES_PHASE,
)

logger = logging.getLogger(__name__)

# AYA semantic-map repository branches. Each branch is a SITE. The mapping
# file lives at different paths on some branches (IGR-Paris was missing from
# earlier runs because of this), so each site lists candidate paths tried
# in order; --mapping-path overrides them all.
AYA_REPO = "STRONGAYA/AYA-cancer-semantic-map"
AYA_SITES: dict[str, tuple[str, ...]] = {
    "NKI-Amsterdam": ("AYA_cancer_schema.jsonld",),
    "TheChristie-Manchester": ("AYA_cancer_schema.jsonld",),
    "CLB-Lyon": ("AYA_cancer_schema.jsonld",),
    "IGR-Paris": ("AYA_cancer_schema.jsonld", "mapping.jsonld"),
    "INT-Milan": ("AYA_cancer_schema.jsonld",),
    "MSCI-Warsaw": ("AYA_cancer_schema.jsonld",),
    "YSRCCYP-Leeds": ("AYA_cancer_schema.jsonld",),
}

SWEEP_THRESHOLDS = [round(0.60 + 0.05 * i, 2) for i in range(8)]  # 0.60..0.95
SWEEP_MARGINS = [round(0.02 + 0.01 * i, 2) for i in range(14)]  # 0.02..0.15


# ---------------------------------------------------------------------------
# Site pool: fetch + merge
# ---------------------------------------------------------------------------


def _raw_url(branch: str, path: str) -> str:
    return f"https://raw.githubusercontent.com/{AYA_REPO}/{branch}/{path}"


def _load_site_mapping(
    site: str,
    candidate_paths: tuple[str, ...],
    override_path: Optional[str],
    cache_dir: Path,
) -> Optional[dict]:
    """Fetch a site's mapping JSON, trying each candidate path in turn."""
    local_file = cache_dir / f"{site}.json"
    if local_file.exists():
        try:
            return json.loads(local_file.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to load cached %s: %s", local_file, exc)

    paths = (override_path,) if override_path else candidate_paths
    for path in paths:
        url = _raw_url(site, path)
        try:
            logger.info("Fetching %s ...", url)
            with urlopen(url, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.warning("Missing/unreachable %s: %s", url, exc)
            continue
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("Fetched %s from %s", site, path)
        return data
    return None


def _merge_others(sites: dict[str, dict], evaluated: str) -> dict:
    """Merged JSON-LD of every OTHER site: union schema + renamed databases.

    Database names are prefixed with the site name so two sites that call
    a database the same thing stay distinct in the pool (graph name
    matching would otherwise treat them as one site). The evaluated site's
    databases are absent entirely — that is the leave-one-site-out.
    """
    merged: dict = {
        "@context": next(iter(sites.values())).get("@context", {}),
        "schema": {"@id": "schema:benchmark", "variables": {}},
        "databases": {},
    }
    for site, data in sites.items():
        if site == evaluated:
            continue
        for var_key, var in (data.get("schema", {}).get("variables", {})).items():
            merged["schema"]["variables"].setdefault(var_key, var)
        for db_key, db in (data.get("databases", {}) or {}).items():
            pooled_key = f"{site}__{db_key}"
            pooled_db = json.loads(json.dumps(db))  # deep copy
            pooled_db["name"] = f"{site}-{db.get('name') or db_key}"
            merged["databases"][pooled_key] = pooled_db
    return merged


def _values_mapping(sites: dict[str, dict], evaluated: str) -> dict:
    """The values-phase mapping for the evaluated site.

    Mimics the browser's job-local values mapping (WS4.2): the site's own
    columns are assigned to their variables (so value groups resolve) but
    their localMappings are stripped — the values the site is about to
    describe are exactly what the user would still have to fill in.
    Alias memory therefore only sees the OTHER sites' value mappings.
    """
    merged = _merge_others(sites, evaluated)
    own = sites[evaluated]
    for db_key, db in (own.get("databases", {}) or {}).items():
        pooled_key = f"{evaluated}__{db_key}"
        pooled_db = json.loads(json.dumps(db))
        pooled_db["name"] = f"{evaluated}-{db.get('name') or db_key}"
        # The site's columns keep their variable assignment (so the
        # values-phase groups resolve) but lose their localMappings.
        for table in (pooled_db.get("tables", {}) or {}).values():
            for column in (table.get("columns", {}) or {}).values():
                column.pop("localMappings", None)
        merged["databases"][pooled_key] = pooled_db
    return merged


# ---------------------------------------------------------------------------
# Fakes: the benchmark drives SuggestionService like the app does
# ---------------------------------------------------------------------------


class _FakeRdfStore:
    """RDF-store stand-in: the evaluated site's columns and values."""

    def __init__(
        self,
        columns_by_db: dict[str, list[str]],
        categories_by_col: Optional[dict[tuple[str, str], str]] = None,
    ) -> None:
        self._columns_by_db = columns_by_db
        self._categories = categories_by_col or {}

    def get_column_info_by_database(self) -> dict[str, list[str]]:
        return self._columns_by_db

    def get_categories(self, col: str, db: str) -> str:
        return self._categories.get((db, col), "")


class _FakeSessionCache:
    """Session-cache stand-in with the attributes the service reads."""

    def __init__(self, mapping: Any, details: Optional[dict] = None) -> None:
        self.jsonld_mapping = mapping
        self.suggestion_jobs = None
        self.DescriptiveInfoDetails = details or {}


def _make_service(threshold: float, margin: float) -> SuggestionService:
    cfg = SuggestionConfig.__new__(SuggestionConfig)
    cfg.tiers = [1]
    cfg.compute = "host"
    cfg.threshold = threshold
    cfg.margin = margin
    return SuggestionService(cfg)


# ---------------------------------------------------------------------------
# Ground truth extraction
# ---------------------------------------------------------------------------


def _site_columns(data: dict) -> dict[str, tuple[str, list[str]]]:
    """The site's databases: db name -> (db key, [local columns])."""
    out: dict[str, tuple[str, list[str]]] = {}
    for db_key, db in (data.get("databases", {}) or {}).items():
        name = f"{_site_of(data)}-{db.get('name') or db_key}"
        columns: list[str] = []
        for table in (db.get("tables", {}) or {}).values():
            for col in (table.get("columns", {}) or {}).values():
                if col.get("localColumn"):
                    columns.append(str(col["localColumn"]))
        if columns:
            out[name] = (db_key, columns)
    return out


_SITE_KEY = "__site__"


def _site_of(data: dict) -> str:
    return data.get(_SITE_KEY, "site")


def _column_truth(data: dict) -> dict[tuple[str, str], str]:
    """(db name, local column) -> true variable key."""
    out: dict[tuple[str, str], str] = {}
    for db_key, db in (data.get("databases", {}) or {}).items():
        name = f"{_site_of(data)}-{db.get('name') or db_key}"
        for table in (db.get("tables", {}) or {}).values():
            for col in (table.get("columns", {}) or {}).values():
                local = col.get("localColumn")
                var_key = col.get("mapsTo", "").rsplit("/", 1)[-1]
                if local and var_key:
                    out[(name, str(local))] = var_key
    return out


def _value_truth_and_csv(
    data: dict,
) -> tuple[dict[tuple[str, str, str], str], dict[tuple[str, str], str]]:
    """Value ground truth and per-column category CSVs from localMappings.

    Returns ((db, column, value) -> term, (db, column) -> categories CSV).
    """
    truth: dict[tuple[str, str, str], str] = {}
    csvs: dict[tuple[str, str], str] = {}
    for db_key, db in (data.get("databases", {}) or {}).items():
        name = f"{_site_of(data)}-{db.get('name') or db_key}"
        for table in (db.get("tables", {}) or {}).values():
            for col in (table.get("columns", {}) or {}).values():
                local = str(col.get("localColumn") or "")
                if not local:
                    continue
                values: list[str] = []
                for term, vals in (col.get("localMappings", {}) or {}).items():
                    if vals is None:
                        continue
                    if not isinstance(vals, list):
                        vals = [vals]
                    for v in vals:
                        if v is None:
                            continue
                        sv = str(v)
                        truth[(name, local, sv)] = str(term)
                        if sv not in values:
                            values.append(sv)
                if values:
                    buf = io.StringIO()
                    writer = csv.writer(buf)
                    writer.writerow(["value", "count"])
                    for v in values:
                        writer.writerow([v, 1])
                    csvs[(name, local)] = buf.getvalue()
    return truth, csvs


# ---------------------------------------------------------------------------
# Metric collection (exact counts; rates computed only for display)
# ---------------------------------------------------------------------------


def _empty_counts() -> dict[str, int]:
    return {
        "total": 0,
        "correct": 0,
        "correct3": 0,
        "abstain": 0,
        "false_accept": 0,
        "accepted": 0,
        "alias_matched": 0,
        "value_regex_matched": 0,
        "string_matched": 0,
    }


def _update_counts(
    counts: dict[str, int],
    record: Optional[dict],
    truth: Optional[str],
    threshold: float,
) -> None:
    counts["total"] += 1
    rec = record or {}
    match = rec.get("match")
    confidence = rec.get("confidence", 0.0) or 0.0
    if match is None:
        counts["abstain"] += 1
        return
    counts["accepted"] += 1
    source = rec.get("source", "")
    if source == "alias":
        counts["alias_matched"] += 1
    elif source == "value_regex":
        counts["value_regex_matched"] += 1
    elif source == "string":
        counts["string_matched"] += 1
    if truth and match == truth:
        counts["correct"] += 1
        counts["correct3"] += 1
        return
    if truth and confidence >= threshold:
        counts["false_accept"] += 1
    # recall@3: the truth may sit in the alternatives.
    alts = rec.get("alternatives") or []
    if truth and any(a.get("match") == truth for a in alts):
        counts["correct3"] += 1


def _rate(counts: dict[str, int], key: str) -> str:
    if not counts["total"]:
        return "n/a"
    return f"{counts[key] / counts['total']:.1%}"


def _add_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + value


# ---------------------------------------------------------------------------
# Phase runs
# ---------------------------------------------------------------------------


def _run_variables(
    service: SuggestionService, others: dict, site_data: dict
) -> tuple[dict[str, int], float]:
    columns_by_db = {
        name: list(cols) for name, (_key, cols) in _site_columns(site_data).items()
    }
    truth = _column_truth(site_data)
    cache = _FakeSessionCache(JSONLDMapping.from_dict(others))
    rdf = _FakeRdfStore(columns_by_db)

    started = time.perf_counter()
    result = service.start(VARIABLES_PHASE, cache, rdf)
    elapsed = time.perf_counter() - started
    if result.get("status") not in ("started", "already_done"):
        logger.warning("Variables job did not start: %s", result)
        return _empty_counts(), elapsed
    records = service.get_state(cache, VARIABLES_PHASE)["records"]

    counts = _empty_counts()
    for (db, col), var_key in truth.items():
        _update_counts(
            counts, records.get(f"{db}_{col}"), var_key, service.config.threshold
        )
    return counts, elapsed


def _run_values(
    service: SuggestionService, values_mapping: dict, site_data: dict
) -> tuple[dict[str, int], float]:
    truth, csvs = _value_truth_and_csv(site_data)
    columns_by_db = {}
    for db, col in csvs:
        columns_by_db.setdefault(db, [])
        if col not in columns_by_db[db]:
            columns_by_db[db].append(col)
    cache = _FakeSessionCache(JSONLDMapping.from_dict(values_mapping), details={})
    rdf = _FakeRdfStore(columns_by_db, categories_by_col=csvs)

    started = time.perf_counter()
    result = service.start(VALUES_PHASE, cache, rdf)
    elapsed = time.perf_counter() - started
    if result.get("status") not in ("started", "already_done", "nothing_to_suggest"):
        logger.warning("Values job did not start: %s", result)
        return _empty_counts(), elapsed
    records = service.get_state(cache, VALUES_PHASE)["records"]

    counts = _empty_counts()
    for (db, col, value), term in truth.items():
        _update_counts(
            counts, records.get(f"{db}_{col}_{value}"), term, service.config.threshold
        )
    return counts, elapsed


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _counts_row(label: str, counts: dict[str, int], seconds: float) -> str:
    precision = "n/a"
    if counts["accepted"]:
        precision = f"{counts['correct'] / counts['accepted']:.1%}"
    return (
        f"| {label} | {counts['total']} | {_rate(counts, 'correct')} | "
        f"{_rate(counts, 'correct3')} | {_rate(counts, 'abstain')} | "
        f"{_rate(counts, 'false_accept')} | {precision} | "
        f"{counts['alias_matched']} / {counts['value_regex_matched']} / {counts['string_matched']} | "
        f"{seconds:.2f}s |"
    )


_HEADER = (
    "| Site | Items | Recall@1 | Recall@3 | Abstain | False-accept | Precision (accepted) | "
    "alias / value_regex / string | Wall-clock |"
)
_SEP = "|---|---|---|---|---|---|---|---|---|"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark tier-1 mapping suggestions against the AYA sites."
    )
    parser.add_argument(
        "--sites",
        type=str,
        default="all",
        help="Comma-separated site (branch) names, or 'all'.",
    )
    parser.add_argument(
        "--mapping-path",
        type=str,
        default=None,
        help="Override the mapping file path for every branch (default: try each site's candidates).",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Directory to cache fetched JSON files (default: $TMPDIR/aya-benchmark).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Acceptance threshold (default: {DEFAULT_THRESHOLD}).",
    )
    parser.add_argument(
        "--margin",
        type=float,
        default=DEFAULT_MARGIN,
        help=f"Top-2 margin for abstaining (default: {DEFAULT_MARGIN}).",
    )
    parser.add_argument(
        "--skip-values",
        action="store_true",
        help="Skip the values-phase run.",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help=f"Sweep threshold {SWEEP_THRESHOLDS[0]}-{SWEEP_THRESHOLDS[-1]} and margin "
        f"{SWEEP_MARGINS[0]}-{SWEEP_MARGINS[-1]} over the variables phase and report the best combination.",
    )
    parser.add_argument(
        "--out", type=str, default=None, help="Output Markdown file path."
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    site_names = list(AYA_SITES) if args.sites == "all" else args.sites.split(",")
    cache_dir = (
        Path(args.cache_dir)
        if args.cache_dir
        else Path(tempfile.gettempdir()) / "aya-benchmark"
    )

    sites: dict[str, dict] = {}
    for site in site_names:
        data = _load_site_mapping(
            site,
            AYA_SITES.get(site, (args.mapping_path,)),
            args.mapping_path,
            cache_dir,
        )
        if data is None:
            logger.warning("Skipping %s (could not load a mapping)", site)
            continue
        data[_SITE_KEY] = site
        sites[site] = data
    if not sites:
        logger.error("No site mappings loaded.")
        sys.exit(1)
    logger.info("Loaded %d sites: %s", len(sites), ", ".join(sorted(sites)))

    lines: list[str] = []
    lines.append("# Tier-1 benchmark results\n")

    # --- variables phase at the requested settings -----------------------
    service = _make_service(args.threshold, args.margin)
    lines.append(
        f"\nRun at threshold={args.threshold:.2f}, margin={args.margin:.2f} "
        f"(production defaults: {DEFAULT_THRESHOLD}/{DEFAULT_MARGIN}).\n"
    )
    lines.append("## Variables phase (column -> variable)\n")
    lines.append(_HEADER)
    lines.append(_SEP)
    pooled = _empty_counts()
    pooled_seconds = 0.0
    for site in sorted(sites):
        others = _merge_others(sites, site)
        counts, seconds = _run_variables(service, others, sites[site])
        _add_counts(pooled, counts)
        pooled_seconds += seconds
        lines.append(_counts_row(site, counts, seconds))
    lines.append(_counts_row("**Pooled**", pooled, pooled_seconds))

    # --- values phase -----------------------------------------------------
    if not args.skip_values:
        lines.append("\n## Values phase (distinct value -> term, over localMappings)\n")
        lines.append(_HEADER)
        lines.append(_SEP)
        pooled = _empty_counts()
        pooled_seconds = 0.0
        for site in sorted(sites):
            values_mapping = _values_mapping(sites, site)
            counts, seconds = _run_values(service, values_mapping, sites[site])
            _add_counts(pooled, counts)
            pooled_seconds += seconds
            lines.append(_counts_row(site, counts, seconds))
        lines.append(_counts_row("**Pooled**", pooled, pooled_seconds))

    # --- parameter sweep ---------------------------------------------------
    if args.sweep:
        lines.append("\n## Parameter sweep (variables phase, pooled)\n")
        lines.append(
            "Scores pooled over all sites: recall@1 minus false-accept (higher is better).\n"
        )
        lines.append("| Threshold | Margin | Recall@1 | False-accept | Score |")
        lines.append("|---|---|---|---|---|")
        best: Optional[tuple[float, float, float]] = None
        sweep_counts: dict[tuple[float, float], dict[str, int]] = {}
        for threshold in SWEEP_THRESHOLDS:
            for margin in SWEEP_MARGINS:
                svc = _make_service(threshold, margin)
                counts = _empty_counts()
                for site in sorted(sites):
                    others = _merge_others(sites, site)
                    site_counts, _seconds = _run_variables(svc, others, sites[site])
                    _add_counts(counts, site_counts)
                sweep_counts[(threshold, margin)] = counts
        for (threshold, margin), counts in sweep_counts.items():
            if not counts["total"]:
                continue
            recall = counts["correct"] / counts["total"]
            false = counts["false_accept"] / counts["total"]
            score = recall - false
            lines.append(
                f"| {threshold:.2f} | {margin:.2f} | {recall:.1%} | {false:.1%} | {score:+.3f} |"
            )
            if best is None or score > best[2]:
                best = (threshold, margin, score)
        if best:
            lines.append(
                f"\nBest combination: threshold={best[0]:.2f}, margin={best[1]:.2f} "
                f"(score {best[2]:+.3f}). If it differs from DEFAULT_THRESHOLD/"
                f"DEFAULT_MARGIN, update services/suggestions/__init__.py and rerun."
            )

    output = "\n".join(lines) + "\n"
    print(output)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output, encoding="utf-8")
        logger.info("Results written to %s", out_path)


if __name__ == "__main__":
    main()
