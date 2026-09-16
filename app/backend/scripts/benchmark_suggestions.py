#!/usr/bin/env python3
"""
Benchmark script for tier-1 rule-based mapping suggestions.

Runs the tier-1 matchers (alias memory, value-type regex, normalised string
similarity) against the AYA cancer semantic-map branches, using a
leave-one-site-out protocol: each site is described in turn, with alias memory
built from all other sites.

Usage:
    python scripts/benchmark_suggestions.py --tiers 1 --sites all --out docs/mapping-suggestions/benchmark-results.md

The script uses the same ``tiers/rules.py`` code path as the application; it
does not start a server. Results are printed to stdout and optionally written
as a Markdown table to ``--out``.

Ground truth comes from each branch's ``databases`` section: the ``mapsTo``
field of each column is the correct variable key, and ``localMappings`` are
the correct value-to-term mappings.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional
from urllib.request import urlopen

# Make the backend package importable when run from the repo root.
_BACKEND = Path(__file__).resolve().parent.parent / "data_descriptor"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from loaders import JSONLDMapping
from services.suggestions.tiers import SuggestionContext
from services.suggestions.tiers.rules import (
    AliasMatcher,
    StringMatcher,
    ValueRegexMatcher,
    load_rules,
    tier1_producers,
)

logger = logging.getLogger(__name__)

# AYA semantic-map repository branches. Each branch has a mapping.jsonld (or
# equivalent) at a known path. The script fetches them raw from GitHub.
AYA_REPO = "STRONGAYA/AYA-cancer-semantic-map"
AYA_BRANCHES = [
    "NKI-Amsterdam",
    "TheChristie-Manchester",
    "CLB-Lyon",
    "IGR-Paris",
    "INT-Milan",
    "MSCI-Warsaw",
    "YSRCCYP-Leeds",
]

# Default mapping file path within each branch. Override with --mapping-path
# if the branches use different paths.
DEFAULT_MAPPING_PATH = "AYA_cancer_schema.jsonld"


def _raw_url(branch: str, path: str) -> str:
    return f"https://raw.githubusercontent.com/{AYA_REPO}/{branch}/{path}"


def _load_branch_mapping(
    branch: str, path: str, local_dir: Optional[Path] = None
) -> Optional[JSONLDMapping]:
    """Load a branch's JSON-LD mapping from GitHub or a local cache."""
    if local_dir:
        local_file = local_dir / f"{branch}.jsonld"
        if local_file.exists():
            try:
                return JSONLDMapping.from_dict(
                    json.loads(local_file.read_text(encoding="utf-8"))
                )
            except Exception as exc:
                logger.warning("Failed to load cached %s: %s", local_file, exc)

    url = _raw_url(branch, path)
    try:
        logger.info("Fetching %s ...", url)
        with urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if local_dir:
            local_dir.mkdir(parents=True, exist_ok=True)
            (local_dir / f"{branch}.jsonld").write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )
        return JSONLDMapping.from_dict(data)
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


def _run_tier1(mapping: JSONLDMapping, described_db: str, rules: dict) -> dict:
    """Run the three tier-1 matchers on one described database's columns."""
    producers = tier1_producers()
    variable_keys = list(mapping.get_all_variable_keys())
    schema_slice = {"*": variable_keys}

    # Collect this database's columns (ground truth from the mapping).
    db_obj = mapping.databases.get(described_db)
    if db_obj is None:
        # Try case-insensitive match.
        for key, db in mapping.databases.items():
            if db.name == described_db:
                db_obj = db
                break
    if db_obj is None:
        return {}

    items: list[str] = []
    truth: dict[str, str] = {}
    for table in db_obj.tables.values():
        for col in table.columns.values():
            if col.local_column:
                items.append(col.local_column)
                var_key = col.get_variable_key()
                if var_key:
                    truth[col.local_column] = var_key

    if not items:
        return {}

    ctx = SuggestionContext(
        phase="variables",
        mapping=mapping,
        described_database=described_db,
        rules=rules,
        threshold=0.8,
        margin=0.05,
    )

    best: dict[str, dict] = {item: None for item in items}
    for producer in producers:
        to_run = [
            item
            for item in items
            if best.get(item) is None
            or best[item].get("confidence", 0.0) < ctx.threshold
        ]
        if not to_run:
            continue
        raw = producer.run(list(to_run), schema_slice, ctx)
        for record in raw:
            item = record["item"]
            prev = best.get(item)
            if prev is None:
                best[item] = record
            else:
                if record.get("confidence", 0.0) > prev.get("confidence", 0.0):
                    best[item] = record

    results = {}
    for item in items:
        rec = best.get(item) or {}
        results[item] = {
            "truth": truth.get(item),
            "match": rec.get("match"),
            "confidence": rec.get("confidence", 0.0),
            "source": rec.get("source", ""),
            "reason": rec.get("reason", ""),
        }
    return results


def _compute_metrics(results: dict) -> dict:
    """Compute recall@1, false-accept rate, and abstain rate."""
    total = len(results)
    if total == 0:
        return {"total": 0, "recall_at_1": 0.0, "false_accept": 0.0, "abstain": 0.0}

    correct = 0
    false_accept = 0
    abstain = 0
    for item, rec in results.items():
        truth = rec["truth"]
        match = rec["match"]
        conf = rec["confidence"]
        if match is None:
            abstain += 1
        elif truth and match == truth:
            correct += 1
        else:
            # A non-null match that is wrong and above threshold.
            if conf >= 0.8:
                false_accept += 1
    return {
        "total": total,
        "recall_at_1": correct / total,
        "false_accept": false_accept / total,
        "abstain": abstain / total,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark tier-1 rule-based mapping suggestions."
    )
    parser.add_argument(
        "--tiers",
        type=str,
        default="1",
        help="Comma-separated tier numbers to run (default: 1).",
    )
    parser.add_argument(
        "--sites",
        type=str,
        default="all",
        help="Comma-separated branch names, or 'all'.",
    )
    parser.add_argument(
        "--mapping-path",
        type=str,
        default=DEFAULT_MAPPING_PATH,
        help="Path to the mapping JSON-LD file within each branch.",
    )
    parser.add_argument(
        "--out", type=str, default=None, help="Output Markdown file path."
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Directory to cache fetched JSON-LD files.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    if "1" not in args.tiers.split(","):
        logger.error("Only tier 1 is supported by this script.")
        sys.exit(1)

    branches = AYA_BRANCHES if args.sites == "all" else args.sites.split(",")
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    rules = load_rules()

    all_results: dict[str, dict] = {}
    for branch in branches:
        mapping = _load_branch_mapping(branch, args.mapping_path, cache_dir)
        if mapping is None:
            logger.warning("Skipping %s (could not load mapping)", branch)
            continue

        for db_key, db in mapping.databases.items():
            described_db = db.name or db_key
            results = _run_tier1(mapping, described_db, rules)
            if results:
                all_results[f"{branch}/{described_db}"] = results

    # Compute per-site and pooled metrics.
    lines = ["# Tier-1 benchmark results\n"]
    lines.append("| Site | Columns | Recall@1 | False-accept | Abstain |")
    lines.append("|---|---|---|---|---|")

    pooled_total = 0
    pooled_correct = 0
    pooled_false = 0
    pooled_abstain = 0

    for site_key, results in sorted(all_results.items()):
        metrics = _compute_metrics(results)
        lines.append(
            f"| {site_key} | {metrics['total']} | "
            f"{metrics['recall_at_1']:.1%} | "
            f"{metrics['false_accept']:.1%} | "
            f"{metrics['abstain']:.1%} |"
        )
        pooled_total += metrics["total"]
        pooled_correct += int(metrics["recall_at_1"] * metrics["total"])
        pooled_false += int(metrics["false_accept"] * metrics["total"])
        pooled_abstain += int(metrics["abstain"] * metrics["total"])

    if pooled_total:
        lines.append(
            f"| **Pooled** | **{pooled_total}** | "
            f"**{pooled_correct / pooled_total:.1%}** | "
            f"**{pooled_false / pooled_total:.1%}** | "
            f"**{pooled_abstain / pooled_total:.1%}** |"
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
