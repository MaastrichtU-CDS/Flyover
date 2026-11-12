"""
A result, written for people: the job summary, the pull-request comment and
the row of the history page. All three are made from the result file alone.
"""

import statistics
from typing import Any, List, Mapping, Optional

from . import result as results
from .runner import BASELINE, CANDIDATE
from .stats import IMPROVED, REGRESSED

_WORDS = {
    # unit: (regressed, improved)
    "s": ("slower", "faster"),
    "MB": ("more", "less"),
}


def headline(result: Mapping[str, Any]) -> str:
    """What the run found, in a sentence or two."""
    outcome = result["outcome"]
    threshold = f"{result['threshold'] * 100:.0f} %"
    if outcome == results.FAILED:
        return (
            "❌ **The candidate did not do the work it was given**, so nothing "
            "was compared."
        )
    if outcome == results.VOID:
        return (
            "⚪ **The baseline did not do the work it was given**, so nothing can "
            "be said about the candidate."
        )
    flagged = [
        c for c in result["comparisons"] if c["verdict"] in (REGRESSED, IMPROVED)
    ]
    if not flagged:
        return f"✅ **No figure moved by {threshold} or more.**"
    parts = []
    for comparison in flagged:
        worse = comparison["verdict"] == REGRESSED
        parts.append(
            f"{'⚠️' if worse else '🟢'} **{comparison['label']} is "
            f"{_change(comparison['ratio'], signed=False)} {_word(comparison)}** "
            f"({_interval(comparison)}; {comparison['scenario']})"
        )
    return "<br>".join(parts)


def summary(result: Mapping[str, Any]) -> str:
    """The whole result, for a job summary or a terminal."""
    sides = result["sides"]
    workload = result["workload"]
    lines = [
        f"## {result['workflow'].upper()} performance",
        "",
        headline(result),
        "",
        f"**Baseline** `{sides[BASELINE]['spec']}` · "
        f"**Candidate** `{sides[CANDIDATE]['spec']}`  ",
        f"Profile `{result['profile'].get('name', '?')}`: "
        f"{len(workload['tables'])} tables of {workload['rows_per_table']} patients "
        f"× {workload['columns_per_table']} columns, "
        f"{_count(result['profile']['repetitions'], 'repetition')}; flagged from "
        f"{result['threshold'] * 100:.0f} %.",
        "",
    ]
    lines += _faults(result)
    for scenario in result["profile"]["scenarios"]:
        lines += _scenario(result, scenario)
    lines += _machine(result)
    return "\n".join(lines).rstrip() + "\n"


def comment(result: Mapping[str, Any], run_url: str = "") -> str:
    """The pull-request comment: the verdict and the judged figures."""
    lines = [
        f"### {result['workflow'].upper()} performance",
        "",
        headline(result),
        "",
    ]
    lines += _faults(result)
    for scenario in result["profile"]["scenarios"]:
        table = _figures_table(result, scenario)
        if table:
            lines += [f"`{scenario}`", ""] + table + [""]
    sides = result["sides"]
    lines.append(
        f"Candidate `{sides[CANDIDATE]['spec']}` against baseline "
        f"`{sides[BASELINE]['spec']}`, both on the same runner. Timings advise; "
        "they never fail the build."
        + (f" [Full result]({run_url})." if run_url else "")
    )
    return "\n".join(lines).rstrip() + "\n"


def history_header(result: Mapping[str, Any]) -> str:
    labels = _labels(result)
    return (
        "| Date | Candidate | Baseline | Scenario | "
        + " | ".join(labels)
        + " | Outcome |\n|---|---|---|---|"
        + "---|" * len(labels)
        + "---|"
    )


def history_rows(result: Mapping[str, Any], candidate: str = "", link: str = "") -> str:
    """The run as rows of the history page, one per scenario. Each figure
    shows the candidate's median and its change against the baseline."""
    sides = result["sides"]
    name = candidate or sides[CANDIDATE]["spec"]
    outcome = f"[{result['outcome']}]({link})" if link else result["outcome"]
    rows = []
    for scenario in result["profile"]["scenarios"]:
        cells = []
        for label in _labels(result):
            found = [
                c
                for c in result["comparisons"]
                if c["scenario"] == scenario and c["label"] == label
            ]
            if not found or found[0]["ratio"] is None:
                cells.append("n/a")
                continue
            comparison = found[0]
            mark = {REGRESSED: " ⚠️", IMPROVED: " 🟢"}.get(comparison["verdict"], "")
            cells.append(
                f"{_number(comparison['candidate'])} {comparison['unit']} "
                f"({_change(comparison['ratio'])}){mark}"
            )
        if not result["comparisons"]:
            cells = ["n/a"] * len(_labels(result))
        rows.append(
            f"| {result['created'][:10]} | {name} | {sides[BASELINE]['spec']} | "
            f"{scenario} | " + " | ".join(cells) + f" | {outcome} |"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------


def _labels(result: Mapping[str, Any]) -> List[str]:
    labels: List[str] = []
    for comparison in result["comparisons"]:
        if comparison["label"] not in labels:
            labels.append(comparison["label"])
    return labels or [
        "Conversion",
        "Annotation",
        "Read queries",
        "Flyover's peak memory",
    ]


def _faults(result: Mapping[str, Any]) -> List[str]:
    lines: List[str] = []
    for side in (CANDIDATE, BASELINE):
        faults = result["faults"].get(side) or []
        if faults:
            lines.append(f"**What went wrong on the {side}:**")
            lines += [f"- {fault}" for fault in faults[:10]]
            if len(faults) > 10:
                lines.append(f"- … and {len(faults) - 10} more")
            lines.append("")
    return lines


def _scenario(result: Mapping[str, Any], scenario: str) -> List[str]:
    records = [r for r in result["repetitions"] if r["scenario"] == scenario]
    lines = [f"### `{scenario}`", ""]
    table = _figures_table(result, scenario)
    if table:
        lines += table + [""]
    complete = [r for r in records if BASELINE in r and CANDIDATE in r]
    measured = [
        r for r in complete if not r[BASELINE]["faults"] and not r[CANDIDATE]["faults"]
    ]
    if not measured:
        return lines + ["No repetition completed on both sides.", ""]

    lines += [
        "| Intake | Conversion | Annotation |",
        "|---|---|---|",
    ]
    intakes = measured[0][BASELINE]["reported"]["intakes"]
    for index, intake in enumerate(intakes):
        cells = []
        for key in ("conversion_s", "annotation_s"):
            base = _median(
                r[BASELINE]["reported"]["intakes"][index][key] for r in measured
            )
            cand = _median(
                r[CANDIDATE]["reported"]["intakes"][index][key] for r in measured
            )
            cells.append(f"{_number(base)} s → {_number(cand)} s")
        lines.append(
            f"| {index + 1}: `{intake['table']}` | " + " | ".join(cells) + " |"
        )
    growth = []
    for key, label in (("conversion", "conversion"), ("annotation", "annotation")):
        base = _median(r[BASELINE]["reported"]["growth"][key] for r in measured)
        cand = _median(r[CANDIDATE]["reported"]["growth"][key] for r in measured)
        if base is not None and cand is not None:
            growth.append(f"{label} ×{_number(base)} → ×{_number(cand)}")
    if growth:
        lines += ["", "Last intake against the first: " + "; ".join(growth) + "."]

    lines += ["", "<details><summary>Read queries, CPU and memory</summary>", ""]
    names = sorted(measured[0][BASELINE]["reported"]["queries"])
    if names:
        lines += ["| Read query | Baseline | Candidate |", "|---|---|---|"]
        for name in names:
            base = _median(
                r[BASELINE]["reported"]["queries"].get(name) for r in measured
            )
            cand = _median(
                r[CANDIDATE]["reported"]["queries"].get(name) for r in measured
            )
            lines.append(f"| `{name}` | {_number(base)} s | {_number(cand)} s |")
        lines.append("")
    for key, label, unit in (
        ("app_cpu_s", "Flyover's CPU time over the sequence", "s"),
        ("store_cpu_s", "The store's CPU time over the sequence", "s"),
        ("store_memory_mb", "The store's memory at the end", "MB"),
        ("statements", "Statements given to the store", ""),
    ):
        base = _median(r[BASELINE]["reported"].get(key) for r in measured)
        cand = _median(r[CANDIDATE]["reported"].get(key) for r in measured)
        if base is not None or cand is not None:
            suffix = f" {unit}" if unit else ""
            lines.append(
                f"- {label}: {_number(base)}{suffix} → {_number(cand)}{suffix}"
            )
    lines += ["", "</details>", ""]
    return lines


def _figures_table(result: Mapping[str, Any], scenario: str) -> List[str]:
    mine = [c for c in result["comparisons"] if c["scenario"] == scenario]
    if not mine:
        return []
    lines = [
        "| Figure | Baseline | Candidate | Change | 95 % interval | Spread | |",
        "|---|---|---|---|---|---|---|",
    ]
    for comparison in mine:
        if comparison["ratio"] is None:
            lines.append(
                f"| {comparison['label']} | n/a | n/a | | | | {comparison['note']} |"
            )
            continue
        flag = ""
        if comparison["verdict"] == REGRESSED:
            flag = f"⚠️ {_word(comparison)}"
        elif comparison["verdict"] == IMPROVED:
            flag = f"🟢 {_word(comparison)}"
        elif comparison["note"]:
            flag = comparison["note"]
        lines.append(
            f"| {comparison['label']} | {_number(comparison['baseline'])} "
            f"{comparison['unit']} | {_number(comparison['candidate'])} "
            f"{comparison['unit']} | {_change(comparison['ratio'])} | "
            f"{_interval(comparison)} | {_spread(comparison)} | {flag} |"
        )
    return lines


def _machine(result: Mapping[str, Any]) -> List[str]:
    machine = result["environment"]
    sides = result["sides"]
    lines = ["<details><summary>Where and what</summary>", ""]
    lines.append(
        f"- Machine: {machine.get('cpu')}, {machine.get('cpus')} CPUs, "
        f"{machine.get('memory_gb')} GB; Docker {machine.get('docker')}; "
        f"kernel {machine.get('kernel')}"
    )
    for name in (BASELINE, CANDIDATE):
        side = sides[name]
        lines.append(
            f"- {name.capitalize()}: app `{side['app_image']}` "
            f"({_short(side.get('app_image_id'))}), store `{side['store_image']}` "
            f"({_short(side.get('store_image_id'))})"
        )
    workload = result["workload"]
    lines.append(
        f"- Data: schema {workload['schema_version']}, seed {workload['seed']}, "
        f"sha256 `{workload['sha256'][:16]}`; variables: "
        f"{', '.join(workload['variables'])}"
    )
    lines.append(f"- The run took {result['duration_s'] / 60:.1f} minutes.")
    lines += ["", "</details>", ""]
    return lines


def _word(comparison: Mapping[str, Any]) -> str:
    regressed, improved = _WORDS.get(comparison["unit"], ("higher", "lower"))
    return regressed if comparison["verdict"] == REGRESSED else improved


def _change(ratio: Optional[float], signed: bool = True) -> str:
    if ratio is None:
        return "n/a"
    percent = (ratio - 1) * 100
    if not signed:
        # "30 % slower", "23 % faster": how far the candidate is from the
        # baseline, as a share of the baseline.
        return f"{abs(percent):.0f} %"
    return f"{percent:+.0f} %"


def _interval(comparison: Mapping[str, Any]) -> str:
    if comparison.get("low") is None:
        return "n/a"
    return f"{_change(comparison['low'])} to {_change(comparison['high'])}"


def _spread(comparison: Mapping[str, Any]) -> str:
    parts = []
    for key in ("baseline_spread", "candidate_spread"):
        value = comparison.get(key)
        parts.append("n/a" if value is None else f"{value * 100:.0f} %")
    return " / ".join(parts)


def _median(values) -> Optional[float]:
    usable = [v for v in values if isinstance(v, (int, float))]
    return statistics.median(usable) if usable else None


def _number(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, int) or float(value).is_integer() and abs(value) >= 1000:
        return f"{int(value):,}"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def _count(number: int, word: str) -> str:
    return f"{number} {word}" if number == 1 else f"{number} {word}s"


def _short(identifier: Optional[str]) -> str:
    if not identifier:
        return "unknown"
    return identifier.replace("sha256:", "")[:12]
