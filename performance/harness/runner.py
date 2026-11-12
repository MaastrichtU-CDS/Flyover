"""
A comparison: both sides do the same work, in turns, on the same machine.

Each repetition runs every scenario once on each side, one side after the
other, never both at once. The order of the sides alternates from one
repetition to the next, so neither is always first on a machine that is
still warming up or always second on one that is slowing down. Both stacks
stay up for the whole run; the one that is not measured sits idle.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Protocol, Sequence

from .. import __version__
from . import result as results
from . import stats
from .result import Sample
from .sides import Side, image_identity, prepare
from .stack import Stack

BASELINE = "baseline"
CANDIDATE = "candidate"


@dataclass(frozen=True)
class Figure:
    """A figure that is judged: its key in a sample, and how to show it."""

    key: str
    label: str
    unit: str


class Workflow(Protocol):
    """What a workflow's folder gives the harness."""

    name: str
    figures: Sequence[Figure]

    def profile(self, name: str) -> Dict[str, Any]: ...

    def workload(self, profile: Mapping[str, Any]) -> Any: ...

    def warm_up(self, stack: Stack, side: Side, workload: Any) -> List[str]: ...

    def run(self, stack: Stack, side: Side, workload: Any, scenario: str) -> Sample: ...


def compare(
    workflow: Workflow,
    baseline: Side,
    candidate: Side,
    profile: Mapping[str, Any],
    workdir: Path,
    repository_root: Path,
    threshold: float = 0.2,
    keep: bool = False,
    log: Callable[[str], None] = print,
    make_stack: Callable[..., Stack] = Stack,
    prepare_side: Callable[[Side, Path], None] = prepare,
    identify: Callable[[Side], Dict[str, Optional[str]]] = image_identity,
) -> Dict[str, Any]:
    """Run ``profile`` on both sides and judge every figure. Returns the
    result as it is written to file."""
    started = time.monotonic()
    sides = {BASELINE: baseline, CANDIDATE: candidate}
    stacks: Dict[str, Stack] = {}
    records: List[Dict[str, Any]] = []
    faults: Dict[str, List[str]] = {BASELINE: [], CANDIDATE: []}
    workload = workflow.workload(profile)

    try:
        for index, (name, side) in enumerate(sides.items()):
            log(f"{name}: preparing {side.spec}")
            prepare_side(side, repository_root)
            stacks[name] = make_stack(side, index, workdir, profile.get("settings"))
            log(f"{name}: stack up in {stacks[name].up()} s")

        for name, stack in stacks.items():
            stack.fresh()
            for fault in workflow.warm_up(stack, sides[name], workload):
                faults[name].append(f"warm-up: {fault}")
            log(f"{name}: warmed up")

        repetitions = int(profile["repetitions"])
        for repetition in range(1, repetitions + 1):
            if any(faults.values()):
                break
            order = [BASELINE, CANDIDATE] if repetition % 2 else [CANDIDATE, BASELINE]
            for scenario in profile["scenarios"]:
                record: Dict[str, Any] = {
                    "repetition": repetition,
                    "scenario": scenario,
                    "order": order,
                }
                for name in order:
                    stacks[name].fresh()
                    sample = workflow.run(stacks[name], sides[name], workload, scenario)
                    record[name] = sample.as_dict()
                    for fault in sample.faults:
                        faults[name].append(
                            f"{scenario}, repetition {repetition}: {fault}"
                        )
                    log(
                        f"repetition {repetition}/{repetitions}, {scenario}, {name}: "
                        f"{_brief(sample, workflow.figures)}"
                    )
                records.append(record)
    finally:
        logs = {
            name: stack.logs(tail=200) for name, stack in stacks.items() if faults[name]
        }
        if not keep:
            for stack in stacks.values():
                stack.down()

    for name in sides:
        faults[name].extend(_broken_invariants(records, name))

    comparisons = (
        []
        if any(faults.values())
        else judge(records, workflow.figures, profile["scenarios"], threshold)
    )
    return {
        "schema": results.SCHEMA,
        "created": results.now(),
        "harness": __version__,
        "workflow": workflow.name,
        "profile": dict(profile),
        "workload": workload.describe(),
        "threshold": threshold,
        "environment": results.environment(),
        "sides": {
            name: {**side.describe(), **identify(side)} for name, side in sides.items()
        },
        "repetitions": records,
        "comparisons": comparisons,
        "faults": faults,
        "logs": logs,
        "outcome": outcome(faults, comparisons),
        "duration_s": round(time.monotonic() - started, 1),
    }


def judge(
    records: Sequence[Mapping[str, Any]],
    figures: Sequence[Figure],
    scenarios: Sequence[str],
    threshold: float,
) -> List[Dict[str, Any]]:
    """Every figure of every scenario, compared between the sides."""
    comparisons = []
    for scenario in scenarios:
        mine = [record for record in records if record["scenario"] == scenario]
        for figure in figures:
            comparison = stats.compare(
                figure.key,
                [record[BASELINE]["figures"].get(figure.key) for record in mine],
                [record[CANDIDATE]["figures"].get(figure.key) for record in mine],
                threshold,
            )
            comparisons.append(
                {
                    "scenario": scenario,
                    "label": figure.label,
                    "unit": figure.unit,
                    **comparison.as_dict(),
                    "baseline_spread": stats.spread(
                        _values(mine, BASELINE, figure.key)
                    ),
                    "candidate_spread": stats.spread(
                        _values(mine, CANDIDATE, figure.key)
                    ),
                }
            )
    return comparisons


def outcome(
    faults: Mapping[str, Sequence[str]], comparisons: Sequence[Mapping[str, Any]]
) -> str:
    if faults[CANDIDATE]:
        return results.FAILED
    if faults[BASELINE]:
        return results.VOID
    if any(c["verdict"] != stats.NO_FLAG for c in comparisons):
        return results.FLAGGED
    return results.OK


def _values(records: Sequence[Mapping[str, Any]], side: str, key: str) -> List[float]:
    values = [record[side]["figures"].get(key) for record in records]
    return [value for value in values if isinstance(value, (int, float))]


def _broken_invariants(records: Sequence[Mapping[str, Any]], side: str) -> List[str]:
    """The same work must leave the same behind on every repetition; when it
    does not, something failed without saying so."""
    seen: Dict[tuple, Any] = {}
    broken = []
    for record in records:
        if side not in record:
            continue
        for key, value in record[side]["invariants"].items():
            first = seen.setdefault((record["scenario"], key), value)
            if first != value:
                broken.append(
                    f"{record['scenario']}, repetition {record['repetition']}: "
                    f"{key} is {value}, but was {first} before"
                )
    return broken


def _brief(sample: Sample, figures: Sequence[Figure]) -> str:
    if sample.faults:
        return f"FAULT: {sample.faults[0][:200]}"
    parts = []
    for figure in figures:
        value = sample.figures.get(figure.key)
        if value is not None:
            parts.append(f"{figure.label.lower()} {value:.4g} {figure.unit}")
    return ", ".join(parts)
