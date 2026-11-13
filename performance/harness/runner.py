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
class Metric:
    """A metric that is judged: its key in a sample, and how to show it."""

    key: str
    label: str
    unit: str


class Workflow(Protocol):
    """What a workflow's folder gives the harness."""

    name: str
    metrics: Sequence[Metric]

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
    """Run ``profile`` on both sides and judge every metric. Returns the
    result as it is written to file."""
    started = time.monotonic()
    sides = {BASELINE: baseline, CANDIDATE: candidate}
    stacks: Dict[str, Stack] = {}
    records: List[Dict[str, Any]] = []
    errors: Dict[str, List[str]] = {BASELINE: [], CANDIDATE: []}
    workload = workflow.workload(profile)

    try:
        for index, (name, side) in enumerate(sides.items()):
            log(f"{name}: preparing {side.spec}")
            prepare_side(side, repository_root)
            stacks[name] = make_stack(side, index, workdir, profile.get("settings"))
            log(f"{name}: stack up in {stacks[name].up()} s")

        for name, stack in stacks.items():
            stack.reset()
            for error in workflow.warm_up(stack, sides[name], workload):
                errors[name].append(f"warm-up: {error}")
            log(f"{name}: warmed up")

        repetitions = int(profile["repetitions"])
        for repetition in range(1, repetitions + 1):
            if any(errors.values()):
                break
            order = [BASELINE, CANDIDATE] if repetition % 2 else [CANDIDATE, BASELINE]
            for scenario in profile["scenarios"]:
                record: Dict[str, Any] = {
                    "repetition": repetition,
                    "scenario": scenario,
                    "order": order,
                }
                for name in order:
                    stacks[name].reset()
                    sample = workflow.run(stacks[name], sides[name], workload, scenario)
                    record[name] = sample.as_dict()
                    for error in sample.errors:
                        errors[name].append(
                            f"{scenario}, repetition {repetition}: {error}"
                        )
                    log(
                        f"repetition {repetition}/{repetitions}, {scenario}, {name}: "
                        f"{_brief(sample, workflow.metrics)}"
                    )
                records.append(record)
    finally:
        logs = {
            name: stack.logs(tail=200) for name, stack in stacks.items() if errors[name]
        }
        if not keep:
            for stack in stacks.values():
                stack.down()

    for name in sides:
        errors[name].extend(_broken_invariants(records, name))

    comparisons = (
        []
        if any(errors.values())
        else judge(records, workflow.metrics, profile["scenarios"], threshold)
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
        "errors": errors,
        "logs": logs,
        "outcome": outcome(errors, comparisons),
        "duration_s": round(time.monotonic() - started, 1),
    }


def judge(
    records: Sequence[Mapping[str, Any]],
    metrics: Sequence[Metric],
    scenarios: Sequence[str],
    threshold: float,
) -> List[Dict[str, Any]]:
    """Every metric of every scenario, compared between the sides."""
    comparisons = []
    for scenario in scenarios:
        mine = [record for record in records if record["scenario"] == scenario]
        for metric in metrics:
            comparison = stats.compare(
                metric.key,
                [record[BASELINE]["metrics"].get(metric.key) for record in mine],
                [record[CANDIDATE]["metrics"].get(metric.key) for record in mine],
                threshold,
            )
            comparisons.append(
                {
                    "scenario": scenario,
                    "label": metric.label,
                    "unit": metric.unit,
                    **comparison.as_dict(),
                    "baseline_spread": stats.spread(
                        _values(mine, BASELINE, metric.key)
                    ),
                    "candidate_spread": stats.spread(
                        _values(mine, CANDIDATE, metric.key)
                    ),
                }
            )
    return comparisons


def outcome(
    errors: Mapping[str, Sequence[str]], comparisons: Sequence[Mapping[str, Any]]
) -> str:
    if errors[CANDIDATE]:
        return results.FAILED
    if errors[BASELINE]:
        return results.INVALID
    if any(c["verdict"] != stats.NO_FLAG for c in comparisons):
        return results.FLAGGED
    return results.OK


def _values(records: Sequence[Mapping[str, Any]], side: str, key: str) -> List[float]:
    values = [record[side]["metrics"].get(key) for record in records]
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


def _brief(sample: Sample, metrics: Sequence[Metric]) -> str:
    if sample.errors:
        return f"ERROR: {sample.errors[0][:200]}"
    parts = []
    for metric in metrics:
        value = sample.metrics.get(metric.key)
        if value is not None:
            parts.append(f"{metric.label.lower()} {value:.4g} {metric.unit}")
    return ", ".join(parts)
