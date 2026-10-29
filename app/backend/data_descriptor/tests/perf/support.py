"""
Measurement helpers and the report for the performance checks.

Every check times requests through Flask's test client against the app
``create_app()`` builds, with the same doubles as the API layer (so the
timings are the HTTP layer's and the request-boundary validation's own),
and records what it measured in ``REPORT``. The terminal summary prints the
report; with ``FLYOVER_PERF_REPORT=<path>`` it is also written as Markdown
(CI appends that file to the job summary).

Timing: ``best_of`` (the minimum of several runs) for scaling checks, since
the minimum is the least disturbed by a busy machine; percentiles over many
runs for per-request latency. Budgets are deliberately generous (several
times what a laptop measures) so they catch regressions in kind, such as a
quadratic loop, rather than flake on a slow runner.
"""

import json
import math
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from tests.fixtures.mappings import TABLE, valid_mapping

JSON = "application/json"


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


def durations(action: Callable[[], Any], repeat: int, warmup: int = 2) -> List[float]:
    """Wall-clock seconds of ``repeat`` runs of ``action`` after a warm-up."""
    for _ in range(warmup):
        action()
    measured = []
    for _ in range(repeat):
        start = time.perf_counter()
        action()
        measured.append(time.perf_counter() - start)
    return measured


def best_of(action: Callable[[], Any], repeat: int = 5, warmup: int = 1) -> float:
    return min(durations(action, repeat, warmup))


def percentile(values: Sequence[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(pct / 100 * len(ordered)) - 1))
    return ordered[index]


def growth_exponent(sizes: Sequence[float], times: Sequence[float]) -> float:
    """Least-squares slope of log(time) against log(size).

    About 1 for work linear in the input, 2 for quadratic work.
    """
    xs = [math.log(s) for s in sizes]
    ys = [math.log(max(t, 1e-9)) for t in times]
    mean_x, mean_y = statistics.fmean(xs), statistics.fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    return numerator / denominator


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class Measurement:
    check: str
    value: float
    unit: str
    budget: Optional[str] = None
    note: str = ""


@dataclass
class PerfReport:
    rows: List[Measurement] = field(default_factory=list)

    def record(
        self,
        check: str,
        value: float,
        unit: str,
        budget: Optional[str] = None,
        note: str = "",
    ) -> None:
        self.rows.append(Measurement(check, value, unit, budget, note))

    def markdown(self) -> str:
        lines = [
            "| Check | Measured | Budget | Note |",
            "|---|---|---|---|",
        ]
        for row in self.rows:
            lines.append(
                f"| {row.check} | {row.value:.3g} {row.unit} | {row.budget or ''} "
                f"| {row.note} |"
            )
        return "\n".join(lines) + "\n"


REPORT = PerfReport()


# ---------------------------------------------------------------------------
# Inputs of a chosen size
# ---------------------------------------------------------------------------


def mapping_with_variables(count: int) -> Dict[str, Any]:
    """The shared mapping with ``count`` extra categorical variables, each
    mapped to its own column ``col_<i>`` of the one table."""
    mapping = valid_mapping()
    variables = mapping["schema"]["variables"]
    columns = mapping["databases"]["centre_x"]["tables"][TABLE]["columns"]
    for i in range(count):
        key = f"variable_{i:04d}"
        variables[key] = {
            "@id": f"schema:variable/{key}",
            "@type": "schema:CategoricalVariable",
            "dataType": "categorical",
            "predicate": "sio:SIO_000008",
            "class": f"ncit:C{10000 + i}",
            "valueMapping": {
                "terms": {
                    "yes": {"targetClass": "ncit:C49488"},
                    "no": {"targetClass": "ncit:C49487"},
                }
            },
        }
        columns[key] = {
            "mapsTo": f"schema:variable/{key}",
            "localColumn": f"col_{i:04d}",
            "localMappings": {"yes": "1", "no": "0"},
        }
    return mapping


def mapping_with_databases(count: int, columns_per_table: int = 10) -> Dict[str, Any]:
    """``count`` databases of one table each, every column mapped."""
    mapping = valid_mapping()
    template = mapping["databases"].pop("centre_x")
    for d in range(count):
        name = f"table_{d:04d}"
        database = json.loads(json.dumps(template))
        database["@id"] = f"mapping:database/db_{d:04d}"
        database["name"] = name
        table = database["tables"].pop(TABLE)
        table["@id"] = f"mapping:table/{name}"
        table["sourceFile"] = name
        table["columns"] = {
            f"c{c}": {
                "mapsTo": "schema:variable/age_at_diagnosis",
                "localColumn": f"col_{c}",
            }
            for c in range(columns_per_table)
        }
        database["tables"] = {name: table}
        mapping["databases"][f"db_{d:04d}"] = database
    return mapping


def body(document: Any) -> bytes:
    """Serialise once, outside the timed request."""
    return json.dumps(document).encode("utf-8")
