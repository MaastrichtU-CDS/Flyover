"""
The RDF workflow's scenarios: what is done to a stack, and what is measured.

Repeated intake is the one that matters. A centre rarely loads its data
once: a first table goes in and is annotated, and later more data follows,
under new file names, for the same patients. Each intake then lands in a
store that already holds annotated data, and that is where the time goes.
The scenario therefore measures a sequence of intakes, not one:

    for each table, in turn:
        upload it                         (conversion)
        link it to the first table        (follow-up tables only)
        load the map and annotate         (annotation)
    then send the read queries a consumer sends, and check their answers

Two ways of annotating after an intake are measured, because centres do
both: only the table that was just added (``annotate-new``), or everything
the map names, the earlier tables again included (``annotate-all``).
"""

import time
from typing import Any, Dict, List, Optional, Sequence

import requests

from ..harness.measure import cpu_between, timed
from ..harness.result import Sample
from ..harness.sides import Side
from ..harness.stack import APP_SERVICE, STORE_SERVICE, Stack
from . import oracle, queries
from .flow import FlowError, RdfFlow
from .workload.generator import Table, Workload

ANNOTATE_NEW = "repeated-intake/annotate-new"
ANNOTATE_ALL = "repeated-intake/annotate-all"
SCENARIOS = (ANNOTATE_NEW, ANNOTATE_ALL)

# Flyover starts its link jobs three and five seconds after an upload has
# answered, and they read what the session holds at that moment: a job that
# wakes up after the next upload has begun links the wrong tables. A user
# is still reading the next page by then. The benchmark leaves at least this
# long between one upload's answer and the next upload, so every run does
# the same work in the same order. (A follow-up table is not annotated
# before its link is in the store either, see ``wait_for_link``.)
SETTLE_SECONDS = 5.5

QUERY_TIMEOUT = 3600


def run(
    stack: Stack,
    side: Side,
    workload: Workload,
    scenario: str,
    tables: Optional[Sequence[Table]] = None,
) -> Sample:
    """One sequence of intakes on a fresh stack, then the read queries."""
    if scenario not in SCENARIOS:
        raise ValueError(f"'{scenario}' is not one of {', '.join(SCENARIOS)}")
    sample = Sample(scenario)
    flow = RdfFlow(stack)
    tables = list(tables or workload.tables)
    first = workload.tables[0]

    try:
        flow.enter()
    except FlowError as error:
        sample.errors.append(str(error))
        return sample

    app_before = stack.counters(APP_SERVICE)
    store_before = stack.counters(STORE_SERVICE)
    intakes: List[Dict[str, Any]] = []
    ingested: List[str] = []
    answered: Optional[float] = None  # when the last upload was answered

    for index, table in enumerate(tables):
        if answered is not None:
            remaining = SETTLE_SECONDS - (time.monotonic() - answered)
            if remaining > 0:
                time.sleep(remaining)
        upload = flow.upload(table, link_to=first if index else None)
        answered = time.monotonic()
        sample.steps.append(upload)
        if not upload.ok:
            sample.errors.append(f"{upload.name}: {upload.error}")
            return sample
        ingested.append(table.name)

        link_seconds = None
        if index:
            link = flow.wait_for_link(table)
            sample.steps.append(link)
            if not link.ok:
                sample.errors.append(f"{link.name}: {link.error}")
                return sample
            link_seconds = link.seconds

        names = [table.name] if scenario == ANNOTATE_NEW else list(ingested)
        loaded = flow.load_map(workload.mapping_bytes(names))
        sample.steps.append(loaded)
        if not loaded.ok:
            sample.errors.append(f"{loaded.name} for {table.name}: {loaded.error}")
            return sample

        annotated = flow.annotate(f"annotate after {table.name}")
        sample.steps.append(annotated)
        if not annotated.ok:
            sample.errors.append(f"{annotated.name}: {annotated.error}")
            return sample
        error = _annotation_error(flow, len(names) * len(workload.variables))
        if error:
            sample.errors.append(f"{annotated.name}: {error}")
            return sample

        intakes.append(
            {
                "table": table.name,
                "conversion_s": upload.seconds,
                "annotation_s": annotated.seconds,
                "map_s": loaded.seconds,
                "link_wait_s": link_seconds,
            }
        )

    query_seconds = _read_queries(stack, side, workload, ingested, sample)

    app_after = stack.counters(APP_SERVICE)
    store_after = stack.counters(STORE_SERVICE)
    sample.metrics = {
        "conversion_s": round(sum(i["conversion_s"] for i in intakes), 4),
        "annotation_s": round(sum(i["annotation_s"] for i in intakes), 4),
        "queries_s": (
            None if not query_seconds else round(sum(query_seconds.values()), 4)
        ),
        "app_peak_memory_mb": app_after.memory_peak_mb,
    }
    statements = _statements(stack)
    sample.reported = {
        "intakes": intakes,
        "growth": {
            "conversion": _growth(intakes, "conversion_s"),
            "annotation": _growth(intakes, "annotation_s"),
        },
        "queries": query_seconds,
        "app_cpu_s": cpu_between(app_before, app_after),
        "store_cpu_s": cpu_between(store_before, store_after),
        "store_memory_mb": store_after.memory_current_mb,
        "statements": statements,
    }
    if statements is not None:
        sample.invariants["statements"] = statements
    return sample


def _annotation_error(flow: RdfFlow, expected_variables: int) -> str:
    """Flyover answers that annotation succeeded; this asks what it did."""
    done = flow.annotation_outcome()
    if done.get("error"):
        return f"the outcome could not be read: {done['error']}"
    if done["failed"]:
        return f"not annotated: {', '.join(done['failed'][:5])}"
    if done["variables"] != expected_variables:
        return f"{done['variables']} variables annotated, {expected_variables} expected"
    return ""


def _read_queries(
    stack: Stack,
    side: Side,
    workload: Workload,
    ingested: Sequence[str],
    sample: Sample,
) -> Dict[str, float]:
    """Send the side's read queries to its store, as a consumer does, and
    hold every answer against the generated data."""
    seconds: Dict[str, float] = {}
    for name, text in queries.load(side.queries, holding=workload.variables).items():
        step = timed(
            f"query {name}",
            lambda text=text: requests.post(
                stack.repository_url,
                data={"query": text},
                headers={"Accept": "application/sparql-results+json"},
                timeout=QUERY_TIMEOUT,
            ),
        )
        response = step.detail.pop("response", None)
        sample.steps.append(step)
        if not step.ok or step.status != 200:
            detail = step.error or (response.text[:300] if response is not None else "")
            sample.errors.append(
                f"{step.name}: the store answered {step.status} {detail}"
            )
            continue
        try:
            bindings = response.json()["results"]["bindings"]
        except (ValueError, KeyError):
            sample.errors.append(f"{step.name}: the answer is not a SPARQL result")
            continue
        wrong = oracle.difference(name, bindings, workload, ingested)
        if wrong:
            sample.errors.append(f"{step.name}: wrong answer: {wrong}")
        seconds[name] = step.seconds
    return seconds


def _statements(stack: Stack) -> Optional[int]:
    """How many statements the store holds as given (not inferred)."""
    try:
        answer = requests.get(f"{stack.repository_url}/size", timeout=600)
        return int(answer.text.strip()) if answer.status_code == 200 else None
    except (requests.RequestException, ValueError):
        return None


def _growth(intakes: Sequence[Dict[str, Any]], key: str) -> Optional[float]:
    """How many times as long the last intake took as the first."""
    if len(intakes) < 2 or not intakes[0][key]:
        return None
    return round(intakes[-1][key] / intakes[0][key], 2)
