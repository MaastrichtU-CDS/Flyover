"""
The right answers to the read queries.

The tables are generated, so what a consumer should get back is known
without asking any version of Flyover: every cell that holds a value is one
record of its patient, a coded value comes back as the schema's class for
that code, and an empty cell is no record at all.

That makes the check independent of both sides of a comparison. A rewritten
query, a lighter ruleset or a change to annotation is right when the
consumer still gets these answers, and wrong when it does not, however
fast it has become.
"""

from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .queries import variables_of
from .workload.generator import MISSING_TERM, Workload, load_schema, schema_variables

# The schema's own namespace for classes it defines itself; the queries
# accept a value's class from it whatever ontology the variable is in.
OWN_PREFIX = "strongaya"

Row = Tuple[Optional[str], ...]


def expected(file_name: str, workload: Workload, tables: Sequence[str]) -> Counter:
    """The rows a query should return once ``tables`` are in the store and
    annotated, as (patient, class, value) for a single variable and (patient,
    class, class, value, value) for a pair. How often a row occurs counts."""
    variables = variables_of(file_name)
    records = [_records(variable, workload, tables) for variable in variables]
    if len(variables) == 1:
        return Counter(records[0])
    # A pair: every record of the first variable beside every record of the
    # second that belongs to the same patient, whichever table each is in.
    by_patient: Dict[str, List[Row]] = {}
    for patient, sub_class, value in records[1]:
        by_patient.setdefault(patient, []).append((sub_class, value))
    rows: Counter = Counter()
    for patient, sub_class, value in records[0]:
        for other_class, other_value in by_patient.get(patient, []):
            rows[(patient, sub_class, other_class, value, other_value)] += 1
    return rows


def returned(file_name: str, bindings: Iterable[Mapping[str, Any]]) -> Counter:
    """A query's answer (the bindings of a SPARQL JSON result) in the same
    form as ``expected``."""
    pair = len(variables_of(file_name)) == 2
    rows: Counter = Counter()
    for binding in bindings:
        get = lambda name: (binding.get(name) or {}).get("value")  # noqa: E731
        if pair:
            row = (
                get("patientID"),
                get("subClass"),
                get("subClass2"),
                get("any_value"),
                get("any_value2"),
            )
        else:
            row = (get("patientID"), get("subClass"), get("any_value"))
        rows[row] += 1
    return rows


def difference(
    file_name: str,
    bindings: Iterable[Mapping[str, Any]],
    workload: Workload,
    tables: Sequence[str],
    show: int = 3,
) -> str:
    """What is wrong with a query's answer, or '' when it is right."""
    want = expected(file_name, workload, tables)
    got = returned(file_name, bindings)
    if want == got:
        return ""
    missing, unexpected = want - got, got - want
    parts = [
        f"{sum(got.values())} rows returned, {sum(want.values())} expected",
    ]
    if missing:
        parts.append(
            f"{sum(missing.values())} missing, such as {_examples(missing, show)}"
        )
    if unexpected:
        parts.append(
            f"{sum(unexpected.values())} not expected, such as "
            f"{_examples(unexpected, show)}"
        )
    return "; ".join(parts)


def _records(
    variable: str, workload: Workload, tables: Sequence[str]
) -> List[Tuple[str, Optional[str], str]]:
    """(patient, class of the value, value) of every filled cell."""
    records = []
    for name in tables:
        table = workload.table(name)
        column = table.column(variable)
        for patient, value in table.values(variable):
            if value == "":
                continue
            records.append((patient, sub_class(variable, column.term_of(value)), value))
    return records


def sub_class(variable: str, term: Optional[str]) -> Optional[str]:
    """The class a coded value comes back as: the schema's target class of
    its term, when that class is in the variable's own ontology or in the
    schema's own namespace. The queries return no class otherwise."""
    if term is None or term == MISSING_TERM:
        return None
    definition = schema_variables()[variable]
    target = (
        (definition.get("valueMapping") or {})
        .get("terms", {})
        .get(term, {})
        .get("targetClass")
    )
    if not target:
        return None
    address = expand(target)
    accepted = (
        _namespace(definition.get("class", "").partition(":")[0]),
        _namespace(OWN_PREFIX),
    )
    return address if any(ns and address.startswith(ns) for ns in accepted) else None


def expand(name: str) -> str:
    """A prefixed name of the schema as a full address."""
    prefix, _, local = name.partition(":")
    return f"{_namespace(prefix)}{local}"


def _namespace(prefix: str) -> str:
    schema = load_schema()
    for source in (schema["schema"].get("prefixes", {}), schema.get("@context", {})):
        address = source.get(prefix)
        if isinstance(address, str) and address.startswith(("http://", "https://")):
            return address
    return ""


def _examples(rows: Counter, show: int) -> str:
    return ", ".join(str(row) for row in sorted(rows, key=str)[:show])
