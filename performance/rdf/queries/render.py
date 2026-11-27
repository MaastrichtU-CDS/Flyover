"""
Render the benchmark's read queries from STRONG AYA's own templates.

The queries a consumer sends to the store are those of v6-tools-rdf
(https://github.com/STRONGAYA/v6-tools-rdf): two templates, filled in per
variable from the schema. This script fills those templates for the
benchmark's pinned schema and writes the finished queries beside itself,
one ``.rq`` file each:

    single_column__<variable>.rq
    multi_column__<variable>__<variable>.rq

The benchmark only ever reads the ``.rq`` files; this script is how they
were made, and how to make them again when the library or the schema moves:

    git clone https://github.com/STRONGAYA/v6-tools-rdf <somewhere>
    python -m performance.rdf.queries.render --upstream <somewhere>

Nothing of the library is executed. Two of its data files are read: the
query templates, and its own record of every variable's class and predicate
path (``tests/unit/snapshots/predicate_paths.json``, which the library's
tests hold its parser to). The few lines below that put a query together
repeat what the library's ``_prepare_query_template``,
``_process_variable_query`` and ``_process_multi_column_query`` do with
them. ``UPSTREAM.md`` records which commit the files in this folder came
from.

Pairs: the library also correlates two variables on the questionnaire or
record they were filled in with, when one of them is that container's own
entry (a recording time, say). Working that out takes its parser, so the
pairs here are ones it does not correlate: two ordinary attributes.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ..workload.generator import IDENTIFIER, load_schema
from . import pair_name, single_name

HERE = Path(__file__).resolve().parent
TEMPLATES = "src/vantage6_strongaya_rdf/query_templates"
SNAPSHOT = "tests/unit/snapshots/predicate_paths.json"

# What the triplifier's own structure is named; not part of any schema.
STRUCTURAL_PREFIXES = {
    "dbo": "http://um-cds/ontologies/databaseontology/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}

# The variables the benchmark can query on their own, and the pairs it can
# query together. A profile uses those among them that its tables hold.
SINGLE = (
    "biological_sex",
    "age_at_initial_diagnosis",
    "year_of_initial_diagnosis",
    "gender",
    "educational_level",
    "comorb_mi",
    "comorb_dm",
    "overall_stage",
    "laterality",
    "tumour_topography_or_location_icd_10",
    "tumour_morphology_icd_o",
    "clinical_t_stage",
    "clinical_n_stage",
    "clinical_m_stage",
    "treatment_intent",
    "chemotherapy",
    "radiotherapy",
    "surgery",
    "bespoke_q2_cancer_spread",
    "bespoke_q87_fear_of_recurrence",
    "eortc_qlq_aya_q1",
    "eortc_qlq_aya_q2",
)
PAIRS = (
    ("biological_sex", "age_at_initial_diagnosis"),
    ("clinical_t_stage", "chemotherapy"),
)

# A prefixed name, and a property path made of prefixed names.
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_.\-]+$")
_PATH = re.compile(r"^[A-Za-z0-9_:.\-/|()]+$")


def schema_prefixes(schema: Mapping[str, Any]) -> Dict[str, str]:
    """The ontologies a schema names: in its ``@context`` and, with
    precedence, in ``schema.prefixes``. Only absolute addresses count."""
    prefixes = {}
    for source in (schema.get("@context", {}), schema["schema"].get("prefixes", {})):
        for name, address in source.items():
            if (
                not name.startswith("@")
                and isinstance(address, str)
                and address.startswith(("http://", "https://"))
            ):
                prefixes[name] = address
    return prefixes


def load_upstream(checkout: Path) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    """The library's two templates, and its record of every variable."""
    snapshot_path = checkout / SNAPSHOT
    if not snapshot_path.is_file():
        raise SystemExit(f"{checkout} is not a checkout of v6-tools-rdf")
    templates = {
        name: (checkout / TEMPLATES / f"{name}.rq").read_text(encoding="utf-8")
        for name in ("single_column", "multi_column")
    }
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    pinned = load_schema().get("version")
    if snapshot.get("schema_version") != pinned:
        raise SystemExit(
            f"the library records paths for schema {snapshot.get('schema_version')}, "
            f"the benchmark's schema is {pinned}: bring the two to the same release"
        )
    variables = snapshot["variables"]
    for name, entry in variables.items():
        if not (
            _NAME.match(entry.get("class", ""))
            and _PATH.match(entry.get("predicate_path", ""))
            and re.match(r"^[A-Za-z][A-Za-z0-9_]*:$", entry.get("ontology_prefix", ""))
        ):
            raise SystemExit(f"the library's record of '{name}' is not as expected")
    return templates, variables


def prepare_template(
    template: str, schema: Mapping[str, Any], recorded: Mapping[str, Dict[str, str]]
) -> str:
    """The prefixes and the patient's identifier, which every query shares."""
    prefixes = {**schema_prefixes(schema), **STRUCTURAL_PREFIXES}
    declarations = "\n".join(
        f"PREFIX {prefix}: <{address}>" for prefix, address in sorted(prefixes.items())
    )
    identifier = recorded[IDENTIFIER]
    return (
        template.replace("PLACEHOLDER_PREFIXES", declarations)
        .replace("PLACEHOLDER_ID_PREDICATE", identifier["predicate_path"])
        .replace("PLACEHOLDER_ID_CLASS", identifier["class"])
    )


def render_single(variable: str, template: str, recorded: Mapping) -> str:
    entry = _recorded(variable, recorded)
    return (
        template.replace("PLACEHOLDER_CLASS", entry["class"])
        .replace("PLACEHOLDER_ONTOLOGY", entry["ontology_prefix"])
        .replace("PLACEHOLDER_PREDICATE_PATH", entry["predicate_path"])
    )


def render_pair(variables: Tuple[str, str], template: str, recorded: Mapping) -> str:
    query = template
    for index, variable in enumerate(variables, start=1):
        entry = _recorded(variable, recorded)
        attribute = "attr" if index == 1 else "attr2"
        query = (
            query.replace(f"PLACEHOLDER_CLASS_{index}", entry["class"])
            .replace(f"PLACEHOLDER_ONTOLOGY_{index}", entry["ontology_prefix"])
            .replace(
                f"PLACEHOLDER_FETCH_BLOCK_{index}",
                f"?p{index} {entry['predicate_path']} ?{attribute} .",
            )
            .replace(f"PLACEHOLDER_INSTANCE_BLOCK_{index}", "")
        )
    return query


def _recorded(variable: str, recorded: Mapping) -> Dict[str, str]:
    if variable not in recorded:
        raise SystemExit(f"the library records no variable '{variable}'")
    return recorded[variable]


def render_all(
    checkout: Path,
    single: Optional[Iterable[str]] = None,
    pairs: Optional[Iterable[Tuple[str, str]]] = None,
) -> Dict[str, str]:
    """Every query of the benchmark, by file name."""
    single = SINGLE if single is None else single
    pairs = PAIRS if pairs is None else pairs
    templates, recorded = load_upstream(checkout)
    schema = load_schema()
    one = prepare_template(templates["single_column"], schema, recorded)
    two = prepare_template(templates["multi_column"], schema, recorded)
    queries = {}
    for variable in single:
        queries[single_name(variable)] = render_single(variable, one, recorded)
    for first, second in pairs:
        queries[pair_name(first, second)] = render_pair((first, second), two, recorded)
    for name, text in queries.items():
        if "PLACEHOLDER" in text:
            raise SystemExit(f"{name}: the template has a part this script leaves open")
    return queries


def upstream_commit(checkout: Path) -> str:
    done = subprocess.run(
        ["git", "-C", str(checkout), "log", "-1", "--format=%H %ad", "--date=short"],
        capture_output=True,
        text=True,
    )
    return done.stdout.strip() or "unknown"


def main(arguments: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "--upstream", required=True, type=Path, help="a checkout of v6-tools-rdf"
    )
    parser.add_argument(
        "--output", type=Path, default=HERE, help="where to write the .rq files"
    )
    options = parser.parse_args(arguments)

    queries = render_all(options.upstream)
    options.output.mkdir(parents=True, exist_ok=True)
    for stale in options.output.glob("*.rq"):
        stale.unlink()
    for name, text in sorted(queries.items()):
        (options.output / name).write_text(text.rstrip() + "\n", encoding="utf-8")
    print(
        f"{len(queries)} queries written to {options.output} from v6-tools-rdf "
        f"{upstream_commit(options.upstream)}; record that commit in UPSTREAM.md"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
