# Where the read queries come from

The `.rq` files in this folder are the queries of STRONG AYA's
[v6-tools-rdf](https://github.com/STRONGAYA/v6-tools-rdf), the library its
federated algorithms read a centre's store with:

| | |
|---|---|
| Commit | `a9aa2359c9afcf24d9540f1d3f104176620789a2` (2026-08-25) |
| Templates | `src/vantage6_strongaya_rdf/query_templates/single_column.rq` and `multi_column.rq` |
| Paths and classes | `tests/unit/snapshots/predicate_paths.json`, for schema v2.0.1 |
| Licence | Apache-2.0, as Flyover's |

`render.py` fills the two templates in for the variables listed in it and
writes one file per query. Nothing of the library is executed: its templates
and its own record of every variable's class and predicate path are read as
data.

Not included: pairs of variables that the library correlates on the
questionnaire or record they were filled in with (a questionnaire answer
with its recording time, for one). Working those out takes the library's
parser.

## Changing the queries

These queries are expected to be optimised. To try a rewritten set, put it in
a folder of its own, with the same file names, and name that folder as a
side's `queries` (see `performance/README.md`). Both sets are then timed on
the same data, and every answer is held against what the generated tables
imply (`../oracle.py`), so a faster query that returns something else is
caught.

A rewrite that holds up belongs in v6-tools-rdf; once it is there, render
this folder again and update the commit above.
