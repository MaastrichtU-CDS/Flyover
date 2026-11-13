# Performance benchmark

Says whether one set-up of Flyover is slower than another. It runs the same
work on both, on the same machine, in turns, and compares them pairwise: a
release against a branch, one store ruleset against another, today's read
queries against rewritten ones.

It is the line to measure optimisation work against. It does not optimise
anything itself.

- [What it measures](#what-it-measures)
- [Running it](#running-it)
- [Sides](#sides)
- [Reading a result](#reading-a-result)
- [What it found so far](#what-it-found-so-far)
- [In CI](#in-ci)
- [How the folder is laid out](#how-the-folder-is-laid-out)
- [What it does not cover](#what-it-does-not-cover)

## What it measures

### The scenario: repeated intake

A centre rarely loads its data once. A first table goes in and is annotated;
later more data follows for the same patients, under a new file name. Each
of those intakes lands in a store that already holds annotated data, and
that is where the time goes. On a laptop, three intakes of 50 patients with
ten variables each took 1.9 s, 6.7 s and 15.5 s to annotate: the third costs
eight times the first.

So the benchmark measures a sequence, on the real stack (gunicorn and the
store, over HTTP, no browser):

1. upload a first table, load the map, annotate;
2. upload a follow-up table of the same patients, linked to the first by
   the patient's identifier, load the map, annotate; and again for every
   further table;
3. send the read queries a consumer of the store sends, and check their
   answers.

Centres annotate in two ways after adding data, and each is a scenario:

| Scenario | After each intake |
|---|---|
| `repeated-intake/annotate-new` | only the table just added is annotated |
| `repeated-intake/annotate-all` | everything the map names is annotated, the earlier tables again |

### The data

Synthetic, and the same bytes on every machine. The schema is the one STRONG
AYA publishes (208 variables, pinned to a release, see
[`rdf/workload/schema/UPSTREAM.md`](rdf/workload/schema/UPSTREAM.md)); the
tables, their local codes and the map's local mapping are generated from it
with a seed. No centre's data or map is involved.

Categories are stored as codes, as centres store them, and a class has the
same code in every column that uses it: "yes" is the same number whether
it answers chemotherapy or radiotherapy. That is on purpose, see
[what the benchmark ran into](#what-the-benchmark-ran-into).

### The metrics

Four metrics are judged, each over the whole sequence:

| Metric | What it is |
|---|---|
| Conversion | time of the `/upload` requests: parse, triplify, load into the store |
| Annotation | time of the `/start-annotation` requests |
| Read queries | time of the read queries, sent to the store as a consumer does |
| Flyover's peak memory | the app container's peak, from its cgroup |

Beside them every result reports, without judging: each intake's times, how
many times as long the last intake took as the first, each read query's
time, the CPU seconds of the app and of the store, the store's memory, and
the number of statements the store was given.

Times are taken at the client; CPU and memory come from Docker. Nothing is
asked of the application, so the same measurement works on an old release.

### The right answers

The tables are generated, so the right answer to every read query is known
without asking any version of Flyover ([`rdf/oracle.py`](rdf/oracle.py)):
every filled cell is one record of its patient, a coded value comes back as
the schema's class for that code, an empty cell is no record. Every answer of
every run is held against that.

This is what makes it safe to optimise: a lighter ruleset or a rewritten
query that is faster but returns something else is caught. It also guards
against a quiet failure the application does not report: Flyover can answer
that annotation succeeded while the store refused part of it.

### The read queries

They are STRONG AYA's own, from
[v6-tools-rdf](https://github.com/STRONGAYA/v6-tools-rdf), kept as finished
`.rq` files in [`rdf/queries/`](rdf/queries) (see its `UPSTREAM.md`). They
are expected to be optimised. To try a rewritten set, put it in a folder
with the same file names and name it in a [side file](#sides).

## Running it

It needs Docker (with Compose) and Python 3.10 or later:

```bash
pip install -r performance/requirements.txt
```

All commands run from the repository's root.

```bash
# this checkout against the last release
python -m performance compare --baseline release:3.5.0 --candidate tree:.

# this checkout against main: what an optimisation branch does
python -m performance compare --baseline ref:main --candidate tree:.

# one side against itself (an A/A run): how much this machine alone makes two runs differ
python -m performance calibrate --side tree:.

# what the profiles are
python -m performance profiles
```

A run prints its progress and then its summary, and writes everything it
measured to `performance-result.json` (`--output` to put it elsewhere).
Useful options:

| Option | |
|---|---|
| `--profile pr` | how much work: `pr` (default), `full` or `smoke`, see [`rdf/profiles.yml`](rdf/profiles.yml) |
| `--repetitions 6` | measured runs per side, instead of the profile's |
| `--threshold 0.1` | the smallest change to flag, as a fraction (default 0.2) |
| `--summary file.md` | also write the summary to a file |
| `--keep` | leave both stacks running afterwards, to look inside |

On a laptop the `pr` profile takes about six minutes once the images exist;
the first run also builds or pulls them.

The benchmark starts two stacks of its own, in Compose projects named
`flyover-bench-baseline` and `flyover-bench-candidate`, on ports 5210, 5211,
7210 and 7211, each with its own network. A Flyover you have running
yourself is not touched. Close other heavy work while it runs: both sides
share the machine with it.

A result can be written out again in other forms:

```bash
python -m performance report performance-result.json                    # the summary
python -m performance report performance-result.json --format comment   # the PR comment
```

The exit code says whether the work was done, not whether it was fast:

| Code | |
|---|---|
| 0 | measured, flagged or not; or the baseline failed |
| 1 | the candidate did not do the work: a failed step or wrong answers |
| 2 | the run could not be made: a side, profile or stack that does not work |

## Sides

A side is a whole set-up: the app's image, the store's image (which carries
the store's settings, such as GraphDB's ruleset), settings for both, and the
set of read queries. It is written as `kind:value`:

| Side | What runs |
|---|---|
| `release:3.5.0` | the two images that release published |
| `tree:.` | built from a checkout: `<path>/app` and `<path>/stores/rdf/graphdb` |
| `ref:main` | built from a git ref of this repository, as committed |
| `images:<app>,<store>` | two images that exist already |
| `file:side.yml` | any of the above, with changes |

A side file takes one of the others as its `base` and changes what it names.
Everything but `base` is optional:

```yaml
base: tree:.
store_build: ./graphdb-lighter-ruleset     # a folder with a Dockerfile
store_image: ghcr.io/example/graphdb:test  # or an image that exists
app_build: ../another/checkout/app
app_image: ghcr.io/example/flyover-app:test
environment:                               # Flyover's settings
  FLYOVER_MATERIALIZE_INFERENCES: "true"
store_environment:                         # the store's settings
  GDB_HEAP_SIZE: 4g
queries: ./queries-rewritten               # a folder of .rq files
```

Three comparisons this makes possible:

```bash
# another ruleset: a copy of stores/rdf/graphdb with its config changed
python -m performance compare --baseline tree:. --candidate file:lighter-ruleset.yml

# rewritten read queries, on otherwise the same set-up
python -m performance compare --baseline tree:. --candidate file:new-queries.yml

# two releases
python -m performance compare --baseline release:3.5.0 --candidate release:3.6.0
```

GraphDB is the only store so far. Another one needs a compose file of its
own in [`compose/`](compose) and an entry in `STORE_KINDS`
([`harness/sides.py`](harness/sides.py)); nothing else names GraphDB.

## Reading a result

```
| Metric     | Baseline | Candidate | Change | 95 % interval | Spread    |           |
| Annotation | 24.3 s   | 31.6 s    | +30 %  | +26 % to +34 % | 2 % / 2 % | ⚠️ slower |
```

- **Baseline, Candidate**: each side's median over the repetitions.
- **Change**: the candidate against the baseline, from the pairs: each
  repetition's two values are compared with each other, so the machine's
  speed at that moment is in both and cancels out.
- **95 % interval**: how well the repetitions agree on the change.
- **Spread**: how much each side's own repetitions differ (baseline /
  candidate). A large spread means a noisy machine.
- **Flag**: a metric is flagged only when the change is at least the
  threshold (20 %) *and* the interval excludes no change. ⚠️ is worse, 🟢 is
  better. Fewer than three repetitions are never flagged.

How a run ended:

| Outcome | |
|---|---|
| `ok` | measured, nothing flagged |
| `flagged` | measured, at least one metric moved by the threshold or more |
| `failed` | the candidate did not do the work; nothing was compared |
| `invalid` | the baseline did not do the work; nothing can be said of the candidate |

Seconds from two runs are not comparable unless the runs were on the same
machine. Percentages are: both sides of a run shared one.

### How small a change can be seen

Calibrate before trusting a small change on a machine or runner: a side
compared with itself should show changes near 0 %, and what it shows
instead is what that machine cannot tell apart.

On the laptop this was built on, one run's repetitions agree within 1 to
4 % (the spread), yet the same set-up compared with itself came out 1 to
4 % apart, the same way round, in two runs, with intervals that excluded
no change: the side whose stack was started second was the faster one.
Each side's store is one Java process for the whole run, and the interval
only knows how the repetitions differ, not how two such processes do. So:

- a change below about 5 % says nothing, however narrow its interval;
- the 20 % flag is far enough above that to be trusted;
- to judge something smaller, run the comparison more than once, and swap
  the two sides in one of the runs.

A CI runner has not been calibrated yet. Run the workflow on demand with
the same side as baseline and candidate a few times before reading much
into its numbers; the outcome belongs on the history page.

## What it found so far

Measured while the benchmark was built (November 2026, a laptop with an
Intel i7-10750H, GraphDB 10.8.5 with the `owl-max-optimized` ruleset).
These are starting points for the optimisation work, not results of it.

| | |
|---|---|
| Annotation is store time | Over a sequence Flyover used 1.5 s of CPU and GraphDB 41 s. |
| Each intake costs more than the last | Three intakes of 50 patients × 11 columns: 1.9 s, 6.7 s and 15.5 s to annotate, 0.7 s, 1.7 s and 3.0 s to upload. |
| Annotating everything again costs far more | Three intakes of 60 patients × 15 columns: 4 s, 14 s and 36 s when only the new table is annotated; 3.5 s, 32 s and 169 s when everything is. |
| It is the annotations, not the data | Uploading five tables of 15,000 rows one after another, without annotating, took 65 to 80 s each. With annotation in between, three uploads of 150 rows took 1.2 s, 3.3 s and 6.1 s. |
| Size, for one table of 11 columns in an empty store | 150 rows: 1 s to upload, 3 s to annotate. 1,500 rows: 5.5 s and 70 s. 15,000 rows: 58 s and over ten minutes. |
| Conversion is linear, and hungry | About 80 µs and 5 KB of memory per cell, the same for long and wide tables: 9 GB for 150,000 rows of 11 columns. |
| Two-variable read queries are the slow ones | On three tables of 50 patients a single-variable query took 0.04 to 0.2 s, a pair 3 to 6 s. |
| Since v3.5.0 | Nothing is flagged. On the `full` profile the branch's read queries are 7 to 9 % slower than the release's in both scenarios, and conversion and annotation 2 to 3 %; on the `pr` profile, with fewer variables, none of that shows. |

### What the benchmark ran into

Three things in Flyover itself, found because every answer is checked. None
is fixed here; the benchmark steps around each so that it can measure.

- **One class, two codes.** When two variables give the same class different
  local codes ("yes" is 2 for chemotherapy and 1 for another variable), the
  read queries return wrong classes and the same record more than once.
  Annotation describes a value mapping to the store as "this class is
  whatever has this value in this variable"; two such descriptions of one
  class make the store treat them as the same thing. It happens on v3.5.0
  and since. The benchmark's data therefore uses one code per class.
- **Success that is not.** `/start-annotation` answers that it succeeded
  even when the store refused part of the work (seen when the store ran
  out of space). The benchmark reads back what was annotated and checks the
  answers instead of trusting the reply.
- **Links made at the wrong moment, and too many.** Flyover links a
  follow-up table to the first one in a background job that starts five
  seconds after the upload and reads whatever the session holds by then. An
  upload that begins within those five seconds is linked by the earlier
  upload's job as well as by its own, and when that happens after a table
  was annotated, tables end up linked to themselves. And once tables are
  annotated, a new table is linked to every earlier follow-up table, not
  only to the first. The benchmark waits between uploads, so each run does
  the same work.

## In CI

[`performance-rdf.yml`](../.github/workflows/performance-rdf.yml) runs the
comparison:

| When | Baseline | Candidate | Profile |
|---|---|---|---|
| a pull request to `main` that touches the backend, the app's Dockerfile, the stores, the compose files or this folder | `main` | the pull request | `pr` |
| a push to `main` that touches the same | the last release | `main` | `full` |
| on demand (*Run workflow*) | any side | any side | any |

- The job summary always holds the full report, and the result file is kept
  as an artefact for 90 days.
- A pull request gets one comment, written when something is flagged or the
  candidate failed, and kept up to date after that.
- After a push to `main` the run is added to the project's wiki, under
  `performance/rdf`: a row on the page and the result file beside it.

**Timings advise; they never fail the job.** The job fails when the
benchmark's own tests fail, or when the candidate did not do the work.

Two smaller checks run with the backend's tests on every pull request
(see [`docs/testing.md`](../docs/testing.md)): how conversion time grows with
rows and columns (advisory), and how many requests the store gets per
annotated variable (exact, blocking).

## How the folder is laid out

```
performance/
├── harness/            shared by every workflow
│   ├── cli.py          the command line
│   ├── sides.py        what a side is, and building or pulling its images
│   ├── stack.py        a side's stack: its own Compose project
│   ├── measure.py      timing requests, reading a container's counters
│   ├── runner.py       the paired loop, and how a run is judged
│   ├── stats.py        the statistics behind a flag
│   ├── result.py       the result file
│   ├── report.py       the summary, the comment, the history row
│   └── history.py      the history page
├── compose/            the stack a side runs in
├── rdf/                the RDF workflow
│   ├── flow.py         the flow's requests
│   ├── scenarios.py    repeated intake, both ways of annotating
│   ├── workload/       the generator and the pinned schema
│   ├── queries/        the read queries, and how they were rendered
│   ├── oracle.py       the right answers
│   ├── profiles.yml    sizes and repetitions
│   └── workflow.py     the above, as the harness sees it
└── tests/              the benchmark's own tests; they start no stack
```

The folder is not part of the application and never ends up in an image:
the app does not import it, and no image is built from a folder that
contains it. `tests/test_layout.py` holds both to that.

Its tests run with:

```bash
pip install -r performance/requirements.txt -r performance/requirements-test.txt
python -m pytest performance/tests -q
```

### Adding a workflow

A new workflow (OMOP, CSV) gets a folder beside `rdf/` with its own flow,
scenarios, data and profiles, and a class like `rdf/workflow.py`'s, entered
in `WORKFLOWS` in `harness/cli.py`. The harness, the report and the history
are shared; its history goes to `performance/<workflow>` in the wiki.

## What it does not cover

- **Large tables.** Sizes are small on purpose: annotating one table of
  15,000 rows takes GraphDB about ten minutes, and conversion needs about
  5 KB of memory per cell (9 GB for 150,000 rows of 11 columns). The store
  runs on a 2 GB tmpfs, which one annotated table of 15,000 rows fills; a
  profile can raise it with `settings: {BENCH_STORE_TMPFS: 4g}`.
- **The describe pages.** The sequence goes from upload to map to
  annotation; the describe forms are not sent.
- **The same file name uploaded again.** Flyover names a row after its
  position in the file, so a second file of the same name does not add rows
  but lands on the first one's. There is no right answer to check, so it is
  not a scenario.
- **PostgreSQL and Excel as sources.** CSV only.
- **The frontend.** No browser is involved.
- **The link jobs' own time.** Flyover links a follow-up table in the
  background, a few seconds after the upload; the benchmark waits for the
  link before it annotates, and reports the wait, but does not judge it.
