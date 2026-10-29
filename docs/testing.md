# Testing

Several test layers, each with a different speed/realism trade-off. This doc tells you what each layer does, how to run it locally, and when to write which kind of test.

## The layers

| Layer        | Tool                | Lives at                                                              | What it tests                                                              | Speed       | CI workflow                                                                 |
|--------------|---------------------|-----------------------------------------------------------------------|----------------------------------------------------------------------------|-------------|-----------------------------------------------------------------------------|
| Backend unit | pytest              | [`app/backend/data_descriptor/tests/unit/`](../app/backend/data_descriptor/tests/unit/) | Services, repositories, loaders, validators — mocked GraphDB              | <2 s        | [`unit-tests.yml`](../.github/workflows/unit-tests.yml)                     |
| Backend integration | pytest       | [`tests/integration/`](../app/backend/data_descriptor/tests/integration/) | Controllers → services → repositories/loaders/validators with the real wiring; RDF store faked at the HTTP boundary, triplifier runs for real | ~30 s       | [`unit-tests.yml`](../.github/workflows/unit-tests.yml) (second step)       |
| Backend API  | pytest + Hypothesis | [`tests/api/`](../app/backend/data_descriptor/tests/api/) | Every route's HTTP behaviour (status, type, redirects, errors, session, compression, security) through the Flask test client, every service mocked | ~2 min      | [`unit-tests.yml`](../.github/workflows/unit-tests.yml) (third step, with a coverage gate) |
| Backend performance | pytest       | [`tests/perf/`](../app/backend/data_descriptor/tests/perf/) | Latency, growth with input size, uploads, compression and parallel annotation of the HTTP layer | ~10 s       | [`unit-tests.yml`](../.github/workflows/unit-tests.yml) (non-blocking step) |
| Frontend unit | Vitest             | [`app/frontend/tests/unit/`](../app/frontend/tests/unit/) | Stores, composables, view components — happy-dom, no backend               | <2 s        | [`frontend-checks.yml`](../.github/workflows/frontend-checks.yml)           |
| Frontend E2E | Playwright          | [`app/frontend/tests/e2e/`](../app/frontend/tests/e2e/) | Multi-page workflow against a real stack (Flask + GraphDB)                 | ~minutes    | [`frontend-smoke.yml`](../.github/workflows/frontend-smoke.yml) (1 spec); [`frontend-e2e-full.yml`](../.github/workflows/frontend-e2e-full.yml) (all specs) |

Plus a non-test code-quality job: [`code-quality.yml`](../.github/workflows/code-quality.yml) runs Black, flake8, mypy, Bandit, and Safety. See the **CI workflow map** below.

## Running tests locally

### Backend (pytest)

```bash
# from the repo root; uses uv to spin up an ephemeral env
cd app/backend/data_descriptor
uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock \
  python -m pytest tests/unit/ -q

# the integration layer additionally needs `responses` (HTTP-level fake of the RDF store)
uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock --with responses \
  python -m pytest tests/integration/ -q
```

```bash
# the API layer additionally needs Hypothesis (property-based fuzzing)
uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock --with hypothesis \
  python -m pytest tests/api/ -q

# the performance checks: run on their own, never under coverage
uv run --with-requirements ../requirements.txt --with pytest --with pytest-mock --with hypothesis \
  python -m pytest tests/perf/ -m perf -q
```

Add `--cov=. --cov-report=term-missing` if you want a coverage rollup. Drop `tests/unit/` and target a single file or class for faster iteration.

Each layer carries a pytest marker named after its folder (`unit`, `integration`, `api`, `perf`, `e2e`; registered in [`tests/conftest.py`](../app/backend/data_descriptor/tests/conftest.py)), so `-m integration` selects the same tests as the path. `-k integration` and `-k api` work too, but match by name: `-k api` also picks up a few integration tests named after the API they drive, so prefer `-m api` to run exactly the API layer. The performance checks only run when selected: with `-m perf`, or when pytest is pointed at `tests/perf/` alone; in a run of the whole tree they are skipped.

### Backend integration tests

The integration layer ([`tests/integration/`](../app/backend/data_descriptor/tests/integration/)) drives a Flask app wired like `data_descriptor_main` with the real controllers, services, repositories, query builder, loaders and validators. Nothing inside `data_descriptor` is mocked; only the two process boundaries are faked, where the real thing would leave the process:

- **The RDF store** is `FakeRdfStore` in [`support.py`](../app/backend/data_descriptor/tests/integration/support.py): a backend-agnostic SPARQL 1.1 endpoint served through `responses` at the `requests` level. It answers the SELECTs the query builder produces from a small in-memory model (`seed_table(...)`), answers ASK checks (`ask_answer`), accepts UPDATEs, serves the Graph Store Protocol, and records every request so a test can assert exactly which SPARQL crossed the boundary (`store.selects`, `store.asks`, `store.updates`, `store.graph_downloads`). Knobs cover the error paths: `outage`, `outage_after`, `select_status`, `ask_status`, `update_status`.
- **`curl`**, which the ingest service shells out to for graph uploads, is a recording script on `PATH` (`fake_curl` fixture; `fake_curl.uploads`, `fake_curl.fail()`).

The triplifier (`pythonTool`, from `requirements.txt`) runs for real into a temp directory, so the ingest tests triplify an actual CSV. The PostgreSQL ingest path runs against a real PostgreSQL: CI starts one as a service container, locally set `FLYOVER_TEST_POSTGRES_DSN=postgresql://user:pass@host:port/db` (for example from `docker run -d -e POSTGRES_USER=flyover -e POSTGRES_PASSWORD=flyover -e POSTGRES_DB=flyover -p 55432:5432 postgres:16-alpine`); without it those tests skip. Coverage runs read `app/backend/data_descriptor/.coveragerc` (pass `--cov-config` with that path when running from the repo root, as CI does), which leaves the test modules and the gunicorn entrypoint out of the denominator. Tests that document a known defect are marked `xfail(strict=True)` with the reason, so they flip to a failure once the defect is fixed.

One file per flow: `test_integration_ingest.py` and `..._ingest_uploads.py` (upload strategies, background jobs, PostgreSQL), `..._describe.py`, `..._annotate.py` and `..._annotation_cli.py`, `..._share.py`, `..._suggestions.py` (tier-1 matchers, both phases, prompt export and paste-back), `..._store_edges.py` (what the store should never answer), plus `..._loader_validation.py` (loader ↔ validator) and `..._session.py` (server-side state across requests). The older `test_loaders_real_files.py` and `test_validation_real_files.py` read `example_data/` directly.

A few practical notes about the layout:
- [`tests/conftest.py`](../app/backend/data_descriptor/tests/conftest.py) defines fixtures shared across `unit/` and `integration/` (sample DataFrame, JSON-LD mapping, mock RDF responses).
- [`tests/unit/conftest.py`](../app/backend/data_descriptor/tests/unit/conftest.py) adds unit-only stubs so unit tests stay isolated from real deps. Integration tests deliberately don't inherit those stubs.
- Many test modules do `sys.path.insert(0, str(Path(__file__).parent.parent.parent))` to make `data_descriptor` importable as a top-level package. If you add a new test under a new sub-folder, mirror that pattern.

### Backend API tests

The API layer ([`tests/api/`](../app/backend/data_descriptor/tests/api/), issue #109) tests every route's HTTP behaviour: status codes, content types, redirects and their `Location`, error responses, how the request body, query string or form is read (JSON, multipart, urlencoded, and what happens with anything else), the session across requests, compression and security. It drives the app [`create_app()`](../app/backend/data_descriptor/app_factory.py) builds, the same wiring gunicorn serves, through Flask's test client and with production settings (`TESTING` off), so an unhandled exception is the 500 a browser would get.

What is real and what is mocked:

- **Mocked, every one of them:** the RDF store service and the suggestion service in `APP_CONTEXT`, every `APP_CONTEXT` helper (triplifier run, graph upload, background jobs, PostgreSQL ingest, the semantic-map lookups, the name matcher) and the service classes the controllers import directly (`IngestService`, `DescribeService`, `AnnotateService`, `ShareService`). The doubles are `create_autospec`s: a controller calling a method that does not exist, or with the wrong arguments, fails the test; the helpers are specced from the factory's own callables, so they follow its signatures. See [`tests/api/support.py`](../app/backend/data_descriptor/tests/api/support.py).
- **Real:** Flask and Werkzeug, the controllers, and the request-boundary validation the controllers run before any service is involved (`MappingValidator`, `JSONLDMapping` parsing, the orphan-column clean-up), because those decide the HTTP answer. The session cache is the real `SessionCache`, so a misspelt attribute fails instead of landing on a MagicMock.
- **Never:** the network. An autouse guard refuses and records every socket connection and DNS lookup and fails the test that tried; the ingest controller, for one, builds a real store client when the context has none.

That is the line with the integration layer: there the services, repositories, loaders and the triplifier run for real against a store faked at HTTP level; here the services are doubles and the test is about what crosses HTTP. A scenario may appear in both (for example a store failure), checked at a different boundary.

**Route marks and the inventory.** Each test says which route it exercises and whether it is that route's success or failure path: `@pytest.mark.route("POST", "/upload", "failure")` (or `pytest.param(..., marks=pytest.mark.route(...))`, with literal arguments or module-level constants). A hook checks after the test body that the test really sent that request, so a label cannot lie. [`test_api_routes.py`](../app/backend/data_descriptor/tests/api/test_api_routes.py) then requires a success test for every method of every route and a failure test for every route (a refused method counts), pins the route table, Werkzeug's precedence between specific routes and the SPA fallback, and a 405 with an `Allow` header for every undeclared method. A new route therefore fails the build until it is in the inventory and has its tests.

**Property-based tests.** [`test_api_fuzz.py`](../app/backend/data_descriptor/tests/api/test_api_fuzz.py) uses [Hypothesis](https://hypothesis.readthedocs.io/) to send every route that reads input arbitrary JSON, mutated mappings, bytes that are not JSON, query strings, upload forms, filenames, error messages and paths, and checks that each answer is deliberate (2xx, 3xx or 4xx in the promised shape) and never a 500, a file from outside the served folders, a broken header or a half-written session. Known defects are characterised by exact predicates and pinned by their own xfail tests, so a failing property is always a new finding. Two profiles, chosen with `HYPOTHESIS_PROFILE`: `ci` (the default) replays a fixed set of 150 examples per property, so CI is deterministic; `thorough` explores 2,000 new examples per property for local bug hunting:

```bash
HYPOTHESIS_PROFILE=thorough python -m pytest tests/api/test_api_fuzz.py -q
```

**Known defects** are pinned like in the integration layer: `xfail(strict=True)` with the reason, so a fixed defect turns the test red until its marker is removed. A file per controller, `test_api_<controller>.py`, plus `test_api_app.py` (SPA shell, static folder, error pages), `test_api_security.py` (traversal, cookies, redirects, cross-site requests), `test_api_session.py` (state across requests, concurrency), `test_api_compression.py`, `test_api_factory.py` (settings, default wiring, the gunicorn entry point in a fresh process), `test_api_fuzz.py` and `test_api_routes.py`.

To add a test for a new route: put it in the controller's module, request the `client` and the doubles you arrange (`store`, `session_cache`, `ingest_service`, …), mark it with `pytest.mark.route`, and add the route to `EXPECTED_ROUTES` in `test_api_routes.py`.

CI holds the API layer on its own to at least 90 % line coverage of the HTTP layer it owns (the controllers, the factory, the compression and request-map helpers); it is at 99 %, 98 % with branch coverage.

### Backend performance checks

[`tests/perf/`](../app/backend/data_descriptor/tests/perf/) times requests through the API layer's app and doubles, so each figure is the HTTP layer's own: request latency, how the request-boundary work grows with the input (as a fitted growth exponent: about 1 for linear work, 2 for quadratic), upload and form parsing, compression, the real mock-data generator and the parallel annotation run. Production runs one gunicorn worker with gevent, where a request that keeps the CPU busy holds up every other request, so the checks bound single-request time rather than throughput.

Budgets are 10 to 30 times what a laptop measures, and scaling checks take the best of several runs, so they catch a regression in kind (an accidental quadratic loop, a lost cache) rather than flake on a slow runner; the exponents move by less than 0.1 under CPU contention. CI runs them in a non-blocking step and appends the report to the job summary; locally the report is printed after the run, and `FLYOVER_PERF_REPORT=<path>` also writes it as Markdown.

What they found (a laptop; CI runners are roughly two to three times slower):

| Check | Measured | What it means |
|---|---|---|
| Cheap requests (graph check, status, redirects, SPA shell) | 0.3–0.4 ms p50 | The HTTP layer adds next to nothing. |
| Validating a small mapping (validate, browser-map submit) | 1.6 ms p50 | Dominated by JSON Schema validation. |
| Validation vs mapping size, 50–400 variables | exponent 0.95; 90 ms at 400 | Linear. A 400-variable map holds the single worker for about 90 ms. |
| Validation vs databases, 25–200 | exponent 1.1; 33 ms at 50, 170 ms at 200 | The orphan check compares every database with every other, but those are cheap string comparisons; validation dominates well beyond realistic sizes. |
| Details state, 75–300 categorical variables × 50 values | exponent 1.1; 45 ms; 547 KB (9 KB gzip) | Linear; the response is large and compresses 60-fold. |
| Multipart upload of a 34 MB CSV | about 600 MB/s | Parsing the upload is never the bottleneck; the triplifier is. |
| The variables form, 300 variables (900 fields, 35 KB) | 1.3 ms | Cheap, but see the form limit below. |
| Compressing 0.7 MB of JSON | gzip 9 ms/MB (9×), Brotli 5 ms/MB (18×), zstd 1 ms/MB (29×) | Browsers that offer zstd (current Chrome and Firefox) get the cheapest and smallest. |
| Mock data, 10,000 rows (the page's maximum, real generator) | 23 ms, 357 KB | Fast at the page's limit; the endpoint itself sets no limit (pinned). |
| Annotation of 64 databases × 50 ms of store time | 109 ms | 29 times faster than one at a time; at most 32 in flight. |

Limits found on the way, pinned as defects in the API layer: a describe form over Flask's 500 KB `MAX_FORM_MEMORY_SIZE` (about 2,400 category values on the details page) is refused with a bare 413, and `/api/generate-mock-data` passes any `num_rows` to the generator, so one request can ask the single worker for a billion rows.

### Frontend unit (Vitest)

```bash
cd app/frontend
npm install                  # first time only
npm run test:unit            # one-shot
npm run test:unit:watch      # watch mode while you edit
```

Vitest runs in [`happy-dom`](https://github.com/capricorn86/happy-dom) — a lightweight DOM implementation — so components can mount, render, and emit events without a real browser. Specs live in `tests/unit/{stores,lib,views}/*.spec.js`. There's no backend; calls through `services/api.js` should be mocked (look at existing view specs for the pattern).

### Frontend E2E (Playwright)

Playwright assumes the stack is **already running** on `http://localhost:5000`. The Playwright config doesn't auto-start anything.

```bash
# 1. install browsers once
cd app/frontend
npm run test:e2e:install     # downloads chromium ~150 MB

# 2. bring up the test stack (tmpfs GraphDB + Flask)
./../../scripts/start-test-stack.sh

# 3. run the smoke spec (fast — ~30s)
npx playwright test tests/e2e/smoke.spec.js --reporter=line

# 4. or run the whole suite (a few minutes)
npm run test:e2e

# 5. teardown
docker compose -f ../../docker-compose.yml -f ../../docker-compose.test.yml down -v
```

Playwright only ships Chromium for the Ubuntu releases it knows; on a newer one `npm run test:e2e:install` stops with "does not support chromium on ubuntuXX.XX". Set `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64` for the install and the test runs; the 24.04 build works on later releases.

The suggestion flows (`tests/e2e/suggestions.spec.js`) need the default `FLYOVER_SUGGESTION_TIERS=1` and upload their own semantic map; their "disabled" flow mocks `/api/v1/suggestions/status`. The real feature-off run is `tests/e2e/suggestions-disabled.spec.js`: start the stack with the flag empty and set `E2E_SUGGESTIONS_DISABLED=1` so an enabled stack fails instead of skipping. Each suggestion spec skips itself against a stack in the other state, so `npm run test:e2e` passes on either.

```bash
FLYOVER_SUGGESTION_TIERS= ./../../scripts/start-test-stack.sh
E2E_SUGGESTIONS_DISABLED=1 npx playwright test tests/e2e/suggestions-disabled.spec.js --reporter=line
```

The LLM round-trip flows (`tests/e2e/llm-prompt-roundtrip.spec.js`) need no tier: they generate the prompt for the ingested database, paste a canned answer and accept an imported pill, and run against either stack.

The full e2e workflow runs both as a matrix: the whole suite on the default stack, and the disabled spec on a stack started with the empty flag.

On failure, Playwright drops a `playwright-report/` HTML report and a `test-results/` dir with traces and screenshots. Open the report with `npx playwright show-report`.

## The Docker test stack

```mermaid
flowchart LR
    base["docker-compose.yml<br/>(prod defaults)"]
    overlay["docker-compose.test.yml<br/>(test overrides)"]
    merged["effective compose<br/>(applied by start-test-stack.sh)"]
    rdf["rdf-store<br/>tmpfs at /opt/graphdb/home<br/>+ baked-in seed"]
    app["flyover<br/>Flask + Gunicorn<br/>(no overrides)"]

    base & overlay --> merged
    merged --> rdf
    merged --> app
```

The overlay does two important things:

1. **Replaces the GraphDB bind mount with a tmpfs.** Prod mounts `./stores/rdf/graphdb/data` from the host to persist state. Tests mount a 2 GB tmpfs to `/opt/graphdb/home`, so every `up` starts from an empty store. Faster, hermetic, and avoids polluting your dev triples.
2. **Builds the `rdf-store` image locally from [`graphdb/Dockerfile`](../stores/rdf/graphdb/Dockerfile)** instead of pulling the prod image. The locally built image bakes in `users.js`, `settings.js`, and `userRepo/config.ttl` as a seed under `/opt/graphdb-seed/`. On container start, [`stores/rdf/graphdb/seed-entrypoint.sh`](../stores/rdf/graphdb/seed-entrypoint.sh) does a non-clobbering `cp -rn /opt/graphdb-seed/. /opt/graphdb/home/` — on an empty tmpfs that copies the seed in (so `userRepo` exists); on a bind-mount with existing state the copy is a no-op.

The startup script [`scripts/start-test-stack.sh`](../scripts/start-test-stack.sh) brings the stack up and polls until both `http://localhost:7200/rest/repositories` returns `userRepo` and `http://localhost:5000/app/` returns 200, then exits.

## When to write what kind of test

```
Is the thing under test pure Python with deterministic inputs/outputs?
  └── yes → pytest unit (tests/unit/)
  └── no  → is it what a route does with a request (status, redirect, error shape,
            body/form/query parsing, session across requests, headers)?
            └── yes → pytest API (tests/api/), services mocked
            └── no  → does it cross layers (controller → service → repository/loader/validator)
            or read real fixture files?
            └── yes → pytest integration (tests/integration/)
            └── no  → is it a Vue component, store, or composable?
                      └── yes → Vitest (frontend/tests/unit/)
                      └── no  → does it exercise a multi-page user workflow?
                                └── yes → Playwright (frontend/tests/e2e/)
                                └── no  → reconsider whether it needs a test; if it does, you probably have the layers wrong
```

Practical heuristics:

- **Prefer the cheaper layer.** A unit test runs in milliseconds; an E2E test takes minutes and is flakier. If you can mock the services and test the route's HTTP behaviour in the API layer, do that before reaching for a stack.
- **Unit tests do not use the Flask test client.** Anything that goes through a route belongs to the API layer (services mocked) or the integration layer (services real).
- **Don't unit-test what Vitest is good at.** Component rendering, click handlers, derived UI state belong in Vitest, not in Playwright. Playwright is for "does the whole ingest workflow still work?", not "does this button enable when the field is filled?"
- **One E2E happy-path per workflow** is the goal — that's what `smoke.spec.js` is for. The richer specs (`ingest.spec.js`, `annotate.spec.js`, etc.) cover the variants; the full suite runs on every push to `main` and every PR, like the other layers.

## CI workflow map

All of these live in [`.github/workflows/`](../.github/workflows/).

| Workflow                  | Triggers                                                  | Blocking?       | What it does                                                                                      |
|---------------------------|-----------------------------------------------------------|-----------------|---------------------------------------------------------------------------------------------------|
| `unit-tests.yml`          | push to `main`, all PRs                                   | yes (perf: no)  | pytest unit, integration and API tests with combined coverage, a 90 % gate on the API layer's coverage of the HTTP layer, then the performance checks (non-blocking, report in the job summary) |
| `frontend-checks.yml`     | push to `main`, all PRs (frontend paths)                  | yes             | ESLint, `vue-tsc` type-check, Vitest, Vite production build                                       |
| `frontend-smoke.yml`      | push to `main`, all PRs                                   | yes             | Brings up the tmpfs test stack and runs `smoke.spec.js` only                                      |
| `frontend-e2e-full.yml`   | push to `main`, all PRs; manual dispatch                 | yes             | Full Playwright suite against the test stack (both the default and the suggestions-disabled stack) |
| `code-quality.yml`        | push to `main`, all PRs                                   | partial         | Black (auto-applies and commits with `[skip ci]`), flake8, mypy, Safety (HIGH severity blocking), Bandit |
| `release.yaml`            | tag push                                                  | n/a             | Builds and publishes the production flyover-app + flyover-graphdb images to ghcr.io                        |

### The Copilot-PR approval gate

PRs from `copilot/*` branches do **not** auto-run CI. A maintainer has to open the PR's *Checks* tab and click **Approve and run workflows** before any job fires. This is a GitHub org-level setting, not anything in this repo. If you see a Copilot PR with no checks queued at all, that's why — give it the approval click and the workflows fire.

## Learn more

- [Playwright — Getting started](https://playwright.dev/docs/intro) — read at minimum the "Writing tests" and "Test runners" pages. The [Trace Viewer](https://playwright.dev/docs/trace-viewer-intro) tutorial is worth doing once.
- [Vitest guide](https://vitest.dev/guide/) — the API is very close to Jest if you've used that.
- [Vue Test Utils](https://test-utils.vuejs.org/guide/) — what you'll reach for when writing component tests in Vitest.
- [pytest fixtures](https://docs.pytest.org/en/stable/explanation/fixtures.html) — `conftest.py` patterns, fixture scopes, parametrize.
- [The Practical Test Pyramid (Martin Fowler)](https://martinfowler.com/articles/practical-test-pyramid.html) — the philosophy behind the unit / integration / E2E split.

If something here is wrong or unclear, fix it — that's the easiest contribution you'll ever make.
