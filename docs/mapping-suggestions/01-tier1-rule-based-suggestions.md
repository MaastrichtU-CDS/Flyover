# Tier 1: rule-based mapping suggestions + shared suggestion infrastructure

Part of #35. Shared design (contract, API, cascade, compute flag, invariants): [`docs/mapping-suggestions/README.md`](README.md).

## Context & motivation

Users map every local column to a `schema.variables` key on `DescribeVariablesView` and every distinct value to a `valueMapping.terms` key on `DescribeVariableDetailsView`, by hand. A large share of that work is mechanical: the same column has already been mapped by another site in the loaded semantic map, or the distinct values (`{ja, nee}`, `^[CM]\d{2}(\.\d)?$`) give the answer away. None of this needs a model, and it must work on a 4-core/8 GB air-gapped box.

This issue delivers tier 1 **and** the plumbing every later tier reuses: the suggestion record, the `/api/v1/suggestions/*` blueprint, the `SuggestionService` job model, the Pinia `suggestions` store, and the `SuggestionBadge` UI. The LLM branch `feature/llm-mapping-suggestions` already has most of the plumbing; we cherry-pick it here, with the LLM provider code left for issue 4.

## Goal

- Users see clearly-marked suggestions in both describe dropdowns, with confidence and reason, and accept or dismiss them explicitly.
- Suggestions are produced by three deterministic matchers in the backend and flow through the same record/API/store the embedding and LLM tiers will use later.
- A benchmark script against the AYA semantic-map branches reports how good tier 1 is and fixes the thresholds.

## Non-goals

- Any model inference (issues 3, 4) or prompt generation (issue 2).
- `FLYOVER_SUGGESTION_COMPUTE=browser` behaviour — the flag is parsed and reported but is a no-op for tier 1.
- Writing suggestions into the JSON-LD automatically.
- Hand-maintained regex lists for column *names*; names are handled by alias memory and fuzzy strings only.

## Design

### Suggestion record (inlined from the README)

```json
{ "item": "morf", "match": "tumour_morphology_icd_o", "confidence": 0.92,
  "reason": "Alias: column 'morph' in database 'christie' is mapped to this variable",
  "source": "alias", "tier": 1, "status": "done" }
```

- `match` is an exact schema key or `null` (abstain); `confidence` ∈ [0, 1]; `reason` non-empty.
- `source ∈ {alias, value_regex, string, embedding, llm, pasted_llm, manual}`; tier 1 uses the first three.
- `status ∈ {pending, running, done, failed, unavailable}`.
- Keys: variables `${db}_${column}` → `match` ∈ `schema.variables`; values `${db}_${column}_${value}` → `match` ∈ `valueMapping.terms` of the mapped variable.

### Backend

Cherry-picks from `origin/feature/llm-mapping-suggestions` (old layout → current layout):

| Branch file | New location | What changes |
|---|---|---|
| `backend/flyover/services/llm/matching.py` (`MATCH_OUTPUT_SCHEMA`, `sanitise_pairs`) | `flyover/backend/data_descriptor/services/suggestions/contract.py` | Add `source`, `tier`, `status`; `sanitise_pairs` becomes the generic server-side validator used by every producer |
| `backend/flyover/services/llm/suggestion_service.py` (jobs, fingerprint, chunking, priority) | `flyover/backend/data_descriptor/services/suggestions/__init__.py` (`SuggestionService`) | Drop the provider dependency; the job runs a list of tier producers with the cascade rules from the README |
| `backend/flyover/controllers/llm_controller.py` | `flyover/backend/data_descriptor/controllers/suggestions_controller.py` | Routes renamed `/api/v1/llm/*` → `/api/v1/suggestions/*`; `_maybe_adopt_mapping` kept; registered in `data_descriptor_main.py` next to `describe_bp` |
| `backend/flyover/tests/unit/test_llm_suggestion_service.py`, `test_llm_controller.py`, `test_llm_matching.py` | `tests/unit/test_suggestions_service.py`, `test_suggestions_controller.py`, `test_suggestions_contract.py` | Provider mocks replaced by a fake tier producer |

New files:

```
flyover/backend/data_descriptor/services/suggestions/tiers/__init__.py   # Producer protocol: run(items, schema_slice, ctx) -> list[Record]
flyover/backend/data_descriptor/services/suggestions/tiers/rules.py      # tier 1: the three matchers below
flyover/backend/data_descriptor/resources/suggestion_rules.json          # value regexes + abbreviation table, versioned
flyover/backend/data_descriptor/tests/unit/test_suggestions_rules.py
scripts/benchmark_suggestions.py
```

Routes delivered in this issue: `GET /status`, `POST /{variables|values}/start`, `GET /{variables|values}`, `POST /{variables|values}/priority`. `/ingest` and `/prompt` are added in issues 2/3 but the blueprint and `SuggestionService.ingest()` signature are reserved now. Inputs come from what `describe_controller.py` already exposes: `column_info` per database (`/api/v1/describe-variables-state`) and distinct categorical values (`rdf_store_service.get_categories`), plus the session JSON-LD.

### The three tier-1 matchers (`tiers/rules.py`)

**(a) Alias memory** — `source: alias`. Walk every `databases.<db>.tables.<t>.columns.<c>` of the loaded JSON-LD (the Python twin of `jsonld.forEachColumn`) and collect `normalise(localColumn) → mapsTo` pairs, excluding the database currently being described. Same for values: `normalise(localValue) → term` per variable from `localMappings`.
Example: Christie has `morph → tumour_morphology_icd_o`; NKI's `morf` normalises to a Jaro-Winkler ≥ 0.95 hit on `morph` → confidence `0.9 × similarity`, reason names the source database. Exact normalised hits get `1.0`.

**(b) Value-type regexes** — `source: value_regex`, defined in `suggestion_rules.json` (not in code, not in `schema`):

| Pattern set | Distinct values | Suggests |
|---|---|---|
| yes/no (nl/en/fr/it/pl) | `{ja, nee}`, `{yes, no}`, `{0, 1}` | Variables whose `valueMapping.terms` are exactly a yes/no pair; values → the matching term |
| sex | `{M, F}`, `{M, V}`, `{male, female}` | `biological_sex` (+ value terms) |
| ICD-O | `^[CM]\d{2}(\.\d)?$`, `^\d{4}/[0-3]$` | `tumour_topography_icd_o`, `tumour_morphology_icd_o` |
| TNM | `^[cp]?T[0-4]`, `^N[0-3]`, `^M[01]` | `*_stage_*` variables with TNM terms |
| year | 4-digit ints in 1900..current | `year_of_*` variables — **abstain** if more than one candidate |
| missing codes | `""`, `999`, `-1`, `NA`, `onbekend` | `missing_or_unspecified`-style term in the values phase |

A rule only fires when the full distinct-value set is covered (minus missing codes) and, in the variables phase, when the schema side has ≤ `margin`-distinct candidates; otherwise it abstains with `reason: "value pattern matches N variables"`.

**(c) Normalised token similarity** — `source: string`. Normalise both sides (lowercase, split snake/camel/digits, drop stopwords `van/de/of/the/…`, expand abbreviations from `suggestion_rules.json`: `jaar→year`, `leeft→age`, `diag→diagnosis`, `ther→therapy`, `rt→radiotherapy`, `chemo→chemotherapy`), then score with Jaro-Winkler on the joined tokens and Jaccard on the token sets against variable keys *and* labels. Confidence = best score. **Abstain** when `top1 − top2 < 0.05`.
Examples: `jaar_van_diagnose → year_of_initial_diagnosis` (0.86, accepted); `ther_chemo → chemotherapy_administered`; `alg_v1b`, `alg_v7`, `surv1..surv72` → abstain (near-identical numbered candidates), escalated to the next tier. `Rnnummer → identifier` is not reachable by any tier-1 rule and correctly abstains.

Cascade inside tier 1: alias → value_regex → string; merge per README (highest confidence, tie → earlier matcher).

### Frontend

| Branch file | New location | What changes |
|---|---|---|
| `frontend/src/stores/suggestions.js` | `flyover/frontend/src/stores/suggestions.js` | Endpoints renamed; records keep `source`/`tier`; `applied`/`touched`/`dismissed` marks persisted in the IndexedDB `metadata` store (`src/lib/db.js`) under `suggestion_marks_<phase>` |
| `frontend/tests/unit/stores/suggestions.spec.js` | `flyover/frontend/tests/unit/stores/suggestions.spec.js` | Same, plus a `source`-agnostic rendering test |
| `frontend/src/views/DescribeVariablesView.vue`, `DescribeVariableDetailsView.vue` — the suggestion **aesthetics** (template + `<style scoped>` blocks) | `flyover/frontend/src/components/SuggestionBadge.vue` + `SuggestionStatusBar.vue`; per-view styles moved into the components | Extract, don't redraw: the branch already has the purple dashed look for suggestions; we keep it and only rename `llm-*` → `suggestion-*` and make the labels source-agnostic |

**Cherry-pick the existing look and feel.** The branch's describe views already contain a finished suggestion UI; reuse these pieces verbatim (renamed, componentised) rather than designing new ones:

| Branch element (class / markup) | Becomes | Notes |
|---|---|---|
| `.llm-badge` pill next to the column label (`fa-robot` icon + `NN%` confidence, `title=reason`, `×` dismiss button) and `.llm-badge.confirmed` green "✓ reviewed" state | `SuggestionBadge.vue` | Icon and label come from `source`/`tier` (`alias`/`value_regex`/`string` → rule icon and "tier 1", `llm` → robot), the rest is unchanged |
| `.llm-suggested :deep(.searchable-select-input)` — dashed purple border + faint purple fill on a `SearchableSelect` holding a suggestion | `.suggestion-highlight` on `SearchableSelect` | This is the #137 "clearly a recommendation" cue; cleared when the user touches the field |
| `.llm-retry` inline link ("retry AI") on a failed item | `retry` slot of `SuggestionBadge` | Text becomes "retry suggestion" |
| `.llm-status-bar` (purple left border, progress "N of M", `pulling_model`/`running`/`done`/`failed`/`unavailable` messages, `.llm-provider-note` "by …", orange `.llm-remote-badge`, `.llm-clear-all` button) | `SuggestionStatusBar.vue` | Shows the `/status` tier list (`tier 1 active · tiers 2, 3 inactive`) instead of a single provider; remote badge stays for issue 4 |
| `.llm-section-button` "Suggest this section first" / "Suggest now" (`fa-robot`) per database / variable | `SuggestionStatusBar` per-section slot | Wired to `POST /{phase}/priority` as today |
| `.info-purple` explanatory panel | kept as-is | Wording updated for tiers |

Added on top of the branch UI: an `alternatives` popover on the badge (losing records from the merge) and an explicit **accept** action, see the next point.

Integration points:

- `DescribeVariablesView.vue`: when a record is `done` and the key has no `formStateCache` entry and no `preselectedDescriptions` value, render the dropdown with the suggested `match` pre-highlighted **but not selected**; `accept` sets it via the existing `onDescriptionChange` path so `syncToIndexedDB` → `jsonld.updateMappingFromForm` is the only writer. The one-variable-per-database check runs as today. This deliberately differs from the branch's `applyArrivedSuggestions()`, which pre-*filled* untouched fields and relied on the dashed style + badge to signal "not yet reviewed" — see open questions.
- `DescribeVariableDetailsView.vue`: same pattern around `updateCategoryMapping` for values.
- Both views call `POST /{phase}/start` on mount and `POST /{phase}/priority` with the visible keys; polling via the store.
- `StatusBanner.vue` / a small header chip shows `/status`: `Suggestions: tier 1 active · tiers 2, 3 inactive (disabled)`.

### Compose / env

```yaml
# docker-compose.yml, flyover service
- FLYOVER_SUGGESTION_TIERS=${FLYOVER_SUGGESTION_TIERS:-1}
- FLYOVER_SUGGESTION_COMPUTE=${FLYOVER_SUGGESTION_COMPUTE:-host}   # parsed, no-op for tier 1
- FLYOVER_SUGGESTION_THRESHOLD=${FLYOVER_SUGGESTION_THRESHOLD:-0.8}
```

Setting `FLYOVER_SUGGESTION_TIERS=` (empty) disables the feature; `/status` then reports all tiers `inactive (disabled by FLYOVER_SUGGESTION_TIERS)` and the views render exactly as today.

### Benchmark (`scripts/benchmark_suggestions.py`)

Protocol per README. CLI: `python scripts/benchmark_suggestions.py --tiers 1 --sites all --out docs/mapping-suggestions/benchmark-results.md`. Uses the same `tiers/rules.py` code path as the app; leave-one-site-out for alias memory.

## Acceptance criteria

- [ ] `GET /api/v1/suggestions/status` returns `{compute: "host", tiers: {1: {state: "active"}, 2: {state: "inactive", reason: ...}, 3: {...}}}`.
- [ ] `POST /api/v1/suggestions/variables/start` and `/values/start` create a job; `GET` returns records conforming to `contract.py` (validated in tests with the JSON schema).
- [ ] Every record's `match` is either `null` or an exact key from the relevant schema slice — enforced by `sanitise_pairs` in the service, covered by a negative test.
- [ ] Alias memory: with the AYA JSON-LD loaded and NKI as the described database, `morf` receives `tumour_morphology_icd_o` with `source: alias` and a reason naming the source database.
- [ ] Value regexes: a column with distinct values `{ja, nee}` receives yes/no term suggestions in the values phase; `{M, V}` suggests `biological_sex`; a 4-digit year column abstains when multiple `year_of_*` variables exist.
- [ ] String matcher: `jaar_van_diagnose → year_of_initial_diagnosis` with confidence ≥ 0.8; `surv1` and `alg_v7` abstain (`match: null`, reason mentions margin).
- [ ] Benchmark on the 7 AYA branches: tier-1 recall@1 ≥ `TBD from benchmark` on English-header sites, false-accept rate (confidence ≥ threshold but wrong) ≤ `TBD from benchmark` pooled; results committed to `docs/mapping-suggestions/benchmark-results.md`.
- [ ] Zero writes to the JSON-LD without an explicit accept: a Vitest test loads suggestions, asserts `jsonld.getMapping()` unchanged, accepts one, asserts only that column changed.
- [ ] `SuggestionBadge` renders identically for `source: alias | value_regex | string` and for a fabricated `source: llm` record (source-agnostic).
- [ ] `applied` / `touched` / `dismissed` marks survive a page reload (IndexedDB) and are not overwritten by a re-run job.
- [ ] `FLYOVER_SUGGESTION_TIERS=` disables the feature with no UI regressions (existing Playwright describe flow passes unchanged).
- [ ] `suggestion_rules.json` has a `version` field and is loaded once; the `schema` section of the session JSON-LD is byte-identical before/after a job.

## Test plan

- `tests/unit/test_suggestions_contract.py` — schema validation, `sanitise_pairs` nulling non-members, confidence clamping.
- `tests/unit/test_suggestions_rules.py` — each matcher on a fixture JSON-LD with two databases (an English-header site and the NKI slice); abstain cases; leave-one-site-out alias memory.
- `tests/unit/test_suggestions_service.py` — job lifecycle, fingerprint reuse, `force`, merge/tie-breaking, user marks immutable, one-variable-per-database conflict downgrade.
- `tests/unit/test_suggestions_controller.py` — routes, env flag parsing, `/status` shape, 400 on unknown phase.
- Vitest `tests/unit/stores/suggestions.spec.js` and `components/SuggestionBadge.spec.js`.
- Playwright: existing describe flows with suggestions disabled; one new flow accepting and dismissing a suggestion.
- Manual: run `scripts/benchmark_suggestions.py --tiers 1` and attach the summary to the PR.

## Open questions

- Should alias memory also read an optional site-provided data dictionary (CSV `column,label`) uploaded on the ingest page? Proposed: follow-up issue, keep the hook (`ctx.dictionary`) in the producer protocol.
- Abbreviation table language coverage: start with NL/EN from the AYA branches; FR/IT/PL added as sites contribute.
- Default threshold `0.8` and margin `0.05` are placeholders until the benchmark runs.
- Pre-fill vs pre-highlight: the branch pre-fills untouched dropdowns with the suggestion (dashed style until the user confirms); this issue proposes pre-highlighting only, with an explicit accept, to counter over-trust. Decide before implementing; the cherry-picked aesthetics work for either.

## Depends on / blocks

- Depends on: nothing (first increment; merged as soon as green).
- Blocks: #TBD (issue 2 — prompt export + paste-back).
