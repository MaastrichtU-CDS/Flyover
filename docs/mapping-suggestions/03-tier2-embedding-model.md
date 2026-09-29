# Tier 2: small multilingual embedding model (host or browser compute)

Part of #35. Shared design (contract, API, cascade, compute flag, invariants): [`docs/mapping-suggestions/README.md`](README.md). Builds on issue 1 (infrastructure, tier 1) and issue 2 (`/ingest`).

## Context & motivation

Tier 1 abstains on anything that is neither a known alias, a recognisable value pattern nor a close string: Dutch/French/Italian/Polish descriptive names such as `leeft`, `geslacht`, `data_diagnosi`, `date_de_naissance`. A small multilingual sentence-embedding model closes that gap cheaply — embedding ~600 column names plus a few thousand value labels takes seconds on 4 cores — without any LLM, network access or GPU.

Because Flyover is often reached over an SSH tunnel, the heavier machine may be on either end. This issue is the first real consumer of `FLYOVER_SUGGESTION_COMPUTE`: `host` runs the model in the backend with `onnxruntime`; `browser` ships the same model bundle to the client and runs it in a Web Worker with transformers.js, posting results back through `/ingest`.

## Goal

- Add a tier-2 producer that embeds the local side (column names / values) and the schema side (variable keys, labels, descriptions, term labels), suggests by cosine similarity with a margin abstain, and only runs on items tier 1 escalated.
- Make the producer run either on the host or in the browser, selected by a single compose flag, with identical output records (`source: embedding, tier: 2`).
- Ship the model inside the image or a mounted volume; never download at runtime.

## Non-goals

- Any generative model or LLM (issue 4).
- Fine-tuning; we use an off-the-shelf sentence-embedding model.
- Cross-encoder re-ranking (possible follow-up if the benchmark shows a need).
- Browser mode for tier 3 (that is prompt export, issue 2).

## Design

### Model choice

- Default bundle: `paraphrase-multilingual-MiniLM-L12-v2` (50+ languages incl. NL/FR/IT/PL/EN), quantised ONNX (`model_quantized.onnx`, ~120 MB) plus tokenizer files; the same artefacts work in `onnxruntime` and transformers.js. Alternative for tighter budgets: `multilingual-e5-small` quantised (~110 MB). Model id and revision are pinned in `resources/suggestion_models.json`.
- Delivery: a build stage in `flyover/backend/Dockerfile` copies the bundle into `flyover/backend/data_descriptor/static/models/<bundle>/` **or** operators mount a volume at the same path (`docker-compose.suggestions-embedding.yml`). Missing bundle → tier 2 reported `inactive (model bundle not found)`; nothing is fetched.
- Schema-side text per variable: `label + ". " + description + ". " + " / ".join(term labels)`; per term: `term label (+ variable label)`. Local-side text per column: `column name (normalised as in tier 1) + optional first 5 distinct values`; per value: the raw value.

### Host mode (`FLYOVER_SUGGESTION_COMPUTE=host`)

`flyover/backend/data_descriptor/services/suggestions/tiers/embedding.py`:

- Loads the ONNX model lazily on first job with `onnxruntime` (CPU provider, `intra_op_num_threads` = min(4, cores)); tokenizer via `tokenizers`. Both are optional dependencies (`requirements-suggestions.txt`); import failure → `inactive (onnxruntime not installed)`.
- Embeds the schema side once per JSON-LD fingerprint and caches the matrix in memory (and on disk under `$FLYOVER_CACHE_DIR/embeddings/<fingerprint>.npy` so restarts are cheap).
- For each escalated item: cosine against the matrix; `confidence = cos`; `match = argmax` if `cos ≥ FLYOVER_SUGGESTION_EMBED_THRESHOLD` (default `0.6`, TBD from benchmark) **and** `top1 − top2 ≥ 0.05`; otherwise abstain with `reason: "embedding: best 0.58 (year_of_initial_diagnosis), runner-up 0.55 — too close"`.
- Reason for matches: `"embedding: cos 0.81 to label 'Age at initial diagnosis'"` (names the schema label that matched, since that is what the user recognises).
- Runs in the same chunked background job as tier 1 (`SuggestionService`), after tier 1, only over items whose best record is `null` or below `FLYOVER_SUGGESTION_THRESHOLD`.

### Browser mode (`FLYOVER_SUGGESTION_COMPUTE=browser`)

- Backend: `GET /api/v1/suggestions/status` advertises `compute: "browser"` and `tier2.bundle_url: "/static/models/<bundle>/"`; the static route serves the bundle with long-lived cache headers. The backend job still runs tier 1 and marks tier-2 items `status: pending, tier: 2` so the UI knows what to compute.
- Frontend: `flyover/frontend/src/lib/embeddingWorker.js` — a Web Worker using `@huggingface/transformers` with `env.allowRemoteModels = false` and `env.localModelPath = bundle_url`; WASM backend by default, WebGPU when `navigator.gpu` exists. It receives `{schemaTexts, itemTexts}` from the store, returns `{item, match, confidence, reason}` computed with the same threshold/margin logic (thresholds come from `/status` so host and browser agree).
- Store (`stores/suggestions.js`): `runBrowserTier2(phase)` — collects pending tier-2 items from the snapshot, posts to the worker, then `POST /api/v1/suggestions/{phase}/ingest` with `source: "embedding"`; the server stamps `tier: 2` and validates keys with `sanitise_pairs` exactly like pasted answers. Progress is shown in the status chip (`Tier 2 · 214/380 in browser`).
- The model download (~120 MB, once per browser, cached by the browser) is shown with a progress bar and a "Run tier 2 in this browser" button rather than started automatically.

### Compose

```yaml
# docker-compose.suggestions-embedding.yml (overlay)
services:
  flyover:
    environment:
      - FLYOVER_SUGGESTION_TIERS=${FLYOVER_SUGGESTION_TIERS:-1,2}
      - FLYOVER_SUGGESTION_COMPUTE=${FLYOVER_SUGGESTION_COMPUTE:-host}
      - FLYOVER_SUGGESTION_EMBED_THRESHOLD=${FLYOVER_SUGGESTION_EMBED_THRESHOLD:-0.6}
    volumes:
      - ${FLYOVER_SUGGESTION_MODEL_DIR:-./models}:/app/data_descriptor/static/models:ro
```

`docker compose -f docker-compose.yml -f docker-compose.suggestions-embedding.yml up`. Images built with the bundle stage do not need the volume.

Compute flag semantics for this tier (recap of the README table): `host` → `tiers/embedding.py` on the backend; `browser` → `embeddingWorker.js` + `/ingest`. Tier 1 is unaffected either way.

### Worked example (AYA NKI-Amsterdam)

| Item | Tier 1 | Tier 2 | Outcome |
|---|---|---|---|
| `jaar_van_diagnose` | string 0.86 → `year_of_initial_diagnosis` | not run (above threshold) | tier 1 stands |
| `leeft` | abstain (`leeft` unknown to abbreviation table) | cos 0.78 → `age_at_initial_diagnosis` | tier 2 suggestion |
| `geslacht` | value_regex `{M, V}` → `biological_sex` 0.9 | not run | tier 1 stands |
| `taal` | abstain | cos 0.71 → `administered_prom_language` | tier 2 suggestion |
| `surv70` | abstain (margin) | best 0.41, runner-up 0.40 → abstain | escalate to tier 3 / human |
| `alg_v7` | abstain (margin) | best 0.33 → abstain | escalate |
| `Rnnummer` | abstain | best 0.52 (`identifier`) below threshold → abstain | escalate |

## Acceptance criteria

- [ ] `GET /api/v1/suggestions/status` reports tier 2 `active` with `model`, `revision`, `compute` when the bundle is present and `FLYOVER_SUGGESTION_TIERS` includes `2`; `inactive (model bundle not found)` or `inactive (onnxruntime not installed)` otherwise. No outbound network call in any case (asserted with a socket-blocking test fixture).
- [ ] Host mode: on a 4-core/8 GB container the values+variables job for the NKI fixture (~600 columns, ~2,000 value labels) completes in ≤ 30 s wall-clock, schema embedding included; a second run with an unchanged fingerprint takes ≤ 5 s (cache hit).
- [ ] Tier 2 only receives items tier 1 abstained on or scored below `FLYOVER_SUGGESTION_THRESHOLD`; tested by counting producer calls.
- [ ] `leeft → age_at_initial_diagnosis` and `taal → administered_prom_language` are produced with `source: embedding, tier: 2`; `surv70`, `alg_v7` abstain with a margin reason.
- [ ] Browser mode: `/status` exposes `bundle_url`; the worker loads the model from that URL only (`allowRemoteModels=false`), and the resulting records reach the snapshot via `/ingest` with `tier: 2`; invalid keys are nulled server-side.
- [ ] Host and browser produce identical `match` for the fixture set (same thresholds from `/status`); allowed confidence drift ≤ 0.02.
- [ ] Benchmark: cumulative tier 1+2 recall@1 improves over tier 1 alone by ≥ `TBD from benchmark` points pooled across the 7 AYA sites, with false-accept rate not worse than tier 1 alone; results appended to `docs/mapping-suggestions/benchmark-results.md`.
- [ ] Model bundle is pinned by id + revision in `resources/suggestion_models.json` and its sha256 is verified at load.
- [ ] `schema` section of the session JSON-LD is byte-identical before/after a tier-2 job; no suggestion is written without an explicit accept.
- [ ] Existing tier-1-only deployments (`FLYOVER_SUGGESTION_TIERS=1`) show no behavioural change and do not import `onnxruntime`.

## Test plan

- `tests/unit/test_suggestions_embedding.py` — producer with a tiny fake ONNX/tokenizer fixture (or a stub encoder returning fixed vectors): threshold, margin abstain, cache by fingerprint, escalation filter, reasons, inactive states, no-network fixture.
- `tests/unit/test_suggestions_controller.py` (extend) — `/status` fields per compute mode; static bundle route headers; `/ingest` with `source: embedding`.
- Vitest — `lib/embeddingWorker.spec.js` with a mocked `@huggingface/transformers` pipeline (fixed vectors) asserting identical decisions to the Python stub on the shared fixture; `stores/suggestions.spec.js` (extend) for `runBrowserTier2` and progress state.
- Integration — `docker compose -f docker-compose.yml -f docker-compose.suggestions-embedding.yml` smoke run in CI with the bundle mounted from a cached artefact; time budget assertion.
- Benchmark — `scripts/benchmark_suggestions.py --tiers 1,2` per site and pooled.

## Open questions

- Bundle in the default image (+120 MB) or overlay-only? Proposal: overlay/volume by default, with a `flyover:suggestions` image variant published alongside for air-gapped shops.
- Should browser mode also cache the schema-side embeddings in IndexedDB keyed by fingerprint? Cheap win; decide after measuring in-browser embedding time.
- Value-phase embedding of very large distinct sets (free-text-like columns): cap per column (e.g. 200 values) and mark the rest as `unavailable`?

## Depends on / blocks

- Depends on: #TBD (issue 2 — prompt export + `/ingest`).
- Blocks: #TBD (issue 4 — tier 3 integrated LLM).
