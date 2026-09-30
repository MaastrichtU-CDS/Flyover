# Tier 3: integrated LLM suggestions (local sidecar or remote providers)

Part of #35, supersedes the standalone scope of #138. Shared design (contract, API, cascade, compute flag, invariants): [`docs/mapping-suggestions/README.md`](README.md). Builds on issues 1–3; harvests `origin/feature/llm-mapping-suggestions`.

## Context & motivation

After tiers 1 and 2, what remains are items that need context or world knowledge: semantic leaps (`taal` with values `nl_NL, en_GB` → `administered_prom_language`), columns whose meaning is only evident from their value set, and cases where several plausible variables must be weighed. An LLM handles a share of those; the rest goes to the human.

The `feature/llm-mapping-suggestions` branch already contains a working implementation: provider abstraction (Ollama, OpenAI-compatible, Anthropic), remote-egress gating, chunked async jobs with priority bumping, a strict JSON output schema and the `sanitise_pairs` hallucination guard. It predates the `data_descriptor` layout and the tiered pipeline, so this issue is a **port**, not a rewrite: the LLM becomes one more producer that only sees escalated items and receives lower-tier candidates as hints.

Two deployment shapes must both work: a **local** model in restricted environments (Ollama sidecar, CPU or GPU) and **remote frontier APIs** (Anthropic, OpenAI, Mistral, Azure, Groq, OpenRouter, self-hosted vLLM via the OpenAI-compatible provider) where policy allows. Prompt export (issue 2) remains available in all cases and *is* tier 3 for `FLYOVER_SUGGESTION_COMPUTE=browser`.

## Goal

- Tier-3 producer `tiers/llm/` behind the same `/api/v1/suggestions/*` API, records tagged `source: llm, tier: 3`.
- Provider factory with Ollama / OpenAI-compatible / Anthropic, fail-closed configuration, explicit remote acknowledgement.
- Compose overlays for CPU sidecar, GPU sidecar and cloud providers adapted to the current `docker-compose.yml`.
- Benchmark line showing how many items still reach the human after all tiers.

## Non-goals

- Replacing or removing prompt export (issue 2) — it stays.
- Fine-tuning or hosting models ourselves beyond the Ollama sidecar.
- Multi-turn chat UI inside Flyover.
- In-browser LLM (WebLLM) — listed as a stretch goal only.

## Design

### Port list (old branch layout → current layout)

| Branch file | New location | What changes |
|---|---|---|
| `backend/flyover/services/llm/base.py` | `flyover/backend/data_descriptor/services/suggestions/tiers/llm/base.py` | Unchanged provider protocol (`complete_json(system, user, schema) -> str`) |
| `backend/flyover/services/llm/config.py` | `.../tiers/llm/config.py` | Keep `LLMConfig`, `KNOWN_PROVIDERS`, loopback/remote classification, fail-closed warnings; expose `describe()` for `/status` |
| `backend/flyover/services/llm/factory.py` | `.../tiers/llm/factory.py` | Unchanged |
| `backend/flyover/services/llm/ollama_provider.py`, `openai_provider.py`, `anthropic_provider.py` | `.../tiers/llm/providers/{ollama,openai,anthropic}.py` | Unchanged apart from imports |
| `backend/flyover/services/llm/matching.py` | Split: schema/`sanitise_pairs` already live in `contract.py` (issue 1); prompt text → `.../tiers/llm/prompting.py` | `build_user_prompt(list_a, list_b)` gains a `hints` section (tier-1/2 candidates) and the value-set context per item |
| `backend/flyover/services/llm/suggestion_service.py` | Already ported as `SuggestionService` in issue 1 | Only the `LLMProducer` adapter is new: chunking (`FLYOVER_LLM_CHUNK_SIZE`), timeouts, model fallbacks, per-chunk `sanitise_pairs` |
| `backend/flyover/controllers/llm_controller.py` | Folded into `controllers/suggestions_controller.py` (issue 1) | `/api/v1/llm/status` fields become the `tier3` block of `/api/v1/suggestions/status`; no separate blueprint |
| `backend/flyover/tests/unit/test_llm_{config,matching,ollama_provider,openai_provider,anthropic_provider,suggestion_service,controller}.py` | `flyover/backend/data_descriptor/tests/unit/test_suggestions_llm_{config,prompting,providers,producer}.py` | Import paths; controller tests merged into `test_suggestions_controller.py` |
| `docker-compose.llm.yml`, `docker-compose.llm-gpu.yml`, `docker-compose.llm-cloud.example.yml` | Same names at repo root | Service name `flyover`, network `proxynet`, and `include:` of `stores/rdf/compose.yml` aligned with the current `docker-compose.yml`; add `FLYOVER_SUGGESTION_TIERS=1,2,3` |
| `frontend/src/stores/suggestions.js` | Already ported (issue 1) | No change; `source: llm` renders through the existing badge |

### Pipeline integration

- `LLMProducer.run(items, schema_slice, ctx)` receives **only escalated items** (best record `null` or below `FLYOVER_SUGGESTION_THRESHOLD` after tiers 1–2) — never the whole column list. Typical NKI: ~600 columns in, a few hundred escalated.
- Prompt per chunk (default 8 items): system prompt from the branch (`EXACT string from list_b`, `null` when no equivalent, non-empty `reason`), plus per item: normalised name, datatype, up to 5 distinct values, and `hints: [{match, confidence, source}]` from lower tiers ("confirm, overrule or reject").
- Output validated against `contract.MATCH_OUTPUT_SCHEMA`, then `sanitise_pairs`; records stamped `source: llm, tier: 3, status: done`; merged by the README rules (highest confidence wins, ties to lower tier, user marks untouched). Confidence from the model is clamped and additionally capped at `FLYOVER_LLM_MAX_CONFIDENCE` (default `0.95`) so an LLM can never outrank an exact alias.
- Chunk failures (timeout, invalid JSON after one retry, provider error) mark those items `status: failed` with the error in `reason`; the job continues. Priority bumping (`/priority`) reorders pending chunks, as on the branch.
- Values phase: one chunk per column; `list_b` is that variable's `valueMapping.terms`.

### Providers and gating

| Provider (`FLYOVER_LLM_PROVIDER`) | Typical use | Required env | Remote? |
|---|---|---|---|
| `ollama` (default) | Sidecar in restricted shops; CPU or GPU | `FLYOVER_OLLAMA_HOST` (default `http://ollama:11434`), `FLYOVER_LLM_MODEL` (default `llama3.2:3b`), `FLYOVER_LLM_FALLBACK_MODELS` | No |
| `openai` | OpenAI, Azure, **Mistral**, Groq, OpenRouter, self-hosted vLLM / LM Studio / llama.cpp | `FLYOVER_LLM_BASE_URL`, `FLYOVER_LLM_MODEL` (both mandatory), `FLYOVER_LLM_API_KEY` | Derived from `BASE_URL`; override with `FLYOVER_LLM_REMOTE=false` for in-network servers |
| `anthropic` | Claude models | `FLYOVER_LLM_API_KEY`, `FLYOVER_LLM_MODEL` | Yes |

- `FLYOVER_LLM_ALLOW_REMOTE=true` is the explicit acknowledgement that column names, distinct values and schema keys leave the network. Without it, a remote provider makes tier 3 `inactive (remote provider not allowed)` with one boot-time warning — the branch's fail-closed behaviour, kept verbatim.
- Other env kept from the branch: `FLYOVER_LLM_ENABLED`, `FLYOVER_LLM_CHUNK_SIZE` (8), `FLYOVER_LLM_TIMEOUT_S` (180). Tier 3 is active only if `FLYOVER_SUGGESTION_TIERS` contains `3` **and** the resolved `LLMConfig.enabled` is true.
- `/api/v1/suggestions/status.tier3` reports `{state, provider, model, remote: bool, base_url_host}`; the UI chip shows `Tier 3 · ollama/llama3.2:3b (local)` or `Tier 3 · anthropic (remote)`.

### Compute flag semantics for tier 3

- `host` — the backend calls the sidecar or the remote API; browser only polls.
- `browser` — no backend LLM call; the UI offers prompt export / paste-back (issue 2) for the escalated items, pre-filtered so the prompt contains only what tiers 1–2 could not solve. `/status` reports `tier3: {state: "active", mode: "prompt_export"}`.
- Stretch (not in acceptance): WebLLM/WebGPU in-browser model for `browser` mode, posting through `/ingest` with `source: llm` — only worth it if a benchmark shows a ≤ 3B model adds value over tier 2.

### Compose

```
docker compose -f docker-compose.yml -f docker-compose.llm.yml up                       # CPU sidecar
docker compose -f docker-compose.yml -f docker-compose.llm.yml -f docker-compose.llm-gpu.yml up   # + NVIDIA
docker compose -f docker-compose.yml -f my-llm-cloud.yml up                             # remote provider, copied from the .example
```

`docker-compose.llm.yml` adds the `ollama` service (`ollama/ollama`, `ollama-models` volume, `proxynet`) and sets `FLYOVER_SUGGESTION_TIERS=1,2,3`, `FLYOVER_LLM_ENABLED=true`, `FLYOVER_OLLAMA_HOST`. The model is pulled once at first boot in the background — document that this is the **one** outbound call in the local shape and how to pre-seed the volume for air-gapped hosts (`ollama pull` elsewhere, copy the volume).

### Worked example (AYA NKI-Amsterdam, escalated items only)

| Item | Context sent | LLM answer | Result |
|---|---|---|---|
| `taal` | values `nl_NL, en_GB`; hint tier 2 `administered_prom_language` 0.71 | `administered_prom_language`, 0.9 | confirmed; tier 3 record wins on confidence |
| `Rnnummer` | string, 566 distinct; no hint | `identifier`, 0.7, "registration number" | valid key → suggestion; user verifies |
| `surv70` | values `1,2,3,4`; no hint | `eortc_qlq_c30_q6`, 0.35, "cannot determine item number" | below threshold → shown as low-confidence alternative, stays with the human |
| `alg_v7` | values `0,1`; no hint | `null`, "opaque code, no basis" | abstain → human |

The last two rows are the point of the benchmark line "items still left to the user": opaque codes are a hard ceiling for every automated tier; only a codebook upload (open question in issue 1) or the user resolves them.

## Acceptance criteria

- [ ] With `docker-compose.llm.yml` and `llama3.2:3b` on a 4-core/8 GB host, a variables job over the NKI fixture completes as a background job (UI stays usable, progress visible), tier 3 only processing escalated items; wall-clock recorded in the PR.
- [ ] `FLYOVER_LLM_PROVIDER=anthropic` (or `openai` with a non-loopback `BASE_URL`) without `FLYOVER_LLM_ALLOW_REMOTE=true` → tier 3 `inactive (remote provider not allowed)`, one warning at boot, zero outbound requests (socket-blocking test).
- [ ] `openai` provider works against a mocked OpenAI-compatible endpoint with Mistral-style model names; `FLYOVER_LLM_REMOTE=false` classifies an in-network vLLM URL as local.
- [ ] `/api/v1/suggestions/status.tier3` shows `provider`, `model`, `remote` and, in `browser` compute mode, `mode: "prompt_export"`.
- [ ] Every tier-3 record passes `sanitise_pairs`: a fabricated model answer with a non-existent key is nulled with the `[invalid key from LLM]` reason; confidence is capped at `FLYOVER_LLM_MAX_CONFIDENCE`.
- [ ] Tier 3 never receives items whose best lower-tier record is above threshold (producer-call count test); hints from tiers 1–2 appear in the prompt.
- [ ] Chunk failure (timeout / malformed JSON twice) marks only that chunk's items `failed`; the job finishes; `/priority` reorders pending chunks.
- [ ] User marks and the JSON-LD are untouched by tier-3 jobs; accept still goes through `onDescriptionChange` / `updateCategoryMapping` only.
- [ ] Benchmark `--tiers 1,2,3` (Ollama 3B and one remote provider, keys from CI secrets, skipped when absent) reports cumulative recall@1/@3, false-accept rate and **share of items still unresolved after all tiers**, appended to `docs/mapping-suggestions/benchmark-results.md`.
- [ ] Prompt export from issue 2 remains available and unchanged when tier 3 is active.
- [ ] All three compose overlays start against the current `docker-compose.yml` (`include:` of `stores/rdf/compose.yml`) in the CI compose smoke test.

## Test plan

- `tests/unit/test_suggestions_llm_config.py` — env resolution, defaults, fail-closed cases, remote classification (ported).
- `tests/unit/test_suggestions_llm_providers.py` — Ollama / OpenAI-compatible / Anthropic clients against `responses`/`httpx` mocks, including Mistral-style payloads (ported + extended).
- `tests/unit/test_suggestions_llm_prompting.py` — hints and value context rendered, chunk boundaries, `list_b` per values column.
- `tests/unit/test_suggestions_llm_producer.py` — escalation filter, chunk retry/failure, confidence cap, `sanitise_pairs`, merge precedence vs tier 1/2 records.
- `tests/unit/test_suggestions_controller.py` (extend) — `tier3` status block per provider and compute mode.
- Vitest — badge for `source: llm`, status chip text, browser-mode fallback to the prompt panel.
- Integration — compose smoke tests for the three overlays; optional nightly job with a real Ollama 3B run and the benchmark.

## Open questions

- Confidence calibration: LLM self-reported confidence is poorly calibrated; should tier 3 confidence be replaced by a fixed value per provider (e.g. `0.75`) and let hints/alternatives carry the nuance? Decide from the benchmark.
- Should we let the LLM see *other sites'* mappings (alias pairs) as few-shot examples, as proposed for prompt export? Same privacy scope, likely large accuracy gain.
- Per-item token budget for large `valueMapping.terms` lists (some AYA variables have 50+ terms): truncate terms by tier-2 similarity ranking?

## Depends on / blocks

- Depends on: #TBD (issue 3 — tier 2 embedding model).
- Blocks: nothing; closes the tier ladder for #35. Follow-ups: codebook upload for alias memory, WebLLM stretch goal.
