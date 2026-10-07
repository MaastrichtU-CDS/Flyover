# LLM prompt export + paste-back round-trip

Part of #35, implements #139. Shared design (contract, API, cascade, invariants): [`docs/mapping-suggestions/README.md`](README.md). Builds on the infrastructure from issue 1 (tier 1).

## Context & motivation

Many sites cannot run a model next to Flyover (4-core/8 GB VM, air-gapped network, no GPU) but *do* have an LLM they are allowed to use elsewhere — an institutional ChatGPT/Copilot licence, an internal Ollama box, a national research LLM. The cheapest way to give them LLM help is to generate the prompt for them and let them paste the answer back.

This is deliberately provider-agnostic and needs zero configuration. It is also a **permanent** feature: when the integrated LLM (issue 4) lands, prompt export stays as the fallback for restricted shops and as the `browser` compute mode for tier 3.

Without the paste-back half, a prompt is a nice-to-have. With it, the LLM's answer becomes ordinary suggestion records (`source: pasted_llm`) that render in the same `SuggestionBadge`, obey the same cascade/merge rules and require the same explicit accept.

## Goal

- A "Copy prompt" action per database and phase (variables / values) on both describe views that produces a self-contained prompt with the schema slice, the local column names or distinct values, tier-1 candidates as hints, and a strict JSON answer format.
- A "Paste LLM answer" action that validates the pasted JSON server-side and merges it as suggestions.
- Clear privacy messaging: the prompt never contains data rows and the user sees exactly what leaves the browser.

## Non-goals

- Calling any LLM from Flyover (issue 4).
- Free-form chat inside Flyover; the conversation happens in the user's own LLM client.
- Automatically applying the pasted answer to the JSON-LD.

## Design

### Backend

New route in `flyover/backend/data_descriptor/controllers/suggestions_controller.py`:

```
GET /api/v1/suggestions/prompt?phase=variables|values&database=<db>[&columns=a,b,c][&exclude_free_text=true]
→ { "prompt": "<text>", "answer_schema": {...}, "item_count": 87, "chunk_hint": 40, "contains": ["variable keys", "labels", "column names"] }
```

New module `flyover/backend/data_descriptor/services/suggestions/prompt_export.py`:

- `build_prompt(phase, database, ctx)` composes, in order:
  1. Task framing (one paragraph, English) and the schema/data separation rule: the answer may only use keys from the provided list.
  2. **Schema slice** — variables phase: `key`, `label`, `description` (truncated), datatype for every `schema.variables` entry; values phase: for each column already mapped, the variable key and its `valueMapping.terms` keys + labels.
  3. **Local side** — variables phase: column names from `column_info` (optionally with datatype and up to 5 distinct values for categoricals); values phase: the distinct values per column from `rdf_store_service.get_categories`. **Never data rows.**
  4. **Hints** — tier-1 records for these items (`match`, `confidence`, `source`) so the LLM can confirm or overrule instead of starting blind; abstains are listed as "no candidate".
  5. Constraints and the answer format: a JSON array of `{item, match, confidence, reason}`; `match` must be an exact key or `null`; one object per item; wording lifted from the branch's `matching.py` system prompt (`build_user_prompt`, the "EXACT string from list_b" rules).
- `answer_schema` is `contract.MATCH_OUTPUT_SCHEMA` minus `source`/`tier`/`status` (the server fills those in).
- Chunking: if `item_count > chunk_hint` (default 40, env `FLYOVER_SUGGESTION_PROMPT_CHUNK`), the response includes `chunks: [{columns: [...], prompt: "..."}]` so the UI can offer "Copy chunk 1/3".
- Free-text columns (datatype string with high cardinality per `column_info`) are excluded by default from the values phase and flagged in `contains`.

Paste-back uses the `/ingest` route reserved in issue 1:

```
POST /api/v1/suggestions/{variables|values}/ingest
body: { "source": "pasted_llm", "database": "<db>", "records": [ {item, match, confidence, reason}, ... ] }
→ { "accepted": 85, "nulled": 2, "rejected": 0, "job": <snapshot> }
```

`SuggestionService.ingest()`:

- Parses tolerant JSON (strips code fences, trailing prose) then validates with `answer_schema`.
- Runs `contract.sanitise_pairs`: unknown `item` → rejected; `match` not an exact schema key → nulled with `reason` prefixed `"[invalid key from LLM] "`; `confidence` clamped to [0, 1]; duplicates → highest confidence kept.
- Stamps `source: pasted_llm`, `tier: 3`, `status: done` and merges via the README cascade rules (highest confidence wins, user marks untouched).

### Frontend

Both `DescribeVariablesView.vue` and `DescribeVariableDetailsView.vue` get a small "LLM help" panel per database (collapsed by default):

- **Copy prompt** — calls `/prompt`, shows the `contains` list and a privacy notice ("This prompt contains variable keys/labels, your column names and distinct values. It contains no data rows. Review before sending it to an external service."), copies to clipboard; for chunked responses shows one button per chunk. Checkbox "include free-text columns" (off).
- **Paste answer** — textarea + "Import" button → `POST /ingest`; result toast via `useStatusStore()` (`85 suggestions imported, 2 had invalid keys`). Imported records appear through the normal store poll with a `pasted_llm` badge.
- Store additions in `flyover/frontend/src/stores/suggestions.js`: `fetchPrompt(phase, db, opts)`, `ingest(phase, db, records)`; no new state beyond `lastIngestResult`.

Reuse the branch's `frontend/src/stores/suggestions.js` polling; nothing in `jsonld.js` changes.

### Worked example (AYA NKI-Amsterdam slice, variables phase, excerpt)

Prompt excerpt:

```
Schema variables (use these EXACT keys):
- identifier: "Identifier" — pseudonymised patient id
- administered_prom_language: "Administered PROM language" — language of the questionnaire
- age_at_initial_diagnosis: "Age at initial diagnosis" (integer, years)
- year_of_initial_diagnosis: "Year of initial diagnosis" (integer)
...
Local columns to map (database "nki"):
- Rnnummer (string, 566 distinct)
- taal (categorical: nl_NL, en_GB)
- leeft (integer)
- jaar_van_diagnose (integer)        hint: year_of_initial_diagnosis (0.86, string)
- surv70 (categorical: 1,2,3,4)      hint: no candidate
Answer with a JSON array [{"item","match","confidence","reason"}] ...
```

Canned answer (used verbatim as the round-trip test fixture):

```json
[
  {"item": "taal", "match": "administered_prom_language", "confidence": 0.9, "reason": "Dutch 'taal' = language; values are locale codes."},
  {"item": "leeft", "match": "age_at_initial_diagnosis", "confidence": 0.85, "reason": "'leeft' abbreviates 'leeftijd' (age)."},
  {"item": "Rnnummer", "match": "identifier", "confidence": 0.7, "reason": "Looks like a registration number."},
  {"item": "surv70", "match": "eortc_qlq_c30_question_6", "confidence": 0.4, "reason": "Guessing a questionnaire item."}
]
```

Expected ingest result: `taal`, `leeft`, `Rnnummer` accepted as `pasted_llm`; `surv70` **nulled** because `eortc_qlq_c30_question_6` is not an exact schema key (the real key is `eortc_qlq_c30_q6`), reason prefixed `[invalid key from LLM]` — the item stays with the human.

## Acceptance criteria

- [x] `GET /api/v1/suggestions/prompt?phase=variables&database=nki` returns `prompt`, `answer_schema`, `item_count`, `contains`; 400 for unknown phase/database.
- [x] The prompt contains every `schema.variables` key not already used by this database for the variables phase (the used ones appear in the context section) and only the mapped variables' `valueMapping.terms` for the values phase.
- [x] The prompt contains **no data rows**: a unit test builds a prompt from a fixture with known cell values (non-categorical) and asserts none appear.
- [x] A values-phase column whose values look like free text, dates or identifiers is held back and named in `held_back`; `include` asks for it explicitly. *(Reworked: the `exclude_free_text` option became a per-column guard — see Implementation notes.)*
- [x] Prompts over `FLYOVER_SUGGESTION_PROMPT_CHUNK` items are chunked; each chunk is self-contained (repeats the schema slice).
- [x] Tier-1 hints appear for items that have a tier-1 record, and abstains are listed as "no candidate".
- [x] `POST /api/v1/suggestions/variables/ingest` with the canned answer above yields `accepted: 3, nulled: 1`, records tagged `source: pasted_llm, tier: 3`.
- [x] Ingest tolerates code fences and leading/trailing prose around the JSON; malformed JSON returns 400 with a readable message.
- [x] Ingested records never overwrite `applied` / `touched` / `dismissed` marks and never write to the JSON-LD (Vitest: `jsonld.getMapping()` unchanged after import).
- [x] Both describe views show the Copy prompt / Paste answer panel with the privacy notice; Playwright flow: copy → paste canned answer → badge appears → accept one.
- [x] `/api/v1/suggestions/status` lists `prompt_export: active` regardless of `FLYOVER_SUGGESTION_TIERS` (it has no runtime dependency).

## Test plan

- `tests/unit/test_suggestions_prompt_export.py` — composition order, schema slice completeness, no-row guarantee, chunking, free-text exclusion, hint rendering.
- `tests/unit/test_suggestions_pasted_answer.py` — fences, prose, trailing commas, nested/bare/flat answer shapes, `valueNotes`, dedupe.
- `tests/unit/test_suggestions_service.py` (extend) — `ingest()` round-trip with the canned answer; nulling of invalid keys; clamping; dedupe; marks immutable.
- `tests/unit/test_suggestions_controller.py` (extend) — `/prompt` and `/ingest` routes, error codes.
- Vitest `stores/suggestions.spec.js` (extend) — `fetchPrompt`, `ingest`, toast content; component test for the panel.
- Playwright `tests/e2e/llm-prompt-roundtrip.spec.js` — one end-to-end round trip per phase (generate → check prompt → paste → pill → accept); runs with the tiers flag set or empty.

## Implementation notes (as built)

Branch `feature/llm-prompt-export-roundtrip`. Where the build deviates from the design above, this section is authoritative.

- **The answer is a JSON-LD section, not a record array.** The prompt asks the LLM for the `databases.<db>.tables.<table>.columns` object Flyover itself writes when something is mapped (`mapsTo` + `localColumn`; `localMappings` in the values phase), so the reply is a valid slice of the semantic map that concatenates with the entries already there. Optional `confidence` / `reason` per column entry and `valueNotes` per value carry the reviewer hints; the server strips them into the record fields. The flat `[{item, match, confidence, reason}]` form above is still accepted by `/ingest`, and so are code fences, surrounding prose, trailing commas, a bare `columns` object or an echoed full document (`services/suggestions/pasted_answer.py`).
- **Only what still needs a decision.** The prompt lists the unmapped columns (variables phase) or the unmapped distinct values of the mapped categorical columns (values phase). The existing mappings are shown as context in their JSON-LD form and their variables are withheld from the candidate list; `/ingest` skips items the JSON-LD already maps and nulls a reuse of an already-mapped variable (`[already mapped] ...`). A nulled record is one whose match the server rejected (invalid key or already mapped); the toast and the panel summary count them as "left for you to decide", the plan's "the item stays with the human".
- **Any model, any client.** Plain text, no system prompt, no JSON mode, no tool use; a worked answer skeleton; `Items per prompt` is the user's choice in the panel (20–400, default `FLYOVER_SUGGESTION_PROMPT_CHUNK` = 40) and every part repeats the schema slice and the existing section so it is self-contained. Copying tries the async clipboard API and falls back to the legacy copy command, then to an on-page preview and a `.txt` download, so it works on plain-http hosts. The collapsed panel's toggle sits in the per-database button row (next to "Dismiss all suggestions"), not on its own line, and the section keeps its copy short: a four-line intro (no language model inside Flyover; copy the prompt into an institutional LLM; its answer helps the mapping; review — nothing is saved until accepted), the privacy note and one "Review it before sending".
- **No data rows — and no values in the variables phase.** The variables prompt lists only the column names (with tier-1 hints); it shares no distinct values at all and costs no per-column store query (decision after review: values belong to the values-phase prompt, which is where they are mapped). In the values phase every mapped categorical column is asked, **unless its values look like free text, dates or identifiers**: more than 50 distinct values, or values averaging more than 30 characters (`prompt_export.looks_like_free_text`). A column mapped to a categorical variable should hold a handful of short codes; one that does not is more likely a mis-mapping on the variables page, and the guard keeps its values from leaving the browser. Such a column is listed in the response's `held_back` (column, variable, distinct count, reason) and the panel shows it with a checkbox; ticking it sends the column in `include` on the next generate, which bypasses the guard for exactly that column. The response's `asked` lists the columns in the prompt with their value counts, which the panel shows under "What leaves the browser", and the values-phase privacy note says that a distinct value can still identify someone. The design's `exclude_free_text` flag (one switch for all columns) is not implemented; the guard replaced it.
- **`/prompt` also accepts POST** with the browser's semantic map in the body (the describe pages work on the map in IndexedDB, which the session may not hold); GET without a body falls back to the session's map. The design's `columns` query parameter is not implemented — the columns always come from the RDF store — and a chunk part carries `index`, `items` (the column names) and `item_count`, not the design's `columns` key.
- **Tiers off.** `prompt_export` is `active` in `/status` regardless of `FLYOVER_SUGGESTION_TIERS` and also carries the site's clamped `FLYOVER_SUGGESTION_PROMPT_CHUNK` as `chunk`, which the panel adopts as its "Items per prompt" default; the panel renders whenever the state is `active`, and a phase snapshot reports `enabled: true` as soon as a paste created a job, so the pills render on a stack with every tier off. Once a prompt is generated, the panel shows the server's privacy note, which is worded for the phase.
- **A paste re-opens dismissed fields.** A dismissal judges one suggestion, not the field. The browser sends its dismissed keys with the paste; for those keys a pasted record with a match takes the field outright (the dismissed candidate stays an alternative), `/ingest` returns them as `reopened`, and the browser drops those marks so the pre-fill and pill show again. Reviewed fields are already in the JSON-LD and are skipped, so nothing filled in is touched; a nulled paste re-opens nothing.
- **Pasted records survive a rebuild.** They are kept on the session and re-applied after a reload or forced re-run; the public job fingerprint gains an ingest hash so the browser expires marks per key (D3) without the tier job being rebuilt.

## Open questions

- Should the prompt include the site's *other* databases' mappings as few-shot examples? Cheap and helpful, but lengthens the prompt; proposal: include up to 10 alias pairs when available.
- Persist the pasted raw text for audit (who suggested what)? Proposal: keep only records; raw text stays in the user's LLM client.
- Language of the prompt: English only for now; schema labels are already English.

## Depends on / blocks

- Depends on: #TBD (issue 1 — tier 1 + shared infrastructure).
- Blocks: #TBD (issue 3 — tier 2 embedding model).
