# Tier 1 remediation: close the review findings before merge

Follow-up to [`01-tier1-rule-based-suggestions.md`](01-tier1-rule-based-suggestions.md) on branch `mapping-suggestions-rule-based`. A review against that plan found that the scaffolding is in place but several invariants and acceptance criteria are not met. This document turns each finding into work, plus one new UX item: a first-visit cue on the first suggestion pill.

Every finding from the review maps to a workstream (WS) below; the checklist at the end ties them together.

## Decisions to take first

These change what the workstreams build, so settle them before starting.

| # | Decision | Recommendation | Why |
|---|---|---|---|
| D1 | Pre-fill vs pre-highlight (open question in 01) | **Pre-fill the dropdown in the view, persist to the JSON-LD only on review.** | Keeps the team's pre-fill UX and the submit gate, and restores README invariant 2 (no auto-write). The reload logic already restores pre-fills from marks + records, not from the JSON-LD, so nothing else depends on the early write. |
| D2 | Conflict rule (README rule 5 says downgrade both; branch keeps the strongest) | Keep the strongest, but put the loser's match in its `alternatives` and document the change in the README. | Keeping a hint on the winner is more useful; the loser should still see what it lost to. |
| D3 | Marks on a new fingerprint (branch wipes all; README rule 4 says never overwrite) | **Expire per key**: keep `touched`/`dismissed` for keys whose `match` is unchanged in the new job; drop marks only for keys whose match changed or disappeared. | Satisfies the intent of commit `37f7941` (a stale dismissal must not hide a *different* suggestion) without discarding reviews the user already did. |
| D4 | IngestView FK review gate (not in the plan, not behind the flag) | **Move it to its own branch/PR.** | Keeps this PR reviewable and keeps `FLYOVER_SUGGESTION_TIERS=` → "renders exactly as today" true. |
| D5 | Value-based *variable* suggestions (disabled by flag for cost) | Keep disabled; move the `{M,V} → biological_sex` and year-abstain criteria to a follow-up issue and say so in 01. | Enabling needs a cheap way to get distinct values without one store query per column; that is its own piece of work. |

## WS1 — No write without review (P0)

**Problem.** Both describe views pre-fill and immediately persist: `DescribeVariablesView.vue:328–359` calls `syncToIndexedDB()` → `jsonld.updateMappingFromForm`, and `DescribeVariableDetailsView.vue:318–339` calls `_persistCategorySelection` → `jsonld.updateCategoryMapping`. The zero-writes test passes only because its mocked store is not reactive, so the watcher never fires. Comments at `:174` / `:240` say the opposite of what the code does.

**Changes.**

1. Variables view: keep pre-filled values in `formStateCache` for display, but build the `syncToIndexedDB` payload without keys that are applied-but-not-touched. Verify first that `updateMappingFromForm` leaves columns absent from `formData` untouched (it groups by the keys it receives; confirm Pass 1 does not tombstone absent columns).
2. Details view: remove `_persistCategorySelection` from the pre-fill watcher. Persist on accept/confirm through `onCategoryChange`. Track which keys were actually persisted so dismissing a never-persisted pre-fill does not call `updateCategoryMapping` with a `previousOption` that was never written.
3. Visual state: an applied-but-unreviewed field keeps the purple dashed pill **and** the dashed select border (fix the `.suggestion-highlight` condition to include `applied && !touched`); only reviewed fields turn green. Today unreviewed (`.applied`, green) and reviewed (`.confirmed`, green) look almost the same.
4. Explicit accept of a suggestion that was *not* pre-filled (e.g. after "clear all") must mark it reviewed, not leave it as "needs review".
5. "Clear all suggestions" must record dismissals, otherwise the watcher pre-fills the same fields again on the next reload.
6. Remove `markUserTouched` from `onDatatypeChange`: changing the datatype is not a review of the description.
7. Fix the stale "do NOT pre-fill" comments.

**Tests.**

- Rewrite `tests/unit/views/suggestions-zero-writes.spec.js` against the real Pinia store (mock only `api` and `db`), so records arrive through `refresh()` and the watcher fires. Assert `updateMappingFromForm` is never called with a suggestion key before review, and that accepting one changes only that column.
- Same test for `DescribeVariableDetailsView` / `updateCategoryMapping`.
- Commit the untracked `DescribeVariablesView.reload.spec.js` and extend it: reload restores the pre-fill *without* writing it.

## WS2 — First-visit cue on the first suggestion pill (new)

**Goal.** The first time a user lands on a describe page with suggestions, a small tooltip-style callout appears on the first pill. It tells them these fields were filled in for them and must be reviewed before they can continue. It appears once per phase, and the user can bring it back.

**Behaviour.**

- **When:** suggestions enabled, the phase job is `done`, at least one unreviewed suggestion exists, and the "seen" flag for this phase is not set. Show about 300 ms after the target pill mounts so it does not flash during layout.
- **Where:** the first pre-filled pill awaiting review on the current page of the first section the user opens (a database table on the variables page, a variable section on the values page), wherever that section sits in the list; opening more sections afterwards does not move it. While every section is folded nothing shows. An earlier revision anchored to a database header while the tables were collapsed; that header variant was dropped in the browser-testing pass (see the third pass).
- **Copy (variables page):**
  > **Review suggested mappings**
  > Flyover filled in this field from its mapping suggestions. Nothing is saved until you review it: check the dropdown, then click the pill to confirm, or × to dismiss. You can continue once every suggestion is reviewed.
  > [Got it]

  Values page: same, with "value" in place of "field".
- **Closes on:** "Got it", Escape, clicking the pill (accept) or its ×. Any of these sets the "seen" flag. It never blocks clicking the pill or the dropdown underneath.
- **Persistence:** IndexedDB `metadata` store, key `suggestion_coachmark_seen`, value `{ variables: bool, values: bool }`, wrapped in try/catch like `_loadMarks`. If IndexedDB fails, show at most once per page session.
- **Recovery:** a "How do suggestions work?" link in `SuggestionStatusBar` reopens it.

**Implementation.**

- New `src/components/SuggestionCoachmark.vue`: non-modal callout, `role="dialog"` + `aria-labelledby`, positioned absolutely under its anchor with an upward arrow, `max-width: 280px`. Reuse the look of the existing CSS-only `.bootstrap-tooltip` (`HomeView.vue:203`, `IngestView.vue:1391`); there is no Bootstrap JS in the app. Announce it through a polite live region, and do not steal focus on page load. Respect `prefers-reduced-motion`.
- `SuggestionBadge` gets a `coachmark` boolean prop and emits `coachmark-close`; its root becomes `position: relative` so the callout sits next to the pill without a positioning library.
- Each view computes `coachmarkKey` (first unreviewed key on the current page of the first expanded database) and passes `:coachmark="key === coachmarkKey && showCoachmark"`.
- Store: `coachmarkSeen` state with `loadCoachmark()` / `markCoachmarkSeen(phase)`, persisted as above.

**Tests.**

- Vitest: shows on the first pill only; not shown when disabled, when there are no suggestions, or once seen; Got it / Escape / accept all close it and persist the flag; nothing shows while every section is folded.
- Playwright: first visit shows it, dismiss, reload, gone. Add a `dismissCoachmarkIfPresent(page)` helper so the existing accept/dismiss e2e flow is not blocked by the callout.

**Depends on** WS1 (the copy promises "nothing is saved until you review it") and WS5 item 1 (the pill must be keyboard-reachable).

## WS3 — Matcher correctness (P0/P1)

1. **Margin and sibling guard on fuzzy alias** (`tiers/rules.py:318–325`). Today `surv1` matches another site's `surv7` (Jaro-Winkler 0.92 → confidence 0.84, above threshold), which is the README's own warning case.
   - Reject fuzzy hits where the two normalised labels differ only in digits.
   - Abstain when the best and second-best *distinct targets* are within `margin`.
   - Keep the 0.84 floor so `morf → morph` still hits (0.76, below threshold, so it escalates).
   - Tests: `surv1` vs `surv7` → no alias match; `alg_v7` vs `alg_v1b` → none; `morf` still matches.
2. **Conflicting aliases.** `build_alias_memory` uses `setdefault`, so the first site wins silently. When the same normalised label maps to different variables at different sites, abstain and list both in the reason.
3. **Reason text.** Match the plan: "Alias: column 'morph' in database 'christie' is mapped to this variable" (name the matched alias, not just the database).
4. **Abbreviation table** (`resources/suggestion_rules.json`).
   - Remove `rnnummer` (an NKI column name; the plan says `Rnnummer` must abstain).
   - Review `nr → identifier` and `stad → stage` ("stad" is Dutch for city).
   - State in the file's `description` that entries are generic abbreviations, never column names taken from a site. Bump `version`.
5. **Merge.**
   - Put only records with a non-null `match` different from the winner's into `alternatives`.
   - When every matcher abstains, keep the most informative reason (the string matcher's margin text), so the plan's "reason mentions margin" criterion holds end-to-end.
6. **Values phase.**
   - Apply a value-set rule only when all non-missing distinct values of the column fall inside one set (today each value is matched on its own).
   - Add the missing-codes rule: missing codes → a term matching `missing|unknown|unspecified`, driven by new `term_predicates` in the rules file.
7. **Test at production settings.** Use `DEFAULT_MARGIN` (0.05) in the rule tests, not 0.1. Add the missing `surv1` / `alg_v7` abstain tests through `SuggestionService`, not only through the matcher.
8. **Flag hygiene (D5).** Read `VALUE_BASED_VARIABLE_SUGGESTIONS` at call time from one place (it is imported by value into `services/suggestions/__init__.py` today), so tests and a future toggle cannot diverge.

## WS4 — Service and API: honesty, safety, speed (P0/P1)

1. **`/status` must not claim unimplemented tiers.** Derive each tier's state from the registered producers: tier 2 or 3 in `FLYOVER_SUGGESTION_TIERS` without a producer → `inactive`, reason "not implemented yet (issue 3/4)". Collapse the duplicate branches in `SuggestionConfig.tier_state`.
2. **Stop overwriting the session mapping** (`suggestions_controller.py:88–95`).
   - Restore `_maybe_adopt_mapping` from `origin/feature/llm-mapping-suggestions`: adopt only when the session has no mapping, after `MappingValidator` passes.
   - For the values phase, which needs the browser's latest variable selections, parse and validate the body mapping into a job-local object passed to `start()`, never assigned to `session_cache.jsonld_mapping`.
   - Frontend: send the mapping on the values phase, or on the variables phase only after `/start` answered `no_semantic_map`, instead of on every mount.
3. **Values-phase leave-one-site-out bug.** Value groups set `database` but `_run_group` reads `described_database` (`__init__.py:354`), so every group excludes only the first database. Set `described_database` on value groups and add a two-database test.
4. **Fallback payload** (`__init__.py:673`). Look up a column's variable by (database, local column) using `graph_database_find_name_match`, not by local name across all databases. Build the index once instead of scanning all columns per column.
5. **Fingerprint.** Include the rules `version` and a hash of the alias memory (other databases' column → variable pairs). Use `g["database"]` / `g["column"]` instead of the `key_for("").rsplit` trick.
6. **Marks on a new fingerprint (D3).** Frontend `_syncMarksToFingerprint` keeps marks for keys whose `match` is unchanged in the new records.
7. **Reserve `SuggestionService.ingest(phase, records, source)`** as the plan says: a stub that validates through `sanitise_pairs` and raises `NotImplementedError`, with no route yet. Fix the controller docstring.
8. **Speed.** Measured 12.3 s for fuzzy alias on 600 columns × 3,500 remembered labels (synthetic), synchronous inside the request. Target: under 1 s on the 4-core/8 GB profile.
   - Precompute candidate tokens once in `StringMatcher` (today `_score_string` re-tokenises every candidate for every item).
   - Dedupe alias memory by normalised label.
   - Block fuzzy comparisons by first character and length difference.
   - If that is not enough, consider `rapidfuzz` (a wheel in the image keeps it offline). That adds a dependency, so decide explicitly.
   - Report wall-clock in the benchmark (WS6).
9. **Cleanup.**
   - One helper for the duplicated polars category parsing; imports at module top.
   - Remove or use `SuggestionRecord`.
   - Pass the database-name matcher into `rules.py` instead of importing `RDFStoreService`.

## WS5 — Frontend polish (P1/P2)

1. **Keyboard access.** The pill is a clickable `<span>`. Make accept and dismiss two sibling `<button>`s inside a wrapper (a button cannot nest inside another), with `aria-label`s.
2. **Alternatives.** Replace the icon with the popover the plan asks for: list non-null alternatives with source and confidence, and let the user apply one via the same path as accept. Fix the "true alternative(s)" tooltip (`SuggestionBadge.vue:81`).
3. **Render `tierLabel`** (computed but unused). Add the `retry` slot ("retry suggestion"); it can stay unused until tier 3.
4. **Status bar.** "N of M variables" becomes "values" on the details page. Remove the unused `suggestionProgress` (`DescribeVariablesView.vue:279`).

## WS6 — Benchmark that measures what the plan says (P1)

1. **Leave one *site* out.**
   - Load all branches into one pool, and treat a branch as a site.
   - When evaluating a site, hide all of its databases (NKI prospective and retrospective together), so alias memory only sees other sites.
   - Today each branch is loaded alone, so single-database sites get no alias memory and NKI/Leeds leak into themselves.
2. **Same code path as the app.** Run through `SuggestionService` with a fake session cache and RDF service, so `sanitise_pairs`, merge and conflict handling apply.
3. **Metrics.**
   - Recall@1, recall@3 (from `alternatives`), abstain rate, false-accept rate at the threshold, and precision of accepted matches.
   - A breakdown per source (`alias` / `value_regex` / `string`), a values-phase run over `localMappings`, and wall-clock.
   - Count exactly instead of `int(rate * total)`.
4. **Sweep the parameters.** Threshold 0.60–0.95 and margin 0.02–0.15; pick the defaults from the results and set `DEFAULT_THRESHOLD` / `DEFAULT_MARGIN` and compose defaults to match.
5. **IGR-Paris** is missing from the results: find out why (different file path?) and allow `--mapping-path` per branch.
6. **Record the results.** Commit the new `benchmark-results.md`, fill the `TBD from benchmark` values in 01, and remove the unused imports in `scripts/benchmark_suggestions.py`.

## WS7 — Scope and repo hygiene (P1)

- **D4:** move the IngestView FK changes (`16e8f3f`, `bcd10c1`, `f6dede1`, `cd11601`, `7cef964`, `4db0db8`, and the IngestView parts of `1b37119` / `5639fc0` / `95f92bc`) to their own branch. There, give the FK record its own source instead of `source: 'alias'` at 100 %, and decide whether the gate follows the feature flag.
- **Docs.**
  - Commit `docs/mapping-suggestions/README.md` and 02–04: the committed 01 links to the README.
  - Update 01's paths (`flyover/backend/...` → `app/backend/...`, `scripts/` → `app/backend/scripts/`) and record decisions D1–D5.
- **Do not commit:** the local `docker-compose.yml` image→build switch. Add the repo-root `flyover/` (target of the `app/frontend/node_modules` symlink) to `.gitignore` or move it outside the repo. Decide what happens to `mapping_plan.md`.
- **Squash on merge:** the branch has 45 commits with many fix-ups.

## Order of work

1. D1–D5 agreed; WS7 split of IngestView (small, unblocks review).
2. WS1, then WS5 item 1, then WS2. The cue depends on no-auto-write and on keyboard-accessible pills.
3. WS3 and WS4 in parallel.
4. WS6 last, since it measures the fixed matchers and sets the defaults.
5. Remaining WS5 and WS7 items.

## Checklist (review finding → workstream)

- [x] Pre-fill writes to JSON-LD without review; zero-writes test is vacuous → WS1
- [x] Unreviewed and reviewed pills look alike → WS1.3
- [x] "Clear all" undone on reload; explicit accept leaves "needs review" → WS1.4–1.5
- [x] First-visit review cue on the first pill → WS2
- [x] Fuzzy alias has no margin; `surv1` confidently mis-mapped → WS3.1
- [x] `rnnummer` / risky abbreviations in the rules file → WS3.4
- [x] Abstain reason and noisy `alternatives` from the merge → WS3.5
- [x] Values matched per value; missing-codes rule absent → WS3.6
- [x] Tests use margin 0.1; no `surv1` / `alg_v7` tests → WS3.7
- [x] Value-based variable suggestions disabled vs. acceptance criteria → D5, WS3.8
- [x] `/status` reports tiers 2 and 3 active → WS4.1
- [x] Session mapping overwritten by unvalidated request body → WS4.2
- [x] Values-phase "self" database is always the first database → WS4.3
- [x] Fallback maps columns via other sites; O(n²) → WS4.4
- [x] Fingerprint ignores alias memory and rules version → WS4.5
- [x] Marks wiped on any new fingerprint → D3, WS4.6
- [x] `ingest()` not reserved → WS4.7
- [x] Alias / string matching too slow for the target box → WS4.8
- [x] Pill not keyboard-accessible; no alternatives popover; "true alternative(s)" → WS5
- [x] Benchmark not leave-one-site-out; bypasses the service; TBDs unfilled → WS6
- [x] IngestView FK gate out of scope and outside the flag → D4, WS7
- [x] Untracked docs / reload spec; stale plan paths; local compose change → WS7

## Second review pass

A check of the ticked checklist against the code found items that were ticked but not done, and some new issues. Fixed on this branch:

- [x] Values-phase whole-column rule depended on column order: the column context was one payload-wide `value → column values` map, so a 1/2/3 column's `1` became `yes` when a 1/0 column came first. It is now built per column.
- [x] Alias reason named the item instead of the remembered column ("column 'morf' in database 'christie'"; christie's column is `morph`).
- [x] Values fallback still scanned every mapping column per store column (WS4.4); it is indexed by local name now.
- [x] D2 was not implemented: conflict losers now keep the contested variable in their own `alternatives`, and the variables page shows it as an alternatives-only pill. The D2 wording in the README and 01 ("the winner carries the losers' matches") is corrected.
- [x] An accept blocked by the one-variable-per-database rule did nothing silently; it now says which column holds the variable.
- [x] A dismissed suggestion's pill came back in the unreviewed look; it now disappears.
- [x] The accept button's `aria-label` read "(undefined%)".
- [x] The variables pre-fill overrode mappings preselected from the loaded JSON-LD.
- [x] The variables page pre-filled suggestions of any confidence; it now pre-fills only at or above the threshold and shows the rest as hints. The values page is deliberately not gated (see 01, open questions).
- [x] Benchmark run and committed, its sweep scoring corrected (it rewarded a higher threshold for free), and 01's TBDs filled (WS6).

Still open, for the team to decide:

- [ ] Margin: `0.05` stays; `0.02` trades +6.4 pp correct pre-fills for +4.2 pp wrong hint pills (see `benchmark-results.md`).
- [ ] Whether the values page gets its own threshold (`0.60`–`0.65` measured best).
- [x] `mapping_plan.md` stays out of the repo on purpose; the plan lives in the GitHub issues.
- [x] The Playwright suggestion flows ran against the branch's stack on 2026-09-30 (the three suggestion flows and the full suite, 25 tests). The flows had never run against a live stack: they skipped the semantic-map upload, which since the "no map, no suggestions" fix means no suggestions, and still used the pre-WS1.3 pill states. Rewritten in `69b4f4d`; the map is uploaded re-labelled as another site so alias memory has something to remember. The new suggestions-disabled flow found that the variables page still posted priority bumps with the feature off, fixed in `f120bb4`.

## Third pass: browser testing

Found while using the app, fixed on this branch:

- [x] Once any map had been adopted, the variables job kept running on it: re-uploading a map, or opening another (incognito) browser, showed suggestions computed on the first map. This reverses WS4.2's "variables phase sends its map only after `no_semantic_map`": both phases now send the browser's map and the backend runs the job on it job-locally. The session mapping is still never overwritten.
- [x] With no map in the browser the store still started a job, and the page pre-filled values its dropdown could not show. It now reports "Upload a semantic map to enable suggestions", and the variables page only uses suggestions whose variable is a dropdown option.
- [x] Uploading a semantic map did not reset the review marks, so after a re-upload a field could be empty while its pill said "reviewed". The describe landing page now resets them.
- [x] The first-visit callout inherited its anchor's font (huge in a table heading, tiny next to a pill) and was wordy; it now has its own size and two short sentences.
- [x] The callout's header variant (anchored to the first database's heading while every table was folded, then jumping to a pill) is gone: while everything is folded nothing shows, and the cue pops up on the first pre-filled pill of the first section the user opens, wherever that section sits in the list. "How do suggestions work?" reopens it; when no open section has a pre-filled pill it opens the first section that has one, at that pill's page.
- [x] The status bar wrapped its message and tier note into each other; it now has two rows.
- [x] The details page's category dropdowns collected their value-mapping options from the browser's map, but the state they rendered (variables, preselected values, JSON-LD details population) came from the session's adopted mapping — so in a browser whose map differed from the session's (fresh/incognito, or after re-uploading a map) the dropdowns fell back to the generic Yes/No list and preselected values the dropdown could not show. The details-state request now POSTs the browser's map and the backend renders that response on it, request-locally (same pattern as the suggestions job); the session's details state is only ever populated from the session's own mapping. A browser with no map at all still shows the session's state, so the response also carries each categorical variable's value-mapping terms (`category_options`) and the view falls back to them when its own map has none — the dropdown then offers the terms of the variables it is listing.
