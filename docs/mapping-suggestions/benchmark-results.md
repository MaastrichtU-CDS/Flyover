# Tier-1 benchmark results

## Status

The numbers below were produced by the **pre-remediation benchmark
protocol** and are superseded: each branch was loaded alone, so
single-database sites got no alias memory at all while NKI and Leeds
(each carrying two databases) leaked into their own alias memory, the
run bypassed `SuggestionService` (no `sanitise_pairs`, merge, or
conflict handling), IGR-Paris was silently missing, and recall@3,
precision, per-source counts, the values phase, and wall-clock were not
measured. They are kept only as a reference point.

The benchmark script (`app/backend/scripts/benchmark_suggestions.py`)
was reworked in WS6 of the tier-1 remediation to fix all of that. It
still needs one run with network access to fetch the AYA branches
(`--cache-dir` caches them afterwards); until then the "New protocol"
table below is a placeholder.

```bash
python app/backend/scripts/benchmark_suggestions.py \
    --cache-dir "$TMPDIR/aya-benchmark" --sweep \
    --out docs/mapping-suggestions/benchmark-results.md
```

## New protocol (pending run)

- Leave one **site** out: all branches are loaded into one pool and a
  branch counts as a site; when a site is evaluated, all of its
  databases are hidden from the alias memory (NKI prospective and
  retrospective together).
- The run goes through `SuggestionService` with a fake session cache
  and RDF store, so `sanitise_pairs`, the cascade merge, conflict
  handling, and the values-phase column guards all apply.
- Metrics per site and pooled, with exact counts: recall@1, recall@3
  (from `alternatives`), abstain rate, false-accept rate at the
  threshold, precision of accepted matches, a per-source breakdown
  (`alias` / `value_regex` / `string`), a values-phase run over
  `localMappings`, and wall-clock time.
- `--sweep` runs the variables phase over threshold 0.60–0.95 and margin
  0.02–0.15 and reports the best combination (recall@1 minus
  false-accept) to inform `DEFAULT_THRESHOLD` / `DEFAULT_MARGIN`.
- Per-branch mapping paths are tried in order (IGR-Paris was missing
  before because its file lives at a different path);
  `--mapping-path` overrides them.

| Site | Items | Recall@1 | Recall@3 | Abstain | False-accept | Precision (accepted) | alias / value_regex / string | Wall-clock |
|---|---|---|---|---|---|---|---|---|
| _pending a run with network access_ | | | | | | | | |

## Old protocol numbers (superseded, kept for reference)

Produced before the WS6 rework; see "Status" above for why they do not
measure what the plan asks for. IGR-Paris is absent because its mapping
file lives at a different path than the one the old script requested.

| Site | Columns | Recall@1 | False-accept | Abstain |
|---|---|---|---|---|
| CLB-Lyon/CLB_retrospective_data | 19 | 10.5% | 0.0% | 89.5% |
| INT-Milan/STRONGAYA_Flyover_DATA_LABELS_2026_08_13_1756 | 38 | 68.4% | 5.3% | 23.7% |
| MSCI-Warsaw/MSCI_prospective_data | 133 | 15.0% | 3.8% | 74.4% |
| NKI-Amsterdam/NKI_prospective_data | 403 | 17.9% | 3.0% | 77.2% |
| NKI-Amsterdam/NKI_retrospective_data | 143 | 57.3% | 2.1% | 39.2% |
| TheChristie-Manchester/TheChristie_retrospective_data | 13 | 15.4% | 0.0% | 84.6% |
| YSRCCYP-Leeds/YSRCCYP_prospective_data | 100 | 5.0% | 1.0% | 82.0% |
| YSRCCYP-Leeds/YSRCCYP_retrospective_data | 27 | 25.9% | 3.7% | 70.4% |
| **Pooled** | **876** | **24.7%** | **2.7%** | **68.9%** |
