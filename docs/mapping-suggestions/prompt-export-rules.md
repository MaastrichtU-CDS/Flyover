# Prompt export: what leaves the browser, and the rules that limit it

The "Use an LLM" panel on the describe pages composes a prompt that a user
copies into an LLM of their own choosing and pastes the answer back. The
prompt is the only thing Flyover hands to an external service, and it does
so through the user's clipboard, so this page lists exactly what a prompt
can contain, the rules that keep data out of it, where each rule is
enforced and how to tune it. Design and build history:
[`02-llm-prompt-export-roundtrip.md`](02-llm-prompt-export-roundtrip.md).

## What a prompt contains

| Phase | In the prompt | Never in the prompt |
|---|---|---|
| Variables (describe page) | Schema variable keys, labels, descriptions and datatypes; the names of the local columns still unmapped; the existing mappings as `mapsTo` / `localColumn` pairs; tier-1 hints (match, confidence, source) | Any cell value, any distinct value, row counts, column comments |
| Values (details page) | The above for the mapped columns only, plus their term keys and ontology codes; the distinct values of the mapped categorical columns that still need a term, after the guards below; values already mapped, as context | Data rows, row counts, values of unmapped columns, values of held-back columns, values under the frequency floor |

The existing-mappings section is built from a whitelist of fields
(`mapsTo`, `localColumn`, `localMappings`), so comments or free text stored
in the semantic map never go along. The database name and the table key
do appear, since the answer is a JSON-LD slice keyed by them.

## Rules in the values phase

1. **Only mapped categorical columns are asked.** A column enters the
   values prompt only when the semantic map maps it to a variable that
   defines terms. An identifier variable has no terms, so an id column
   never qualifies through a correct mapping.
2. **Shape guard: free-text-like columns are held back.** A column whose
   values do not look like category codes is left out and named in the
   response (`held_back`: column, variable, distinct count, reason):
   - more than `MAX_VALUES_PER_COLUMN` (50) distinct values, or
   - values averaging more than `MAX_MEAN_VALUE_LENGTH` (30) characters.

   This catches the common mistake of mapping a notes, date, identifier
   or continuous column to a categorical variable on the variables page.
   The panel lists the held-back columns with the reason. The guard can
   be bypassed per column through the API (`include`, a list in the
   `/prompt` body or comma-separated in the query string); the panel
   offers no control for it, so a column that is wrongly held back is
   fixed by correcting its mapping on the variables page.
3. **Frequency floor: rare values are left out.** A value shared by fewer
   rows than `FLYOVER_SUGGESTION_MIN_VALUE_COUNT` (default 10) is not
   asked about, because a rare diagnosis, an unusual code or a stray
   free-text entry can identify a person although no row is sent. The
   counts come from the same store query that lists the values, so the
   floor costs nothing. Left-out values are reported per column
   (`suppressed`) and in total; a column whose every value fell under the
   floor is listed with zero values so the user sees why it is missing.
   Setting the floor to 1 disables it. When the store is unavailable the
   counts are unknown and the floor cannot apply; every value is asked.
4. **Values already mapped are context, not work.** They appear in the
   "already mapped" line of their column and are withheld from the list
   to map.
5. **Whole columns per part.** A long prompt is split into parts of at
   most `chunk` values (`FLYOVER_SUGGESTION_PROMPT_CHUNK`, default 40,
   5 to 1000); a column is never split across parts, so a part with one
   large column may exceed the size.

## Rules in the variables phase

The variables prompt shares column names only. It runs no per-column
store query, lists no distinct values and no sample, and the hints it
carries are the match key, confidence and source of the current job
record, never the reason text.

## What the user sees before copying

Generating a prompt opens a modal rather than rendering the text inline.
The modal shows first the part of the prompt that carries the user's own
data: section 3, the local column names (variables phase) or the values
per column (values phase) of the selected part. Below it, one line names
what the guards left out (held-back columns with the reason, the number
of rare values under the floor), then the privacy line, then the full
prompt folded away. Copy and Download are disabled until the user ticks
one acknowledgement: that they checked the values above for personal data
and accept the risk of sending them to the LLM they use. The
acknowledgement resets every time the modal opens. The panel itself keeps
one line of summary (items still to map, parts, held back, left out) and
a "Show prompt" button that reopens the modal.

## Settings

| Setting | Default | Range | Effect |
|---|---|---|---|
| `FLYOVER_SUGGESTION_PROMPT_CHUNK` | 40 | 5 to 1000 | Items per part; the panel's "Items per prompt" default |
| `FLYOVER_SUGGESTION_MIN_VALUE_COUNT` | 10 | 1 to 10000 | Frequency floor; 1 disables it |
| `MAX_VALUES_PER_COLUMN` (constant) | 50 | | Shape guard, distinct values |
| `MAX_MEAN_VALUE_LENGTH` (constant) | 30 | | Shape guard, mean value length in characters |
| `SAMPLE_VALUES` (constant) | 8 | | Values quoted per asked column in the panel |

`/api/v1/suggestions/status` reports the chunk default and the floor
under `prompt_export`, and every `/prompt` response carries the floor
that was applied (`min_value_count`).

## Where each rule lives

| Rule | Code | Tests |
|---|---|---|
| Whitelisted existing-mappings section | `services/suggestions/prompt_export.py`, `existing_section` | `test_suggestions_prompt_export.py` |
| Names only in the variables phase | `prompt_export.py`, `PromptExport.variable_items` | `test_prompt_contains_no_data_rows` |
| Mapped categorical columns only | `prompt_export.py`, `PromptExport.value_groups` | `test_only_mapped_variables_terms_and_unmapped_values` |
| Shape guard and `include` | `prompt_export.py`, `looks_like_free_text`, `value_groups` | `test_free_text_like_column_is_held_back`, `test_included_column_bypasses_the_guard`, `test_long_values_are_held_back_even_when_few` |
| Frequency floor | `prompt_export.py`, `value_groups`; counts from `jobs.py`, `_parse_category_counts` | `TestValueFrequencyFloor` |
| Prompt modal: section 3 first, left-out line, acknowledgement before copy | `app/frontend/src/components/LlmPromptPanel.vue` | `LlmPromptPanel.spec.js` |
| End to end, both phases, on a real store | | `tests/e2e/llm-prompt-roundtrip.spec.js` |

## What the rules do not cover

- **Quasi-identifiers in genuinely categorical columns.** Ages, years and
  site names pass the shape guard; the floor removes the rare ones, but a
  common value is still shared. The modal shows the values and asks for
  an acknowledgement; it cannot judge them.
- **Where the user pastes.** Flyover cannot tell an institutional LLM
  from a public chatbot. The intro asks for an LLM the institution
  allows; nothing enforces it.
- **The pasted answer.** It is parsed and validated, never stored as raw
  text, and never written to the semantic map without an explicit accept.
