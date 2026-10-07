import { test, expect } from '@playwright/test'
import fs from 'node:fs/promises'
import { DEFAULT_MAPPING_JSONLD, runIngestFlow, watchConsoleErrors } from './helpers/ingest.js'
import {
  dismissCoachmarkIfPresent,
  expandAllDatabases,
  otherSiteMapping,
  suggestionsEnabled,
  uploadSemanticMapAndContinue,
} from './helpers/suggestions.js'

// ---------------------------------------------------------------------------
// LLM prompt export + paste-back round trip, one flow per phase.
//
// Both flows ingest the example CSV and upload a semantic map, open the
// "Use an LLM" panel of the ingested database, generate the prompt
// (and check it names the unmapped items, carries hints and no data rows),
// paste a canned answer, and accept one of the imported suggestions.
//
// The round trip needs no tier, so these flows run against a stack with
// FLYOVER_SUGGESTION_TIERS set or empty alike.
// ---------------------------------------------------------------------------

/** Name of the store database the example CSV ingests as. */
const DB = 'synthetic_english_150'

/**
 * The `.variable-row` of one column of the ingested database. The local
 * store may hold databases from earlier runs with the same column names,
 * so rows are pinned by the description select's id, not by label text.
 */
function rowFor(page, column) {
  return page
    .locator('.variable-row')
    .filter({ has: page.locator(`[id="ncit_comment_${DB}_${column}"]`) })
}

/** The panel of the ingested database, whichever section order the store has. */
function panelFor(page, phase) {
  return page.locator(`.llm-help-panel[data-phase="${phase}"][data-database="${DB}"]`)
}

/**
 * Open every collapsed categorical variable on the details page: the
 * value pills live inside the per-variable sections.
 */
async function expandAllVariables(page) {
  const collapsed = page.locator('.item-toggle-button:not(.open)')
  for (let i = 0; i < 100 && (await collapsed.count()) > 0; i++) {
    await collapsed.first().click()
  }
  await expect(collapsed).toHaveCount(0)
}

async function openAndGenerate(page, phase) {
  const panel = panelFor(page, phase)
  await expect(panel).toHaveCount(1)
  await panel.locator('.llm-help-toggle').click()
  await expect(panel.locator('.llm-help-privacy')).toContainText(/no data rows/)
  // The assertions below read the first part only, and which column lands
  // there depends on the store's column order and the part size (the
  // sample data has well over 40 values), so ask for the largest part.
  const sizes = panel.locator('.llm-help-chunk-size option')
  await panel.locator('.llm-help-chunk-size').selectOption(await sizes.last().getAttribute('value'))
  await panel.locator('.llm-help-generate').click()
  await expect(panel.locator('.llm-help-summary')).toBeVisible({ timeout: 60_000 })
  // The values prompt is shown as soon as it is generated; the variables
  // prompt (names only) sits behind the toggle.
  const preview = panel.locator('.llm-help-preview').first()
  if (!(await preview.isVisible())) await panel.locator('.llm-help-preview-toggle').first().click()
  const promptText = await preview.textContent()
  return { panel, promptText }
}

async function importAnswer(panel, answer) {
  await panel.locator('.llm-help-answer').fill(answer)
  await panel.locator('.llm-help-import').click()
  await expect(panel.locator('.llm-help-import-result')).toBeVisible({ timeout: 30_000 })
  return panel.locator('.llm-help-import-result')
}

test.describe('LLM prompt export round trip', () => {
  test.setTimeout(240_000)

  test('variables: copy prompt, paste answer, accept the imported suggestion', async ({
    page,
    request,
  }) => {
    const errors = watchConsoleErrors(page)
    const { enabled: tiersOn } = await suggestionsEnabled(request)
    await runIngestFlow(page)

    // The other-site map remembers every column by name, so tier 1 gives
    // each a 1.0 alias hit. The user's scenario: that suggestion is poor,
    // they dismiss it for clin_t and ask an LLM instead — the paste must
    // then take the field although the dismissed record scored higher.
    await uploadSemanticMapAndContinue(page, await otherSiteMapping())
    await expandAllDatabases(page)
    await dismissCoachmarkIfPresent(page)
    if (tiersOn) {
      const clinT = rowFor(page, 'clin_t')
      await expect(clinT.locator('.suggestion-badge.applied')).toBeVisible({ timeout: 30_000 })
      await clinT.locator('.suggestion-dismiss').click()
      await expect(clinT.locator('.suggestion-badge')).toHaveCount(0)
      await expect(clinT.locator('.description-select')).toHaveValue('')
    }

    const { panel, promptText } = await openAndGenerate(page, 'variables')
    await expect(panel.locator('.llm-help-summary')).toContainText(/columns still to map/)
    // The prompt: JSON-LD answer skeleton, the unmapped column, a hint from
    // tier 1 — and no values at all: the variables phase shares only the
    // column names.
    expect(promptText).toContain('"databases"')
    expect(promptText).toContain(`"${DB}"`)
    expect(promptText).toContain('- clin_t')
    // Hints come from the tier-1 job, so only a stack with tiers on has them.
    if (tiersOn) expect(promptText).toMatch(/hint: /)
    else expect(promptText).not.toMatch(/hint: /)
    expect(promptText).toMatch(/^- id( {4}hint: .*)?$/m)
    expect(promptText).not.toContain('male, female')
    expect(promptText).not.toContain('distinct')
    expect(promptText).not.toContain('P0001')
    expect(promptText).not.toContain('P0002')

    const answer = `Here is the mapping:\n\`\`\`json\n${JSON.stringify({
      databases: {
        [DB]: {
          tables: {
            data: {
              columns: {
                t_stage: {
                  mapsTo: 'schema:variable/t_stage',
                  localColumn: 'clin_t',
                  confidence: 0.9,
                  reason: 'clin_t is the clinical T category.',
                },
                n_stage: {
                  mapsTo: 'schema:variable/clinical_n_category',
                  localColumn: 'clin_n',
                  confidence: 0.6,
                  reason: 'Guessing a key that does not exist.',
                },
              },
            },
          },
        },
      },
    }, null, 2)}\n\`\`\`\nLet me know if you need anything else.`
    const result = await importAnswer(panel, answer)
    await expect(result).toContainText(/1 imported, 1 left for you to decide/)

    // The imported record shows as a pasted_llm pill on the clin_t row
    // (confident, so pre-filled for review) although the user dismissed
    // tier 1's suggestion there; accepting it reviews the field.
    const row = rowFor(page, 'clin_t')
    const badge = row.locator('.suggestion-badge').filter({ has: page.locator('.fa-clipboard') })
    await expect(badge).toBeVisible({ timeout: 30_000 })
    await expect(row.locator('.description-select')).toHaveValue('T stage')
    // The first pre-filled pill on the page pops the first-visit cue.
    await dismissCoachmarkIfPresent(page)
    await badge.locator('.suggestion-accept').click()
    await expect(row.locator('.suggestion-badge.confirmed')).toBeVisible()
    await expect(row.locator('.description-select')).toHaveValue('T stage')

    // The invalid key left clin_n with the human: no pasted pill there.
    const nRow = rowFor(page, 'clin_n')
    await expect(nRow.locator('.suggestion-badge').filter({ has: page.locator('.fa-clipboard') })).toHaveCount(0)

    expect(errors, 'JS errors during the variables round trip').toEqual([])
  })

  test('values: copy prompt, paste answer, accept the imported suggestion', async ({
    page,
    request,
  }) => {
    const errors = watchConsoleErrors(page)
    const { enabled: tiersOn } = await suggestionsEnabled(request)
    await runIngestFlow(page)

    // The example map of this very database, with its value mappings
    // removed: every column is mapped (so the values page has work) but no
    // value is, and 'event_overall_survival' codes 0/1 have no string match
    // among the terms Dead/Alive, so tier 1 abstains on them.
    const mapping = JSON.parse(await fs.readFile(DEFAULT_MAPPING_JSONLD, 'utf8'))
    for (const db of Object.values(mapping.databases)) {
      for (const table of Object.values(db.tables)) {
        for (const column of Object.values(table.columns)) delete column.localMappings
      }
    }
    await uploadSemanticMapAndContinue(page, mapping)

    // Every column of the ingested database is already described by the
    // map. Tier 1 may still pre-fill columns of databases the local store
    // kept from earlier runs (alias memory from this map); clear those so
    // the review gate opens, then submit to the details page.
    if (tiersOn) {
      await expect(page.locator('.suggestion-status-bar')).toBeVisible({ timeout: 30_000 })
      await expandAllDatabases(page)
      await dismissCoachmarkIfPresent(page)
      const clearAll = page.locator('.suggestion-clear-all')
      if (await clearAll.isVisible({ timeout: 5_000 }).catch(() => false)) {
        await clearAll.click()
      }
    }
    const submit = page.getByRole('button', { name: /^Submit$/ })
    await expect(submit).toBeEnabled({ timeout: 60_000 })
    await Promise.all([
      page.waitForURL(/\/describe\/variable-details(?:[?#].*)?$/, { timeout: 120_000 }),
      submit.click(),
    ])
    await expect(page.locator('h1').first()).toContainText(/Describe categories and units/)
    await expandAllDatabases(page)
    await dismissCoachmarkIfPresent(page)

    const { panel, promptText } = await openAndGenerate(page, 'values')
    await expect(panel.locator('.llm-help-summary')).toContainText(/values still to map/)
    expect(promptText).toContain('"localMappings"')
    expect(promptText).toContain('Column "event_overall_survival" → variable survival_status')
    expect(promptText).toMatch(/terms: Dead \(/)
    expect(promptText).toContain('- "1"')
    expect(promptText).not.toContain('P0001')

    const answer = JSON.stringify({
      databases: {
        [DB]: {
          tables: {
            data: {
              columns: {
                survival_status: {
                  mapsTo: 'schema:variable/survival_status',
                  localColumn: 'event_overall_survival',
                  localMappings: { Dead: ['1'], Alive: ['0'], Unknown: ['9'] },
                  valueNotes: { '1': { confidence: 0.9, reason: '1 = event occurred (death).' } },
                },
              },
            },
          },
        },
      },
    })
    const result = await importAnswer(panel, answer)
    // '9' is not a value of the column: ignored; Dead/Alive imported.
    await expect(result).toContainText(/2 imported/)
    await expect(result).toContainText(/1 ignored/)

    await expandAllVariables(page)
    const pasted = page.locator('.suggestion-badge').filter({ has: page.locator('.fa-clipboard') })
    await expect(pasted.first()).toBeVisible({ timeout: 30_000 })
    await expect(pasted).toHaveCount(2)
    await dismissCoachmarkIfPresent(page)
    await pasted.first().locator('.suggestion-accept').click()
    await expect(page.locator('.suggestion-badge.confirmed').first()).toBeVisible()

    expect(errors, 'JS errors during the values round trip').toEqual([])
  })
})
