import { test, expect } from '@playwright/test'
import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { runIngestFlow, watchConsoleErrors, DEFAULT_MAPPING_JSONLD } from './helpers/ingest.js'
import {
  disableSuggestionsViaStatus,
  expandAllDatabases,
  otherSiteMapping,
  readMetadata,
  uploadSemanticMapAndContinue,
} from './helpers/suggestions.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
// mapping_centre_a.jsonld references synthetic_english_150 — match it.
const ENGLISH_CSV = path.resolve(
  __dirname,
  '../../../../example_data/centre_a_english/synthetic_english_150.csv'
)
const DUTCH_CSV = path.resolve(
  __dirname,
  '../../../../example_data/centre_b_dutch/synthetic_dutch_150.csv'
)

// Pick a description on the variables page, paging through the table (ten
// columns a page) to the column first.
async function describeColumn(page, table, column, label) {
  const heading = page.getByRole('heading', { level: 2, name: table, exact: true })
  const section = heading.locator('xpath=..')
  const select = page.locator(`[id="ncit_comment_${table}_${column}"]`)
  const previous = section.locator('button.prev-btn')
  while ((await previous.count()) && (await previous.isEnabled())) await previous.click()
  const next = section.locator('button.next-btn')
  for (let i = 0; i < 20 && !(await select.isVisible()); i++) {
    if (!(await next.count()) || !(await next.isEnabled())) break
    await next.click()
  }
  await select.selectOption({ label })
}

// Seed IndexedDB directly with a semantic map so the annotate review page has
// something to consume. Going through the AnnotationLandingView upload form
// would also work but adds churn that's not what we're verifying here.
async function seedSemanticMap(page, mapping) {
  // Visit the SPA first so the IndexedDB origin matches.
  await page.goto('/')
  await page.evaluate(async (data) => {
    const dbi = await new Promise((resolve, reject) => {
      const req = indexedDB.open('FlyoverDB', 2)
      req.onupgradeneeded = () => {
        const d = req.result
        if (!d.objectStoreNames.contains('metadata')) {
          d.createObjectStore('metadata', { keyPath: 'key' })
        }
      }
      req.onsuccess = () => resolve(req.result)
      req.onerror = () => reject(req.error)
    })
    await new Promise((resolve, reject) => {
      const tx = dbi.transaction('metadata', 'readwrite')
      tx.objectStore('metadata').put({
        key: 'semantic_map',
        data,
        timestamp: new Date().toISOString(),
      })
      tx.oncomplete = resolve
      tx.onerror = () => reject(tx.error)
    })
  }, mapping)
}

test.describe('Annotate flow', () => {
  test.setTimeout(240_000)

  test('annotate landing page renders without console errors', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await page.goto('/annotate')
    await expect(page.locator('h1').first()).toContainText(/Semantic Annotation/)
    expect(errors).toEqual([])
  })

  test('review page renders the annotation table when IndexedDB has a semantic map', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await runIngestFlow(page, ENGLISH_CSV)

    const mapping = JSON.parse(await fs.readFile(DEFAULT_MAPPING_JSONLD, 'utf8'))
    await seedSemanticMap(page, mapping)

    await page.goto('/annotate/review')
    await expect(page.locator('h1').first()).toContainText(/Review Annotation Data/)

    // Either a "Start Annotation" button or a "No semantic map" warning will
    // appear. We assert the happy path (button shows up) since IndexedDB is
    // seeded and a CSV was ingested.
    const startBtn = page.getByRole('button', { name: /Start Annotation/i })
    await expect(startBtn).toBeVisible({ timeout: 15_000 })

    expect(errors, 'JS errors during annotate review render').toEqual([])
  })

  test('review page surfaces the no-data warning when /start-annotation fails', async ({ page }) => {
    await page.route('**/start-annotation', (route) =>
      route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ success: false, error: 'forced failure' }),
      })
    )
    await page.addInitScript(() => {
      window.alert = () => {}
    })

    await runIngestFlow(page)
    const mapping = JSON.parse(await fs.readFile(DEFAULT_MAPPING_JSONLD, 'utf8'))
    await seedSemanticMap(page, mapping)
    await page.goto('/annotate/review')

    const startBtn = page.getByRole('button', { name: /Start Annotation/i })
    await expect(startBtn).toBeVisible({ timeout: 15_000 })
    await startBtn.click()

    // Wait long enough for the two-fetch sequence to attempt-and-fail.
    await page.waitForTimeout(3_000)
    // We should NOT have navigated to /annotate/verify.
    await expect(page).toHaveURL(/\/annotate\/review/)
  })
  // The describe pages used to save an answer whose variable key has capitals
  // under a lower-cased key the map does not have, so the review page left
  // it out (and a table whose answers were all of that kind), and a map table
  // without a name took in every table's answers.
  test('two described tables both reach the review page and are annotated', async ({ page }) => {
    await disableSuggestionsViaStatus(page)
    await runIngestFlow(page, [ENGLISH_CSV, DUTCH_CSV])

    // The example map as another site's (none of its tables is ingested
    // here) and without a database name, with T stage keyed 'T_stage'.
    const mapping = await otherSiteMapping()
    for (const db of Object.values(mapping.databases)) delete db.name
    mapping.schema.variables.T_stage = mapping.schema.variables.t_stage
    delete mapping.schema.variables.t_stage
    for (const db of Object.values(mapping.databases)) {
      for (const table of Object.values(db.tables)) {
        for (const column of Object.values(table.columns)) {
          if (column.mapsTo === 'schema:variable/t_stage') {
            column.mapsTo = 'schema:variable/T_stage'
          }
        }
      }
    }
    await uploadSemanticMapAndContinue(page, mapping)
    await expandAllDatabases(page)

    // The Dutch table's one answer has a capitalised key, like a table of
    // EORTC scale scores in the AYA map.
    const answers = {
      synthetic_english_150: { biological_sex: 'Biological sex', clin_t: 'T stage' },
      synthetic_dutch_150: { t_stad: 'T stage' },
    }
    for (const [table, columns] of Object.entries(answers)) {
      for (const [column, label] of Object.entries(columns)) {
        await describeColumn(page, table, column, label)
      }
    }

    const toDetails = page.locator('form[action="/units"] button[type="submit"]')
    await expect(toDetails).toBeEnabled()
    await Promise.all([page.waitForURL(/\/describe\/variable-details/), toDetails.click()])
    const toReview = page.locator('form[action="/end"] button[type="submit"]')
    await expect(toReview).toBeEnabled({ timeout: 30_000 })
    await Promise.all([page.waitForURL(/\/annotate\/review/), toReview.click()])

    for (const table of Object.keys(answers)) {
      await expect(
        page.getByRole('heading', { level: 2, name: table, exact: true })
      ).toBeVisible({ timeout: 30_000 })
    }

    // Each table's answers sit in that table's own entry, under the schema's
    // own keys.
    const stored = (await readMetadata(page, 'semantic_map')).data
    const described = {}
    for (const db of Object.values(stored.databases)) {
      for (const [key, table] of Object.entries(db.tables)) {
        const name = table.sourceFile || key
        if (!(name in answers)) continue
        described[name] = Object.fromEntries(
          Object.values(table.columns).map((c) => [c.localColumn, c.mapsTo])
        )
      }
    }
    expect(described).toEqual({
      synthetic_english_150: {
        biological_sex: 'schema:variable/biological_sex',
        clin_t: 'schema:variable/T_stage',
      },
      synthetic_dutch_150: { t_stad: 'schema:variable/T_stage' },
    })

    await page.getByRole('button', { name: /Start Annotation/i }).click()
    await page.waitForURL(/\/annotate\/verify/, { timeout: 180_000 })
    const state = await (await page.request.get('/api/v1/annotation-verify-state')).json()
    const annotated = Object.entries(state.annotation_status)
      .filter(([, status]) => status?.success)
      .map(([key]) => key)
      .sort()
    expect(annotated).toEqual([
      'synthetic_dutch_150.T_stage',
      'synthetic_english_150.T_stage',
      'synthetic_english_150.biological_sex',
    ])
  })
})
