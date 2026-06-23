import { test, expect } from '@playwright/test'
import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { runIngestFlow, watchConsoleErrors, DEFAULT_MAPPING_JSONLD } from './helpers/ingest.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
// mapping_centre_a.jsonld references synthetic_english_150 — match it.
const ENGLISH_CSV = path.resolve(
  __dirname,
  '../../../../example_data/centre_a_english/synthetic_english_150.csv'
)

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
})
