import { test, expect } from '@playwright/test'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

// This test posts a real CSV through /upload, which runs the triplifier
// and then redirects to /describe on success. It mutates the running
// GraphDB — after running, the graph holds an extra dataset until you
// recreate the rdf-store container or wipe ./graphdb/data/.

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const FIXTURE_CSV = path.resolve(
  __dirname,
  '../../../../example_data/centre_a_english/synthetic_english_150.csv'
)

test.describe('Ingest flow', () => {
  // Triplifier can take 30–60s+ on the first run.
  test.setTimeout(180_000)

  test('uploads a CSV and lands on the describe page', async ({ page }) => {
    const errors = []
    page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`))
    page.on('console', (msg) => {
      if (msg.type() !== 'error') return
      const text = msg.text()
      if (/Failed to load resource/.test(text)) return
      errors.push(text)
    })

    await page.goto('/ingest')

    // Pick CSV as the data source and select the fixture file.
    await page.locator('#CSV').check()
    await page.locator('#csvFile').setInputFiles(FIXTURE_CSV)

    // The visible path field should reflect the chosen filename.
    await expect(page.locator('#csvPath')).toHaveValue(/synthetic_dutch_150\.csv/)

    // The submit button enables once a CSV is selected.
    const submit = page.getByRole('button', { name: /^\s*(?:Processing\.\.\.)?\s*Submit Files\s*$/i })
    await expect(submit).toBeEnabled()

    // Submit and wait for the post-triplifier redirect into the SPA.
    await Promise.all([
      page.waitForURL(/\/describe(?:[?#].*)?$/, { timeout: 150_000 }),
      submit.click(),
    ])

    // Describe landing renders one of two h1s depending on data state.
    await expect(page.locator('h1').first()).toContainText(/Data submission finalised|Describe/)

    expect(errors, 'JS errors during ingest flow').toEqual([])
  })
})
