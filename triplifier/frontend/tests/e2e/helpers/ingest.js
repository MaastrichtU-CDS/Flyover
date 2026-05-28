import { expect } from '@playwright/test'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

export const DEFAULT_CSV = path.resolve(
  __dirname,
  '../../../../../example_data/centre_a_english/synthetic_dutch_150.csv'
)

export const DEFAULT_MAPPING_JSONLD = path.resolve(
  __dirname,
  '../../../../../example_data/centre_a_english/mapping_centre_a.jsonld'
)

/**
 * Drive the SPA through a complete CSV ingest. Leaves the page on /app/describe
 * with a populated GraphDB. Triplifier can take 30-60s+ on a cold start.
 */
export async function runIngestFlow(page, csvPath = DEFAULT_CSV) {
  await page.goto('/app/ingest')
  await page.locator('#CSV').check()
  await page.locator('#csvFile').setInputFiles(csvPath)
  await expect(page.locator('#csvPath')).toHaveValue(new RegExp(path.basename(csvPath).replace('.', '\\.')))

  const submit = page.getByRole('button', { name: /^\s*(?:Processing\.\.\.)?\s*Submit Files\s*$/i })
  await expect(submit).toBeEnabled()
  await Promise.all([
    page.waitForURL(/\/app\/describe(?:[?#].*)?$/, { timeout: 150_000 }),
    submit.click(),
  ])
}

/**
 * Attach handlers that record any unexpected JS errors / console errors. Pass
 * the returned array to `expect(errors).toEqual([])` at the end of the test.
 * Network 4xx/5xx "Failed to load resource" entries are intentionally ignored.
 */
export function watchConsoleErrors(page) {
  const errors = []
  page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`))
  page.on('console', (msg) => {
    if (msg.type() !== 'error') return
    const text = msg.text()
    if (/Failed to load resource/.test(text)) return
    errors.push(text)
  })
  return errors
}
