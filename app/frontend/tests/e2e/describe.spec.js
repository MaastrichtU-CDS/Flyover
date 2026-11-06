import { readFileSync } from 'node:fs'
import path from 'node:path'
import { test, expect } from '@playwright/test'
import { DEFAULT_CSV, runIngestFlow, watchConsoleErrors } from './helpers/ingest.js'

// Describe flow walks from the post-ingest landing into the variables form.
// Filling all per-column descriptions+datatypes to drive the form all the way
// to /describe/variable-details is beyond a single-shot spec — that's left as
// a deeper integration check. Here we verify the route plumbing and that the
// form's built-in validation keeps the user from submitting an empty mapping.
test.describe('Describe flow', () => {
  test.setTimeout(180_000)

  test('navigates from describe-landing to describe-variables and form requires input', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await runIngestFlow(page)

    await expect(page.locator('h1').first()).toContainText(/Data submission finalised|Describe/)

    await page.getByRole('button', { name: /^\s*Skip\s*$/i }).click()
    await page.getByRole('button', { name: /Click here to describe the data/i }).click()
    await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })
    await expect(page.locator('h1').first()).toContainText(/Describe your data/)

    // Submit is disabled until at least one column is described — this guards
    // against accidentally posting an empty mapping to /units.
    const submit = page.getByRole('button', { name: /^Submit$/ })
    await expect(submit).toBeDisabled()

    expect(errors, 'JS errors during describe flow').toEqual([])
  })

  test('lists the ingested variables in the order of the CSV header', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await runIngestFlow(page)
    await page.getByRole('button', { name: /^\s*Skip\s*$/i }).click()
    await page.getByRole('button', { name: /Click here to describe the data/i }).click()
    await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })

    // Each variable has a data type select named <table>_<column>. The RDF
    // store keeps no column order, so these follow the header only because
    // the ingest recorded it. Other tables in the store are ignored.
    const table = path.basename(DEFAULT_CSV, '.csv')
    const header = readFileSync(DEFAULT_CSV, 'utf8').split(/\r?\n/, 1)[0].split(',')
    const expected = header.map((column) => `${table}_${column}`)
    const listed = async () =>
      (
        await page
          .locator('select.datatype-select')
          .evaluateAll((selects) => selects.map((select) => select.id))
      ).filter((id) => expected.includes(id))
    await expect.poll(async () => (await listed()).length, { timeout: 30_000 }).toBeGreaterThan(1)

    // The page shows the first variables only; they must be the header's first.
    const ids = await listed()
    expect(ids).toEqual(expected.slice(0, ids.length))

    expect(errors, 'JS errors while listing the variables').toEqual([])
  })

  test('describe page handles no-data case gracefully', async ({ page }) => {
    // Without an ingest, the describe landing should warn rather than crash.
    // We use a fresh page; if the previous test ingested data the warning
    // won't show, but the page must still render without console errors.
    const errors = watchConsoleErrors(page)
    await page.goto('/describe')
    await expect(page.locator('h1').first()).toBeVisible()
    expect(errors).toEqual([])
  })
})
