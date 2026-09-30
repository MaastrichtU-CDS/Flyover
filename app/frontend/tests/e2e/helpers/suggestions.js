import { expect } from '@playwright/test'
import fs from 'node:fs/promises'
import { DEFAULT_MAPPING_JSONLD, runIngestFlow } from './ingest.js'

/**
 * The example semantic map, re-labelled as if it came from another site.
 *
 * Tier-1 suggestions come from alias memory: columns that *other* databases
 * in the loaded map already mapped. The example map describes the very
 * database the e2e flow ingests (its table's `sourceFile` matches the CSV
 * name), so uploaded as-is it pre-selects every dropdown and the page shows
 * "already filled in" pills instead of suggestions. Renaming the database
 * and table turns it into a remembered site whose column names match the
 * ingested CSV exactly, which gives deterministic 100% alias suggestions.
 */
export async function otherSiteMapping() {
  const mapping = JSON.parse(await fs.readFile(DEFAULT_MAPPING_JSONLD, 'utf8'))
  const databases = {}
  for (const [dbName, db] of Object.entries(mapping.databases || {})) {
    const tables = {}
    for (const [tableName, table] of Object.entries(db.tables || {})) {
      tables[`${tableName}_other_site`] = {
        ...table,
        '@id': `mapping:table/${tableName}_other_site`,
        sourceFile: `${table.sourceFile || tableName}_other_site`,
      }
    }
    databases[`${dbName}_other_site`] = {
      ...db,
      '@id': `mapping:database/${dbName}_other_site`,
      tables,
    }
  }
  return { ...mapping, databases }
}

/**
 * On the describe landing page (where `runIngestFlow` leaves the browser),
 * upload a semantic map and continue to the variables page.
 */
export async function uploadSemanticMapAndContinue(page, mapping) {
  await page.locator('input[type="file"]').first().setInputFiles({
    name: 'mapping_other_site.jsonld',
    mimeType: 'application/ld+json',
    buffer: Buffer.from(JSON.stringify(mapping)),
  })
  await page.getByRole('button', { name: /Upload Semantic Map/i }).click()

  const continueButton = page.getByRole('button', { name: /Click here to describe the data/i })
  await expect(continueButton).toBeEnabled({ timeout: 30_000 })
  await continueButton.click()
  await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })
}

/**
 * Ingest the example CSV, upload the other-site map, and land on the
 * variables page with tier-1 suggestions on their way.
 */
export async function goToDescribeVariablesWithSuggestions(page) {
  await runIngestFlow(page)
  await uploadSemanticMapAndContinue(page, await otherSiteMapping())
  await expect(page.locator('h1').first()).toContainText(/Describe your data/)
}

/** Open every collapsed database section on the variables page. */
export async function expandAllDatabases(page) {
  const toggles = page.locator('.toggle-button', { hasText: /Show more/ })
  const count = await toggles.count()
  for (let i = 0; i < count; i++) {
    // Each click flips one "Show more" into "Show less", so always take
    // the first remaining one.
    await page.locator('.toggle-button', { hasText: /Show more/ }).first().click()
  }
}

/**
 * Make the backend look as if FLYOVER_SUGGESTION_TIERS= (empty) had been
 * set, without restarting the stack: `/status` is the only call the store
 * makes before deciding whether suggestions are enabled, and it stops there
 * when the answer is `enabled: false`.
 */
export async function disableSuggestionsViaStatus(page) {
  const reason = 'disabled by FLYOVER_SUGGESTION_TIERS'
  await page.route('**/api/v1/suggestions/status', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        enabled: false,
        compute: 'host',
        threshold: 0.8,
        rules_version: null,
        tiers: {
          1: { state: 'inactive', reason },
          2: { state: 'inactive', reason },
          3: { state: 'inactive', reason },
        },
      }),
    }),
  )
}

/**
 * Read one record of the SPA's IndexedDB `metadata` store (FlyoverDB), where
 * the suggestion marks and the coachmark seen-flag live. Writes are
 * asynchronous, so a flow polls this before reloading instead of assuming
 * the click that triggered the write has landed.
 */
export async function readMetadata(page, key) {
  return page.evaluate(
    (k) =>
      new Promise((resolve, reject) => {
        const req = indexedDB.open('FlyoverDB')
        req.onerror = () => reject(req.error)
        req.onsuccess = () => {
          const db = req.result
          try {
            const get = db.transaction(['metadata'], 'readonly').objectStore('metadata').get(k)
            get.onsuccess = () => {
              db.close()
              resolve(get.result || null)
            }
            get.onerror = () => {
              db.close()
              reject(get.error)
            }
          } catch (e) {
            db.close()
            reject(e)
          }
        }
      }),
    key,
  )
}

/**
 * Dismiss the first-visit suggestion coachmark (WS2) when it is showing, so
 * the rest of an e2e flow can interact with badges and dropdowns freely.
 * The callout is non-modal, but hiding it keeps later visibility
 * assertions on pills unambiguous.
 */
export async function dismissCoachmarkIfPresent(page, { timeout = 2_000 } = {}) {
  const callout = page.locator('.suggestion-coachmark')
  const visible = await callout
    .first()
    .isVisible({ timeout })
    .catch(() => false)
  if (!visible) return false
  await callout.first().getByRole('button', { name: 'Got it' }).click()
  await expect(callout).toHaveCount(0)
  return true
}
