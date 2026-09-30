import { expect, test } from '@playwright/test'
import fs from 'node:fs/promises'
import { DEFAULT_MAPPING_JSONLD, runIngestFlow } from './ingest.js'

/**
 * Whether the running stack has mapping suggestions on, straight from the
 * backend. The suggestion specs use it to skip the flows that do not apply
 * to the stack they are pointed at (see `skipUnlessSuggestions`).
 */
export async function suggestionsEnabled(request) {
  const response = await request.get('/api/v1/suggestions/status')
  expect(response.ok(), `/api/v1/suggestions/status answered ${response.status()}`).toBe(true)
  const status = await response.json()
  return { enabled: status.enabled !== false, status }
}

/**
 * Skip the current test unless the stack's suggestion feature is in the
 * wanted state. With `E2E_SUGGESTIONS_DISABLED=1` set (the CI job that
 * starts the stack with `FLYOVER_SUGGESTION_TIERS=` empty) a stack that
 * still reports the feature enabled is a failure, not a skip: the whole
 * point of that job is that the empty flag reaches the container.
 */
export async function skipUnlessSuggestions(request, wanted) {
  const { enabled } = await suggestionsEnabled(request)
  if (wanted === false && process.env.E2E_SUGGESTIONS_DISABLED) {
    expect(
      enabled,
      'E2E_SUGGESTIONS_DISABLED is set but the stack reports suggestions enabled: ' +
        'was it started with FLYOVER_SUGGESTION_TIERS= (empty)?',
    ).toBe(false)
  }
  test.skip(
    enabled !== wanted,
    wanted
      ? 'the stack runs with suggestions disabled; start it with FLYOVER_SUGGESTION_TIERS=1'
      : 'the stack runs with suggestions enabled; start it with FLYOVER_SUGGESTION_TIERS= (empty) to run this spec',
  )
}

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

/**
 * Open every collapsed database section on the variables page. The
 * sections render once the describe state has loaded, so wait for the
 * first toggle before counting, and keep clicking the first remaining
 * "Show more" until none is left (each click flips one into "Show less").
 */
export async function expandAllDatabases(page) {
  await expect(page.locator('.toggle-button').first()).toBeVisible({ timeout: 30_000 })
  const collapsed = page.locator('.toggle-button', { hasText: /Show more/ })
  for (let i = 0; i < 50 && (await collapsed.count()) > 0; i++) {
    await collapsed.first().click()
  }
  await expect(collapsed).toHaveCount(0)
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

/** Collect every request the page makes to the suggestions API. */
export function watchSuggestionRequests(page) {
  const urls = []
  page.on('request', (req) => {
    if (/\/api\/v1\/suggestions\//.test(req.url())) urls.push(req.url())
  })
  return urls
}

/**
 * The variables page with the feature off must look and behave exactly as
 * it did before suggestions existed: no status bar, pills, callout or
 * pre-fill, no suggestions request beyond `/status`, and only the old "at
 * least one description" rule between the user and Submit. Shared by the
 * mocked-status flow and the real empty-flag run.
 */
export async function expectDescribeWithoutSuggestions(page, { errors, suggestionRequests }) {
  await expandAllDatabases(page)
  const firstSelect = page.locator('.description-select').first()
  await expect(firstSelect).toBeVisible()

  await page.waitForTimeout(1_500)
  await expect(page.locator('.suggestion-status-bar')).toHaveCount(0)
  await expect(page.locator('.suggestion-badge')).toHaveCount(0)
  await expect(page.locator('.suggestion-coachmark')).toHaveCount(0)
  await expect(page.locator('.suggestion-highlight')).toHaveCount(0)
  await expect(firstSelect).toHaveValue('')
  expect(suggestionRequests.filter((u) => !/\/status(?:\?|$)/.test(u))).toEqual([])

  const submit = page.getByRole('button', { name: /^Submit$/ })
  await expect(submit).toBeDisabled()
  await firstSelect.selectOption({ index: 1 })
  await expect(submit).toBeEnabled()

  expect(errors, 'JS errors with suggestions disabled').toEqual([])
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
