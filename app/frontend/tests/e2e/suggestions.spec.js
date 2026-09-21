import { test, expect } from '@playwright/test'
import { runIngestFlow, watchConsoleErrors } from './helpers/ingest.js'

// ---------------------------------------------------------------------------
// E2E tests for mapping suggestions on the describe pages.
//
// These tests require `docker compose up` with FLYOVER_SUGGESTION_TIERS=1
// (the default). The example data (centre_a_english) has columns that match
// schema variables by exact string and abbreviation alias, so tier-1
// suggestions should arrive shortly after the describe page loads.
// ---------------------------------------------------------------------------

test.describe('Suggestions on describe pages', () => {
  test.setTimeout(180_000)

  test('suggestion badges appear and can be accepted and dismissed', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await runIngestFlow(page)

    // Navigate to the variables describe page.
    await page.getByRole('button', { name: /^\s*Skip\s*$/i }).click()
    await page.getByRole('button', { name: /Click here to describe the data/i }).click()
    await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })

    // Expand the first database section if collapsed.
    const dbToggle = page.locator('.database-header, .card-header').first()
    if (await dbToggle.isVisible()) {
      await dbToggle.click().catch(() => {})
    }

    // Wait for at least one suggestion badge to appear (tier-1 runs on the
    // host and should produce results for the example columns).
    const badge = page.locator('.suggestion-badge:not(.confirmed):not(.applied)').first()
    await expect(badge).toBeVisible({ timeout: 30_000 })

    // --- Accept a suggestion ------------------------------------------------
    // Clicking the badge body (not the dismiss ×) triggers accept.
    await badge.click()

    // The badge should now show the "applied" state.
    await expect(page.locator('.suggestion-badge.applied').first()).toBeVisible()

    // The corresponding description dropdown should have a value selected.
    const selectedDesc = page.locator('.description-select').first()
    await expect(selectedDesc).not.toHaveValue('')

    // --- Dismiss a different suggestion ------------------------------------
    // Find another non-applied badge with a dismiss button.
    const dismissBadge = page.locator('.suggestion-badge:not(.applied):not(.confirmed) .suggestion-dismiss').first()
    if (await dismissBadge.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const badgeCountBefore = await page.locator('.suggestion-badge:not(.confirmed)').count()
      await dismissBadge.click()
      // The dismissed badge should disappear (count of non-confirmed badges drops).
      await expect(page.locator('.suggestion-badge:not(.confirmed)')).toHaveCount(
        Math.max(0, badgeCountBefore - 1),
      )
    }

    expect(errors, 'JS errors during suggestions flow').toEqual([])
  })

  test('describe page works without errors when suggestions are idle', async ({ page }) => {
    // Even if suggestions are enabled, the describe page should render
    // without console errors before any suggestions arrive. This covers
    // the "suggestions disabled / not yet arrived" case.
    const errors = watchConsoleErrors(page)
    await runIngestFlow(page)

    await page.getByRole('button', { name: /^\s*Skip\s*$/i }).click()
    await page.getByRole('button', { name: /Click here to describe the data/i }).click()
    await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })
    await expect(page.locator('h1').first()).toContainText(/Describe your data/)

    expect(errors).toEqual([])
  })
})
