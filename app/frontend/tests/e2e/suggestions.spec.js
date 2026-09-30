import { test, expect } from '@playwright/test'
import { watchConsoleErrors } from './helpers/ingest.js'
import {
  disableSuggestionsViaStatus,
  dismissCoachmarkIfPresent,
  expandAllDatabases,
  goToDescribeVariablesWithSuggestions,
  readMetadata,
} from './helpers/suggestions.js'

// ---------------------------------------------------------------------------
// E2E tests for mapping suggestions on the describe pages.
//
// These tests require `docker compose up` with FLYOVER_SUGGESTION_TIERS=1
// (the default). Suggestions need a semantic map in the browser, so each flow
// uploads the example map re-labelled as another site (see
// helpers/suggestions.js): its columns match the ingested CSV by name, which
// gives every column a confident alias suggestion that the variables page
// pre-fills for review.
//
// Pill states (SuggestionBadge): `.applied` = pre-filled, awaiting review;
// `.confirmed` = reviewed (accepted); a dismissed suggestion has no pill.
//
// The disabled flow does not restart the stack with FLYOVER_SUGGESTION_TIERS=
// (empty): it answers the `/status` call the way the backend does when the
// flag is empty, which is the only input the store uses for that decision.
// The backend side of the flag is covered by the controller unit tests.
// ---------------------------------------------------------------------------

/** The `.variable-row` that holds the given description `<select>` id. */
function rowForSelectId(page, id) {
  return page.locator('.variable-row').filter({ has: page.locator(`[id="${id}"]`) })
}

test.describe('Suggestions on describe pages', () => {
  test.setTimeout(180_000)

  test('pre-filled suggestions can be accepted and dismissed, and the review survives a reload', async ({
    page,
  }) => {
    const errors = watchConsoleErrors(page)
    await goToDescribeVariablesWithSuggestions(page)

    // The status bar reports the job; pills arrive once tier 1 has run.
    await expect(page.locator('.suggestion-status-bar')).toBeVisible({ timeout: 30_000 })
    await expandAllDatabases(page)
    const applied = page.locator('.suggestion-badge.applied')
    await expect(applied.first()).toBeVisible({ timeout: 30_000 })
    await expect(applied.nth(1)).toBeVisible({ timeout: 30_000 })
    await dismissCoachmarkIfPresent(page)

    // --- Accept ----------------------------------------------------------
    // Pin rows by their select id: a row filtered on "has an applied pill"
    // would drift to the next row as soon as this one is reviewed.
    const appliedRows = page.locator('.variable-row').filter({ has: page.locator('.suggestion-badge.applied') })
    const acceptId = await appliedRows.first().locator('.description-select').getAttribute('id')
    const acceptRow = rowForSelectId(page, acceptId)
    const acceptSelect = acceptRow.locator('.description-select')
    // A pre-filled field already shows the suggested variable (D1: display
    // only, persisted when reviewed).
    const suggestedValue = await acceptSelect.inputValue()
    expect(suggestedValue).not.toBe('')

    await acceptRow.locator('.suggestion-accept').click()
    await expect(acceptRow.locator('.suggestion-badge.confirmed')).toBeVisible()
    await expect(acceptRow.locator('.suggestion-badge.confirmed')).toContainText(/reviewed/i)
    await expect(acceptSelect).toHaveValue(suggestedValue)

    // --- Dismiss ---------------------------------------------------------
    // The accepted row's pill is `.confirmed` now, so the first row that
    // still holds an `.applied` pill is a different column.
    const dismissId = await appliedRows.first().locator('.description-select').getAttribute('id')
    expect(dismissId).not.toBe(acceptId)
    const dismissRow = rowForSelectId(page, dismissId)
    const dismissSelect = dismissRow.locator('.description-select')
    expect(await dismissSelect.inputValue()).not.toBe('')

    await dismissRow.locator('.suggestion-dismiss').click()
    // The pill disappears and the pre-filled value is cleared.
    await expect(dismissRow.locator('.suggestion-badge')).toHaveCount(0)
    await expect(dismissSelect).toHaveValue('')

    // --- Reload ----------------------------------------------------------
    // Marks live in IndexedDB (`suggestion_marks_variables`). Wait for the
    // write to land, then reload: the accepted field stays reviewed with
    // its value, the dismissed one stays empty with no pill, and a re-run
    // job does not bring the dismissed suggestion back.
    const acceptKey = acceptId.replace(/^ncit_comment_/, '')
    const dismissKey = dismissId.replace(/^ncit_comment_/, '')
    await expect
      .poll(async () => {
        const marks = await readMetadata(page, 'suggestion_marks_variables')
        return {
          touched: (marks?.touched || []).includes(acceptKey),
          dismissed: (marks?.dismissed || []).includes(dismissKey),
        }
      })
      .toEqual({ touched: true, dismissed: true })
    await page.reload()
    await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })
    await expandAllDatabases(page)
    await expect(page.locator('.suggestion-badge').first()).toBeVisible({ timeout: 30_000 })

    const acceptedAfter = rowForSelectId(page, acceptId)
    await expect(acceptedAfter.locator('.suggestion-badge.confirmed')).toBeVisible()
    await expect(acceptedAfter.locator('.description-select')).toHaveValue(suggestedValue)

    const dismissedAfter = rowForSelectId(page, dismissId)
    await expect(dismissedAfter.locator('.description-select')).toHaveValue('')
    await expect(dismissedAfter.locator('.suggestion-badge')).toHaveCount(0)

    expect(errors, 'JS errors during suggestions flow').toEqual([])
  })

  test('first-visit coachmark appears once, is dismissible, and stays gone after reload', async ({
    page,
  }) => {
    const errors = watchConsoleErrors(page)
    await goToDescribeVariablesWithSuggestions(page)

    // Nothing shows while every table is folded.
    const callout = page.locator('.suggestion-coachmark')
    await expect(page.locator('.suggestion-status-bar')).toBeVisible({ timeout: 30_000 })
    await page.waitForTimeout(1_000)
    await expect(callout).toHaveCount(0)

    // Opening a table pops the callout up on its first pre-filled pill.
    await page.locator('.toggle-button').first().click()
    await expect(callout.first()).toBeVisible({ timeout: 30_000 })
    await expect(callout.first()).toContainText(/Check this suggestion/)
    await expect(callout.first()).toContainText(/nothing is saved until you do/i)

    // Dismiss it with "Got it".
    await callout.first().getByRole('button', { name: 'Got it' }).click()
    await expect(callout).toHaveCount(0)

    // The seen flag is written to IndexedDB (`suggestion_coachmark_seen`)
    // asynchronously; wait for it before reloading.
    await expect
      .poll(async () => (await readMetadata(page, 'suggestion_coachmark_seen'))?.variables)
      .toBe(true)

    // Reload and open the table again: the seen flag persisted, so the
    // callout must not return.
    await page.reload()
    await page.waitForURL(/\/describe\/variables(?:[?#].*)?$/, { timeout: 30_000 })
    await page.locator('.toggle-button').first().click()
    await expect(page.locator('.suggestion-badge').first()).toBeVisible({ timeout: 30_000 })
    // The callout mounts ~300 ms after its target; give it a chance before
    // asserting it stays gone.
    await page.waitForTimeout(1_000)
    await expect(callout).toHaveCount(0)

    expect(errors, 'JS errors during coachmark flow').toEqual([])
  })

  test('with suggestions disabled the describe page renders as before, with no pills or gate', async ({
    page,
  }) => {
    const errors = watchConsoleErrors(page)
    const suggestionRequests = []
    page.on('request', (req) => {
      if (/\/api\/v1\/suggestions\//.test(req.url())) suggestionRequests.push(req.url())
    })
    await disableSuggestionsViaStatus(page)
    // Upload a map all the same, so the flag is the only reason nothing shows.
    await goToDescribeVariablesWithSuggestions(page)
    await expandAllDatabases(page)
    const firstSelect = page.locator('.description-select').first()
    await expect(firstSelect).toBeVisible()

    // Exactly as today: no status bar, no pills, no callout, no pre-fill,
    // and no job started.
    await page.waitForTimeout(1_500)
    await expect(page.locator('.suggestion-status-bar')).toHaveCount(0)
    await expect(page.locator('.suggestion-badge')).toHaveCount(0)
    await expect(page.locator('.suggestion-coachmark')).toHaveCount(0)
    await expect(page.locator('.suggestion-highlight')).toHaveCount(0)
    await expect(firstSelect).toHaveValue('')
    expect(suggestionRequests.filter((u) => !/\/status(?:\?|$)/.test(u))).toEqual([])

    // The review gate must not hold the form: the existing rule (at least
    // one description) is the only thing between the user and Submit.
    const submit = page.getByRole('button', { name: /^Submit$/ })
    await expect(submit).toBeDisabled()
    await firstSelect.selectOption({ index: 1 })
    await expect(submit).toBeEnabled()

    expect(errors, 'JS errors with suggestions disabled').toEqual([])
  })
})
