import { expect } from '@playwright/test'

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
