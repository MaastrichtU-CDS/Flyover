import { test, expect } from '@playwright/test'
import { watchConsoleErrors } from './helpers/ingest.js'

// Share-publish is mostly an informational landing page (codebook + DCAT-AP).
// It has no required backend dependencies, so we just verify it renders without
// console errors.
test.describe('Share-publish flow', () => {
  test('renders the publish landing page', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await page.goto('/share/publish')
    await expect(page.locator('h1').first()).toContainText(/Publish/i)
    expect(errors, 'JS errors on share-publish page').toEqual([])
  })
})
