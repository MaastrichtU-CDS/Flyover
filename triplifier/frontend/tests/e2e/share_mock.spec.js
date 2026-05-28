import { test, expect } from '@playwright/test'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { watchConsoleErrors, DEFAULT_MAPPING_JSONLD } from './helpers/ingest.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// Share-mock generates synthetic data from a JSON-LD mapping. The page is
// self-contained (no GraphDB or ingested data needed) — uploading the mapping
// unlocks the generation controls.
test.describe('Share-mock flow', () => {
  test.setTimeout(60_000)

  test('uploading a mapping reveals generation controls', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await page.goto('/app/share/mock')
    await expect(page.locator('h1').first()).toContainText(/Generate Mock Data/)

    await page.locator('input[type="file"]').first().setInputFiles(DEFAULT_MAPPING_JSONLD)
    await page.getByRole('button', { name: /Upload JSON-LD/i }).click()

    await expect(page.locator('.alert-success')).toContainText(
      /JSON-LD semantic map loaded successfully/i,
      { timeout: 15_000 }
    )
    await expect(page.locator('#sampleCount')).toBeVisible()

    expect(errors, 'JS errors during share-mock flow').toEqual([])
  })

  test('renders without crashing when no mapping is uploaded', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    await page.goto('/app/share/mock')
    await expect(page.locator('h1').first()).toBeVisible()
    // Generation controls remain hidden without a mapping.
    await expect(page.locator('#sampleCount')).toHaveCount(0)
    expect(errors).toEqual([])
  })
})
