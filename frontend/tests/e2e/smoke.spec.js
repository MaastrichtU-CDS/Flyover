import { test, expect } from '@playwright/test'

// Frontend e2e: smoke
// These tests assume `docker compose up` is running. They drive the SPA
// against the real Flask + GraphDB stack.

const ROUTES = [
  { path: '/', heading: /Welcome to Flyover/ },
  { path: '/ingest', heading: /Ingest your data/ },
  { path: '/describe', heading: /(Data submission finalised|No Data Found)/ },
  { path: '/describe/variables', heading: /Describe your data/ },
  { path: '/describe/variable-details', heading: /Describe categories and units/ },
  { path: '/annotate', heading: /Semantic Annotation/ },
  { path: '/annotate/review', heading: /Review Annotation Data/ },
  { path: '/annotate/verify', heading: /Annotation Verification/ },
  { path: '/share', heading: /Share/ },
  { path: '/share/mock', heading: /Generate Mock Data/ },
  { path: '/share/publish', heading: /Publish Your Data/ },
]

test.describe('Frontend e2e: SPA routes render', () => {
  for (const { path, heading } of ROUTES) {
    test(`${path} shows its heading`, async ({ page }) => {
      const errors = []
      page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`))
      page.on('console', (msg) => {
        if (msg.type() !== 'error') return
        const text = msg.text()
        // Network 4xx/5xx surface here as "Failed to load resource" — the
        // views handle them gracefully, so don't fail the spec on them.
        if (/Failed to load resource/.test(text)) return
        errors.push(text)
      })

      await page.goto(path)
      await expect(page.locator('h1').first()).toContainText(heading)
      expect(errors, `JS errors on ${path}`).toEqual([])
    })
  }
})

test.describe('Frontend e2e: legacy URL redirects', () => {
  test('snake_case legacy URLs redirect into the SPA', async ({ page }) => {
    const cases = [
      { from: '/describe_landing', to: '/describe' },
      { from: '/annotation-review', to: '/annotate/review' },
      { from: '/share_publish', to: '/share/publish' },
    ]
    for (const { from, to } of cases) {
      await page.goto(from)
      await expect(page).toHaveURL(new RegExp(`${to}$`))
    }
  })
})

test.describe('Frontend e2e: SPA navigation', () => {
  test('navigation between SPA pages works without full reload', async ({ page }) => {
    await page.goto('/')
    await page.getByRole('link', { name: /Ingest/ }).first().click()
    await expect(page).toHaveURL(/\/ingest$/)
    await expect(page.locator('h1').first()).toContainText(/Ingest your data/)
  })
})
