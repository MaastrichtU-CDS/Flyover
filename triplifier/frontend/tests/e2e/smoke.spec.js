import { test, expect } from '@playwright/test'

// These tests assume `docker compose up` is running. They drive the SPA
// against the real Flask + GraphDB stack.

const ROUTES = [
  { path: '/app/', heading: /Welcome to Flyover/ },
  { path: '/app/ingest', heading: /Ingest your data/ },
  { path: '/app/describe', heading: /(Data submission finalised|No Data Found)/ },
  { path: '/app/describe/variables', heading: /Describe your data/ },
  { path: '/app/describe/variable-details', heading: /Describe categories and units/ },
  { path: '/app/annotate', heading: /Semantic Annotation/ },
  { path: '/app/annotate/review', heading: /Review Annotation Data/ },
  { path: '/app/annotate/verify', heading: /Annotation Verification/ },
  { path: '/app/share', heading: /Share/ },
  { path: '/app/share/mock', heading: /Generate Mock Data/ },
  { path: '/app/share/publish', heading: /Publish Your Data/ },
]

test.describe('SPA routes render', () => {
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

test('legacy URLs redirect into the SPA', async ({ page }) => {
  const cases = [
    { from: '/', to: '/app/' },
    { from: '/ingest', to: '/app/ingest' },
    { from: '/describe_landing', to: '/app/describe' },
    { from: '/annotation-review', to: '/app/annotate/review' },
    { from: '/share_publish', to: '/app/share/publish' },
  ]
  for (const { from, to } of cases) {
    await page.goto(from)
    await expect(page).toHaveURL(new RegExp(`${to}$`))
  }
})

test('navigation between SPA pages works without full reload', async ({ page }) => {
  await page.goto('/app/')
  await page.getByRole('link', { name: /Ingest/ }).first().click()
  await expect(page).toHaveURL(/\/app\/ingest$/)
  await expect(page.locator('h1').first()).toContainText(/Ingest your data/)
})
