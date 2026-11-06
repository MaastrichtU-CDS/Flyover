import { test, expect } from '@playwright/test'
import fs from 'node:fs/promises'
import { DEFAULT_MAPPING_JSONLD, runIngestFlow } from './helpers/ingest.js'

// Projects: the pop-up Flyover shows when it opens, the status bar above the
// steps, switching and leaving, and a step's map upload that belongs to a
// project. Unlike the other specs, this one answers the pop-up itself, so it
// uses Playwright's own `test` rather than the shared fixture.
//
// The Centre A example map names localhost's userRepo as its endpoint,
// which Flyover reads as its configured store: opening it keeps the stack's
// store, so the specs that follow find their data where they expect it.

const CENTRE_A = 'Centre A - English Hospital Mapping'
const OTHER = 'E2E other project mapping'

const picker = (page) => page.getByRole('dialog', { name: 'Choose a project' })
const statusBar = (page) => page.locator('#project-status-bar')

async function centreA() {
  return JSON.parse(await fs.readFile(DEFAULT_MAPPING_JSONLD, 'utf8'))
}

// A map of another project: another schema, so another project.
async function otherProject() {
  const map = await centreA()
  map.name = OTHER
  map.version = '2.0.0'
  map.schema = { ...map.schema, '@id': 'schema:e2e-other/v1', name: 'E2E other schema' }
  return map
}

function asFile(map, name) {
  return { name, mimeType: 'application/ld+json', buffer: Buffer.from(JSON.stringify(map)) }
}

async function openFromPicker(page, map, name) {
  await picker(page).locator('input[type="file"]').setInputFiles(asFile(map, name))
}

async function uploadOnDescribeLanding(page, map, name) {
  await page.locator('input[type="file"]').first().setInputFiles(asFile(map, name))
  await page.getByRole('button', { name: /Upload Semantic Map/i }).click()
}

test.describe('Projects', () => {
  test.afterEach(async ({ request }) => {
    // Leave the stack without a project for the specs that follow.
    await request.delete('/api/v1/project')
  })

  test('Flyover asks which project to work on, and names the standard RDF flow', async ({
    page,
    context,
  }) => {
    await page.goto('/')
    const dialog = picker(page)
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText('standard RDF flow')
    await expect(dialog).toContainText("Flyover's configured RDF store")
    await expect(dialog).toContainText(/Repository\s+userRepo/)

    // At start-up only a choice leaves it.
    await expect(dialog.getByRole('button', { name: 'Close' })).toHaveCount(0)
    await page.keyboard.press('Escape')
    await expect(dialog).toBeVisible()

    await dialog.getByRole('button', { name: 'Continue without a project' }).click()
    await expect(dialog).toBeHidden()
    await expect(statusBar(page)).toContainText('No project')
    await expect(statusBar(page)).toContainText(/RDF store\s*·\s*userRepo/)
    await expect(statusBar(page).locator('.statusbar-dot')).toHaveClass(/is-reachable/)

    // The choice lasts for the tab: a reload does not ask again...
    await page.reload()
    await expect(statusBar(page)).toContainText('No project')
    await expect(picker(page)).toHaveCount(0)
    // ...but Flyover opened in another tab does.
    const otherTab = await context.newPage()
    await otherTab.goto('/')
    await expect(picker(otherTab)).toBeVisible()
  })

  test('an opened project shows in the status bar and survives a reload', async ({ page }) => {
    await page.goto('/describe')
    await openFromPicker(page, await centreA(), 'mapping_centre_a.jsonld')

    // The step page reloads to read the project afresh, then says so.
    await expect(statusBar(page)).toContainText(
      new RegExp(`Project:\\s*${CENTRE_A}\\s*·\\s*v1\\.0\\.0`)
    )
    await expect(page.locator('.alert-success')).toContainText('Flyover now works on the project')
    // The map names localhost, which Flyover reads as its configured store.
    await expect(statusBar(page).locator('#statusbar-store')).toHaveAttribute('title', /localhost/)

    await page.reload()
    await expect(picker(page)).toHaveCount(0)
    await expect(statusBar(page)).toContainText(CENTRE_A)
    const server = await (await page.request.get('/api/v1/project')).json()
    expect(server.project.name).toBe(CENTRE_A)
    expect(server.store.repository).toBe('userRepo')
  })

  test('switching asks first and offers the download; leaving goes back to the standard flow', async ({
    page,
  }) => {
    await page.goto('/')
    await openFromPicker(page, await centreA(), 'mapping_centre_a.jsonld')
    await expect(statusBar(page)).toContainText(CENTRE_A)

    await statusBar(page).getByRole('button', { name: 'Switch project' }).click()
    const dialog = picker(page)
    await expect(dialog.getByRole('button', { name: 'Cancel' })).toBeVisible()
    await openFromPicker(page, await otherProject(), 'other.jsonld')
    await expect(dialog).toContainText(new RegExp(`Open ${OTHER}\\s*·\\s*v2\\.0\\.0\\s*\\?`))
    await expect(dialog).toContainText('It replaces the open project')

    const [download] = await Promise.all([
      page.waitForEvent('download'),
      dialog.getByRole('button', { name: 'Download the current map' }).click(),
    ])
    expect(download.suggestedFilename()).toBe('mapping_centre_a.jsonld')

    await dialog.getByRole('button', { name: 'Open the project' }).click()
    await expect(statusBar(page)).toContainText(OTHER)

    await statusBar(page).getByRole('button', { name: 'Switch project' }).click()
    await dialog.getByRole('button', { name: 'Continue without a project' }).click()
    await expect(dialog).toContainText(`Leave ${OTHER}?`)
    await expect(dialog).toContainText('standard RDF flow')
    await dialog.getByRole('button', { name: 'Leave the project' }).click()
    await expect(statusBar(page)).toContainText('No project')
    const server = await (await page.request.get('/api/v1/project')).json()
    expect(server.project).toBe(null)
  })

  test("a step upload of another project's map asks, and keeping sets it aside", async ({
    page,
  }) => {
    test.setTimeout(240_000)
    await page.goto('/')
    await openFromPicker(page, await centreA(), 'mapping_centre_a.jsonld')
    await expect(statusBar(page)).toContainText(CENTRE_A)

    await runIngestFlow(page)
    await uploadOnDescribeLanding(page, await otherProject(), 'other.jsonld')

    const question = page.getByRole('dialog', { name: new RegExp(`Switch to ${OTHER}`) })
    await expect(question).toBeVisible()
    await question.getByRole('button', { name: `Keep going with ${CENTRE_A}` }).click()
    await expect(page.locator('.alert-dismissible.alert-info')).toContainText(
      'The upload was set aside'
    )
    await expect(statusBar(page)).toContainText(CENTRE_A)
  })

  test('without a project, a step upload offers to open the map as one', async ({ page }) => {
    test.setTimeout(240_000)
    await page.goto('/')
    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()

    await runIngestFlow(page)
    await uploadOnDescribeLanding(page, await centreA(), 'mapping_centre_a.jsonld')

    const offer = page.getByRole('dialog', { name: /as a project\?$/ })
    await expect(offer).toContainText('standard RDF flow')
    await offer.getByRole('button', { name: 'Open as the project' }).click()
    await expect(statusBar(page)).toContainText(CENTRE_A)
    await expect(
      page.getByRole('button', { name: /Click here to describe the data/i })
    ).toBeEnabled()
  })

  test('a map that cannot be fetched from a web address says why', async ({ page }) => {
    await page.goto('/')
    const dialog = picker(page)

    await dialog.locator('#project-map-url').fill('http://localhost:5000/static/no-such-map.jsonld')
    await dialog.getByRole('button', { name: 'Load' }).click()

    await expect(dialog.locator('.project-picker-errors')).toContainText(
      'The address answered with HTTP 404.'
    )
    await expect(statusBar(page)).toContainText('No project')
  })
})
