import { test, expect } from '@playwright/test'
import fs from 'node:fs/promises'
import { DEFAULT_MAPPING_JSONLD, runIngestFlow } from './helpers/ingest.js'

// Projects: the home page that offers the workflows, the pop-up that asks
// for the project as a tab enters the RDF workflow, the status bar above the
// steps (with Exit RDF), switching and leaving, and a step's map upload that
// belongs to a project. Unlike the other specs, this one answers the pop-up
// itself, so it uses Playwright's own `test` rather than the shared fixture.
//
// The Centre A example map names localhost's userRepo as its endpoint,
// which Flyover reads as its configured store: opening it keeps the stack's
// store, so the specs that follow find their data where they expect it.

const CENTRE_A = 'Centre A - English Hospital Mapping'
const OTHER = 'E2E other project mapping'

const picker = (page) => page.getByRole('dialog', { name: 'Choose a project' })
const statusBar = (page) => page.locator('#project-status-bar')
const heading = (page) => page.locator('h1').first()

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

  test('the home page offers the workflows and asks nothing', async ({ page }) => {
    await page.goto('/')

    await expect(heading(page)).toContainText('Welcome to Flyover')
    for (const tile of ['#rdf-card', '#omop-card', '#csv-card']) {
      await expect(page.locator(tile)).toBeVisible()
    }
    await expect(page.locator('.nav-step .step-text')).toHaveText(['RDF', 'OMOP', 'CSV'])
    await expect(page.locator('#rdf-card img')).toHaveAttribute('alt', 'W3C Semantic Web Logo')
    await expect(page.locator('#omop-card img')).toHaveAttribute('alt', 'OHDSI logo')
    await expect(page.locator('#omop-card')).toContainText('Flyover is not affiliated with OHDSI')
    await expect(page.locator('#omop-card .workflow-note a')).toHaveAttribute(
      'href',
      'https://www.ohdsi.org/'
    )
    await expect(statusBar(page)).toHaveText(/Home\s*·\s*Choose a workflow to begin/)
    await expect(picker(page)).toHaveCount(0)

    // The workflows still under development say so, from a tile or the bar.
    await page.locator('#omop-card').click()
    const soon = page.getByRole('dialog', { name: 'The OMOP workflow' })
    await expect(soon).toContainText('This workflow is under development.')
    await soon.getByRole('button', { name: 'Close' }).click()
    await expect(soon).toBeHidden()
    await page.locator('#csv-workflow-step').click()
    await expect(page.getByRole('dialog', { name: 'The CSV workflow' })).toBeVisible()
    await expect(page).toHaveURL(/\/$/)
  })

  test('the RDF tile asks for the project on the home page, and goes on after the choice', async ({
    page,
    context,
  }) => {
    await page.goto('/')
    await page.locator('#rdf-card').click()
    const dialog = picker(page)
    await expect(dialog).toBeVisible()
    // Nothing opens before the choice.
    await expect(page).toHaveURL(/\/$/)
    await expect(heading(page)).toContainText('Welcome to Flyover')
    await expect(dialog).toContainText('standard RDF flow')
    await expect(dialog).toContainText("Flyover's configured RDF store")
    await expect(dialog).toContainText(/Repository\s+userRepo/)

    await dialog.getByRole('button', { name: 'Cancel' }).click()
    await expect(dialog).toBeHidden()
    await expect(page).toHaveURL(/\/$/)

    // The start button asks too; the choice goes on to the RDF landing.
    await page.locator('#home-start').click()
    await expect(dialog).toBeVisible()
    await dialog.getByRole('button', { name: 'Continue without a project' }).click()
    await expect(dialog).toBeHidden()
    await expect(page).toHaveURL(/\/rdf$/)
    await expect(heading(page)).toHaveText('Flyover × RDF')
    await expect(page.locator('.hero-section p')).toHaveText(
      'Transform your data into Resource Description Framework (RDF) triples through our guided four-step process'
    )
    await expect(page.locator('#rdf-home-step')).toHaveText('Home')
    await expect(page.locator('#ingest-step')).toBeVisible()
    await expect(statusBar(page)).toContainText('No project')
    await expect(statusBar(page)).toContainText(/RDF store\s*·\s*userRepo/)
    await expect(statusBar(page).locator('.statusbar-dot')).toHaveClass(/is-reachable/)
    await expect(statusBar(page).locator('#statusbar-connection')).toHaveText(/Connected$/)

    // The choice lasts for the tab: a reload does not ask again...
    await page.reload()
    await expect(statusBar(page)).toContainText('No project')
    await expect(picker(page)).toHaveCount(0)
    // ...but a new tab that comes by a link to a step asks, the step waiting.
    const otherTab = await context.newPage()
    await otherTab.goto('/describe')
    await expect(picker(otherTab)).toBeVisible()
    await expect(otherTab.locator('.store-wait')).toContainText('Waiting for a project choice')
  })

  test('leaving RDF asks whether to keep the RDF store running, the Back button too', async ({
    page,
  }) => {
    // The stack's real store keeps running for the specs that follow: the
    // stop is answered here, as the server would.
    let stops = 0
    await page.route('**/api/v1/project/store-stop', async (route) => {
      stops += 1
      await route.fulfill({
        json: { state: 'stopped', reachable: false, reason: '', managed: true },
      })
    })
    await page.goto('/')
    await page.locator('#rdf-card').click()
    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()
    await expect(page).toHaveURL(/\/rdf$/)
    const question = page.getByRole('dialog', { name: 'Leave the RDF workflow?' })

    // The Back button asks; Stay keeps the user in the workflow.
    await page.goBack()
    await expect(question).toBeVisible()
    await expect(question).toContainText('Do you want to keep the RDF store running')
    await expect(question).toContainText('Once stopped, the RDF store cannot be queried')
    await question.getByRole('button', { name: 'Stay' }).click()
    await expect(question).toBeHidden()
    await expect(page).toHaveURL(/\/rdf$/)
    await expect(heading(page)).toHaveText('Flyover × RDF')

    // Exit RDF asks too; stopping the store goes home.
    await statusBar(page).getByRole('button', { name: 'Exit RDF' }).click()
    await question.getByRole('button', { name: 'Stop the RDF store' }).click()
    await expect(question).toBeHidden()
    await expect(page).toHaveURL(/\/$/)
    expect(stops).toBe(1)
  })

  test('Home inside RDF points to Exit RDF, until the user clicks "Got it"', async ({ page }) => {
    await page.goto('/rdf')
    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()
    await expect(picker(page)).toBeHidden()

    await page.locator('#ingest-step').click()
    await page.locator('#rdf-home-step').click()
    await expect(page).toHaveURL(/\/rdf$/)
    const notice = statusBar(page).locator('.statusbar-exit .suggestion-coachmark')
    await expect(notice).toBeVisible()
    await expect(notice).toContainText("This is the RDF workflow's home")
    await expect(notice).toContainText('click Exit RDF')

    // Moving on hides it; it shows again from Home, until acknowledged.
    await page.locator('#ingest-step').click()
    await expect(notice).toHaveCount(0)
    await page.locator('#rdf-home-step').click()
    await expect(notice).toBeVisible()
    await notice.getByRole('button', { name: 'Got it' }).click()
    await expect(notice).toHaveCount(0)

    await page.locator('#ingest-step').click()
    await page.locator('#rdf-home-step').click()
    await expect(page).toHaveURL(/\/rdf$/)
    await expect(heading(page)).toHaveText('Flyover × RDF')
    await expect(notice).toHaveCount(0)
  })

  test('Exit RDF goes home; entering again offers the remembered project first', async ({
    page,
  }) => {
    await page.goto('/rdf')
    await openFromPicker(page, await centreA(), 'mapping_centre_a.jsonld')
    await expect(statusBar(page)).toContainText(CENTRE_A)

    await statusBar(page).getByRole('button', { name: 'Exit RDF' }).click()
    // Flyover runs this stack's store, so leaving asks about it.
    await page
      .getByRole('dialog', { name: 'Leave the RDF workflow?' })
      .getByRole('button', { name: 'Keep it running' })
      .click()
    await expect(page).toHaveURL(/\/$/)
    await expect(statusBar(page)).toHaveText(/Choose a workflow to begin/)
    // Leaving the workflow keeps the project open on the server.
    let server = await (await page.request.get('/api/v1/project')).json()
    expect(server.project.name).toBe(CENTRE_A)

    await page.locator('#rdf-card').click()
    const remembered = picker(page).locator('.project-picker-remembered')
    await expect(remembered).toContainText('The project this browser remembers')
    await expect(remembered).toContainText(new RegExp(`${CENTRE_A}\\s*·\\s*v1\\.0\\.0`))
    await remembered.getByRole('button', { name: `Continue with ${CENTRE_A}` }).click()
    await expect(picker(page)).toBeHidden()
    await expect(page).toHaveURL(/\/rdf$/)
    await expect(statusBar(page)).toContainText(new RegExp(`Project:\\s*${CENTRE_A}`))
    server = await (await page.request.get('/api/v1/project')).json()
    expect(server.project.name).toBe(CENTRE_A)
  })

  test('an opened project shows in the status bar and survives a reload', async ({ page }) => {
    await page.goto('/describe')
    await openFromPicker(page, await centreA(), 'mapping_centre_a.jsonld')

    // The step page opens once the project's store answers, and says so.
    await expect(heading(page)).toContainText('Data Description')
    await expect(statusBar(page)).toContainText(
      new RegExp(`Project:\\s*${CENTRE_A}\\s*·\\s*v1\\.0\\.0`)
    )
    await expect(page.locator('.alert-success')).toContainText('Flyover now works on the project')
    // The map names localhost, which Flyover reads as its configured store:
    // the hover shows it at the address users know, in the tiles' tooltip.
    await statusBar(page).locator('#statusbar-store .hover-tip').first().hover()
    await expect(statusBar(page).locator('[role="tooltip"]')).toHaveText(
      'http://localhost:7200/repositories/userRepo'
    )

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
    await page.goto('/rdf')
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
    await page.goto('/rdf')
    await openFromPicker(page, await centreA(), 'mapping_centre_a.jsonld')
    await expect(statusBar(page)).toContainText(CENTRE_A)

    await runIngestFlow(page)
    // The landing knows the project's map, and which of its data sources the
    // store holds.
    const held = page.locator('.held-map')
    await expect(held).toContainText("Continue with the project's semantic map")
    await expect(held.locator('.held-map-name')).toHaveText(
      `${CENTRE_A} · v1.0.0 (mapping_centre_a.jsonld)`
    )
    await expect(held.locator('.sources-ready')).toHaveText(
      /ready to\s+describe:\s+(.*,\s+)?synthetic_english_150\s*$/
    )
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
    await page.goto('/rdf')
    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()
    // The tab is in the workflow once the server has the choice; a page
    // loaded before that asks again.
    await expect(picker(page)).toBeHidden()

    await runIngestFlow(page)
    await uploadOnDescribeLanding(page, await centreA(), 'mapping_centre_a.jsonld')

    const offer = page.getByRole('dialog', { name: /as a project\?$/ })
    await expect(offer).toContainText('standard RDF flow')
    await offer.getByRole('button', { name: 'Open as the project' }).click()
    await expect(statusBar(page)).toContainText(CENTRE_A)
    // The landing now goes on with the project's map.
    await expect(page.locator('.held-map')).toContainText(
      "Continue with the project's semantic map"
    )
    await expect(
      page.getByRole('button', { name: 'Describe your data with this map' })
    ).toBeEnabled()
    await expect(page.getByRole('button', { name: /^\s*Skip\s*$/ })).toHaveCount(0)
  })

  test('before anyone chooses, the strip and the step page wait for a project choice', async ({
    page,
  }) => {
    await page.goto('/describe')
    await expect(picker(page)).toBeVisible()
    await expect(statusBar(page).locator('#statusbar-store')).toHaveText(
      /Waiting for a project choice/
    )
    await expect(page.locator('.store-wait')).toContainText('Waiting for a project choice')

    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()
    await expect(page.locator('h1').first()).toContainText('Data Description')
    await expect(statusBar(page).locator('#statusbar-connection')).toHaveText(/Connected$/)
  })

  test('a step page waits while the store starts, and opens once it answers', async ({
    page,
  }) => {
    // The stack's store runs already: the server's answers say it starts
    // until the test lets it answer.
    let state = 'starting'
    await page.route('**/api/v1/project/store-status', async (route) => {
      const response = await route.fetch()
      const body = await response.json()
      await route.fulfill({ response, json: { ...body, state, reachable: state === 'reachable' } })
    })
    await page.goto('/describe')
    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()

    await expect(picker(page)).toBeHidden()
    await expect(page.locator('.store-wait')).toContainText('Starting the RDF store')
    await expect(statusBar(page).locator('#statusbar-connection')).toContainText('Starting')
    await expect(statusBar(page).locator('.statusbar-dot')).toHaveClass(/is-starting/)

    state = 'reachable'
    await expect(page.locator('h1').first()).toContainText('Data Description')
    await expect(page.locator('.store-wait')).toHaveCount(0)
    await expect(statusBar(page).locator('#statusbar-connection')).toHaveText(/Connected$/)
  })

  test('a store someone stopped waits for Start, on the page and in the strip', async ({
    page,
  }) => {
    // The stack's store runs: the server's answers say it was stopped until
    // the user asks to start it.
    let state = 'stopped'
    let store = null
    await page.route('**/api/v1/project/store-status', async (route) => {
      const response = await route.fetch()
      const body = await response.json()
      store = body.store
      const reason = state === 'stopped' ? 'The RDF store is not running: it was stopped.' : ''
      await route.fulfill({
        response,
        json: { ...body, state, reachable: state === 'reachable', reason },
      })
    })
    await page.route('**/api/v1/project/store-start', (route) => {
      state = 'starting'
      return route.fulfill({ json: { state, reachable: false, reason: '', store } })
    })
    await page.goto('/describe')
    await picker(page).getByRole('button', { name: 'Continue without a project' }).click()

    const notice = page.locator('.store-wait')
    await expect(notice).toContainText('The RDF store is not running')
    await expect(statusBar(page).locator('#statusbar-connection')).toContainText('Stopped')
    await expect(statusBar(page).locator('#statusbar-start')).toBeVisible()

    await notice.getByRole('button', { name: 'Start the RDF store' }).click()
    await expect(notice).toContainText('Starting the RDF store')
    await expect(statusBar(page).locator('#statusbar-start')).toHaveCount(0)

    state = 'reachable'
    await expect(page.locator('h1').first()).toContainText('Data Description')
  })

  test('a map that cannot be fetched from a web address says why', async ({ page }) => {
    await page.goto('/rdf')
    const dialog = picker(page)

    await dialog.locator('#project-map-url').fill('http://localhost:5000/static/no-such-map.jsonld')
    await dialog.getByRole('button', { name: 'Load' }).click()

    await expect(dialog.locator('.project-picker-errors')).toContainText(
      'The address answered with HTTP 404.'
    )
    await expect(statusBar(page)).toContainText('No project')
  })
})
