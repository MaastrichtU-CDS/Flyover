import { test, expect } from './helpers/fixtures.js'
import fs from 'node:fs/promises'
import { DEFAULT_CSV, runIngestFlow } from './helpers/ingest.js'

// The status bar's store opens a panel with the datasets in the store, and
// deletes them. The local stack keeps whatever its users ingested, so this
// spec deletes only a dataset it ingests itself, under a name of its own;
// the deletes of every dataset and every annotation are taken as far as
// their question, then cancelled.

const panel = (page) => page.locator('#store-datasets-panel')

async function ingestOwnDataset(page, testInfo) {
  const name = `e2e_store_panel_${Date.now()}`
  const csv = testInfo.outputPath(`${name}.csv`)
  await fs.mkdir(testInfo.outputDir, { recursive: true })
  await fs.copyFile(DEFAULT_CSV, csv)
  await runIngestFlow(page, csv)
  return name
}

test.describe('The datasets in the store', () => {
  test.setTimeout(240_000)

  test('a dataset of its own is listed, exported and deleted from the status bar', async ({
    page,
  }, testInfo) => {
    const name = await ingestOwnDataset(page, testInfo)

    await page.locator('#statusbar-store-button').click()
    const item = panel(page).locator('.store-panel-item', { hasText: name })
    await expect(item).toContainText('150 rows')
    await expect(item).toContainText('Not described yet')

    await item.getByRole('button', { name: `Delete ${name}` }).click()
    await expect(panel(page)).toContainText(`Delete ${name}?`)
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      panel(page).getByRole('link', { name: /Download it first/ }).click(),
    ])
    expect(download.suggestedFilename()).toBe(`${name}.trig`)
    const trig = await fs.readFile(await download.path(), 'utf8')
    expect(trig).toContain(`<http://data.local/${name}/> {`)
    expect(trig).toContain(`<http://ontology.local/${name}/> {`)

    await panel(page).getByRole('button', { name: `Delete ${name}` }).click()
    // The question gives way to the list only once the delete is done.
    await expect(panel(page).locator('.store-panel-confirm')).toHaveCount(0)
    await expect(panel(page).locator('.store-panel-item', { hasText: name })).toHaveCount(0)

    // Closing the panel reloads the step page, which then says what went.
    await page.keyboard.press('Escape')
    await expect(page.locator('.alert-success')).toContainText(`Deleted ${name}`)
    await page.locator('#statusbar-store-button').click()
    await expect(panel(page).locator('.store-panel-item', { hasText: name })).toHaveCount(0)
  })

  test('deleting every dataset or every annotation asks first and can be cancelled', async ({
    page,
  }, testInfo) => {
    await ingestOwnDataset(page, testInfo)
    await page.locator('#statusbar-store-button').click()
    await expect(panel(page).locator('.store-panel-item').first()).toBeVisible()

    await panel(page).getByRole('button', { name: 'Delete every dataset' }).click()
    await expect(panel(page)).toContainText('Anything else the repository holds stays.')
    await expect(panel(page).getByRole('link', { name: /Download them first/ })).toHaveAttribute(
      'href',
      '/api/v1/store/export'
    )
    await panel(page).getByRole('button', { name: 'Cancel' }).click()
    await expect(panel(page).locator('.store-panel-item').first()).toBeVisible()

    const annotations = panel(page).getByRole('button', { name: 'Delete all annotations' })
    if (await annotations.isEnabled()) {
      await annotations.click()
      await expect(panel(page)).toContainText('The data and the describe answers stay.')
      await panel(page).getByRole('button', { name: 'Cancel' }).click()
    }

    // A click elsewhere closes the panel without changing anything.
    await page.locator('h1').first().click()
    await expect(panel(page)).toHaveCount(0)
  })
})
