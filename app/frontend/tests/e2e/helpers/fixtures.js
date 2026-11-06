import { test as base, expect } from '@playwright/test'

// Flyover asks which project to work on whenever it opens in a new tab.
// The specs of the steps test the standard RDF flow, as Flyover ran before
// projects, so they answer that pop-up through the UI as a user would:
// "Continue without a project", whenever it shows. Choosing so lasts for
// the tab, so it shows once per test. project.spec.js, which tests the
// pop-up itself, imports Playwright's own `test` instead.
export const test = base.extend({
  page: async ({ page }, use) => {
    await page.addLocatorHandler(
      page.getByRole('dialog', { name: 'Choose a project' }),
      async (dialog) => {
        await dialog.getByRole('button', { name: 'Continue without a project' }).click()
      }
    )
    await use(page)
  },
})

export { expect }
