import { test as base, expect } from '@playwright/test'

// Flyover asks which project to work on whenever a tab enters the RDF
// workflow, as a spec's tab does when it first goes to a step's page, and a
// map uploaded in a step without a project offers to open it as one.
// The specs of the steps test the standard RDF flow, as Flyover ran before
// projects, so they answer both through the UI as a user would: "Continue
// without a project" and "Keep going without a project", whenever they
// show. project.spec.js, which tests both, imports Playwright's own `test`
// instead.
export const test = base.extend({
  page: async ({ page }, use) => {
    await page.addLocatorHandler(
      page.getByRole('dialog', { name: 'Choose a project' }),
      async (dialog) => {
        await dialog.getByRole('button', { name: 'Continue without a project' }).click()
      }
    )
    await page.addLocatorHandler(
      page.getByRole('dialog', { name: /as a project\?$/ }),
      async (dialog) => {
        await dialog.getByRole('button', { name: 'Keep going without a project' }).click()
      }
    )
    await use(page)
  },
})

export { expect }
