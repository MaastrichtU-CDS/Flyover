import { defineConfig, devices } from '@playwright/test'

// Tests assume `docker compose up` is running so http://localhost:5000
// serves the Flask app and the SPA at /.

// The specs of Flyover itself, which every workflow shares: the pages
// loading, the home page, projects. Every other spec tests a workflow's
// steps, and only RDF has specs yet, so each spec runs in exactly one
// project: `npx playwright test --project=rdf` runs the RDF workflow's. A
// new workflow gets its own project, taken out of 'rdf' with testIgnore.
const FLYOVER_SPECS = ['**/smoke.spec.js', '**/project.spec.js']

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://localhost:5000',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'flyover',
      testMatch: FLYOVER_SPECS,
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'rdf',
      testIgnore: FLYOVER_SPECS,
      use: { ...devices['Desktop Chrome'] },
    },
  ],
})
