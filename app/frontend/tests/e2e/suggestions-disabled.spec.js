import { test, expect } from '@playwright/test'
import { watchConsoleErrors } from './helpers/ingest.js'
import {
  expectDescribeWithoutSuggestions,
  goToDescribeVariablesWithSuggestions,
  otherSiteMapping,
  skipUnlessSuggestions,
  suggestionsEnabled,
  watchSuggestionRequests,
} from './helpers/suggestions.js'

// ---------------------------------------------------------------------------
// The real "feature off" run: the stack must have been started with
// FLYOVER_SUGGESTION_TIERS= (empty), e.g.
//
//   FLYOVER_SUGGESTION_TIERS= ./scripts/start-test-stack.sh
//   E2E_SUGGESTIONS_DISABLED=1 npx playwright test tests/e2e/suggestions-disabled.spec.js
//
// Against a stack with suggestions on, this spec skips itself, unless
// E2E_SUGGESTIONS_DISABLED is set: then an enabled stack is a failure, which
// is how the CI job notices when the empty flag stops reaching the container
// (docker compose's `${VAR:-default}` used to turn an empty value into `1`).
//
// The mocked variant of the UI checks lives in suggestions.spec.js; this spec
// runs the same checks with the backend really disabled, and checks the
// backend's own answers.
// ---------------------------------------------------------------------------

test.describe('Suggestions disabled by FLYOVER_SUGGESTION_TIERS', () => {
  test.setTimeout(180_000)

  test.beforeEach(async ({ request }) => {
    await skipUnlessSuggestions(request, false)
  })

  test('the API reports every tier inactive and starts no job', async ({ request }) => {
    const { status } = await suggestionsEnabled(request)
    expect(status.enabled).toBe(false)
    for (const tier of ['1', '2', '3']) {
      expect(status.tiers[tier].state, `tier ${tier}`).toBe('inactive')
      expect(status.tiers[tier].reason, `tier ${tier}`).toMatch(/FLYOVER_SUGGESTION_TIERS/)
    }

    for (const phase of ['variables', 'values']) {
      // A pasted LLM answer (llm-prompt-roundtrip.spec.js) may already have
      // created a job on a shared local stack — that round trip needs no
      // tier. What /start must not do is create or change one.
      const before = await (await request.get(`/api/v1/suggestions/${phase}`)).json()

      const start = await request.post(`/api/v1/suggestions/${phase}/start`, {
        data: { mapping: await otherSiteMapping() },
      })
      expect(start.ok()).toBe(true)
      expect((await start.json()).status, `${phase}/start`).toBe('disabled')

      const snapshot = await request.get(`/api/v1/suggestions/${phase}`)
      expect(snapshot.ok()).toBe(true)
      const body = await snapshot.json()
      expect(body, `${phase} snapshot unchanged by /start`).toEqual(before)
      if (before.status === 'idle') {
        expect(body.enabled, `${phase} snapshot`).toBe(false)
        expect(body.records ?? {}, `${phase} records`).toEqual({})
      }
    }
  })

  test('the describe page renders as before, with no pills or gate', async ({ page }) => {
    const errors = watchConsoleErrors(page)
    const suggestionRequests = watchSuggestionRequests(page)
    // Upload a map all the same, so the flag is the only reason nothing shows.
    await goToDescribeVariablesWithSuggestions(page)
    await expectDescribeWithoutSuggestions(page, { errors, suggestionRequests })
  })
})
