import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn().mockResolvedValue(null),
  saveData: vi.fn().mockResolvedValue(true),
}))
vi.mock('@/lib/jsonld', () => ({
  formatToTitleCase: (key) => key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import {
  useSuggestionsStore,
  POLL_INTERVAL_MS,
  POLL_HARD_STOP_MS,
  SOURCE_ICONS,
  ingestSummary,
} from '@/stores/suggestions.js'
import { useStatusStore } from '@/stores/status.js'

// A browser semantic map with variables: without one the store reports
// no_semantic_map instead of starting a job.
const MAPPING = { schema: { variables: { age_at_diagnosis: {} } } }

function statusResponse(enabled = true, extra = {}) {
  return {
    data: {
      enabled,
      compute: 'host',
      tiers: {
        1: { state: 'active' },
        2: { state: 'inactive', reason: 'not enabled' },
        3: { state: 'inactive', reason: 'not enabled' },
      },
      threshold: 0.8,
      rules_version: '1.0.0',
      ...extra,
    },
  }
}

function snapshot({
  status = 'running',
  records = {},
  done = 0,
  total = 3,
  error = null,
  fingerprint = null,
} = {}) {
  return {
    data: {
      enabled: true,
      status,
      fingerprint,
      progress: { done, total },
      error,
      records,
    },
  }
}

describe('Frontend unit: useSuggestionsStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.useFakeTimers()
    api.get.mockReset()
    api.post.mockReset()
    db.getData.mockReset().mockResolvedValue(null)
    db.saveData.mockReset().mockResolvedValue(true)
  })

  afterEach(() => {
    useSuggestionsStore().stopPolling()
    vi.useRealTimers()
  })

  it('init() records tier status and rules version', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse(true))
      .mockResolvedValue(snapshot({ status: 'done' }))
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.enabled).toBe(true)
    expect(s.compute).toBe('host')
    expect(s.tiers[1]).toEqual({ state: 'active' })
    expect(s.tiers[2]).toEqual({ state: 'inactive', reason: 'not enabled' })
    expect(s.rulesVersion).toBe('1.0.0')
    expect(s.threshold).toBe(0.8)
  })

  it('init() with the feature disabled renders no suggestion activity', async () => {
    api.get.mockResolvedValueOnce(statusResponse(false))
    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.enabled).toBe(false)
    expect(api.post).not.toHaveBeenCalled()
    expect(s.isPolling()).toBe(false)
  })

  it('init() starts the job, ingests records, and polls', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(
        snapshot({
          status: 'running',
          records: {
            'db1_leeftijd': {
              status: 'done',
              item: 'leeftijd',
              match: 'age_at_diagnosis',
              confidence: 0.86,
              reason: 'Dutch for age',
              source: 'string',
              tier: 1,
            },
            'db1_gewicht': {
              status: 'done',
              item: 'gewicht',
              match: null,
              confidence: 0,
              reason: 'No match',
              source: 'string',
              tier: 1,
            },
          },
        }),
      )
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })

    // The job runs on the browser's semantic map.
    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/start', {
      mapping: MAPPING,
    })
    expect(s.enabled).toBe(true)
    expect(s.variables.status).toBe('running')
    expect(s.variables.byKey['db1_leeftijd']).toMatchObject({
      match: 'age_at_diagnosis',
      display: 'Age At Diagnosis',
      confidence: 0.86,
      source: 'string',
    })
    expect(s.variables.byKey['db1_gewicht'].match).toBeNull()
    expect(s.isPolling()).toBe(true)
  })

  it('polling stops when the job reaches a terminal status', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValueOnce(snapshot({ status: 'running' }))
      .mockResolvedValue(snapshot({ status: 'done', done: 3 }))
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.isPolling()).toBe(true)

    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS)
    expect(s.variables.status).toBe('done')
    expect(s.isPolling()).toBe(false)
  })

  it('a failed job produces exactly one warning toast', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(snapshot({ status: 'failed' }))
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    await s.refresh('variables')
    await s.refresh('variables')

    const status = useStatusStore()
    expect(status.messages).toHaveLength(1)
    expect(status.messages[0].level).toBe('warning')
  })

  it('polling hard-stops after the time budget', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(snapshot({ status: 'running' }))
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.isPolling()).toBe(true)

    await vi.advanceTimersByTimeAsync(POLL_HARD_STOP_MS + POLL_INTERVAL_MS)
    expect(s.isPolling()).toBe(false)
    // The hard stop also fails the submit gate open.
    expect(s.variables.gaveUp).toBe(true)
  })

  it('gives up after repeated failed polls so the gate fails open', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockRejectedValue(new Error('backend unreachable'))
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    // init already made the first (failing) snapshot poll.
    await s.init('variables', { mapping: MAPPING })
    expect(s.variables.gaveUp).toBe(false)

    await s.refresh('variables')
    await s.refresh('variables')
    expect(s.variables.gaveUp).toBe(true)
    expect(s.isPolling()).toBe(false)
  })

  it('gives up when no job is ever created', async () => {
    api.get.mockResolvedValue(snapshot({ status: 'idle' }))
    const s = useSuggestionsStore()
    await s.refresh('variables')
    await s.refresh('variables')
    expect(s.variables.gaveUp).toBe(false)
    await s.refresh('variables')
    expect(s.variables.gaveUp).toBe(true)
  })

  it('the variables phase always sends the browser mapping', async () => {
    // The describe pages work on the browser's map; the backend session
    // may hold an older one, so the variables job must run on this one.
    api.post.mockResolvedValue({ data: { status: 'started' } })
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(snapshot({ status: 'done' }))

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })

    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/start', {
      mapping: MAPPING,
    })
    expect(api.post).toHaveBeenCalledTimes(1)
  })

  it('reports no_semantic_map without starting when the browser has no map', async () => {
    // The dropdowns are built from the browser's map; with none (or one
    // without variables) suggestions from the backend session's map would
    // have nowhere to go.
    api.get.mockResolvedValueOnce(statusResponse())
    const s = useSuggestionsStore()
    for (const mapping of [null, { schema: { variables: {} } }]) {
      await s.init('variables', { mapping })
      expect(s.variables.status).toBe('unavailable')
      expect(s.variables.reason).toBe('no_semantic_map')
    }
    expect(api.post).not.toHaveBeenCalled()
    expect(s.isPolling()).toBe(false)
  })

  it('the values phase always sends the mapping job-locally', async () => {
    // The values job needs the browser's latest variable selections, so
    // its mapping goes with every start; the backend keeps it job-local.
    api.post.mockResolvedValue({ data: { status: 'started' } })
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(snapshot({ status: 'done' }))

    const s = useSuggestionsStore()
    await s.init('values', { mapping: MAPPING })

    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/values/start', {
      mapping: MAPPING,
    })
    expect(api.post).toHaveBeenCalledTimes(1)
  })

  it('a new fingerprint keeps marks whose match is unchanged and drops the rest', async () => {
    // Decision D3: mark expiry is per key, not wholesale. A stale
    // dismissal must not hide a DIFFERENT suggestion, but reviews the
    // user already did must survive a new job.
    const record = (key, match) => ({
      status: 'done',
      item: key,
      match,
      confidence: 0.9,
      reason: 'r',
      source: 'alias',
      tier: 1,
    })
    const s = useSuggestionsStore()
    s.setPhase('variables')

    // First job: the user reviews db1_a and dismisses db1_b.
    api.get.mockResolvedValue(
      snapshot({
        status: 'done',
        fingerprint: 'old',
        records: {
          db1_a: record('db1_a', 'age_at_diagnosis'),
          db1_b: record('db1_b', 'biological_sex'),
        },
      })
    )
    await s.refresh('variables')
    s.markApplied('db1_a')
    s.markUserTouched('db1_a')
    s.dismiss('db1_b')
    expect(s.isDismissed('db1_b')).toBe(true)

    // New job, new fingerprint: db1_a's suggestion is unchanged, db1_b's
    // now suggests a different variable.
    api.get.mockResolvedValue(
      snapshot({
        status: 'done',
        fingerprint: 'new',
        records: {
          db1_a: record('db1_a', 'age_at_diagnosis'),
          db1_b: record('db1_b', 'year_of_diagnosis'),
        },
      })
    )
    await s.refresh('variables')

    // The finished review survives; the stale dismissal does not.
    expect(s.isApplied('db1_a')).toBe(true)
    expect(s.isTouched('db1_a')).toBe(true)
    expect(s.isDismissed('db1_b')).toBe(false)
    expect(s.marks.variables.fingerprint).toBe('new')

    const saved = db.saveData.mock.calls.at(-1)[1]
    expect(saved.applied).toContain('db1_a')
    expect(saved.dismissed).toEqual([])
  })

  it('a fresh init retries after giving up', async () => {
    api.get.mockResolvedValue(snapshot({ status: 'idle' }))
    const s = useSuggestionsStore()
    await s.refresh('variables')
    await s.refresh('variables')
    await s.refresh('variables')
    expect(s.variables.gaveUp).toBe(true)

    // A fresh page visit (init) resets the stall tracking.
    api.get.mockReset()
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(snapshot({ status: 'done' }))
    api.post.mockResolvedValue({ data: { status: 'started' } })
    await s.init('variables', { mapping: MAPPING })
    expect(s.variables.gaveUp).toBe(false)
  })

  it('exposes the unavailable reason from the snapshot error', async () => {
    api.get.mockResolvedValue(
      snapshot({ status: 'unavailable', error: { kind: 'no_semantic_map' } }),
    )
    const s = useSuggestionsStore()
    await s.refresh('variables')
    expect(s.variables.status).toBe('unavailable')
    expect(s.variables.reason).toBe('no_semantic_map')
  })

  it('ingests values-phase records per category value', async () => {
    api.get.mockResolvedValue(
      snapshot({
        status: 'running',
        records: {
          'db1_geslacht_M': {
            status: 'done',
            item: 'M',
            match: 'male',
            confidence: 0.95,
            reason: 'r',
            source: 'alias',
            tier: 1,
          },
          'db1_geslacht_9': {
            status: 'done',
            item: '9',
            match: null,
            confidence: 0,
            reason: 'no match',
            source: 'manual',
            tier: 1,
          },
        },
      }),
    )

    const s = useSuggestionsStore()
    await s.refresh('values')
    expect(s.values.byKey['db1_geslacht_M']).toMatchObject({
      match: 'male',
      display: 'Male',
      confidence: 0.95,
      source: 'alias',
    })
    expect(s.values.byKey['db1_geslacht_9'].match).toBeNull()
  })

  it('bumpPriority posts the items to the correct endpoint', async () => {
    api.post.mockResolvedValue({ data: { status: 'ok', moved: 0 } })
    const s = useSuggestionsStore()

    await s.bumpPriority('variables', ['db1_a', 'db1_b'])
    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/priority', {
      items: ['db1_a', 'db1_b'],
      retry: false,
    })
  })

  it('bumpPriority does not call the API while suggestions are disabled', async () => {
    api.get.mockResolvedValueOnce(statusResponse(false))
    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.enabled).toBe(false)

    await s.bumpPriority('variables', ['db1_a'])
    expect(api.post).not.toHaveBeenCalled()
  })

  it('dismissed keys are tracked and persisted', async () => {
    const s = useSuggestionsStore()
    s.setPhase('variables')
    s.markApplied('db1_leeftijd')
    s.dismiss('db1_leeftijd')

    expect(s.isDismissed('db1_leeftijd')).toBe(true)
    expect(s.isApplied('db1_leeftijd')).toBe(false)
    const saved = db.saveData.mock.calls.at(-1)[1]
    expect(saved.dismissed).toContain('db1_leeftijd')
  })

  it('resetMarks forgets every mark in both phases and persists that', async () => {
    const s = useSuggestionsStore()
    s.setPhase('variables')
    s.markApplied('db1_a')
    s.markUserTouched('db1_a')
    s.dismiss('db1_b')
    s.setPhase('values')
    s.markApplied('db1_c_1')

    await s.resetMarks()

    for (const phase of ['variables', 'values']) {
      s.setPhase(phase)
      expect(s.unreviewedKeys()).toEqual([])
      const saved = db.saveData.mock.calls
        .map((c) => c[1])
        .filter((row) => row.key === `suggestion_marks_${phase}`)
        .at(-1)
      expect(saved).toMatchObject({ applied: [], touched: [], dismissed: [], fingerprint: null })
    }
    s.setPhase('variables')
    expect(s.isTouched('db1_a')).toBe(false)
    expect(s.isDismissed('db1_b')).toBe(false)
  })

  it('marks are restored from IndexedDB on init', async () => {
    db.getData.mockResolvedValue({
      key: 'suggestion_marks_variables',
      applied: ['db1_a'],
      touched: ['db1_a'],
      dismissed: ['db1_b'],
    })
    api.get.mockResolvedValueOnce(statusResponse(false))

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.isApplied('db1_a')).toBe(true)
    expect(s.isTouched('db1_a')).toBe(true)
    expect(s.isDismissed('db1_b')).toBe(true)
  })

  it('clearAllApplied returns only unreviewed keys, unmarks them, and records dismissals', () => {
    const s = useSuggestionsStore()
    s.setPhase('variables')
    s.markApplied('db1_a')
    s.markApplied('db1_b')
    s.markUserTouched('db1_b')

    const cleared = s.clearAllApplied()
    expect(cleared).toEqual(['db1_a'])
    expect(s.isApplied('db1_a')).toBe(false)
    expect(s.isApplied('db1_b')).toBe(true)
    // The cleared key must be dismissed so the pre-fill watch does not
    // re-fill it on the next visit.
    expect(s.isDismissed('db1_a')).toBe(true)
    expect(s.isDismissed('db1_b')).toBe(false)
  })

  it('touching a non-applied key is a no-op', () => {
    const s = useSuggestionsStore()
    s.markUserTouched('db1_x')
    expect(s.isTouched('db1_x')).toBe(false)
  })

  it('ingests explicit database and column fields from records', async () => {
    api.get.mockResolvedValue(
      snapshot({
        status: 'done',
        records: {
          nki_prospective_age: {
            status: 'done',
            item: 'age',
            match: 'age_at_diagnosis',
            confidence: 0.9,
            reason: 'r',
            source: 'alias',
            tier: 1,
            database: 'nki_prospective',
            column: 'age',
          },
        },
      }),
    )
    const s = useSuggestionsStore()
    await s.refresh('variables')
    expect(s.variables.byKey['nki_prospective_age']).toMatchObject({
      database: 'nki_prospective',
      column: 'age',
    })
  })

  it('marks expire when a job with a different fingerprint arrives', async () => {
    const s = useSuggestionsStore()
    s.setPhase('variables')
    // First job: adopt the fingerprint.
    api.get.mockResolvedValue(snapshot({ status: 'done', fingerprint: 'aaa' }))
    await s.refresh('variables')
    s.markApplied('db1_leeftijd')
    s.dismiss('db1_gewicht')
    expect(s.isApplied('db1_leeftijd')).toBe(true)

    // Same fingerprint: marks survive.
    await s.refresh('variables')
    expect(s.isApplied('db1_leeftijd')).toBe(true)
    expect(s.isDismissed('db1_gewicht')).toBe(true)

    // New dataset / rules (new fingerprint): stale marks must not hide
    // the new suggestions.
    api.get.mockResolvedValue(snapshot({ status: 'done', fingerprint: 'bbb' }))
    await s.refresh('variables')
    expect(s.isApplied('db1_leeftijd')).toBe(false)
    expect(s.isDismissed('db1_gewicht')).toBe(false)
    // The new fingerprint is persisted with the reset marks.
    const saved = db.saveData.mock.calls.at(-1)[1]
    expect(saved.fingerprint).toBe('bbb')
  })

  it('marks are separated per phase', () => {
    const s = useSuggestionsStore()
    s.setPhase('variables')
    s.markApplied('db1_leeftijd')
    s.setPhase('values')
    expect(s.isApplied('db1_leeftijd')).toBe(false)
    s.markApplied('db1_leeftijd_man')
    s.setPhase('variables')
    expect(s.isApplied('db1_leeftijd_man')).toBe(false)
    expect(s.isApplied('db1_leeftijd')).toBe(true)
  })

  it('isConfident compares a record against the /status threshold', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse(true, { threshold: 0.7 }))
      .mockResolvedValue(snapshot({ status: 'done' }))
    api.post.mockResolvedValue({ data: { status: 'started' } })
    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.isConfident({ confidence: 0.7 })).toBe(true)
    expect(s.isConfident({ confidence: 0.69 })).toBe(false)
    expect(s.isConfident({})).toBe(false)
  })

  it('SOURCE_ICONS maps every tier-1 source', () => {
    expect(SOURCE_ICONS.alias).toBeDefined()
    expect(SOURCE_ICONS.value_regex).toBeDefined()
    expect(SOURCE_ICONS.string).toBeDefined()
  })

  // --- Prompt export / paste-back (issue 2) ------------------------------

  it('init() reads prompt_export from /status, active even with tiers off', async () => {
    api.get.mockResolvedValueOnce(
      statusResponse(false, { prompt_export: { state: 'active', chunk: 160 } }),
    )
    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.enabled).toBe(false)
    expect(s.promptExport).toBe(true)
    expect(s.promptExportChunk).toBe(160)
  })

  it('init() leaves prompt export off when /status does not list it', async () => {
    api.get.mockResolvedValueOnce(statusResponse(false))
    const s = useSuggestionsStore()
    await s.init('variables', { mapping: MAPPING })
    expect(s.promptExport).toBe(false)
    expect(s.promptExportChunk).toBe(40)
  })

  it('fetchPrompt posts the phase, database, mapping and options', async () => {
    api.post.mockResolvedValue({
      data: { prompt: 'P', chunks: [{ index: 1, prompt: 'P', items: ['a'] }], item_count: 1 },
    })
    const s = useSuggestionsStore()
    const result = await s.fetchPrompt('variables', 'nki', {
      mapping: MAPPING,
      chunk: 20,
    })
    expect(result.prompt).toBe('P')
    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/prompt', {
      phase: 'variables',
      database: 'nki',
      mapping: MAPPING,
      chunk: 20,
    })
    // Included columns go along only when there are any.
    await s.fetchPrompt('values', 'nki', { mapping: MAPPING, include: [] })
    expect(api.post.mock.calls[1][1]).not.toHaveProperty('include')
    await s.fetchPrompt('values', 'nki', { mapping: MAPPING, include: ['notes'] })
    expect(api.post.mock.calls[2][1].include).toEqual(['notes'])
  })

  it('fetchPrompt surfaces the server message on failure', async () => {
    api.post.mockRejectedValue({ response: { data: { error: "unknown database 'x'" } } })
    const s = useSuggestionsStore()
    await expect(s.fetchPrompt('variables', 'x', { mapping: MAPPING })).rejects.toThrow(
      "unknown database 'x'",
    )
    api.post.mockRejectedValue(new Error('network'))
    await expect(s.fetchPrompt('variables', 'x', { mapping: MAPPING })).rejects.toThrow(
      'Could not generate the prompt.',
    )
  })

  it('ingest posts the pasted answer, enables the UI and merges the snapshot', async () => {
    api.post.mockResolvedValue({
      data: {
        accepted: 2,
        nulled: 1,
        rejected: 0,
        skipped: 1,
        messages: ["'sex' is already mapped"],
        job: {
          status: 'done',
          fingerprint: 'fp+abc',
          progress: { done: 3, total: 3 },
          records: {
            nki_taal: {
              status: 'done',
              item: 'taal',
              match: 'administered_prom_language',
              confidence: 0.9,
              reason: "Dutch 'taal' = language.",
              source: 'pasted_llm',
              tier: 3,
              database: 'nki',
              column: 'taal',
            },
          },
        },
      },
    })
    const s = useSuggestionsStore()
    expect(s.enabled).toBe(null)
    const pasted = '```json\n{}\n```'
    const result = await s.ingest('variables', 'nki', { answer: pasted, mapping: MAPPING })
    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/ingest', {
      database: 'nki',
      source: 'pasted_llm',
      mapping: MAPPING,
      dismissed: [],
      answer: pasted,
    })
    expect(s.enabled).toBe(true)
    expect(s.variables.status).toBe('done')
    expect(s.variables.byKey.nki_taal.source).toBe('pasted_llm')
    expect(s.variables.byKey.nki_taal.display).toBe('Administered Prom Language')
    expect(s.marks.variables.fingerprint).toBe('fp+abc')
    expect(result).toEqual(s.lastIngestResult)
    expect(s.lastIngestResult.accepted).toBe(2)
    const toast = useStatusStore().messages.at(-1)
    expect(toast.level).toBe('success')
    expect(toast.text).toBe('2 suggestions imported, 1 left for you to decide, 1 already mapped')
  })

  it('ingest sends the dismissed keys and drops the marks the server re-opened', async () => {
    api.post.mockResolvedValue({
      data: {
        accepted: 1,
        nulled: 0,
        rejected: 0,
        skipped: 0,
        reopened: ['nki_taal'],
        messages: [],
        job: {
          status: 'done',
          fingerprint: 'fp+abc',
          progress: { done: 2, total: 2 },
          records: {
            nki_taal: {
              status: 'done',
              item: 'taal',
              match: 'administered_prom_language',
              confidence: 0.7,
              reason: 'r',
              source: 'pasted_llm',
              tier: 3,
              reopens: true,
            },
          },
        },
      },
    })
    const s = useSuggestionsStore()
    s.setPhase('variables')
    // The user dismissed tier 1's proposals for two keys; one stays.
    s.variables.byKey.nki_taal = { match: 'identifier' }
    s.variables.byKey.nki_leeft = { match: 'identifier' }
    s.dismiss('nki_taal')
    s.dismiss('nki_leeft')
    db.saveData.mockClear()

    const result = await s.ingest('variables', 'nki', { answer: '{}', mapping: MAPPING })
    expect(api.post.mock.calls[0][1].dismissed).toEqual(['nki_taal', 'nki_leeft'])
    expect(s.isDismissed('nki_taal')).toBe(false)
    expect(s.marks.variables.matches).not.toHaveProperty('nki_taal')
    expect(s.isDismissed('nki_leeft')).toBe(true)
    expect(s.variables.byKey.nki_taal.source).toBe('pasted_llm')
    expect(result.reopened).toBe(1)
    expect(useStatusStore().messages.at(-1).text).toBe(
      '1 suggestion imported, 1 shown again after a dismissal',
    )
    // The cleared dismissal is persisted.
    const saved = db.saveData.mock.calls.map(([, record]) => record).find((r) => r.key === 'suggestion_marks_variables')
    expect(saved.dismissed).toEqual(['nki_leeft'])
  })

  it('ingest rejects with the server message and leaves state alone', async () => {
    api.post.mockRejectedValue({
      response: { status: 400, data: { error: 'Could not find valid JSON in the pasted text.' } },
    })
    const s = useSuggestionsStore()
    await expect(s.ingest('values', 'nki', { answer: 'nope', mapping: MAPPING })).rejects.toThrow(
      'Could not find valid JSON',
    )
    expect(s.enabled).toBe(null)
    expect(s.lastIngestResult).toBe(null)
    expect(useStatusStore().messages).toEqual([])
  })

  it('ingestSummary words the toast', () => {
    expect(ingestSummary({ accepted: 1 })).toBe('1 suggestion imported')
    expect(ingestSummary({ accepted: 0, rejected: 2 })).toBe(
      '0 suggestions imported, 2 ignored (unknown column or value)',
    )
  })
})
