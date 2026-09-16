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
} from '@/stores/suggestions.js'
import { useStatusStore } from '@/stores/status.js'

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
} = {}) {
  return {
    data: {
      enabled: true,
      status,
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
    await s.init('variables')
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
    await s.init('variables')
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
    await s.init('variables', { mapping: { some: 'mapping' } })

    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/start', {
      mapping: { some: 'mapping' },
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
    await s.init('variables')
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
    await s.init('variables')
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
    await s.init('variables')
    expect(s.isPolling()).toBe(true)

    await vi.advanceTimersByTimeAsync(POLL_HARD_STOP_MS + POLL_INTERVAL_MS)
    expect(s.isPolling()).toBe(false)
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

  it('marks are restored from IndexedDB on init', async () => {
    db.getData.mockResolvedValue({
      key: 'suggestion_marks_variables',
      applied: ['db1_a'],
      touched: ['db1_a'],
      dismissed: ['db1_b'],
    })
    api.get.mockResolvedValueOnce(statusResponse(false))

    const s = useSuggestionsStore()
    await s.init('variables')
    expect(s.isApplied('db1_a')).toBe(true)
    expect(s.isTouched('db1_a')).toBe(true)
    expect(s.isDismissed('db1_b')).toBe(true)
  })

  it('clearAllApplied returns only unreviewed keys and unmarks them', () => {
    const s = useSuggestionsStore()
    s.setPhase('variables')
    s.markApplied('db1_a')
    s.markApplied('db1_b')
    s.markUserTouched('db1_b')

    const cleared = s.clearAllApplied()
    expect(cleared).toEqual(['db1_a'])
    expect(s.isApplied('db1_a')).toBe(false)
    expect(s.isApplied('db1_b')).toBe(true)
  })

  it('touching a non-applied key is a no-op', () => {
    const s = useSuggestionsStore()
    s.markUserTouched('db1_x')
    expect(s.isTouched('db1_x')).toBe(false)
  })

  it('SOURCE_ICONS maps every tier-1 source', () => {
    expect(SOURCE_ICONS.alias).toBeDefined()
    expect(SOURCE_ICONS.value_regex).toBeDefined()
    expect(SOURCE_ICONS.string).toBeDefined()
  })
})
