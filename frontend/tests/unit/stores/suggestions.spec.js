import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn().mockResolvedValue(null),
  saveData: vi.fn().mockResolvedValue(true),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import {
  useSuggestionsStore,
  POLL_INTERVAL_MS,
  POLL_HARD_STOP_MS,
} from '@/stores/suggestions.js'
import { useStatusStore } from '@/stores/status.js'

function statusResponse(enabled = true, extra = {}) {
  return {
    data: {
      enabled,
      model: 'llama3.2:3b',
      provider: 'ollama',
      remote: false,
      backend: 'ready',
      ...extra,
    },
  }
}

function snapshot({ status = 'running', suggestions = {}, done = 0, total = 3 } = {}) {
  return {
    data: {
      enabled: true,
      status,
      progress: { chunks_done: done, chunks_total: total },
      error: null,
      suggestions,
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

  it('init() records provider identity and remote classification', async () => {
    api.get
      .mockResolvedValueOnce(
        statusResponse(true, { provider: 'anthropic', model: 'claude-opus-4-8', remote: true }),
      )
      .mockResolvedValue(snapshot({ status: 'done' }))
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables')
    expect(s.provider).toBe('anthropic')
    expect(s.remote).toBe(true)
    expect(s.providerLabel).toBe('claude-opus-4-8 (Anthropic Claude)')
  })

  it('init() with the feature disabled renders no LLM activity', async () => {
    api.get.mockResolvedValueOnce(statusResponse(false))
    const s = useSuggestionsStore()
    await s.init('variables')
    expect(s.enabled).toBe(false)
    expect(api.post).not.toHaveBeenCalled()
    expect(s.isPolling()).toBe(false)
  })

  it('init() starts the job, ingests the snapshot, and polls', async () => {
    api.get
      .mockResolvedValueOnce(statusResponse())
      .mockResolvedValue(
        snapshot({
          suggestions: {
            db1: {
              leeftijd: {
                status: 'done',
                variable_key: 'age_at_diagnosis',
                confidence: 0.86,
                reason: 'Dutch for age',
              },
              gewicht: { status: 'pending' },
            },
          },
        }),
      )
    api.post.mockResolvedValue({ data: { status: 'started' } })

    const s = useSuggestionsStore()
    await s.init('variables', { mapping: { some: 'mapping' } })

    expect(api.post).toHaveBeenCalledWith('/api/v1/llm/suggestions/variables/start', {
      mapping: { some: 'mapping' },
    })
    expect(s.enabled).toBe(true)
    expect(s.variables.status).toBe('running')
    expect(s.variables.byKey['db1_leeftijd']).toMatchObject({
      variableKey: 'age_at_diagnosis',
      display: 'Age at diagnosis',
      confidence: 0.86,
    })
    expect(s.variables.byKey['db1_gewicht']).toMatchObject({ status: 'pending' })
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

  it('ingests values-phase snapshots per category value', async () => {
    api.get.mockResolvedValue(
      snapshot({
        status: 'running',
        suggestions: {
          db1: {
            geslacht: {
              status: 'done',
              variable_key: 'biological_sex',
              values: {
                M: { term_key: 'male', confidence: 0.95, reason: 'r' },
                9: { term_key: null, confidence: 0, reason: 'no match' },
              },
            },
          },
        },
      }),
    )

    const s = useSuggestionsStore()
    await s.refresh('values')
    expect(s.values.byKey['db1_geslacht_M']).toMatchObject({
      termKey: 'male',
      display: 'Male',
      confidence: 0.95,
    })
    expect(s.values.byKey['db1_geslacht_9'].termKey).toBeNull()
  })

  it('bumpPriority posts the phase-appropriate body', async () => {
    api.post.mockResolvedValue({ data: { status: 'ok', moved: 1 } })
    const s = useSuggestionsStore()

    await s.bumpPriority('variables', 'db1', ['a', 'b'])
    expect(api.post).toHaveBeenCalledWith('/api/v1/llm/suggestions/variables/priority', {
      database: 'db1',
      columns: ['a', 'b'],
      retry: false,
    })

    await s.bumpPriority('values', 'db1', ['geslacht'], { retry: true })
    expect(api.post).toHaveBeenCalledWith('/api/v1/llm/suggestions/values/priority', {
      database: 'db1',
      column: 'geslacht',
      retry: true,
    })
  })

  it('dismissed keys are tracked and persisted', async () => {
    const s = useSuggestionsStore()
    s.markApplied('db1_leeftijd')
    s.dismiss('db1_leeftijd')

    expect(s.isDismissed('db1_leeftijd')).toBe(true)
    expect(s.isApplied('db1_leeftijd')).toBe(false)
    const saved = db.saveData.mock.calls.at(-1)[1]
    expect(saved.dismissed).toContain('db1_leeftijd')
  })

  it('marks are restored from IndexedDB on init', async () => {
    db.getData.mockResolvedValue({
      key: 'llm_suggestion_marks',
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
})
