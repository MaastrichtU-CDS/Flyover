// Pinia store for LLM mapping suggestions. Polls the backend suggestion
// jobs and exposes progressively arriving suggestions plus the bookkeeping
// (applied / touched / dismissed marks) the describe views need to merge
// them into form fields without ever overwriting user input. The store
// stays DOM-free: views own reading and writing actual field values.

import { defineStore } from 'pinia'
import { computed, reactive, ref } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import { formatToTitleCase } from '@/lib/jsonld'
import { useStatusStore } from '@/stores/status'

export const POLL_INTERVAL_MS = 2000
export const POLL_HARD_STOP_MS = 15 * 60 * 1000

const TERMINAL_STATUSES = ['done', 'failed', 'unavailable', 'disabled']
const MARKS_KEY = 'llm_suggestion_marks'

const PROVIDER_NAMES = {
  ollama: 'local model',
  openai: 'OpenAI-compatible',
  anthropic: 'Anthropic Claude',
}

export const useSuggestionsStore = defineStore('suggestions', () => {
  // null = not yet checked; false = feature off (render zero LLM UI)
  const enabled = ref(null)
  const model = ref('')
  const provider = ref('')
  const remote = ref(false)

  const providerLabel = computed(() => {
    if (!model.value) return ''
    const name = PROVIDER_NAMES[provider.value] || provider.value
    return name ? `${model.value} (${name})` : model.value
  })

  const variables = reactive({
    status: 'idle',
    reason: null,
    progress: { done: 0, total: 0 },
    byKey: {},
  })
  const values = reactive({
    status: 'idle',
    reason: null,
    progress: { done: 0, total: 0 },
    byKey: {},
  })

  // Marks are plain reactive objects keyed like the form state
  // (`${db}_${col}` / `${db}_${var}_${value}`) so views react per key.
  const applied = reactive({})
  const touched = reactive({})
  const dismissed = reactive({})

  let _pollTimer = null
  let _pollStartedAt = 0
  let _errorToastShown = false

  function _phaseState(phase) {
    return phase === 'values' ? values : variables
  }

  async function _loadMarks() {
    try {
      const stored = await db.getData('metadata', MARKS_KEY)
      for (const key of stored?.applied || []) applied[key] = true
      for (const key of stored?.touched || []) touched[key] = true
      for (const key of stored?.dismissed || []) dismissed[key] = true
    } catch {
      // Marks are cosmetic bookkeeping; a failed load must not block the page.
    }
  }

  async function _persistMarks() {
    try {
      await db.saveData('metadata', {
        key: MARKS_KEY,
        applied: Object.keys(applied).filter((k) => applied[k]),
        touched: Object.keys(touched).filter((k) => touched[k]),
        dismissed: Object.keys(dismissed).filter((k) => dismissed[k]),
        timestamp: new Date().toISOString(),
      })
    } catch {
      // Same as _loadMarks: never let bookkeeping break the flow.
    }
  }

  function _ingestVariablesSnapshot(snapshot) {
    for (const [database, columns] of Object.entries(snapshot.suggestions || {})) {
      for (const [column, entry] of Object.entries(columns)) {
        const key = `${database}_${column}`
        if (entry.status === 'done') {
          variables.byKey[key] = {
            status: 'done',
            database,
            column,
            variableKey: entry.variable_key,
            display: entry.variable_key ? formatToTitleCase(entry.variable_key) : null,
            confidence: entry.confidence,
            reason: entry.reason,
          }
        } else {
          variables.byKey[key] = { status: entry.status, database, column }
        }
      }
    }
  }

  function _ingestValuesSnapshot(snapshot) {
    for (const [database, columns] of Object.entries(snapshot.suggestions || {})) {
      for (const [localVariable, entry] of Object.entries(columns)) {
        for (const [value, suggestion] of Object.entries(entry.values || {})) {
          const key = `${database}_${localVariable}_${value}`
          values.byKey[key] = {
            status: entry.status,
            database,
            localVariable,
            value,
            termKey: suggestion.term_key,
            display: suggestion.term_key ? formatToTitleCase(suggestion.term_key) : null,
            confidence: suggestion.confidence,
            reason: suggestion.reason,
          }
        }
      }
    }
  }

  async function refresh(phase) {
    const state = _phaseState(phase)
    let snapshot
    try {
      const { data } = await api.get(`/api/v1/llm/suggestions/${phase}`)
      snapshot = data
    } catch {
      return
    }

    if (snapshot.enabled === false) {
      enabled.value = false
      stopPolling()
      return
    }

    state.status = snapshot.status
    state.reason = snapshot.error?.kind || null
    state.progress = {
      done: snapshot.progress?.chunks_done ?? 0,
      total: snapshot.progress?.chunks_total ?? 0,
    }
    if (phase === 'values') {
      _ingestValuesSnapshot(snapshot)
    } else {
      _ingestVariablesSnapshot(snapshot)
    }

    if (snapshot.status === 'failed' && !_errorToastShown) {
      _errorToastShown = true
      useStatusStore().warning(
        'AI suggestions are unavailable — please fill in the remaining fields manually.',
      )
    }
    if (TERMINAL_STATUSES.includes(snapshot.status)) {
      stopPolling()
    }
  }

  function startPolling(phase) {
    stopPolling()
    _pollStartedAt = Date.now()
    _pollTimer = setInterval(() => {
      if (Date.now() - _pollStartedAt > POLL_HARD_STOP_MS) {
        stopPolling()
        return
      }
      refresh(phase)
    }, POLL_INTERVAL_MS)
  }

  function stopPolling() {
    if (_pollTimer) {
      clearInterval(_pollTimer)
      _pollTimer = null
    }
  }

  function isPolling() {
    return _pollTimer !== null
  }

  async function init(phase, { mapping } = {}) {
    await _loadMarks()

    if (enabled.value === null) {
      try {
        const { data } = await api.get('/api/v1/llm/status')
        enabled.value = !!data.enabled
        model.value = data.model || ''
        provider.value = data.provider || ''
        remote.value = !!data.remote
      } catch {
        enabled.value = false
      }
    }
    if (!enabled.value) return

    const body = {}
    if (phase === 'variables' && mapping) body.mapping = mapping
    try {
      const { data } = await api.post(`/api/v1/llm/suggestions/${phase}/start`, body)
      if (data.status === 'disabled') {
        enabled.value = false
        return
      }
    } catch {
      // The snapshot poll below reports the job state either way.
    }

    await refresh(phase)
    if (!TERMINAL_STATUSES.includes(_phaseState(phase).status)) {
      startPolling(phase)
    }
  }

  async function bumpPriority(phase, database, columns, { retry = false } = {}) {
    const body =
      phase === 'values'
        ? { database, column: columns[0], retry }
        : { database, columns, retry }
    try {
      await api.post(`/api/v1/llm/suggestions/${phase}/priority`, body)
    } catch {
      return
    }
    if (retry) {
      const state = _phaseState(phase)
      for (const column of columns) {
        const key = phase === 'values' ? null : `${database}_${column}`
        if (key && state.byKey[key]) state.byKey[key] = { status: 'pending' }
      }
      if (!isPolling()) startPolling(phase)
    }
  }

  function markApplied(key) {
    applied[key] = true
    _persistMarks()
  }

  function markUserTouched(key) {
    if (applied[key]) {
      touched[key] = true
      _persistMarks()
    }
  }

  function dismiss(key) {
    dismissed[key] = true
    delete applied[key]
    delete touched[key]
    _persistMarks()
  }

  function isApplied(key) {
    return !!applied[key]
  }

  function isTouched(key) {
    return !!touched[key]
  }

  function isDismissed(key) {
    return !!dismissed[key]
  }

  // Returns the keys the view should clear (applied and never reviewed);
  // the view owns actually emptying the form fields.
  function unreviewedKeys() {
    return Object.keys(applied).filter((k) => applied[k] && !touched[k])
  }

  function clearAllApplied() {
    const cleared = unreviewedKeys()
    for (const key of cleared) {
      delete applied[key]
      delete touched[key]
    }
    _persistMarks()
    return cleared
  }

  return {
    enabled,
    model,
    provider,
    remote,
    providerLabel,
    variables,
    values,
    applied,
    touched,
    dismissed,
    init,
    refresh,
    startPolling,
    stopPolling,
    isPolling,
    bumpPriority,
    markApplied,
    markUserTouched,
    dismiss,
    isApplied,
    isTouched,
    isDismissed,
    unreviewedKeys,
    clearAllApplied,
  }
})
