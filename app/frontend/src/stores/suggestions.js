// Pinia store for rule-based (and future tier) mapping suggestions. Polls the
// backend suggestion jobs and exposes arriving suggestions plus the
// bookkeeping (applied / touched / dismissed marks) the describe views need
// to highlight them without ever overwriting user input. The store stays
// DOM-free: views own reading and writing actual field values.
//
// Adapted from the LLM branch's suggestions.js with provider-specific fields
// replaced by the tier-aware /status shape and the nested `suggestions`
// snapshot replaced by the flat `records` dict the SuggestionService returns.

import { defineStore } from 'pinia'
import { computed, reactive, ref } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import { formatToTitleCase } from '@/lib/jsonld'
import { useStatusStore } from '@/stores/status'

export const POLL_INTERVAL_MS = 2000
export const POLL_HARD_STOP_MS = 15 * 60 * 1000

const TERMINAL_STATUSES = ['done', 'failed', 'unavailable', 'disabled']
const MARKS_KEY_PREFIX = 'suggestion_marks_'

// Source-to-icon mapping for source-agnostic rendering. The badge component
// also uses this; exported here so tests can assert on it.
export const SOURCE_ICONS = {
  alias: 'fa-link',
  value_regex: 'fa-table-list',
  string: 'fa-text-width',
  embedding: 'fa-vector-square',
  llm: 'fa-robot',
  pasted_llm: 'fa-clipboard',
  manual: 'fa-hand',
}

export const useSuggestionsStore = defineStore('suggestions', () => {
  // null = not yet checked; false = feature off (render zero suggestion UI)
  const enabled = ref(null)
  const compute = ref('host')
  const tiers = ref({})
  const threshold = ref(0.8)
  const rulesVersion = ref(null)

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
  // Marks are stored per-phase so dismissing a variable suggestion doesn't
  // affect a value suggestion for the same column.
  const applied = reactive({})
  const touched = reactive({})
  const dismissed = reactive({})

  let _pollTimer = null
  let _pollStartedAt = 0
  let _errorToastShown = false

  function _phaseState(phase) {
    return phase === 'values' ? values : variables
  }

  function _marksKey(phase) {
    return `${MARKS_KEY_PREFIX}${phase}`
  }

  async function _loadMarks(phase) {
    try {
      const stored = await db.getData('metadata', _marksKey(phase))
      for (const key of stored?.applied || []) applied[key] = true
      for (const key of stored?.touched || []) touched[key] = true
      for (const key of stored?.dismissed || []) dismissed[key] = true
    } catch {
      // Marks are cosmetic bookkeeping; a failed load must not block the page.
    }
  }

  async function _persistMarks(phase) {
    try {
      await db.saveData('metadata', {
        key: _marksKey(phase),
        applied: Object.keys(applied).filter((k) => applied[k]),
        touched: Object.keys(touched).filter((k) => touched[k]),
        dismissed: Object.keys(dismissed).filter((k) => dismissed[k]),
        timestamp: new Date().toISOString(),
      })
    } catch {
      // Same as _loadMarks: never let bookkeeping break the flow.
    }
  }

  function _ingestRecords(phase, snapshot) {
    const state = _phaseState(phase)
    const records = snapshot.records || {}
    for (const [key, rec] of Object.entries(records)) {
      const display = rec.match ? formatToTitleCase(rec.match) : null
      state.byKey[key] = {
        status: rec.status || 'done',
        item: rec.item,
        match: rec.match,
        display,
        confidence: rec.confidence ?? 0,
        reason: rec.reason || '',
        source: rec.source || 'manual',
        tier: rec.tier ?? 1,
        alternatives: rec.alternatives || [],
      }
    }
  }

  async function refresh(phase) {
    const state = _phaseState(phase)
    let snapshot
    try {
      const { data } = await api.get(`/api/v1/suggestions/${phase}`)
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
      done: snapshot.progress?.done ?? 0,
      total: snapshot.progress?.total ?? 0,
    }
    _ingestRecords(phase, snapshot)

    if (snapshot.status === 'failed' && !_errorToastShown) {
      _errorToastShown = true
      useStatusStore().warning(
        'Mapping suggestions are unavailable — please fill in the remaining fields manually.',
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
    await _loadMarks(phase)

    if (enabled.value === null) {
      try {
        const { data } = await api.get('/api/v1/suggestions/status')
        enabled.value = !!data.enabled
        compute.value = data.compute || 'host'
        tiers.value = data.tiers || {}
        threshold.value = data.threshold ?? 0.8
        rulesVersion.value = data.rules_version || null
      } catch {
        enabled.value = false
      }
    }
    if (!enabled.value) return

    const body = {}
    if (phase === 'variables' && mapping) body.mapping = mapping
    try {
      const { data } = await api.post(
        `/api/v1/suggestions/${phase}/start`,
        body,
      )
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

  async function bumpPriority(phase, items, { retry = false } = {}) {
    try {
      await api.post(`/api/v1/suggestions/${phase}/priority`, {
        items,
        retry,
      })
    } catch {
      return
    }
    if (retry) {
      const state = _phaseState(phase)
      for (const item of items) {
        const key = item
        if (state.byKey[key]) state.byKey[key] = { ...state.byKey[key], status: 'pending' }
      }
      if (!isPolling()) startPolling(phase)
    }
  }

  function markApplied(key) {
    applied[key] = true
    _persistMarks(_currentPhase)
  }

  function markUserTouched(key) {
    if (applied[key]) {
      touched[key] = true
      _persistMarks(_currentPhase)
    }
  }

  function dismiss(key) {
    dismissed[key] = true
    delete applied[key]
    delete touched[key]
    _persistMarks(_currentPhase)
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
    _persistMarks(_currentPhase)
    return cleared
  }

  // Tracks which phase's marks are active for persistence. Set by the view
  // when it calls init(phase).
  let _currentPhase = 'variables'

  function setPhase(phase) {
    _currentPhase = phase
  }

  return {
    enabled,
    compute,
    tiers,
    threshold,
    rulesVersion,
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
    setPhase,
  }
})
