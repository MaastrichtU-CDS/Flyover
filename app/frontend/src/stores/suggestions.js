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
// After this many consecutive failed polls (backend unreachable) or idle
// polls (no job was ever created), the submit gate must stop waiting on
// suggestions; blocking the core flow forever is worse than proceeding
// without suggestions.
export const MAX_STALLED_POLLS = 3

const TERMINAL_STATUSES = ['done', 'failed', 'unavailable', 'disabled']
const MARKS_KEY_PREFIX = 'suggestion_marks_'
const COACHMARK_KEY = 'suggestion_coachmark_seen'

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

// One-line toast for an ingest result, e.g. "3 suggestions imported, 1
// left for you to decide, 2 already mapped". A nulled record is one whose
// match the server rejected (invalid key, or a variable already mapped
// in this database); the item stays with the human, hence the wording.
// Exported so the tests can assert on it.
export function ingestSummary({
  accepted = 0,
  nulled = 0,
  rejected = 0,
  skipped = 0,
  reopened = 0,
} = {}) {
  const parts = [`${accepted} ${accepted === 1 ? 'suggestion' : 'suggestions'} imported`]
  if (reopened) parts.push(`${reopened} shown again after a dismissal`)
  if (nulled) parts.push(`${nulled} left for you to decide`)
  if (rejected) parts.push(`${rejected} ignored (unknown column or value)`)
  if (skipped) parts.push(`${skipped} already mapped`)
  return parts.join(', ')
}

export const useSuggestionsStore = defineStore('suggestions', () => {
  // null = not yet checked; false = feature off (render zero suggestion UI)
  const enabled = ref(null)
  const compute = ref('host')
  const tiers = ref({})
  const threshold = ref(0.8)
  const rulesVersion = ref(null)
  // The copy-prompt / paste-answer round trip (issue 2). It needs no
  // model and no flag, so /status reports it active even when every tier
  // is off; null until /status answered.
  const promptExport = ref(null)
  // The site's "Items per prompt" default (FLYOVER_SUGGESTION_PROMPT_CHUNK,
  // clamped server-side) so the panel offers it instead of hardcoding 40.
  const promptExportChunk = ref(40)
  // Summary of the last successful ingest ({accepted, nulled, rejected,
  // skipped, messages, phase, database}); the panel and tests read it.
  const lastIngestResult = ref(null)

  const variables = reactive({
    status: 'idle',
    reason: null,
    progress: { done: 0, total: 0 },
    byKey: {},
    // True once the store stopped expecting suggestion results (poll
    // failures, no job, or the hard stop): views use it to fail open
    // instead of holding the submit gate closed forever.
    gaveUp: false,
  })
  const values = reactive({
    status: 'idle',
    reason: null,
    progress: { done: 0, total: 0 },
    byKey: {},
    gaveUp: false,
  })

  // Marks are plain reactive objects keyed like the form state
  // (`${db}_${col}` / `${db}_${col}_${value}`) so views react per key. They
  // are kept per phase so dismissing a variable suggestion can never
  // affect a value suggestion, and each phase carries the job fingerprint
  // its marks belong to: when a job with a different fingerprint arrives
  // (new dataset, changed rules) the marks expire, so a stale dismissal
  // can never hide fresh suggestions forever.
  function _emptyMarks() {
    return { applied: {}, touched: {}, dismissed: {}, matches: {}, fingerprint: null }
  }

  const marks = reactive({
    variables: _emptyMarks(),
    values: _emptyMarks(),
  })

  // First-visit cue (WS2): whether the user has already seen the
  // "review suggested mappings" coachmark per phase, persisted in the
  // metadata store as { variables, values }. `loaded` is false until
  // loadCoachmark() resolved, so a view never flashes the cue before the
  // persisted flags arrive.
  const coachmarkSeen = reactive({ loaded: false, variables: false, values: false })

  let _pollTimer = null
  let _pollStartedAt = 0
  let _errorToastShown = false
  // Consecutive polls that made no progress, per phase: failed GETs and
  // snapshots that are still 'idle' (no job was ever created).
  const _stalledPolls = { variables: 0, values: 0 }

  function _phaseState(phase) {
    return phase === 'values' ? values : variables
  }

  function _marksKey(phase) {
    return `${MARKS_KEY_PREFIX}${phase}`
  }

  async function _loadMarks(phase) {
    try {
      const stored = await db.getData('metadata', _marksKey(phase))
      const m = marks[phase]
      for (const key of stored?.applied || []) m.applied[key] = true
      for (const key of stored?.touched || []) m.touched[key] = true
      for (const key of stored?.dismissed || []) m.dismissed[key] = true
      // `matches` records what each mark was made against, so a later job
      // can expire marks per key (D3). Marks persisted before this field
      // exist simply carry no match and are expired wholesale.
      for (const [key, match] of Object.entries(stored?.matches || {})) {
        m.matches[key] = match
      }
      m.fingerprint = stored?.fingerprint || null
    } catch {
      // Marks are cosmetic bookkeeping; a failed load must not block the page.
    }
  }

  async function _persistMarks(phase) {
    const m = marks[phase]
    try {
      await db.saveData('metadata', {
        key: _marksKey(phase),
        applied: Object.keys(m.applied).filter((k) => m.applied[k]),
        touched: Object.keys(m.touched).filter((k) => m.touched[k]),
        dismissed: Object.keys(m.dismissed).filter((k) => m.dismissed[k]),
        matches: { ...m.matches },
        fingerprint: m.fingerprint,
        timestamp: new Date().toISOString(),
      })
    } catch {
      // Same as _loadMarks: never let bookkeeping break the flow.
    }
  }

  // Load the persisted coachmark "seen" flags. Wrapped in try/catch like
  // _loadMarks: if IndexedDB is unavailable the flags stay false and the
  // cue shows at most once per page session (markCoachmarkSeen keeps the
  // in-memory flag even when the persist fails).
  async function loadCoachmark() {
    try {
      const stored = await db.getData('metadata', COACHMARK_KEY)
      coachmarkSeen.variables = !!stored?.variables
      coachmarkSeen.values = !!stored?.values
    } catch {
      // Fall through: flags stay false until first close.
    }
    coachmarkSeen.loaded = true
  }

  async function markCoachmarkSeen(phase) {
    coachmarkSeen[phase] = true
    try {
      const stored = await db.getData('metadata', COACHMARK_KEY)
      await db.saveData('metadata', {
        ...(stored || {}),
        key: COACHMARK_KEY,
        [phase]: true,
        timestamp: new Date().toISOString(),
      })
    } catch {
      // The in-memory flag above already gives per-session behaviour.
    }
  }

  // Forget every review mark, in both phases. Uploading a semantic map
  // starts the describe flow over: the fields are rebuilt from the new map
  // (even when it is the same file), so a surviving mark would describe a
  // field that no longer holds what was reviewed ("reviewed" next to an
  // empty dropdown). The job fingerprint cannot catch this: the same map
  // over the same data yields the same job.
  async function resetMarks() {
    for (const phase of ['variables', 'values']) {
      const m = marks[phase]
      for (const bucket of ['applied', 'touched', 'dismissed', 'matches']) {
        for (const key of Object.keys(m[bucket])) delete m[bucket][key]
      }
      m.fingerprint = null
      await _persistMarks(phase)
    }
  }

  // Adopt or expire the phase's marks when a job fingerprint arrives.
  // Decision D3: expiry is per key — a new job keeps applied/touched/
  // dismissed marks for keys whose suggestion (match) is unchanged in
  // the new records and drops only the keys whose match changed or
  // disappeared. That satisfies the intent of the fingerprint (a stale
  // dismissal must not hide a DIFFERENT suggestion) without throwing
  // away reviews the user already did.
  function _syncMarksToFingerprint(phase, fingerprint) {
    const m = marks[phase]
    if (m.fingerprint === fingerprint) return
    if (m.fingerprint === null) {
      // Marks persisted before fingerprints existed: keep them once, adopt
      // the fingerprint so any later job change expires them.
      m.fingerprint = fingerprint
      _persistMarks(phase)
      return
    }
    const byKey = _phaseState(phase).byKey
    const markStillApplies = (key) => {
      const record = byKey[key]
      if (!record) return false
      // Marks written before match bookkeeping carry no match value:
      // conservatively drop them (they cannot be verified).
      if (!(key in m.matches)) return false
      return (m.matches[key] ?? null) === (record.match ?? null)
    }
    for (const key of Object.keys(m.applied)) {
      if (!markStillApplies(key)) {
        delete m.applied[key]
        delete m.touched[key]
        delete m.matches[key]
      }
    }
    for (const key of Object.keys(m.dismissed)) {
      if (!markStillApplies(key)) {
        delete m.dismissed[key]
        delete m.matches[key]
      }
    }
    m.fingerprint = fingerprint
    _persistMarks(phase)
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
        // Explicit location fields from the record; null when the backend
        // could not attribute them (e.g. the no-database fallback group).
        database: rec.database || null,
        column: rec.column || null,
        value: rec.value ?? null,
      }
    }
    if (snapshot.fingerprint) _syncMarksToFingerprint(phase, snapshot.fingerprint)
  }

  async function refresh(phase) {
    const state = _phaseState(phase)
    let snapshot
    try {
      const { data } = await api.get(`/api/v1/suggestions/${phase}`)
      snapshot = data
    } catch {
      // The backend may be unreachable. Count the stall so the submit
      // gate fails open instead of waiting on suggestions that can
      // never arrive.
      _noteStalledPoll(phase, state)
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
    } else if (snapshot.status === 'idle') {
      // No job exists; if nothing starts within a few polls, stop waiting.
      _noteStalledPoll(phase, state)
    } else {
      _stalledPolls[phase] = 0
    }
  }

  function _noteStalledPoll(phase, state) {
    _stalledPolls[phase] += 1
    if (_stalledPolls[phase] >= MAX_STALLED_POLLS) {
      state.gaveUp = true
      stopPolling()
    }
  }

  function startPolling(phase) {
    stopPolling()
    _pollStartedAt = Date.now()
    _pollTimer = setInterval(() => {
      if (Date.now() - _pollStartedAt > POLL_HARD_STOP_MS) {
        // Out of time budget: fail open rather than gating the submit
        // button on a job that is taking longer than any tier-1 job should.
        _phaseState(phase).gaveUp = true
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
    // A fresh page visit retries: reset the per-phase stall tracking.
    const state = _phaseState(phase)
    state.gaveUp = false
    _stalledPolls[phase] = 0

    await _loadMarks(phase)
    await loadCoachmark()

    if (enabled.value === null) {
      try {
        const { data } = await api.get('/api/v1/suggestions/status')
        enabled.value = !!data.enabled
        compute.value = data.compute || 'host'
        tiers.value = data.tiers || {}
        threshold.value = data.threshold ?? 0.8
        rulesVersion.value = data.rules_version || null
        promptExport.value = data.prompt_export?.state === 'active'
        promptExportChunk.value = data.prompt_export?.chunk || 40
      } catch {
        enabled.value = false
        promptExport.value = false
      }
    }
    if (!enabled.value) return

    // Suggestions target the variables of the browser's semantic map. With
    // no map (or one without variables) the dropdowns have nothing to show
    // them in, and the backend would fall back to whatever map its session
    // holds, so report "no semantic map" instead of starting a job.
    if (!Object.keys(mapping?.schema?.variables || {}).length) {
      const state = _phaseState(phase)
      state.status = 'unavailable'
      state.reason = 'no_semantic_map'
      return
    }

    // Mapping send policy: both phases send the browser's semantic map.
    // The describe pages work on the map in this browser's IndexedDB, and
    // the backend session may hold an older one (the describe-landing
    // upload never reaches it, and another browser may have started a job
    // on its own map). The backend runs the job on the body mapping
    // job-locally and never lets it overwrite the session's mapping.
    let data
    try {
      ;({ data } = await api.post(`/api/v1/suggestions/${phase}/start`, {
        mapping,
      }))
    } catch {
      data = null
    }
    if (data?.status === 'disabled') {
      enabled.value = false
      return
    }

    await refresh(phase)
    if (!TERMINAL_STATUSES.includes(_phaseState(phase).status)) {
      startPolling(phase)
    }
  }

  async function bumpPriority(phase, items, { retry = false } = {}) {
    // With the feature off there is no job to steer: the describe pages
    // must not talk to the suggestions API at all (beyond /status).
    if (enabled.value === false) return
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

  // Compose the prompt for one database and phase on the server. The
  // browser's semantic map goes along (the describe pages work on it) so
  // the "already mapped" context and the candidate list match what the
  // user sees. Returns the response payload; throws on failure with a
  // readable message on error.message.
  async function fetchPrompt(phase, database, { mapping, chunk } = {}) {
    const body = { phase, database, mapping }
    if (chunk) body.chunk = chunk
    try {
      const { data } = await api.post('/api/v1/suggestions/prompt', body)
      return data
    } catch (err) {
      throw new Error(_apiMessage(err, 'Could not generate the prompt.'))
    }
  }

  // Send the pasted LLM answer to the server, which validates it and
  // merges it into the phase's job as pasted_llm suggestions. Nothing
  // touches the JSON-LD or the review marks: the merged snapshot arrives
  // through the same path as a poll. Returns the ingest summary; throws
  // with a readable message when the paste is unusable.
  async function ingest(phase, database, { answer, records, mapping } = {}) {
    // A dismissal judges one suggestion, not the field: pasting an answer
    // asks for a new one, so the dismissed keys go along and the server
    // lets the paste take those fields (it answers which it re-opened).
    const dismissed = Object.keys(marks[phase].dismissed).filter((k) => marks[phase].dismissed[k])
    const body = { database, source: 'pasted_llm', mapping, dismissed }
    if (answer !== undefined) body.answer = answer
    if (records !== undefined) body.records = records
    let data
    try {
      ;({ data } = await api.post(`/api/v1/suggestions/${phase}/ingest`, body))
    } catch (err) {
      throw new Error(_apiMessage(err, 'Could not import the answer.'))
    }
    // A paste makes the suggestion UI relevant even on a stack with every
    // tier off: the pills must render the imported records.
    enabled.value = true
    // Drop the dismissals the server re-opened before the records land, so
    // the pre-fill watchers treat those fields as fresh. Applied/touched
    // marks are not touched here: a reviewed field is not re-opened.
    const reopened = Array.isArray(data.reopened) ? data.reopened : []
    if (reopened.length) {
      const m = marks[phase]
      for (const key of reopened) {
        delete m.dismissed[key]
        delete m.matches[key]
      }
      _persistMarks(phase)
    }
    if (data.job) {
      const state = _phaseState(phase)
      state.status = data.job.status || 'done'
      state.reason = null
      state.progress = {
        done: data.job.progress?.done ?? 0,
        total: data.job.progress?.total ?? 0,
      }
      _ingestRecords(phase, data.job)
    }
    lastIngestResult.value = {
      phase,
      database,
      accepted: data.accepted ?? 0,
      nulled: data.nulled ?? 0,
      rejected: data.rejected ?? 0,
      skipped: data.skipped ?? 0,
      reopened: reopened.length,
      messages: data.messages || [],
    }
    useStatusStore().success(ingestSummary(lastIngestResult.value))
    return lastIngestResult.value
  }

  function _apiMessage(err, fallback) {
    const body = err?.response?.data
    if (body && typeof body.error === 'string' && body.error) return body.error
    return fallback
  }

  function _currentMarks() {
    return marks[_currentPhase.value]
  }

  // Remember which record (its match) a mark was made against, so a job
  // with a new fingerprint can expire marks per key (D3).
  function _rememberMatch(key) {
    const m = _currentMarks()
    const record = _phaseState(_currentPhase.value).byKey[key]
    m.matches[key] = record?.match ?? null
  }

  function markApplied(key) {
    _currentMarks().applied[key] = true
    _rememberMatch(key)
    _persistMarks(_currentPhase.value)
  }

  function markUserTouched(key) {
    const m = _currentMarks()
    if (m.applied[key]) {
      m.touched[key] = true
      _persistMarks(_currentPhase.value)
    }
  }

  function dismiss(key) {
    const m = _currentMarks()
    m.dismissed[key] = true
    _rememberMatch(key)
    delete m.applied[key]
    delete m.touched[key]
    _persistMarks(_currentPhase.value)
  }

  function isApplied(key) {
    return !!_currentMarks().applied[key]
  }

  function isTouched(key) {
    return !!_currentMarks().touched[key]
  }

  // Only a confident column suggestion (at or above the tier threshold
  // from /status) is pre-filled on the variables page. Below the threshold
  // the cascade treats a record as "escalate to the next tier", so the
  // page shows it as a hint the user can accept, never as a pre-filled
  // answer. The values page does not use it: value scores are calibrated
  // differently (see DescribeVariableDetailsView's pre-fill watcher).
  function isConfident(record) {
    return (record?.confidence ?? 0) >= threshold.value
  }

  function isDismissed(key) {
    return !!_currentMarks().dismissed[key]
  }

  // Returns the keys the view should clear (applied and never reviewed);
  // the view owns actually emptying the form fields.
  function unreviewedKeys() {
    const m = _currentMarks()
    return Object.keys(m.applied).filter((k) => m.applied[k] && !m.touched[k])
  }

  // Returns the keys the view should clear (applied and never reviewed);
  // the view owns actually emptying the form fields. Every cleared key is
  // also recorded as dismissed: without the dismissal the pre-fill watch
  // would fill the same fields again from the same records on the next
  // visit, silently undoing the "clear all".
  function clearAllApplied() {
    const m = _currentMarks()
    const cleared = unreviewedKeys()
    for (const key of cleared) {
      m.dismissed[key] = true
      delete m.applied[key]
      delete m.touched[key]
    }
    _persistMarks(_currentPhase.value)
    return cleared
  }

  // Tracks which phase's marks are active for persistence. Set by the view
  // when it calls init(phase). It is a ref, not a plain let: computeds that
  // read the active phase's marks (unreviewedKeys and friends) must
  // re-evaluate when the phase switches, otherwise the details view's
  // unreviewed count caches against the variables marks and freezes at 0.
  const _currentPhase = ref('variables')

  function setPhase(phase) {
    _currentPhase.value = phase
  }

  return {
    enabled,
    compute,
    tiers,
    threshold,
    rulesVersion,
    promptExport,
    promptExportChunk,
    lastIngestResult,
    variables,
    values,
    marks,
    coachmarkSeen,
    init,
    fetchPrompt,
    ingest,
    refresh,
    startPolling,
    stopPolling,
    isPolling,
    bumpPriority,
    loadCoachmark,
    markCoachmarkSeen,
    resetMarks,
    markApplied,
    markUserTouched,
    dismiss,
    isApplied,
    isTouched,
    isDismissed,
    isConfident,
    unreviewedKeys,
    clearAllApplied,
    setPhase,
  }
})
