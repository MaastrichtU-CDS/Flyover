<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, nextTick, watch } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'
import { formatToTitleCase } from '@/lib/jsonld'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'
import SuggestionBadge from '@/components/SuggestionBadge.vue'
import SuggestionCoachmark from '@/components/SuggestionCoachmark.vue'
import SuggestionStatusBar from '@/components/SuggestionStatusBar.vue'

const status = useStatusStore()
const suggestions = useSuggestionsStore()

const PAGE_SIZE = 10
const AUTO_FILL_FEEDBACK_MS = 3000

const columnInfoData = ref(null)
const databasePages = reactive({})
const expandedDatabases = reactive({})
const formStateCache = reactive({})
const preselectedDescriptions = ref({})
const preselectedDatatypes = ref({})
const descriptionToDatatype = ref({})
const globalVariableNames = ref([])
const autoFilledFields = reactive(new Set())
const manualOverrides = reactive(new Set())
const feedbackVisible = reactive({})
const isSubmitting = ref(false)
const loadingIconIsPen = ref(false)
let _loadingInterval = null

const databaseNames = computed(() =>
  columnInfoData.value ? Object.keys(columnInfoData.value) : []
)

function totalPages(dbName) {
  const cols = columnInfoData.value?.[dbName] || []
  return Math.max(1, Math.ceil(cols.length / PAGE_SIZE))
}

function currentPageItems(dbName) {
  const cols = columnInfoData.value?.[dbName] || []
  const page = databasePages[dbName] || 1
  return cols.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)
}

function getDescriptionValue(dbName, item) {
  const key = `${dbName}_${item}`
  if (formStateCache[key]?.description) return formStateCache[key].description
  return preselectedDescriptions.value[key] || ''
}

function getDatatypeValue(dbName, item) {
  const key = `${dbName}_${item}`
  if (formStateCache[key]?.datatype) return formStateCache[key].datatype
  return preselectedDatatypes.value[key] || ''
}

function getCommentValue(dbName, item) {
  const key = `${dbName}_${item}`
  return formStateCache[key]?.comment || ''
}

function ensureCacheEntry(key, dbName) {
  if (!formStateCache[key]) {
    formStateCache[key] = { database: dbName, description: '', datatype: '', comment: '' }
  } else {
    formStateCache[key].database = dbName
  }
}

// Keys of every (database, column) pair the user can actually see — i.e. every
// CSV column the backend reported. A preselection whose key isn't in this set
// references a column that doesn't exist in the loaded CSV; we treat it as a
// no-op so it doesn't disable global-variable options for real columns.
const visibleColumnKeys = computed(() => {
  const s = new Set()
  if (!columnInfoData.value) return s
  for (const [dbName, cols] of Object.entries(columnInfoData.value)) {
    for (const item of cols || []) s.add(`${dbName}_${item}`)
  }
  return s
})

const selectedDescriptionsByDb = computed(() => {
  const out = {}
  for (const [key, cached] of Object.entries(formStateCache)) {
    if (cached?.description && cached.description !== 'Other' && cached.database) {
      if (!out[cached.database]) out[cached.database] = {}
      out[cached.database][cached.description] = key
    }
  }
  for (const [key, desc] of Object.entries(preselectedDescriptions.value)) {
    if (!desc || desc === 'Other') continue
    if (formStateCache[key]) continue
    if (!visibleColumnKeys.value.has(key)) continue
    // Keys are "${dbName}_${localColumn}" and dbName can itself contain
    // underscores (e.g. "synthetic_dutch_150"), so naive splitting truncates
    // the name. Look up the actual dbName by prefix-matching.
    const dbName = databaseNames.value.find((d) => key.startsWith(`${d}_`))
    if (!dbName) continue
    if (!out[dbName]) out[dbName] = {}
    if (!out[dbName][desc]) out[dbName][desc] = key
  }
  return out
})

function isDescriptionDisabled(dbName, item, optionValue) {
  if (!optionValue || optionValue === 'Other') return false
  const key = `${dbName}_${item}`
  const used = selectedDescriptionsByDb.value[dbName]?.[optionValue]
  return used != null && used !== key
}

function autoPopulateDatatype(dbName, item) {
  const key = `${dbName}_${item}`
  const desc = getDescriptionValue(dbName, item)
  if (desc && desc !== 'Other') {
    const suggested =
      preselectedDatatypes.value[key] || descriptionToDatatype.value[desc]
    if (suggested) {
      const cur = formStateCache[key]?.datatype
      if (!cur || !manualOverrides.has(key)) {
        ensureCacheEntry(key, dbName)
        formStateCache[key].datatype = suggested
        autoFilledFields.add(key)
        feedbackVisible[key] = true
        setTimeout(() => {
          feedbackVisible[key] = false
        }, AUTO_FILL_FEEDBACK_MS)
      }
    }
  } else if (autoFilledFields.has(key) && !manualOverrides.has(key)) {
    if (formStateCache[key]) formStateCache[key].datatype = ''
    autoFilledFields.delete(key)
    feedbackVisible[key] = false
  }
}

function onDescriptionChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  const value = e.target.value
  formStateCache[key].description = value
  // Deselecting must fully clear the preselected hint too; otherwise the stale
  // value leaks back via preselectedHiddenEntries and reappears in the details
  // view as a ghost "none" row.
  if (!value) {
    delete preselectedDescriptions.value[key]
    delete preselectedDatatypes.value[key]
  }
  autoPopulateDatatype(dbName, item)
  suggestions.markUserTouched(key)
  syncToIndexedDB()
}

function onDatatypeChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].datatype = e.target.value
  if (autoFilledFields.has(key)) manualOverrides.add(key)
  // Deliberately no markUserTouched here: changing the datatype is not a
  // review of the description the suggestion pre-filled (WS1.6).
  syncToIndexedDB()
}

function onCommentChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].comment = e.target.value
  syncToIndexedDB()
}

// ---------------------------------------------------------------------------
// Mapping suggestions. Per decision D1 of the tier-1 remediation the watcher
// PRE-FILLS the dropdown for display, but a pre-filled value is never
// persisted to the JSON-LD: syncToIndexedDB drops applied-but-unreviewed
// keys, and only an explicit review (accept via the badge, or a manual
// dropdown change) writes the mapping — exactly as a hand-picked value
// would. The user must click the badge or change the dropdown to mark it
// as "reviewed" before they can submit.
// ---------------------------------------------------------------------------

function suggestionFor(dbName, item) {
  return suggestions.variables.byKey[`${dbName}_${item}`]
}

// A live suggestion: arrived, has a match, and was not dismissed. A
// dismissed suggestion must not render its pill again — dismissal also
// drops the applied mark, so the badge would fall back to the unreviewed
// look and invite a second dismissal that does nothing.
function hasSuggestion(dbName, item) {
  const entry = suggestionFor(dbName, item)
  if (suggestions.isDismissed(`${dbName}_${item}`)) return false
  return entry && entry.status === 'done' && entry.display
}

// True while a suggestion for this column still needs review: either it
// pre-filled the field (applied, never touched) or it arrived for an
// empty field the pre-fill watch could not fill (e.g. the
// one-variable-per-database constraint blocked it). Reviewed and
// dismissed suggestions lose the highlight — reviewed fields turn green
// via the badge instead (WS1.3).
function needsSuggestionReview(dbName, item) {
  const key = `${dbName}_${item}`
  if (suggestions.isDismissed(key)) return false
  if (suggestions.isApplied(key)) return !suggestions.isTouched(key)
  if (!hasSuggestion(dbName, item)) return false
  return !formStateCache[key]?.description
}

function acceptSuggestion(dbName, item, display) {
  const entry = suggestionFor(dbName, item)
  const value = display || entry?.display
  if (!value) return
  const key = `${dbName}_${item}`
  // Check the one-variable-per-database constraint before applying. A
  // blocked accept must say why: silently doing nothing reads as a broken
  // button, and for a conflict loser's alternative it is the normal case.
  if (isDescriptionDisabled(dbName, item, value)) {
    const holder = selectedDescriptionsByDb.value[dbName]?.[value] || ''
    const holderColumn = holder.startsWith(`${dbName}_`)
      ? holder.slice(dbName.length + 1)
      : holder
    status.warning(
      `'${value}' is already used by column '${holderColumn}' in ${dbName}; change that column first to map '${item}' to it.`
    )
    return
  }
  // Mark applied first: the explicit accept is a review, so the
  // markUserTouched inside onDescriptionChange must find the applied mark
  // and mark the field reviewed (WS1.4 — an explicit accept must never
  // leave the field in the "needs review" state).
  suggestions.markApplied(key)
  // Go through the same path as a manual selection.
  onDescriptionChange(dbName, item, { target: { value } })
  maybeCloseCoachmark()
}

// Applying an alternative from the badge popover goes through the same
// accept path as the suggestion itself; the alternative only differs in
// which value it puts in the dropdown.
function applyAlternative(dbName, item, alt) {
  if (!alt?.match) return
  acceptSuggestion(dbName, item, formatToTitleCase(alt.match))
}

function dismissSuggestion(dbName, item) {
  const key = `${dbName}_${item}`
  // Only a field the suggestion pre-filled (applied and never reviewed) is
  // cleared on dismissal; a manually chosen value must survive it.
  const prefilled = suggestions.isApplied(key) && !suggestions.isTouched(key)
  suggestions.dismiss(key)
  maybeCloseCoachmark()
  if (prefilled && formStateCache[key]?.description) {
    formStateCache[key].description = ''
    autoPopulateDatatype(dbName, item)
    syncToIndexedDB()
  }
}

function clearAllSuggestions() {
  for (const key of suggestions.clearAllApplied()) {
    const entry = suggestions.variables.byKey[key]
    // Prefer the record's explicit location fields; fall back to prefix
    // matching for records without them (fallback groups).
    const dbName =
      entry?.database || databaseNames.value.find((d) => key.startsWith(`${d}_`))
    const item = entry?.column || (dbName ? key.slice(dbName.length + 1) : null)
    if (!dbName || !item) continue
    if (formStateCache[key]?.description) {
      formStateCache[key].description = ''
      autoPopulateDatatype(dbName, item)
    }
  }
  syncToIndexedDB()
}

function pendingColumnsFor(dbName) {
  const cols = columnInfoData.value?.[dbName] || []
  return cols.filter((item) => {
    const entry = suggestionFor(dbName, item)
    return !entry || entry.status === 'pending'
  })
}

function hasUnreviewedForDatabase(dbName) {
  const cols = columnInfoData.value?.[dbName] || []
  return cols.some((item) => {
    const key = `${dbName}_${item}`
    const entry = suggestionFor(dbName, item)
    if (!entry || entry.status !== 'done' || !entry.display) return false
    if (suggestions.isDismissed(key)) return false
    if (!suggestions.isApplied(key) && !formStateCache[key]?.description) return true
    if (suggestions.isApplied(key) && !suggestions.isTouched(key)) return true
    return false
  })
}

function dismissAllForDatabase(dbName) {
  const cols = columnInfoData.value?.[dbName] || []
  for (const item of cols) {
    const key = `${dbName}_${item}`
    const entry = suggestionFor(dbName, item)
    if (!entry || entry.status !== 'done' || !entry.display) continue
    if (suggestions.isDismissed(key)) continue
    if (suggestions.isApplied(key) && suggestions.isTouched(key)) continue
    // Only pre-filled, unreviewed fields are cleared; manually chosen
    // values survive the dismissal.
    const prefilled = suggestions.isApplied(key) && !suggestions.isTouched(key)
    suggestions.dismiss(key)
    if (prefilled && formStateCache[key]?.description) {
      formStateCache[key].description = ''
      autoPopulateDatatype(dbName, item)
    }
  }
  syncToIndexedDB()
}

function requestSectionFirst(dbName) {
  const pending = pendingColumnsFor(dbName)
  if (pending.length) {
    const keys = pending.map((c) => `${dbName}_${c}`)
    suggestions.bumpPriority('variables', keys)
  }
}

const unreviewedFieldCount = computed(
  () =>
    suggestions
      .unreviewedKeys()
      .filter((key) => formStateCache[key]?.description).length
)

function jumpToNextUnreviewed() {
  const keys = suggestions.unreviewedKeys().filter((key) => formStateCache[key]?.description)
  if (!keys.length) return
  // Find the first unreviewed key and locate its database + column,
  // preferring the record's explicit location fields.
  for (const key of keys) {
    const entry = suggestions.variables.byKey[key]
    const dbName =
      entry?.database || databaseNames.value.find((d) => key.startsWith(`${d}_`))
    if (!dbName) continue
    const item = entry?.column || key.slice(dbName.length + 1)
    const cols = columnInfoData.value?.[dbName] || []
    const itemIdx = cols.indexOf(item)
    if (itemIdx === -1) continue
    // Expand the database and navigate to the right page.
    if (!expandedDatabases[dbName]) expandedDatabases[dbName] = true
    const page = Math.floor(itemIdx / PAGE_SIZE) + 1
    databasePages[dbName] = page
    // Scroll to the row after Vue updates the DOM.
    nextTick(() => {
      const el = document.getElementById(`ncit_comment_${dbName}_${item}`)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
        el.focus({ preventScroll: true })
      }
    })
    return
  }
}

// ---------------------------------------------------------------------------
// First-visit cue (WS2): a small non-modal callout on the first unreviewed
// suggestion pill, shown once per phase. It tells the user the fields were
// pre-filled for them and must be reviewed before they can continue. The
// "How do suggestions work?" link in the status bar reopens it.
// ---------------------------------------------------------------------------

const COACHMARK_COPY = {
  title: 'Review suggested mappings',
  body: 'Flyover filled in this field from its mapping suggestions. Nothing is saved until you review it: check the dropdown, then click the pill to confirm, or × to dismiss. You can continue once every suggestion is reviewed.',
}

// True when the user reopened the cue via the status-bar link; bypasses
// the persisted "seen" flag until closed again.
const coachmarkRequested = ref(false)

const showCoachmark = computed(
  () =>
    suggestions.enabled &&
    suggestions.variables.status === 'done' &&
    unreviewedFieldCount.value > 0 &&
    (coachmarkRequested.value ||
      (suggestions.coachmarkSeen.loaded && !suggestions.coachmarkSeen.variables)),
)

function unreviewedCountForDatabase(dbName) {
  const cols = columnInfoData.value?.[dbName] || []
  return cols.filter((item) => needsSuggestionReview(dbName, item)).length
}

// The callout anchors to the first unreviewed pill in display order on
// the current page. Databases start collapsed, so when no pill is
// rendered it anchors to the header of the first database that has
// suggestions; once expanded it moves to the first pill.
const coachmarkTarget = computed(() => {
  if (!showCoachmark.value) return null
  for (const dbName of databaseNames.value) {
    if (!expandedDatabases[dbName]) continue
    for (const item of currentPageItems(dbName)) {
      if (needsSuggestionReview(dbName, item)) {
        return { type: 'badge', key: `${dbName}_${item}` }
      }
    }
  }
  for (const dbName of databaseNames.value) {
    if (unreviewedCountForDatabase(dbName) > 0) {
      return { type: 'header', dbName }
    }
  }
  return null
})

function coachmarkHeaderBody(dbName) {
  const n = unreviewedCountForDatabase(dbName)
  return `Suggestions ready for ${n} column${n === 1 ? '' : 's'} — expand to review.`
}

function closeCoachmark() {
  coachmarkRequested.value = false
  suggestions.markCoachmarkSeen('variables')
}

// Accepting or dismissing a suggestion while the cue is visible counts as
// having seen it.
function maybeCloseCoachmark() {
  if (coachmarkTarget.value) closeCoachmark()
}

// Pre-fill: when a suggestion arrives for a column that the user hasn't
// touched yet, auto-set the description dropdown to the suggested value
// and mark it as "applied" (unreviewed). The value lives in the form
// state only — syncToIndexedDB strips applied-but-unreviewed keys, so
// nothing reaches the JSON-LD until the user reviews the field. The user
// must click the badge or change the dropdown to mark it as "reviewed"
// before they can submit.
watch(
  () => suggestions.variables.byKey,
  (byKey) => {
    if (!suggestions.enabled) return
    for (const [key, entry] of Object.entries(byKey)) {
      if (entry.status !== 'done' || !entry.display) continue
      if (suggestions.isDismissed(key)) continue
      if (suggestions.isTouched(key)) continue
      // Re-fill applied keys too: on a hard reload the in-memory form state
      // is lost but the "applied" mark survives in IndexedDB, so restore the
      // field from the suggestion. The existing-value guard below keeps user
      // input safe and dedups repeated watch firings.
      const existing = formStateCache[key]?.description
      if (existing) continue
      // Prefer the record's explicit location fields (database names can
      // contain underscores, making prefix matching ambiguous); fall back
      // to prefix matching for records without them.
      const dbName =
        entry.database ||
        databaseNames.value.find((d) => key.startsWith(`${d}_`))
      if (!dbName) continue
      const item = entry.column || key.slice(dbName.length + 1)
      // Check the one-variable-per-database constraint.
      if (isDescriptionDisabled(dbName, item, entry.display)) continue
      ensureCacheEntry(key, dbName)
      formStateCache[key].description = entry.display
      autoPopulateDatatype(dbName, item)
      suggestions.markApplied(key)
    }
    // No syncToIndexedDB here: a pre-fill is display-only until reviewed.
  },
  { deep: true },
)

async function syncToIndexedDB() {
  // Applied-but-unreviewed pre-fills are display-only (WS1.1): leave their
  // columns out of the payload so updateMappingFromForm — which groups by
  // the keys it receives — never writes or tombstones them. Absent keys
  // are untouched by its passes, so the JSON-LD keeps whatever the user
  // last reviewed.
  const payload = {}
  for (const [key, cached] of Object.entries(formStateCache)) {
    if (suggestions.isApplied(key) && !suggestions.isTouched(key)) continue
    payload[key] = cached
  }
  try {
    await jsonld.updateMappingFromForm(payload)
  } catch (err) {
    console.error('Failed to sync to IndexedDB:', err)
  }
}

function toggleDatabase(dbName) {
  expandedDatabases[dbName] = !expandedDatabases[dbName]
  if (expandedDatabases[dbName]) {
    // Hint the visible columns to the suggestion job when expanded.
    const visible = currentPageItems(dbName).map((c) => `${dbName}_${c}`)
    if (visible.length) suggestions.bumpPriority('variables', visible)
  }
}

function changePage(dbName, direction) {
  const cur = databasePages[dbName] || 1
  const next = cur + direction
  if (next >= 1 && next <= totalPages(dbName)) databasePages[dbName] = next
}

const hasAnyDescription = computed(() => {
  for (const e of Object.values(formStateCache)) {
    if (e?.description) return true
  }
  for (const v of Object.values(preselectedDescriptions.value)) {
    if (v) return true
  }
  return false
})

// True while suggestions are still expected (job pending/running, or no
// snapshot yet) AND the store has not given up on them. The submit gate
// deliberately fails open when waiting can no longer make progress: a
// broken poll or a stalled job must never block the core flow.
const waitingForSuggestions = computed(
  () =>
    suggestions.enabled &&
    !suggestions.variables.gaveUp &&
    ['idle', 'pending', 'running'].includes(suggestions.variables.status)
)

const canSubmit = computed(() => {
  if (!hasAnyDescription.value || isSubmitting.value) return false
  // Block submission while suggestions are still loading and we have
  // unreviewed pre-filled fields. Also block while the suggestion job
  // is still running (status is 'idle' or 'running') and suggestions
  // are enabled, to prevent submitting before pre-fill arrives.
  if (unreviewedFieldCount.value > 0) return false
  if (waitingForSuggestions.value) return false
  return true
})

const submitTooltip = computed(() => {
  if (!hasAnyDescription.value) return 'Fill in at least one description first'
  if (waitingForSuggestions.value)
    return 'Waiting for mapping suggestions to arrive...'
  if (unreviewedFieldCount.value > 0)
    return `${unreviewedFieldCount.value} ${unreviewedFieldCount.value === 1 ? 'suggestion needs' : 'suggestions need'} review — click each highlighted badge to confirm or change the dropdown`
  return ''
})

const hiddenFieldEntries = computed(() => {
  const visible = new Set()
  for (const dbName of databaseNames.value) {
    for (const item of currentPageItems(dbName)) {
      visible.add(`${dbName}_${item}`)
    }
  }
  const out = {}
  for (const [key, cached] of Object.entries(formStateCache)) {
    if (!visible.has(key)) out[key] = cached
  }
  return out
})

const preselectedHiddenEntries = computed(() => {
  const visible = new Set()
  for (const dbName of databaseNames.value) {
    for (const item of currentPageItems(dbName)) {
      visible.add(`${dbName}_${item}`)
    }
  }
  const out = {}
  for (const [key, desc] of Object.entries(preselectedDescriptions.value)) {
    if (!visible.has(key) && !formStateCache[key]) out[key] = desc
  }
  return out
})

const loadingIconClass = computed(() =>
  loadingIconIsPen.value ? 'fa-pen' : 'fa-edit'
)

function startLoadingAnimation() {
  isSubmitting.value = true
  loadingIconIsPen.value = false
  _loadingInterval = setInterval(() => {
    loadingIconIsPen.value = !loadingIconIsPen.value
  }, 1000)
}

function resetSubmitState() {
  isSubmitting.value = false
  loadingIconIsPen.value = false
  if (_loadingInterval) {
    clearInterval(_loadingInterval)
    _loadingInterval = null
  }
}

// The form does a native POST → server redirect → SPA mounts variable-details.
// When the user clicks Back, the browser restores this page from BFCache with
// isSubmitting still true and the interval still ticking — reset on restore.
function onPageShow(e) {
  if (e.persisted) resetSubmitState()
}

function onFormSubmit(e) {
  if (!canSubmit.value) {
    e.preventDefault()
    return
  }
  startLoadingAnimation()
  // native form POSTs to /units → redirects to /describe/variable-details
}

async function loadAndApplySemanticMapping() {
  try {
    await jsonld.loadFromIndexedDB()
    globalVariableNames.value = jsonld
      .getGlobalVariableNames()
      .filter((n) => n !== 'Other')
    const ps = jsonld.computePreselectionsForDatabases(databaseNames.value)
    preselectedDescriptions.value = ps.preselectedDescriptions || {}
    preselectedDatatypes.value = ps.preselectedDatatypes || {}
    descriptionToDatatype.value = ps.descriptionToDatatype || {}
  } catch (err) {
    console.error('Failed to load semantic mapping:', err)
  }
}

onMounted(async () => {
  window.addEventListener('pageshow', onPageShow)
  try {
    const { data } = await api.get('/api/v1/describe-variables-state')
    columnInfoData.value = data.column_info || {}
    if (Object.keys(columnInfoData.value).length) {
      await db.saveData('metadata', {
        key: 'column_info',
        data: columnInfoData.value,
        timestamp: new Date().toISOString(),
      })
    }
  } catch {
    const cached = await db.getData('metadata', 'column_info')
    columnInfoData.value = cached?.data || {}
  }

  for (const dbName of Object.keys(columnInfoData.value || {})) {
    databasePages[dbName] = 1
    expandedDatabases[dbName] = false
  }

  await loadAndApplySemanticMapping()
  // Recompute once column info is in (so the database list is right)
  await nextTick()
  const ps = jsonld.computePreselectionsForDatabases(databaseNames.value)
  preselectedDescriptions.value = ps.preselectedDescriptions || {}
  preselectedDatatypes.value = ps.preselectedDatatypes || {}
  descriptionToDatatype.value = ps.descriptionToDatatype || {}
  dropOrphanPreselections()

  // Idempotent start (the backend usually began at ingest); the mapping is
  // included in case it only survives in this browser's IndexedDB.
  suggestions.setPhase('variables')
  await suggestions.init('variables', { mapping: jsonld.getMapping() })
})

function dropOrphanPreselections() {
  // Orphans = JSON-LD preselections for columns that don't exist in the loaded
  // CSV. Without this, they silently occupy global-variable slots and disable
  // dropdown options for the columns that do exist.
  //
  // Batch the filter into single ref assignments rather than per-key deletes —
  // each delete on a Vue ref triggers reactivity, which can stack up if there
  // are many orphans and ripple into expensive re-renders.
  const visible = visibleColumnKeys.value
  if (!visible.size) return
  const keptDescriptions = {}
  const keptDatatypes = {}
  const dropped = []
  for (const [key, val] of Object.entries(preselectedDescriptions.value)) {
    if (visible.has(key)) {
      keptDescriptions[key] = val
    } else {
      dropped.push(key)
    }
  }
  if (!dropped.length) return
  for (const [key, val] of Object.entries(preselectedDatatypes.value)) {
    if (visible.has(key)) keptDatatypes[key] = val
  }
  preselectedDescriptions.value = keptDescriptions
  preselectedDatatypes.value = keptDatatypes

  const preview = dropped.slice(0, 3).join(', ')
  const rest = dropped.length > 3 ? ` (and ${dropped.length - 3} more)` : ''
  status.warning(
    `Ignoring ${dropped.length} JSON-LD mapping${
      dropped.length === 1 ? '' : 's'
    } whose column${dropped.length === 1 ? '' : 's are'} not in the loaded CSV: ${preview}${rest}`
  )
}

onBeforeUnmount(() => {
  window.removeEventListener('pageshow', onPageShow)
  if (_loadingInterval) clearInterval(_loadingInterval)
  suggestions.stopPolling()
})
</script>

<template>
  <div>
    <h1><i class="fas fa-pencil-ruler" /> Describe your data</h1>
    <hr>
    <p>
      Please inspect the variables (i.e. columns) of your database(s).<br>
      For every database that you would like to describe, please select the type and
      description of your columns from the drop-down menu.
    </p>

    <SuggestionStatusBar
      v-if="suggestions.enabled"
      :phase-state="suggestions.variables"
      :tiers="suggestions.tiers"
      :compute="suggestions.compute"
      :unreviewed-count="unreviewedFieldCount"
      @clear-all="clearAllSuggestions"
      @show-coachmark="coachmarkRequested = true"
    />

    <form
      class="form-horizontal"
      method="POST"
      action="/units"
      @submit="onFormSubmit"
    >
      <hr>

      <div>
        <div
          v-for="dbName in databaseNames"
          :key="dbName"
        >
          <h2 class="database-heading">
            <i class="fas fa-database" /> {{ dbName }}
            <SuggestionCoachmark
              v-if="coachmarkTarget?.type === 'header' && coachmarkTarget.dbName === dbName"
              :title="COACHMARK_COPY.title"
              :body="coachmarkHeaderBody(dbName)"
              @close="closeCoachmark"
            />
          </h2>
          <button
            type="button"
            class="toggle-button"
            :class="{ open: expandedDatabases[dbName] }"
            @click="toggleDatabase(dbName)"
          >
            <span class="toggle-text">
              {{ expandedDatabases[dbName] ? 'Show less' : 'Show more' }}
            </span>
            <i
              class="fas"
              :class="
                expandedDatabases[dbName] ? 'fa-chevron-down' : 'fa-chevron-up'
              "
            />
          </button>
          <button
            v-if="suggestions.enabled && suggestions.variables.status === 'running' && pendingColumnsFor(dbName).length"
            type="button"
            class="btn btn-sm btn-outline-secondary suggestion-section-button"
            title="Move this database to the front of the suggestion queue"
            @click="requestSectionFirst(dbName)"
          >
            <i class="fas fa-lightbulb" /> Suggest this section first
          </button>
          <button
            v-if="suggestions.enabled && hasUnreviewedForDatabase(dbName)"
            type="button"
            class="btn btn-sm btn-outline-secondary suggestion-section-button"
            title="Dismiss all suggestions for this database and clear the fields"
            @click="dismissAllForDatabase(dbName)"
          >
            <i class="fas fa-times" /> Dismiss all suggestions
          </button>

          <div
            class="content"
            :class="{
              active: expandedDatabases[dbName],
              hidden: !expandedDatabases[dbName],
            }"
          >
            <div class="variables-container">
              <div
                v-for="item in currentPageItems(dbName)"
                :key="`${dbName}_${item}`"
                class="variable-row"
              >
                <div class="variable-label">
                  {{ item }}
                  <SuggestionBadge
                    v-if="suggestions.isApplied(`${dbName}_${item}`) || hasSuggestion(dbName, item)"
                    :suggestion="suggestionFor(dbName, item) || {}"
                    :applied="suggestions.isApplied(`${dbName}_${item}`)"
                    :touched="suggestions.isTouched(`${dbName}_${item}`)"
                    :coachmark="
                      coachmarkTarget?.type === 'badge' &&
                        coachmarkTarget.key === `${dbName}_${item}`
                    "
                    :coachmark-copy="COACHMARK_COPY"
                    @dismiss="dismissSuggestion(dbName, item)"
                    @accept="acceptSuggestion(dbName, item)"
                    @apply-alternative="applyAlternative(dbName, item, $event)"
                    @coachmark-close="closeCoachmark"
                  />
                </div>
                <div class="variable-controls">
                  <select
                    :id="`ncit_comment_${dbName}_${item}`"
                    :name="`ncit_comment_${dbName}_${item}`"
                    class="form-control description-select"
                    :class="{
                      'suggestion-highlight': needsSuggestionReview(dbName, item),
                    }"
                    :value="getDescriptionValue(dbName, item)"
                    @change="onDescriptionChange(dbName, item, $event)"
                  >
                    <option value="">
                      Description
                    </option>
                    <option value="Other">
                      Other
                    </option>
                    <option
                      v-for="name in globalVariableNames"
                      :key="name"
                      :value="name"
                      :disabled="isDescriptionDisabled(dbName, item, name)"
                    >
                      {{ name }}
                    </option>
                  </select>

                  <input
                    :id="`comment_${dbName}_${item}`"
                    :name="`comment_${dbName}_${item}`"
                    type="text"
                    class="form-control"
                    placeholder="If other, please specify"
                    :disabled="getDescriptionValue(dbName, item) !== 'Other'"
                    :value="getCommentValue(dbName, item)"
                    @input="onCommentChange(dbName, item, $event)"
                  >

                  <div class="datatype-container">
                    <select
                      :id="`${dbName}_${item}`"
                      :name="`${dbName}_${item}`"
                      class="form-control datatype-select"
                      :value="getDatatypeValue(dbName, item)"
                      @change="onDatatypeChange(dbName, item, $event)"
                    >
                      <option value="">
                        Data type
                      </option>
                      <option value="categorical">
                        Categorical
                      </option>
                      <option value="continuous">
                        Continuous
                      </option>
                      <option value="identifier">
                        Identifier
                      </option>
                      <option value="standardised">
                        Standardised
                      </option>
                    </select>
                    <small
                      v-if="feedbackVisible[`${dbName}_${item}`]"
                      class="datatype-feedback"
                    >
                      <i class="fas fa-magic" /> Auto-filled based on description
                    </small>
                  </div>
                </div>
              </div>
            </div>

            <div
              v-if="totalPages(dbName) > 1"
              class="pagination-controls"
            >
              <button
                type="button"
                class="prev-btn"
                :disabled="(databasePages[dbName] || 1) <= 1"
                @click="changePage(dbName, -1)"
              >
                &#x2190;
              </button>
              <span class="page-indicator">
                Page <span>{{ databasePages[dbName] || 1 }}</span> of
                {{ totalPages(dbName) }}
              </span>
              <button
                type="button"
                class="next-btn"
                :disabled="(databasePages[dbName] || 1) >= totalPages(dbName)"
                @click="changePage(dbName, 1)"
              >
                &#x2192;
              </button>
            </div>
          </div>
          <hr>
        </div>
      </div>

      <template
        v-for="(cached, key) in hiddenFieldEntries"
        :key="`hidden-${key}`"
      >
        <input
          v-if="cached.description"
          type="hidden"
          :name="`ncit_comment_${key}`"
          :value="cached.description"
        >
        <input
          v-if="cached.datatype"
          type="hidden"
          :name="key"
          :value="cached.datatype"
        >
        <input
          v-if="cached.comment"
          type="hidden"
          :name="`comment_${key}`"
          :value="cached.comment"
        >
      </template>

      <template
        v-for="(desc, key) in preselectedHiddenEntries"
        :key="`pre-${key}`"
      >
        <input
          type="hidden"
          :name="`ncit_comment_${key}`"
          :value="desc"
        >
        <input
          v-if="preselectedDatatypes[key]"
          type="hidden"
          :name="key"
          :value="preselectedDatatypes[key]"
        >
      </template>

      <p>
        <button
          type="submit"
          class="btn btn-primary"
          :disabled="!canSubmit"
          :title="submitTooltip"
          :class="{ processing: isSubmitting }"
        >
          <template v-if="!isSubmitting">
            <i class="fas fa-play" /> Submit
          </template>
          <template v-else>
            <i
              class="fas loading-icon"
              :class="loadingIconClass"
            />
            Processing descriptions...
          </template>
        </button>
        <span
          v-if="waitingForSuggestions"
          class="submit-review-hint"
        >
          <i class="fas fa-hourglass-half" />
          Waiting for suggestions...
        </span>
        <span
          v-else-if="unreviewedFieldCount > 0"
          class="submit-review-hint"
        >
          <i class="fas fa-exclamation-circle" />
          {{ unreviewedFieldCount }} {{ unreviewedFieldCount === 1 ? 'suggestion needs' : 'suggestions need' }} review
          <button
            type="button"
            class="btn btn-sm btn-link jump-to-unreviewed"
            title="Jump to the next unreviewed suggestion"
            @click="jumpToNextUnreviewed"
          >
            <i class="fas fa-arrow-down" /> Go to next
          </button>
        </span>
      </p>
    </form>

    <div class="mt-4">
      <div class="alert alert-info py-2 info-purple">
        <i class="fas fa-info-circle" />
        <strong>Reference Guide</strong><br>
        <div class="mt-1 ms-4">
          <strong style="font-size: 0.9em">Data Types:</strong>
          <div class="row g-1 mt-1">
            <div class="col-md-6">
              <i class="fas fa-tags me-2 ref-guide-icon" />
              <strong>Categorical</strong> — distinct categories or groups
            </div>
            <div class="col-md-6">
              <i class="fas fa-chart-line me-2 ref-guide-icon" />
              <strong>Continuous</strong> — numerical variables on a range
            </div>
            <div class="col-md-6">
              <i class="fas fa-fingerprint me-2 ref-guide-icon" />
              <strong>Identifier</strong> — unique values used to identify or link records
            </div>
            <div class="col-md-6">
              <i class="fas fa-clipboard-check me-2 ref-guide-icon" />
              <strong>Standardised</strong> — large-scale standardised variables (ICD,
              EORTC-QLQ, etc.)
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.suggestion-highlight {
  border-color: rgba(118, 75, 162, 0.7);
  border-style: dashed;
  background-color: rgba(118, 75, 162, 0.04);
}

.database-heading {
  position: relative;
  display: inline-block;
}

.suggestion-section-button {
  margin-left: 0.75rem;
  font-size: 0.8em;
}

.info-purple {
  font-size: 0.85em;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: linear-gradient(
    135deg,
    rgba(102, 126, 234, 0.75) 0%,
    rgba(118, 75, 162, 0.75) 100%
  );
  color: white;
}

.submit-review-hint {
  margin-left: 0.75rem;
  color: #764ba2;
  font-size: 0.85em;
}

.jump-to-unreviewed {
  padding: 0 0.25rem;
  margin-left: 0.25rem;
  font-size: 0.85em;
  color: #764ba2;
  text-decoration: none;
  border: none;
  background: none;
  cursor: pointer;
}

.jump-to-unreviewed:hover {
  text-decoration: underline;
}
</style>
