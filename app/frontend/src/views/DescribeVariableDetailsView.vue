<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'
import { formatToTitleCase } from '@/lib/jsonld'
import { useSuggestionsStore } from '@/stores/suggestions'
import SuggestionBadge from '@/components/SuggestionBadge.vue'
import SuggestionStatusBar from '@/components/SuggestionStatusBar.vue'

const DEFAULT_CATEGORY_OPTIONS = [
  { value: 'Yes', label: 'Yes' },
  { value: 'No', label: 'No' },
  { value: 'Male', label: 'Male sex' },
  { value: 'Female', label: 'Female sex' },
  { value: 'Primary Education', label: 'Primary education' },
  { value: 'Secondary Education', label: 'Secondary education' },
  { value: 'Tertiary Education', label: 'Tertiary education' },
  { value: 'Missing', label: 'Missing value' },
  { value: 'Other', label: 'Other' },
]

const suggestions = useSuggestionsStore()

const descriptiveInfo = ref(null)
const descriptiveInfoDetails = ref(null)
const preselectedValues = ref({})
// Value-mapping options per variable display name, from the state response.
// A browser without a semantic map of its own cannot collect them
// client-side; the response carries them so the page's variables,
// preselected values and dropdown options all come from the same map.
const categoryOptionsByVariable = ref({})
const expandedDatabases = reactive({})
const expandedVariables = reactive({})
const isProcessing = ref(false)
const loadingIconIsPen = ref(false)
const mapperLoaded = ref(false)
let _loadingInterval = null

// per-input form state — keyed by stable composite keys
const continuousUnits = reactive({}) // `${db}_${var}` -> unit
const continuousMissing = reactive({}) // `${db}_${var}` -> missing notation
const categorySelections = reactive({}) // `${db}_${var}_${value}` -> selected option
const categoryComments = reactive({}) // `${db}_${var}_${value}` -> comment
const previousSelections = reactive({}) // tracks the last value for updateCategoryMapping
// Keys whose selection was actually persisted to the JSON-LD through the
// change path (accept or manual pick). Suggestion pre-fills are
// display-only and never enter this set, so dismissing a pre-fill cannot
// call updateCategoryMapping with a previousOption that was never written.
const persistedSelections = reactive(new Set())

function buildContinuousVariable(database, displayName, dbIdx, itemIdx) {
  const m = displayName.match(/\(or "([^"]+)"\)/)
  const localVariable = m
    ? m[1]
    : displayName.toLowerCase().replace(/ /g, '_')
  const isMissing = displayName.startsWith('Missing Description')
  const displayLabel = displayName.replace(' (or "', '<br>(or "')
  return {
    type: 'continuous',
    displayName,
    displayLabel,
    localVariable,
    isMissing,
    dbIdx,
    itemIdx,
    unitKey: `${database}_${localVariable}`,
  }
}

function buildCategoricalVariable(database, varName, categories, dbIdx, itemIdx) {
  const m = varName.match(/\(or "([^"]+)"\)/)
  const localVariable = m
    ? m[1]
    : varName.toLowerCase().replace(/ /g, '_')
  const globalVarName = varName.split(' (or')[0].toLowerCase().replace(/ /g, '_')
  const isMissing = varName.startsWith('Missing Description')
  const displayLabel = varName.replace(' (or "', '<br>(or "')

  // The browser's own map is the source for a variable's value-mapping
  // options; the response's copy covers a browser without one. (An empty
  // lookup result is not a usable source — an empty array is truthy.)
  const optionsFromMap = jsonld.getCategoryOptionsForVariable(
    database,
    globalVarName,
    localVariable
  )
  const categoryOptions = optionsFromMap.length
    ? optionsFromMap
    : categoryOptionsByVariable.value[varName] || []
  const localMappings = jsonld.getLocalMappingsForVariable(
    database,
    localVariable,
    globalVarName
  )

  const valueToTermKey = {}
  for (const [termKey, values] of Object.entries(localMappings)) {
    if (Array.isArray(values)) {
      for (const v of values) {
        if (v != null) valueToTermKey[String(v).trim()] = termKey
      }
    } else if (values != null) {
      valueToTermKey[String(values).trim()] = termKey
    }
  }

  const processedCategories = []
  for (const c of categories) {
    const value = c.value !== undefined ? c.value : ''
    const count = c.count || 0
    const safeValue = String(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;')
    const displayValue = value !== '' ? value : 'Empty cells'

    const termKey = valueToTermKey[String(value).trim()]
    const preselectedValue = termKey
      ? termKey.charAt(0).toUpperCase() + termKey.slice(1).replace(/_/g, ' ')
      : ''

    // Pre-seed reactive selections from the local mappings. `parsedDatabases`
    // is recomputed once the JSON-LD mapping finishes loading (it depends on
    // `mapperLoaded`). We seed only when the key has never been set (key
    // presence, not truthiness) so that a deliberate empty selection — the
    // user clearing a preselected category — is preserved and does not snap
    // back. The empty default is deferred until the mapping has loaded;
    // otherwise the first pass would lock in '' before the local mappings
    // (e.g. biological sex → man/vrouw) are available.
    const selKey = `${database}_${localVariable}_${value}`
    const backendKey = `${database}_${localVariable}_category_"${value}"`
    if (!(selKey in categorySelections)) {
      if (preselectedValue) {
        categorySelections[selKey] = preselectedValue
        previousSelections[selKey] = preselectedValue
      } else if (preselectedValues.value?.[backendKey]) {
        // Fall back to the server-computed preselections.
        categorySelections[selKey] = preselectedValues.value[backendKey]
        previousSelections[selKey] = preselectedValues.value[backendKey]
      } else if (mapperLoaded.value) {
        // Default to empty string only once the mapping is loaded.
        categorySelections[selKey] = ''
      }
    }

    processedCategories.push({
      value,
      count,
      safeValue,
      displayValue,
      preselectedValue,
      key: selKey,
      backendKey,
      backendCommentKey: `comment_${database}_${localVariable}_category_"${value}"`,
      backendCountKey: `count_${database}_${localVariable}_category_"${value}"`,
    })
  }

  return {
    type: 'categorical',
    displayName: varName,
    displayLabel,
    localVariable,
    globalVarName,
    isMissing,
    dbIdx,
    itemIdx,
    categoryOptions,
    categories: processedCategories,
  }
}

const parsedDatabases = computed(() => {
  if (!descriptiveInfoDetails.value) return []
  // depend on mapperLoaded so we recompute after the mapping arrives
  void mapperLoaded.value
  const result = []
  let dbIdx = 0
  for (const [database, variables] of Object.entries(descriptiveInfoDetails.value)) {
    dbIdx++
    if (!variables?.length) continue
    const dbEntry = { name: database, dbIdx, variables: [] }
    let itemIdx = 0
    for (const variable of variables) {
      if (typeof variable === 'string') {
        itemIdx++
        dbEntry.variables.push(
          buildContinuousVariable(database, variable, dbIdx, itemIdx)
        )
      } else if (typeof variable === 'object') {
        for (const [varName, categories] of Object.entries(variable)) {
          itemIdx++
          dbEntry.variables.push(
            buildCategoricalVariable(database, varName, categories, dbIdx, itemIdx)
          )
        }
      }
    }
    result.push(dbEntry)
  }
  return result
})

// Exposed for unit tests: the category selection map is the source of truth
// that drives persistence, and asserting on it avoids the v-model/DOM desync.
defineExpose({ categorySelections })

function toggleDatabase(name) {
  expandedDatabases[name] = !expandedDatabases[name]
}

function toggleVariable(database, varIdx) {
  if (!expandedVariables[database]) expandedVariables[database] = {}
  expandedVariables[database][varIdx] = !expandedVariables[database][varIdx]
}

function isVariableExpanded(database, varIdx) {
  return !!expandedVariables[database]?.[varIdx]
}

async function onCategoryChange(database, localVariable, globalVariable, categoryValue, key) {
  const selectedOption = categorySelections[key]
  const previousOption = previousSelections[key]
  previousSelections[key] = selectedOption
  persistedSelections.add(key)
  suggestions.markUserTouched(key)
  try {
    await jsonld.updateCategoryMapping(
      database,
      localVariable,
      globalVariable,
      String(categoryValue),
      selectedOption,
      previousOption
    )
  } catch (e) {
    console.error('Failed to update category mapping:', e)
  }
}

// Clear a category selection in memory only. Used when a display-only
// suggestion pre-fill is dismissed: the pre-fill was never persisted, so
// there is nothing to remove from the JSON-LD.
function clearDisplayOnlySelection(key) {
  categorySelections[key] = ''
}

// ---------------------------------------------------------------------------
// Mapping suggestions. Per decision D1 of the tier-1 remediation the watcher
// PRE-FILLS the category dropdown for display, but the pre-filled value is
// never persisted to the JSON-LD: only an explicit review (accept via the
// badge, or a manual dropdown change) goes through onCategoryChange and
// writes the mapping — exactly as a hand-picked value would.
// ---------------------------------------------------------------------------

function suggestionFor(key) {
  return suggestions.values.byKey[key]
}

// A live suggestion: arrived, has a match, and was not dismissed (a
// dismissed pill must not come back in the unreviewed look).
function hasSuggestion(key) {
  const entry = suggestionFor(key)
  if (suggestions.isDismissed(key)) return false
  return entry && entry.status === 'done' && entry.display
}

// True while a suggestion for this value still needs review: either it
// pre-filled the field (applied, never touched) or it arrived for an empty
// field the pre-fill watch could not fill. Both keep the dashed highlight
// until reviewed or dismissed (WS1.3).
function needsSuggestionReview(key) {
  if (suggestions.isDismissed(key)) return false
  if (suggestions.isApplied(key)) return !suggestions.isTouched(key)
  if (!hasSuggestion(key)) return false
  return !categorySelections[key]
}

// A category value the loaded JSON-LD already mapped — whether or not a
// suggestion also exists for it. The field is filled in, so nothing needs
// reviewing, and the badge shows a quiet "already filled in" pill instead
// of the accept/dismiss one. A selection the user made in THIS session is
// not "already" filled in (it was persisted through onCategoryChange);
// after a reload the value returns from the map and the pill with it.
function isAlreadyMapped(key) {
  if (suggestions.isApplied(key)) return false
  if (persistedSelections.has(key)) return false
  return !!categorySelections[key]
}

async function acceptSuggestion(database, variable, cat, display) {
  const entry = suggestionFor(cat.key)
  const value = display || entry?.display
  if (!value) return
  const options = categoryOptionsFor(variable)
  if (!options.includes(value)) return
  categorySelections[cat.key] = value
  suggestions.markApplied(cat.key)
  await onCategoryChange(database, variable.localVariable, variable.globalVarName, cat.value, cat.key)
  maybeCloseCoachmark()
}

// Applying an alternative from the badge popover goes through the same
// accept path as the suggestion itself.
async function applyAlternative(database, variable, cat, alt) {
  if (!alt?.match) return
  await acceptSuggestion(database, variable, cat, formatToTitleCase(alt.match))
}

function dismissSuggestion(database, variable, cat) {
  // Only a field the suggestion pre-filled (applied and never reviewed) is
  // cleared on dismissal; a manually chosen value must survive it.
  const prefilled = suggestions.isApplied(cat.key) && !suggestions.isTouched(cat.key)
  suggestions.dismiss(cat.key)
  maybeCloseCoachmark()
  if (prefilled && categorySelections[cat.key]) {
    if (persistedSelections.has(cat.key)) {
      onCategoryChange(database, variable.localVariable, variable.globalVarName, cat.value, cat.key)
    } else {
      // Display-only pre-fill: nothing was written, so nothing to unwind.
      clearDisplayOnlySelection(cat.key)
    }
  }
}

function clearAllSuggestions() {
  const cleared = new Set(suggestions.clearAllApplied())
  for (const dbEntry of parsedDatabases.value) {
    for (const variable of dbEntry.variables) {
      if (variable.type !== 'categorical') continue
      for (const cat of variable.categories) {
        if (!cleared.has(cat.key) || !categorySelections[cat.key]) continue
        if (persistedSelections.has(cat.key)) {
          onCategoryChange(dbEntry.name, variable.localVariable, variable.globalVarName, cat.value, cat.key)
        } else {
          clearDisplayOnlySelection(cat.key)
        }
      }
    }
  }
}

function variableSuggestionPending(variable) {
  return variable.categories.some((cat) => {
    const entry = suggestionFor(cat.key)
    return !entry || entry.status === 'pending'
  })
}

function requestVariableFirst(dbName, variable) {
  const keys = variable.categories.map((c) => c.key)
  if (keys.length) suggestions.bumpPriority('values', keys)
}

function categoryOptionsFor(variable) {
  if (variable.categoryOptions.length > 0) {
    return [...variable.categoryOptions, 'Other']
  }
  return DEFAULT_CATEGORY_OPTIONS.map((o) => o.value)
}

const unreviewedFieldCount = computed(
  () => suggestions.unreviewedKeys().filter((key) => categorySelections[key]).length
)

// Pre-fill: when a suggestion arrives for a value that the user hasn't
// touched yet, auto-set the category dropdown to the suggested term and
// mark it as "applied" (unreviewed). The value lives in the selection map
// only — nothing is persisted to the JSON-LD until the user reviews the
// field through onCategoryChange. The user must click the badge or change
// the dropdown to mark it as "reviewed" before they can submit.
watch(
  () => suggestions.values.byKey,
  (byKey) => {
    if (!suggestions.enabled) return
    for (const dbEntry of parsedDatabases.value) {
      for (const variable of dbEntry.variables) {
        if (variable.type !== 'categorical') continue
        for (const cat of variable.categories) {
          const entry = byKey[cat.key]
          if (!entry || entry.status !== 'done' || !entry.display) continue
          // No confidence gate here, unlike the variables page: value
          // scores sit on another scale (a code like '1' against the term
          // 'score_1_not_at_all' scores ~0.69 and is usually right), so the
          // column-name threshold would switch off nearly every value
          // pre-fill. See benchmark-results.md; a values threshold is open.
          if (suggestions.isDismissed(cat.key)) continue
          if (suggestions.isTouched(cat.key)) continue
          // Re-fill applied keys too: on a hard reload the in-memory
          // categorySelections are lost but the "applied" mark survives in
          // IndexedDB, so restore the field from the suggestion. The
          // existing-value guard below keeps user input safe.
          if (categorySelections[cat.key]) continue
          const options = categoryOptionsFor(variable)
          if (!options.includes(entry.display)) continue
          categorySelections[cat.key] = entry.display
          suggestions.markApplied(cat.key)
        }
      }
    }
  },
  { deep: true },
)

function hasUnreviewedForVariable(variable) {
  if (variable.type !== 'categorical') return false
  return variable.categories.some((cat) => {
    const entry = suggestionFor(cat.key)
    if (!entry || entry.status !== 'done' || !entry.display) return false
    if (suggestions.isDismissed(cat.key)) return false
    // Show the button when there are suggestions not yet applied, or
    // applied but still unreviewed (not touched).
    if (!suggestions.isApplied(cat.key) && !categorySelections[cat.key]) return true
    if (suggestions.isApplied(cat.key) && !suggestions.isTouched(cat.key)) return true
    return false
  })
}

async function acceptAllForVariable(database, variable) {
  for (const cat of variable.categories) {
    const entry = suggestionFor(cat.key)
    if (!entry || !entry.display) continue
    if (suggestions.isDismissed(cat.key)) continue
    // If already applied and touched, skip — nothing to do.
    if (suggestions.isApplied(cat.key) && suggestions.isTouched(cat.key)) continue
    // A value already holding a selection the suggestion did not put there
    // (pre-filled from the map, or picked by the user) is not the
    // suggestion's to review: accept-all must not overwrite the selection
    // or mark the value reviewed. It keeps its own pill.
    if (!suggestions.isApplied(cat.key) && categorySelections[cat.key]) continue
    const options = categoryOptionsFor(variable)
    if (!options.includes(entry.display)) continue
    // If not yet applied, fill the selection first.
    if (!suggestions.isApplied(cat.key)) {
      categorySelections[cat.key] = entry.display
      suggestions.markApplied(cat.key)
    }
    // Mark as reviewed (touched) via the normal change path.
    await onCategoryChange(
      database, variable.localVariable, variable.globalVarName,
      cat.value, cat.key,
    )
  }
}

function dismissAllForVariable(database, variable) {
  for (const cat of variable.categories) {
    const entry = suggestionFor(cat.key)
    if (!entry || !entry.display) continue
    if (suggestions.isDismissed(cat.key)) continue
    if (suggestions.isApplied(cat.key) && suggestions.isTouched(cat.key)) continue
    // Same as accept-all: a value the map (or the user) already filled in
    // is not the suggestion's to dismiss — its pill has no × either.
    if (!suggestions.isApplied(cat.key) && categorySelections[cat.key]) continue
    // Only pre-filled, unreviewed fields are cleared; manually chosen
    // values survive the dismissal.
    const prefilled =
      suggestions.isApplied(cat.key) && !suggestions.isTouched(cat.key)
    suggestions.dismiss(cat.key)
    if (prefilled && categorySelections[cat.key]) {
      if (persistedSelections.has(cat.key)) {
        onCategoryChange(database, variable.localVariable, variable.globalVarName, cat.value, cat.key)
      } else {
        clearDisplayOnlySelection(cat.key)
      }
    }
  }
}

// True while suggestions are still expected AND the store has not given
// up on them. The submit gate fails open when waiting can no longer make
// progress: a broken poll or a stalled job must never block the core flow.
const waitingForSuggestions = computed(
  () =>
    suggestions.enabled &&
    !suggestions.values.gaveUp &&
    ['idle', 'pending', 'running'].includes(suggestions.values.status)
)

const canSubmit = computed(() => {
  if (isProcessing.value) return false
  if (unreviewedFieldCount.value > 0) return false
  if (waitingForSuggestions.value) return false
  return true
})

const submitTooltip = computed(() => {
  if (waitingForSuggestions.value)
    return 'Waiting for mapping suggestions to arrive...'
  if (unreviewedFieldCount.value > 0)
    return `${unreviewedFieldCount.value} ${unreviewedFieldCount.value === 1 ? 'suggestion needs' : 'suggestions need'} review — click each highlighted badge to confirm or change the dropdown`
  return ''
})

function jumpToNextUnreviewed() {
  const keys = suggestions.unreviewedKeys().filter((key) => categorySelections[key])
  if (!keys.length) return
  for (const key of keys) {
    // Prefer the record's explicit database; fall back to prefix matching
    // for keys without one (a database name that is a prefix of another
    // would otherwise steal the key).
    const record = suggestionFor(key)
    const dbEntry =
      (record?.database &&
        parsedDatabases.value.find((d) => d.name === record.database)) ||
      parsedDatabases.value.find((d) => key.startsWith(`${d.name}_`))
    if (!dbEntry) continue
    for (let vIdx = 0; vIdx < dbEntry.variables.length; vIdx++) {
      const variable = dbEntry.variables[vIdx]
      if (variable.type !== 'categorical') continue
      const cat = variable.categories.find((c) => c.key === key)
      if (!cat) continue
      // Expand the database and the variable.
      if (!expandedDatabases[dbEntry.name]) expandedDatabases[dbEntry.name] = true
      if (!expandedVariables[dbEntry.name]) expandedVariables[dbEntry.name] = {}
      expandedVariables[dbEntry.name][vIdx] = true
      // Scroll to the category row after Vue updates the DOM.
      nextTick(() => {
        // The backend name embeds the quoted category value, which no CSS
        // attribute selector can carry reliably (the raw quotes made
        // querySelector throw in a real browser, so the scroll never
        // happened). The select carries an id for exactly this lookup.
        const el = document.getElementById(`category_select_${cat.key}`)
        if (el) {
          el.scrollIntoView({ behavior: 'smooth', block: 'center' })
          el.focus({ preventScroll: true })
        }
      })
      return
    }
  }
}

// ---------------------------------------------------------------------------
// First-visit cue (WS2): a small non-modal callout on a pre-filled pill,
// shown once per phase. It pops up when the user first opens a variable
// section and tells them the values were pre-filled and must be reviewed.
// The "How do suggestions work?" link in the status bar reopens it.
// ---------------------------------------------------------------------------

// Kept to two short sentences: the callout sits next to a pill in a
// narrow column, and the submit hint already explains the review gate.
const COACHMARK_COPY = {
  title: 'Check this suggestion',
  body: 'Flyover pre-filled this value. Click the pill to confirm it or × to dismiss it; nothing is saved until you do.',
}

// True when the user reopened the cue via the status-bar link; bypasses
// the persisted "seen" flag until closed again.
const coachmarkRequested = ref(false)

const showCoachmark = computed(
  () =>
    suggestions.enabled &&
    suggestions.values.status === 'done' &&
    unreviewedFieldCount.value > 0 &&
    (coachmarkRequested.value ||
      (suggestions.coachmarkSeen.loaded && !suggestions.coachmarkSeen.values)),
)

// Variable sections ("db|index") in the order the user opened them; a
// section counts as open while both its database and the variable itself
// are unfolded, and drops out when either folds. Watching the open set
// covers every way a section opens (its toggle, "Go to next", the help
// link).
const openedSections = ref([])
watch(
  () => {
    const open = []
    for (const dbEntry of parsedDatabases.value) {
      if (!expandedDatabases[dbEntry.name]) continue
      dbEntry.variables.forEach((_variable, vIdx) => {
        if (isVariableExpanded(dbEntry.name, vIdx)) open.push(`${dbEntry.name}|${vIdx}`)
      })
    }
    return open
  },
  (open) => {
    const kept = openedSections.value.filter((k) => open.includes(k))
    for (const k of open) if (!kept.includes(k)) kept.push(k)
    openedSections.value = kept
  },
)

function sectionVariable(sectionKey) {
  const sep = sectionKey.lastIndexOf('|')
  const dbEntry = parsedDatabases.value.find((d) => d.name === sectionKey.slice(0, sep))
  return dbEntry?.variables[Number(sectionKey.slice(sep + 1))]
}

// The copy says Flyover filled the value in, so the callout only anchors
// on a pre-filled pill awaiting review, never on a low-confidence hint.
function awaitsReview(key) {
  return suggestions.isApplied(key) && !suggestions.isTouched(key)
}

// The callout pops up on the first pre-filled pill of the first variable
// section the user opens, wherever it sits on the page. While everything
// is folded there is nothing to point at, so nothing shows.
const coachmarkTarget = computed(() => {
  if (!showCoachmark.value) return null
  for (const sectionKey of openedSections.value) {
    const variable = sectionVariable(sectionKey)
    if (variable?.type !== 'categorical') continue
    const cat = variable.categories.find((c) => awaitsReview(c.key))
    if (cat) return cat.key
  }
  return null
})

// "How do suggestions work?": show the callout again. When no open section
// has a pre-filled pill, open the first one that does (opened sections
// first, then page order).
function showCoachmarkAgain() {
  coachmarkRequested.value = true
  if (coachmarkTarget.value) return
  const all = []
  for (const dbEntry of parsedDatabases.value) {
    dbEntry.variables.forEach((_variable, vIdx) => all.push(`${dbEntry.name}|${vIdx}`))
  }
  const opened = openedSections.value
  for (const sectionKey of [...opened, ...all.filter((k) => !opened.includes(k))]) {
    const variable = sectionVariable(sectionKey)
    if (variable?.type !== 'categorical') continue
    if (!variable.categories.some((c) => awaitsReview(c.key))) continue
    const sep = sectionKey.lastIndexOf('|')
    const dbName = sectionKey.slice(0, sep)
    expandedDatabases[dbName] = true
    if (!expandedVariables[dbName]) expandedVariables[dbName] = {}
    expandedVariables[dbName][Number(sectionKey.slice(sep + 1))] = true
    return
  }
}

function closeCoachmark() {
  coachmarkRequested.value = false
  suggestions.markCoachmarkSeen('values')
}

// Accepting or dismissing a suggestion while the cue is visible counts as
// having seen it.
function maybeCloseCoachmark() {
  if (coachmarkTarget.value) closeCoachmark()
}

const loadingIconClass = computed(() =>
  loadingIconIsPen.value ? 'fa-pen' : 'fa-edit'
)

function startLoadingAnimation() {
  isProcessing.value = true
  loadingIconIsPen.value = false
  _loadingInterval = setInterval(() => {
    loadingIconIsPen.value = !loadingIconIsPen.value
  }, 1000)
}

async function onFormSubmit() {
  // Persist updated descriptive_info to IndexedDB before the native POST.
  try {
    const updated = JSON.parse(JSON.stringify(descriptiveInfo.value || {}))
    for (const dbEntry of parsedDatabases.value) {
      for (const v of dbEntry.variables) {
        if (v.type === 'continuous') {
          const unit = continuousUnits[v.unitKey]
          if (unit && updated[dbEntry.name]?.[v.localVariable]) {
            updated[dbEntry.name][v.localVariable].units = unit
          }
        } else if (v.type === 'categorical') {
          for (const c of v.categories) {
            const description = categorySelections[c.key]
            if (description && updated[dbEntry.name]?.[v.localVariable]) {
              const comment = categoryComments[c.key] || 'No comment provided'
              const count = c.count != null ? c.count : 'No count available'
              updated[dbEntry.name][v.localVariable][`Category: ${c.value}`] =
                `Category ${c.value}: ${description}, comment: ${comment}, count: ${count}`
            }
          }
        }
      }
    }
    await db.saveData('metadata', {
      key: 'descriptive_info',
      data: updated,
      timestamp: new Date().toISOString(),
    })
  } catch (e) {
    console.error('Failed to update descriptive_info in IndexedDB:', e)
  }
  startLoadingAnimation()
}

onMounted(async () => {
  try {
    // The describe pages work on the map in this browser's IndexedDB: it is
    // what the dropdowns collect their value-mapping options from. The state
    // request carries it so the backend renders the variables, the details
    // population and the preselected values on the same map (request-locally
    // — the session's adopted mapping may be an older one).
    await jsonld.loadFromIndexedDB()
    mapperLoaded.value = true
    const { data } = await api.post('/api/v1/describe-variable-details-state', {
      mapping: jsonld.getMapping(),
    })
    descriptiveInfo.value = data.descriptive_info || {}
    descriptiveInfoDetails.value = data.descriptive_info_details || {}
    preselectedValues.value = data.preselected_values || {}
    categoryOptionsByVariable.value = data.category_options || {}
    await db.saveData('metadata', {
      key: 'descriptive_info',
      data: descriptiveInfo.value,
      timestamp: new Date().toISOString(),
    })
    await db.saveData('metadata', {
      key: 'descriptive_info_details',
      data: descriptiveInfoDetails.value,
      timestamp: new Date().toISOString(),
    })
  } catch (e) {
    console.error('Failed to load variable details state:', e)
  }

  // Idempotent start: the backend kicked this job off when /units was
  // submitted; this covers reloads and backend restarts. Send the updated
  // mapping (reflecting the user's variable selections) so the values phase
  // knows which variable each column maps to and which value mappings are
  // relevant.
  suggestions.setPhase('values')
  await suggestions.init('values', { mapping: jsonld.getMapping() })
})

onBeforeUnmount(() => {
  suggestions.stopPolling()
})
</script>

<template>
  <div>
    <h1><i class="fas fa-pencil-ruler" /> Describe categories and units</h1>
    <hr>
    <p>
      Please provide more information for the categorical and continuous variables that
      were defined in the variable description page.
    </p>

    <SuggestionStatusBar
      v-if="suggestions.enabled"
      :phase-state="suggestions.values"
      :tiers="suggestions.tiers"
      :compute="suggestions.compute"
      :unreviewed-count="unreviewedFieldCount"
      item-label="values"
      @clear-all="clearAllSuggestions"
      @show-coachmark="showCoachmarkAgain"
    />

    <form
      class="form-horizontal"
      method="POST"
      action="/end"
      @submit="onFormSubmit"
    >
      <hr>

      <div>
        <div
          v-for="dbEntry in parsedDatabases"
          :key="dbEntry.name"
          class="database-section"
        >
          <h2 class="database-heading">
            <i class="fas fa-database" /> {{ dbEntry.name }}
          </h2>
          <button
            type="button"
            class="toggle-button"
            :class="{ open: expandedDatabases[dbEntry.name] }"
            @click="toggleDatabase(dbEntry.name)"
          >
            <span class="toggle-text">
              {{ expandedDatabases[dbEntry.name] ? 'Show less' : 'Show more' }}
            </span>
            <i
              class="fas"
              :class="
                expandedDatabases[dbEntry.name]
                  ? 'fa-chevron-down'
                  : 'fa-chevron-up'
              "
            />
          </button>

          <div
            class="content variables-container"
            :class="{
              active: expandedDatabases[dbEntry.name],
              hidden: !expandedDatabases[dbEntry.name],
            }"
          >
            <template
              v-for="(variable, varIdx) in dbEntry.variables"
              :key="`${dbEntry.name}_${varIdx}`"
            >
              <div
                v-if="variable.type === 'continuous'"
                class="variable-row"
              >
                <div
                  class="variable-label"
                  v-html="
                    variable.isMissing
                      ? `<span class='missing-description'>${variable.displayLabel}</span>`
                      : variable.displayLabel
                  "
                />
                <div class="variable-controls">
                  <input
                    v-model="continuousUnits[variable.unitKey]"
                    type="text"
                    :name="`${dbEntry.name}_${variable.localVariable}`"
                    placeholder="Type unit here"
                    class="form-control"
                  >
                  <input
                    v-model="continuousMissing[variable.unitKey]"
                    type="text"
                    :name="`${dbEntry.name}_${variable.localVariable}_notation_missing_or_unspecified`"
                    placeholder="Type missing value notation here"
                    class="form-control"
                  >
                </div>
              </div>

              <template v-if="variable.type === 'categorical'">
                <div class="variable-row">
                  <div
                    class="variable-label"
                    v-html="
                      variable.isMissing
                        ? `<span class='missing-description'>${variable.displayLabel}</span>`
                        : variable.displayLabel
                    "
                  />
                  <div class="variable-controls">
                    <button
                      v-if="suggestions.enabled && suggestions.values.status === 'running' && variableSuggestionPending(variable)"
                      type="button"
                      class="btn btn-sm btn-outline-secondary suggestion-section-button"
                      title="Move this variable to the front of the suggestion queue"
                      @click="requestVariableFirst(dbEntry.name, variable)"
                    >
                      <i class="fas fa-lightbulb" /> Suggest now
                    </button>
                    <button
                      v-if="suggestions.enabled && hasUnreviewedForVariable(variable)"
                      type="button"
                      class="btn btn-sm btn-outline-secondary suggestion-section-button"
                      title="Accept all suggestions for this variable"
                      @click="acceptAllForVariable(dbEntry.name, variable)"
                    >
                      <i class="fas fa-check-double" /> Accept all suggestions
                    </button>
                    <button
                      v-if="suggestions.enabled && hasUnreviewedForVariable(variable)"
                      type="button"
                      class="btn btn-sm btn-outline-secondary suggestion-section-button"
                      title="Dismiss all suggestions for this variable and clear the fields"
                      @click="dismissAllForVariable(dbEntry.name, variable)"
                    >
                      <i class="fas fa-times" /> Dismiss all suggestions
                    </button>
                    <button
                      type="button"
                      class="item-toggle-button"
                      :class="{ open: isVariableExpanded(dbEntry.name, varIdx) }"
                      @click="toggleVariable(dbEntry.name, varIdx)"
                    />
                  </div>
                </div>

                <div
                  class="toggle-content categorical-section"
                  :class="{
                    active: isVariableExpanded(dbEntry.name, varIdx),
                    hidden: !isVariableExpanded(dbEntry.name, varIdx),
                  }"
                >
                  <template
                    v-for="cat in variable.categories"
                    :key="cat.key"
                  >
                    <div class="category-item">
                      <div class="category-label">
                        {{ cat.displayValue }} (counted: {{ cat.count }})
                        <SuggestionBadge
                          v-if="suggestions.isApplied(cat.key) || hasSuggestion(cat.key) || isAlreadyMapped(cat.key)"
                          :suggestion="suggestionFor(cat.key) || {}"
                          :applied="suggestions.isApplied(cat.key)"
                          :touched="suggestions.isTouched(cat.key)"
                          :already-filled="isAlreadyMapped(cat.key)"
                          :coachmark="coachmarkTarget === cat.key"
                          :coachmark-copy="COACHMARK_COPY"
                          @dismiss="dismissSuggestion(dbEntry.name, variable, cat)"
                          @accept="acceptSuggestion(dbEntry.name, variable, cat)"
                          @apply-alternative="applyAlternative(dbEntry.name, variable, cat, $event)"
                          @coachmark-close="closeCoachmark"
                        />
                      </div>
                      <div class="category-controls">
                        <select
                          :id="`category_select_${cat.key}`"
                          v-model="categorySelections[cat.key]"
                          class="form-control category-select"
                          :class="{
                            'suggestion-highlight': needsSuggestionReview(cat.key),
                          }"
                          :name="cat.backendKey"
                          @change="
                            onCategoryChange(
                              dbEntry.name,
                              variable.localVariable,
                              variable.globalVarName,
                              cat.value,
                              cat.key
                            )
                          "
                        >
                          <option value="">
                            Description
                          </option>
                          <template v-if="variable.categoryOptions.length > 0">
                            <option
                              v-for="opt in variable.categoryOptions"
                              :key="opt"
                              :value="opt"
                            >
                              {{ opt }}
                            </option>
                            <option value="Other">
                              Other
                            </option>
                          </template>
                          <template v-else>
                            <option
                              v-for="opt in DEFAULT_CATEGORY_OPTIONS"
                              :key="opt.value"
                              :value="opt.value"
                            >
                              {{ opt.label }}
                            </option>
                          </template>
                        </select>
                        <input
                          v-model="categoryComments[cat.key]"
                          type="text"
                          class="form-control"
                          :name="cat.backendCommentKey"
                          placeholder="If other, please specify"
                          :disabled="categorySelections[cat.key] !== 'Other'"
                        >
                      </div>
                    </div>
                    <input
                      type="hidden"
                      :name="cat.backendCountKey"
                      :value="cat.count"
                    >
                  </template>
                </div>
              </template>
            </template>
          </div>
          <hr>
        </div>
      </div>

      <p>
        <button
          type="submit"
          class="btn btn-primary"
          :disabled="!canSubmit"
          :title="submitTooltip"
          :class="{ processing: isProcessing }"
        >
          <template v-if="!isProcessing">
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
        <RouterLink
          to="/describe/variables"
          class="btn btn-light"
        >
          <i class="fas fa-backward" /> Back to Describe Variables
        </RouterLink>
      </p>
    </form>

    <div class="mt-4">
      <div class="alert alert-info py-2 info-purple">
        <i class="fas fa-info-circle" />
        <strong>Reference Guide</strong><br>
        <div class="mt-1 ms-4">
          <strong>Data Types:</strong>
          <div class="row g-1 mt-1">
            <div class="col-md-6">
              <i class="fas fa-tags me-2 ref-guide-icon" />
              <strong>Categorical</strong> — select the categories that best describe
              the listed values.
            </div>
            <div class="col-md-6">
              <i class="fas fa-chart-line me-2 ref-guide-icon" />
              <strong>Continuous</strong> — specify the unit and missing value notation.
            </div>
          </div>
          <p class="mb-0 mt-2">
            <i class="fas fa-exclamation-triangle" />
            Variables that are by definition standardised (e.g. ICD-10, EORTC-QLQ-C30,
            EuroQoL EQ5D) <u>do not have to be described</u>.
          </p>
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

.suggestion-section-button {
  font-size: 0.8em;
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
</style>
