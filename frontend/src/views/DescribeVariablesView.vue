<script setup>
import { computed, onMounted, onBeforeUnmount, reactive, ref, nextTick, watch } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'
import { useSuggestionsStore } from '@/stores/suggestions'
import SearchableSelect from '@/components/SearchableSelect.vue'

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
let _submitGuardPassed = false

const suggestions = useSuggestionsStore()

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
    // Seed from preselected values so a partial edit (e.g. just the comment)
    // doesn't blank out a description/datatype that the JSON-LD mapping has
    // already provided — otherwise the description input flips back to ''
    // mid-typing and the "Other" comment field disables itself.
    formStateCache[key] = {
      database: dbName,
      description: preselectedDescriptions.value[key] || '',
      datatype: preselectedDatatypes.value[key] || '',
      comment: '',
    }
  } else {
    formStateCache[key].database = dbName
  }
}

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

function onDescriptionChange(dbName, item, value) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].description = value
  autoPopulateDatatype(dbName, item)
  suggestions.markUserTouched(key)
  syncToIndexedDB()
}

const descriptionOptions = computed(() => ['Other', ...globalVariableNames.value])

function onDatatypeChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].datatype = e.target.value
  if (autoFilledFields.has(key)) manualOverrides.add(key)
  suggestions.markUserTouched(key)
  syncToIndexedDB()
}

function onCommentChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].comment = e.target.value
  syncToIndexedDB()
}

// ---------------------------------------------------------------------------
// LLM suggestions: merge arrivals into untouched fields only, always through
// the same code path a manual selection takes so option-disabling, hidden
// field submission, and JSON-LD persistence keep working unchanged.
// ---------------------------------------------------------------------------

function suggestionFor(dbName, item) {
  return suggestions.variables.byKey[`${dbName}_${item}`]
}

function applyArrivedSuggestions() {
  let appliedAny = false
  for (const entry of Object.values(suggestions.variables.byKey)) {
    if (entry.status !== 'done' || !entry.display) continue
    const key = `${entry.database}_${entry.column}`
    if (formStateCache[key]?.description) continue
    if (preselectedDescriptions.value[key]) continue
    if (suggestions.isDismissed(key)) continue
    if (!globalVariableNames.value.includes(entry.display)) continue
    if (isDescriptionDisabled(entry.database, entry.column, entry.display)) continue

    ensureCacheEntry(key, entry.database)
    formStateCache[key].description = entry.display
    autoPopulateDatatype(entry.database, entry.column)
    suggestions.markApplied(key)
    appliedAny = true
  }
  if (appliedAny) syncToIndexedDB()
}

function dismissSuggestion(dbName, item) {
  const key = `${dbName}_${item}`
  suggestions.dismiss(key)
  if (formStateCache[key]) {
    formStateCache[key].description = ''
    autoPopulateDatatype(dbName, item)
    syncToIndexedDB()
  }
}

function retrySuggestion(dbName, item) {
  suggestions.bumpPriority('variables', dbName, [item], { retry: true })
}

function clearAllSuggestions() {
  for (const key of suggestions.clearAllApplied()) {
    if (formStateCache[key]?.description) {
      const dbName = formStateCache[key].database
      const item = key.slice(dbName.length + 1)
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

function requestSectionFirst(dbName) {
  const pending = pendingColumnsFor(dbName)
  if (pending.length) suggestions.bumpPriority('variables', dbName, pending)
}

function hintVisibleColumns(dbName) {
  const pending = currentPageItems(dbName).filter((item) => {
    const entry = suggestionFor(dbName, item)
    return !entry || entry.status === 'pending'
  })
  if (pending.length) suggestions.bumpPriority('variables', dbName, pending)
}

const suggestionProgress = computed(() => {
  const entries = Object.values(suggestions.variables.byKey)
  return {
    done: entries.filter((e) => e.status === 'done' || e.status === 'error').length,
    total: entries.length,
  }
})

const suggestionsActive = computed(() =>
  ['pulling_model', 'running'].includes(suggestions.variables.status)
)

const unreviewedFieldCount = computed(
  () =>
    suggestions
      .unreviewedKeys()
      .filter((key) => formStateCache[key]?.description).length
)

watch(
  () => suggestions.variables.byKey,
  () => applyArrivedSuggestions(),
  { deep: true }
)

async function syncToIndexedDB() {
  try {
    await jsonld.updateMappingFromForm({ ...formStateCache })
  } catch (err) {
    console.error('Failed to sync to IndexedDB:', err)
  }
}

function toggleDatabase(dbName) {
  expandedDatabases[dbName] = !expandedDatabases[dbName]
  if (expandedDatabases[dbName]) hintVisibleColumns(dbName)
}

function changePage(dbName, direction) {
  const cur = databasePages[dbName] || 1
  const next = cur + direction
  if (next >= 1 && next <= totalPages(dbName)) {
    databasePages[dbName] = next
    hintVisibleColumns(dbName)
  }
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

function stopLoadingAnimation() {
  isSubmitting.value = false
  loadingIconIsPen.value = false
  if (_loadingInterval) {
    clearInterval(_loadingInterval)
    _loadingInterval = null
  }
}

function onFormSubmit(e) {
  // Silently submitting unreviewed AI prefills is dangerous in medical data
  // mapping — ask once, then let the native POST through.
  if (!_submitGuardPassed && unreviewedFieldCount.value > 0) {
    const n = unreviewedFieldCount.value
    const ok = window.confirm(
      `${n} description${n === 1 ? ' was' : 's were'} filled in by AI and not ` +
        'reviewed. Submit anyway?'
    )
    if (!ok) {
      e.preventDefault()
      return
    }
    _submitGuardPassed = true
  }
  startLoadingAnimation()
  // native form POSTs to /units → redirects to /describe/variable-details
}

// When the browser restores this page from BFCache (e.g. user hits Back after
// submitting the form), the persisted JS state would otherwise leave the
// submit button stuck in its 'Processing…' state. Reset on pageshow.
function onPageShow(event) {
  if (event.persisted) stopLoadingAnimation()
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

  // Idempotent start (the backend usually began at ingest); the mapping is
  // included in case it only survives in this browser's IndexedDB.
  await suggestions.init('variables', { mapping: jsonld.getMapping() })
  applyArrivedSuggestions()
})

onBeforeUnmount(() => {
  window.removeEventListener('pageshow', onPageShow)
  stopLoadingAnimation()
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

    <div
      v-if="suggestions.enabled && suggestions.variables.status !== 'idle'"
      class="llm-status-bar"
    >
      <i class="fas fa-robot" />
      <span v-if="suggestions.variables.status === 'pulling_model'">
        <i class="fas fa-spinner fa-spin" /> Preparing the AI model…
      </span>
      <span v-else-if="suggestions.variables.status === 'running'">
        <i class="fas fa-spinner fa-spin" />
        AI suggestions: {{ suggestionProgress.done }} of
        {{ suggestionProgress.total }} variables
      </span>
      <span v-else-if="suggestions.variables.status === 'done'">
        AI suggestions ready — review the highlighted fields
      </span>
      <span v-else-if="suggestions.variables.status === 'failed'">
        AI suggestions unavailable — fill in descriptions manually
      </span>
      <span v-else-if="suggestions.variables.status === 'unavailable'">
        Nothing for the AI to suggest
      </span>
      <span
        v-if="suggestions.providerLabel"
        class="llm-provider-note"
      >by {{ suggestions.providerLabel }}</span>
      <span
        v-if="suggestions.remote"
        class="llm-remote-badge"
        title="Suggestions are computed by an external service outside this deployment"
      ><i class="fas fa-cloud" /> external</span>
      <button
        v-if="unreviewedFieldCount"
        type="button"
        class="btn btn-sm btn-outline-secondary llm-clear-all"
        @click="clearAllSuggestions"
      >
        Clear all AI suggestions
      </button>
    </div>

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
            v-if="suggestionsActive && pendingColumnsFor(dbName).length"
            type="button"
            class="btn btn-sm btn-outline-secondary llm-section-button"
            title="Move this database to the front of the AI suggestion queue"
            @click="requestSectionFirst(dbName)"
          >
            <i class="fas fa-robot" /> Suggest this section first
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
                  <span
                    v-if="suggestions.isApplied(`${dbName}_${item}`)"
                    class="llm-badge"
                    :class="{ confirmed: suggestions.isTouched(`${dbName}_${item}`) }"
                    :title="suggestionFor(dbName, item)?.reason || 'AI suggestion'"
                  >
                    <template v-if="suggestions.isTouched(`${dbName}_${item}`)">
                      <i class="fas fa-check" /> reviewed
                    </template>
                    <template v-else>
                      <i class="fas fa-robot" />
                      {{ Math.round((suggestionFor(dbName, item)?.confidence || 0) * 100) }}%
                      <button
                        type="button"
                        class="llm-dismiss"
                        title="Dismiss this AI suggestion"
                        @click="dismissSuggestion(dbName, item)"
                      >
                        &times;
                      </button>
                    </template>
                  </span>
                  <button
                    v-else-if="suggestionFor(dbName, item)?.status === 'error'"
                    type="button"
                    class="llm-retry"
                    title="The AI suggestion for this variable failed — retry"
                    @click="retrySuggestion(dbName, item)"
                  >
                    <i class="fas fa-rotate-right" /> retry AI
                  </button>
                </div>
                <div class="variable-controls">
                  <SearchableSelect
                    :input-id="`ncit_comment_${dbName}_${item}`"
                    :name="`ncit_comment_${dbName}_${item}`"
                    class="description-select"
                    :class="{
                      'llm-suggested':
                        suggestions.isApplied(`${dbName}_${item}`) &&
                        !suggestions.isTouched(`${dbName}_${item}`),
                    }"
                    placeholder="Description"
                    :model-value="getDescriptionValue(dbName, item)"
                    :options="descriptionOptions"
                    :disabled-option="
                      (name) => isDescriptionDisabled(dbName, item, name)
                    "
                    @update:model-value="onDescriptionChange(dbName, item, $event)"
                  />

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
          :disabled="!hasAnyDescription"
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
.llm-status-bar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.5rem;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: rgba(118, 75, 162, 0.08);
  border-radius: 4px;
  font-size: 0.9em;
}

.llm-provider-note {
  color: #777;
  font-size: 0.85em;
}

.llm-remote-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.1rem 0.45rem;
  border-radius: 999px;
  font-size: 0.8em;
  background: rgba(200, 120, 30, 0.12);
  color: rgb(150, 90, 20);
  border: 1px solid rgba(200, 120, 30, 0.5);
  cursor: help;
}

.llm-clear-all {
  margin-left: auto;
}

.llm-section-button {
  margin-left: 0.75rem;
  font-size: 0.8em;
}

.llm-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  margin-left: 0.5rem;
  padding: 0.1rem 0.45rem;
  border-radius: 999px;
  font-size: 0.75em;
  background: rgba(118, 75, 162, 0.12);
  color: rgb(90, 60, 130);
  border: 1px dashed rgba(118, 75, 162, 0.6);
  cursor: help;
}

.llm-badge.confirmed {
  border-style: solid;
  border-color: rgba(40, 140, 80, 0.6);
  background: rgba(40, 140, 80, 0.1);
  color: rgb(30, 110, 60);
}

.llm-dismiss {
  border: none;
  background: none;
  padding: 0 0.1rem;
  line-height: 1;
  font-size: 1.1em;
  color: inherit;
  cursor: pointer;
}

.llm-retry {
  margin-left: 0.5rem;
  border: none;
  background: none;
  padding: 0;
  font-size: 0.75em;
  color: rgb(90, 60, 130);
  text-decoration: underline;
  cursor: pointer;
}

.llm-suggested :deep(.searchable-select-input) {
  border-color: rgba(118, 75, 162, 0.7);
  border-style: dashed;
  background-color: rgba(118, 75, 162, 0.04);
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
