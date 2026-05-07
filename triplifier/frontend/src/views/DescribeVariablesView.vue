<script setup>
import { computed, onMounted, reactive, ref, nextTick } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'

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
let loadingInterval = null

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
    const dbName = key.split('_')[0]
    if (formStateCache[key]) continue
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
  formStateCache[key].description = e.target.value
  autoPopulateDatatype(dbName, item)
  syncToIndexedDB()
}

function onDatatypeChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].datatype = e.target.value
  if (autoFilledFields.has(key)) manualOverrides.add(key)
  syncToIndexedDB()
}

function onCommentChange(dbName, item, e) {
  const key = `${dbName}_${item}`
  ensureCacheEntry(key, dbName)
  formStateCache[key].comment = e.target.value
  syncToIndexedDB()
}

async function syncToIndexedDB() {
  try {
    await jsonld.updateMappingFromForm({ ...formStateCache })
  } catch (err) {
    console.error('Failed to sync to IndexedDB:', err)
  }
}

function toggleDatabase(dbName) {
  expandedDatabases[dbName] = !expandedDatabases[dbName]
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
  loadingInterval = setInterval(() => {
    loadingIconIsPen.value = !loadingIconIsPen.value
  }, 1000)
}

function onFormSubmit() {
  startLoadingAnimation()
  // native form POSTs to /units → redirects to /describe_variable_details (legacy)
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
})
</script>

<template>
  <div>
    <h1><i class="fas fa-pencil-ruler"></i> Describe your data</h1>
    <hr />
    <p>
      Please inspect the variables (i.e. columns) of your database(s).<br />
      For every database that you would like to describe, please select the type and
      description of your columns from the drop-down menu.
    </p>

    <form class="form-horizontal" method="POST" action="/units" @submit="onFormSubmit">
      <hr />

      <div>
        <div v-for="dbName in databaseNames" :key="dbName">
          <h2 class="database-heading">
            <i class="fas fa-database"></i> {{ dbName }}
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
            ></i>
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
                <div class="variable-label">{{ item }}</div>
                <div class="variable-controls">
                  <select
                    :id="`ncit_comment_${dbName}_${item}`"
                    :name="`ncit_comment_${dbName}_${item}`"
                    class="form-control description-select"
                    :value="getDescriptionValue(dbName, item)"
                    @change="onDescriptionChange(dbName, item, $event)"
                  >
                    <option value="">Description</option>
                    <option value="Other">Other</option>
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
                    @change="onCommentChange(dbName, item, $event)"
                  />

                  <div class="datatype-container">
                    <select
                      :id="`${dbName}_${item}`"
                      :name="`${dbName}_${item}`"
                      class="form-control datatype-select"
                      :value="getDatatypeValue(dbName, item)"
                      @change="onDatatypeChange(dbName, item, $event)"
                    >
                      <option value="">Data type</option>
                      <option value="categorical">Categorical</option>
                      <option value="continuous">Continuous</option>
                      <option value="identifier">Identifier</option>
                      <option value="standardised">Standardised</option>
                    </select>
                    <small
                      v-if="feedbackVisible[`${dbName}_${item}`]"
                      class="datatype-feedback"
                    >
                      <i class="fas fa-magic"></i> Auto-filled based on description
                    </small>
                  </div>
                </div>
              </div>
            </div>

            <div v-if="totalPages(dbName) > 1" class="pagination-controls">
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
          <hr />
        </div>
      </div>

      <template v-for="(cached, key) in hiddenFieldEntries" :key="`hidden-${key}`">
        <input
          v-if="cached.description"
          type="hidden"
          :name="`ncit_comment_${key}`"
          :value="cached.description"
        />
        <input
          v-if="cached.datatype"
          type="hidden"
          :name="key"
          :value="cached.datatype"
        />
        <input
          v-if="cached.comment"
          type="hidden"
          :name="`comment_${key}`"
          :value="cached.comment"
        />
      </template>

      <template v-for="(desc, key) in preselectedHiddenEntries" :key="`pre-${key}`">
        <input type="hidden" :name="`ncit_comment_${key}`" :value="desc" />
        <input
          v-if="preselectedDatatypes[key]"
          type="hidden"
          :name="key"
          :value="preselectedDatatypes[key]"
        />
      </template>

      <p>
        <button
          type="submit"
          class="btn btn-primary"
          :disabled="!hasAnyDescription"
          :class="{ processing: isSubmitting }"
        >
          <template v-if="!isSubmitting">
            <i class="fas fa-play"></i> Submit
          </template>
          <template v-else>
            <i class="fas loading-icon" :class="loadingIconClass"></i>
            Processing descriptions...
          </template>
        </button>
      </p>
    </form>

    <div class="mt-4">
      <div class="alert alert-info py-2 info-purple">
        <i class="fas fa-info-circle"></i>
        <strong>Reference Guide</strong><br />
        <div class="mt-1 ms-4">
          <strong style="font-size: 0.9em">Data Types:</strong>
          <div class="row g-1 mt-1">
            <div class="col-md-6">
              <i class="fas fa-tags me-2 ref-guide-icon"></i>
              <strong>Categorical</strong> — distinct categories or groups
            </div>
            <div class="col-md-6">
              <i class="fas fa-chart-line me-2 ref-guide-icon"></i>
              <strong>Continuous</strong> — numerical variables on a range
            </div>
            <div class="col-md-6">
              <i class="fas fa-fingerprint me-2 ref-guide-icon"></i>
              <strong>Identifier</strong> — unique values used to identify or link records
            </div>
            <div class="col-md-6">
              <i class="fas fa-clipboard-check me-2 ref-guide-icon"></i>
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
