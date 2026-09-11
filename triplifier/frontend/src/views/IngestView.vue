<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import api from '@/services/api'
import { useNavigation } from '@/composables/useNavigation'
import { readCSVColumns } from '@/lib/csvParser'
import { readExcelSheetInfo } from '@/lib/excelParser'
import {
  isValidPgUrl,
  preventBlockedKey,
  stripBlockedOnPaste,
  pgHasBlockedChar,
  pgFieldError as pgFieldErrorValue,
  pgUrlErrorValue,
} from '@/lib/postgresValidation'
import {
  filterByExtension,
  detectFileType,
  setFileInputFiles,
} from '@/lib/fileDropUtils'
import {
  computeAutoSuggestions,
  computeClearIndices,
  validatePkFkRelationships as validatePkFk,
  buildPkFkDataJson,
} from '@/lib/pkfkUtils'

const { dataExists: graphExists, refreshDataExists } = useNavigation()

// --- File type configuration ---

const FILE_TYPE_EXTENSIONS = {
  CSV: ['.csv'],
  Excel: ['.xlsx', '.xls'],
}

// --- Reactive state ---

const fileType = ref('')
const csvFiles = ref([])
const csvColumns = reactive({})
const csvPath = ref('')

// Unified list of "tables" for PK/FK. For CSV, each file is a table.
// For Excel, each sheet is a table (name: "filename_sheetname").
const pkFkTables = ref([])
const detectedDecimal = (1.1).toLocaleString(navigator.language).match(/[.,]/)?.[0] || '.'
const csvSeparatorSign = ref(detectedDecimal === ',' ? ';' : ',')
const csvDecimalSign = ref(detectedDecimal)

const existingGraphStructure = ref(null)
const enableDataLinking = ref(false)
const newTableName = ref('')
const newColumnName = ref('')
const existingTableName = ref('')
const existingColumnName = ref('')

const pgUsername = ref('')
const pgPassword = ref('')
const pgUrl = ref('')
const pgDb = ref('')
const pgUrlTouched = ref(false)
const pgFieldsTouched = reactive({
  username: false,
  password: false,
  db: false,
})

const pkSelections = reactive({})
const fkSelections = reactive({})
const fkTableSelections = reactive({})
const fkColumnSelections = reactive({})
const inferredFk = reactive({})

const showPkFkSection = ref(false)
const showDataLinkingSection = ref(false)
const csvFileInput = ref(null)
const submitting = ref(false)
const showOtherTooltip = ref(false)
const dropError = ref('')

const dragCounters = reactive({ CSV: 0, Excel: 0 })
const dragActiveTile = computed(() => {
  if (dragCounters.CSV > 0) return 'CSV'
  if (dragCounters.Excel > 0) return 'Excel'
  return null
})

const pageDragActive = ref(false)
let pageDragLeaveTimer = null

// --- Computed: PG field helpers ---

function pgFieldValue(field) {
  return { username: pgUsername, password: pgPassword, db: pgDb }[field]?.value
}

function pgFieldError(field) {
  return pgFieldErrorValue(field, pgFieldValue(field), pgFieldsTouched[field])
}

function pgHasBlockedCharInView(field) {
  return pgHasBlockedChar(field, pgFieldValue(field))
}

const pgUrlError = computed(() =>
  pgUrlErrorValue(pgUrl.value, pgUrlTouched.value)
)

// --- Computed: table/column lookups ---

const newTableColumns = computed(() => {
  if (!newTableName.value) return []
  const table = pkFkTables.value.find((t) => t === newTableName.value)
  return table ? csvColumns[table] || [] : []
})

const existingTables = computed(
  () => existingGraphStructure.value?.tables || []
)

const existingTableColumns = computed(() => {
  if (!existingTableName.value || !existingGraphStructure.value?.tableColumns) return []
  return existingGraphStructure.value.tableColumns[existingTableName.value] || []
})

const crossGraphLinkError = computed(() => {
  if (!enableDataLinking.value) return ''
  if (!newTableName.value || !existingTableName.value) return ''
  // Prevent linking a table to itself (same sanitised name) — this is
  // the most common cause of circular references in the RDF store.
  const newSanitised = newTableName.value.replace(/\.(csv|xls[x]?)$/i, '').toLowerCase()
  const existingSanitised = existingTableName.value.replace(/\.(csv|xls[x]?)$/i, '').toLowerCase()
  if (newSanitised === existingSanitised) {
    return 'Cannot link a table to itself — this would create a circular reference.'
  }
  return ''
})

// --- PK/FK helpers ---

function getFileColumns(tableName) {
  if (!tableName) return []
  return csvColumns[tableName] || []
}

function getOtherTables(currentName) {
  return pkFkTables.value.filter((t) => t !== currentName)
}

function resetPkFk() {
  for (const k of Object.keys(pkSelections)) delete pkSelections[k]
  for (const k of Object.keys(fkSelections)) delete fkSelections[k]
  for (const k of Object.keys(fkTableSelections)) delete fkTableSelections[k]
  for (const k of Object.keys(fkColumnSelections)) delete fkColumnSelections[k]
  for (const k of Object.keys(inferredFk)) delete inferredFk[k]
}

// --- Computed: form validation & submit ---

const isFormValid = computed(() => {
  const basic =
    (fileType.value === 'CSV' && csvFiles.value.length > 0) ||
    (fileType.value === 'Excel' && csvFiles.value.length > 0) ||
    (fileType.value === 'Postgres' &&
      pgUsername.value &&
      pgPassword.value &&
      pgUrl.value &&
      pgDb.value &&
      isValidPgUrl(pgUrl.value) &&
      !pgHasBlockedCharInView('username') &&
      !pgHasBlockedCharInView('password') &&
      !pgHasBlockedCharInView('url') &&
      !pgHasBlockedCharInView('db'))
  return basic && validatePkFkRelationships()
})

function validatePkFkRelationships() {
  return validatePkFk(
    showPkFkSection.value,
    pkFkTables.value,
    pkSelections,
    fkSelections,
    fkTableSelections
  )
}

const submitButtonTitle = computed(() => {
  if (isFormValid.value) return ''
  if (!validatePkFkRelationships()) {
    return 'Please select primary keys for all tables that are referenced by foreign keys'
  }
  return ''
})

const submitButtonLabel = computed(() => {
  if (submitting.value) return ' Processing...'
  if (fileType.value === 'Postgres') return ' Enter Credentials'
  return ' Submit Files'
})

const submitButtonIcon = computed(() => {
  if (submitting.value) return 'fa-cookie'
  if (fileType.value === 'Postgres') return 'fa-sign-in-alt'
  return 'fa-play'
})

// --- Computed: serialised form payloads ---

const pkFkDataJson = computed(() =>
  buildPkFkDataJson(
    showPkFkSection.value,
    pkFkTables.value,
    pkSelections,
    fkSelections,
    fkTableSelections,
    fkColumnSelections
  )
)

const crossGraphLinkDataJson = computed(() => {
  if (!enableDataLinking.value) return ''
  if (crossGraphLinkError.value) return ''
  const link = {
    newTableName: newTableName.value,
    newColumnName: newColumnName.value,
    existingTableName: existingTableName.value,
    existingColumnName: existingColumnName.value,
  }
  if (
    link.newTableName &&
    link.newColumnName &&
    link.existingTableName &&
    link.existingColumnName
  ) {
    return JSON.stringify(link)
  }
  return ''
})

// --- File processing ---

function triggerFileInput() {
  csvFileInput.value?.click()
}

async function processFiles(files) {
  dropError.value = ''
  csvFiles.value = []
  pkFkTables.value = []
  for (const k of Object.keys(csvColumns)) delete csvColumns[k]
  resetPkFk()

  const paths = []
  for (let i = 0; i < files.length; i++) {
    paths.push(files[i].name)
    csvFiles.value.push(files[i])
  }
  csvPath.value = paths.join(', ')

  if (fileType.value === 'Excel') {
    await processExcelFiles(files)
  } else {
    await processCSVFiles(files)
  }

  showDataLinkingSection.value = graphExists.value && files.length > 0
}

async function processExcelFiles(files) {
  // For Excel, each sheet is a table. Read sheet info from the xlsx zip.
  const allSheetInfo = await Promise.all(
    Array.from(files).map((f) => readExcelSheetInfo(f))
  )
  const tables = []
  allSheetInfo.forEach((sheets, fi) => {
    const file = files[fi]
    const base = file.name.replace(/\.(xlsx|xls)$/i, '')
    if (sheets.length === 0) {
      // Could not read sheets — treat the file as a single table
      tables.push(base)
      csvColumns[base] = []
    } else {
      sheets.forEach((sheet) => {
        const tableName = `${base}_${sheet.name}`
        tables.push(tableName)
        csvColumns[tableName] = sheet.columns
      })
    }
  })
  pkFkTables.value = tables
  showPkFkSection.value = tables.length > 1
  tables.forEach((_, i) => {
    pkSelections[i] = pkSelections[i] || ''
  })
}

async function processCSVFiles(files) {
  // For CSV, each file is a table.
  pkFkTables.value = Array.from(files).map((f) => f.name)

  // Visibility depends only on file counts, not on the column reads. Set it
  // before awaiting so the multi-file UI appears immediately and tests don't
  // race the FileReader.onload macrotask.
  showPkFkSection.value = files.length > 1
  pkFkTables.value.forEach((_, i) => {
    pkSelections[i] = pkSelections[i] || ''
  })

  const cols = await Promise.all(
    Array.from(files).map((f) => readCSVColumns(f))
  )
  Array.from(files).forEach((f, i) => {
    csvColumns[f.name] = cols[i]
  })
}

async function handleFileChange(e) {
  await processFiles(e.target.files)
}

// --- Drag and drop ---

function onTileDragEnter(type) {
  dragCounters[type]++
}

function onTileDragLeave(type) {
  if (dragCounters[type] > 0) dragCounters[type]--
}

async function onTileDrop(type, e) {
  e.preventDefault()
  e.stopPropagation()
  clearTimeout(pageDragLeaveTimer)
  dragCounters[type] = 0
  pageDragActive.value = false
  const dropped = filterByExtension(e.dataTransfer.files, FILE_TYPE_EXTENSIONS[type])
  if (!dropped.length) {
    const exts = type === 'Excel' ? '.xlsx or .xls' : '.csv'
    dropError.value = `Please drop only ${exts} files on the ${type} tile.`
    return
  }
  fileType.value = type
  setFileInputFiles(csvFileInput.value, dropped)
  await processFiles(dropped)
}

function onPageDragEnter() {
  clearTimeout(pageDragLeaveTimer)
  pageDragLeaveTimer = null
  pageDragActive.value = true
}

function onPageDragOver() {
  clearTimeout(pageDragLeaveTimer)
  pageDragLeaveTimer = null
}

function onPageDragLeave() {
  pageDragLeaveTimer = setTimeout(() => {
    pageDragActive.value = false
    pageDragLeaveTimer = null
  }, 100)
}

async function onPageDrop(e) {
  e.preventDefault()
  clearTimeout(pageDragLeaveTimer)
  pageDragActive.value = false
  const allFiles = Array.from(e.dataTransfer.files)
  if (!allFiles.length) return
  const detected = detectFileType(allFiles, FILE_TYPE_EXTENSIONS)
  if (!detected) {
    const names = allFiles.map((f) => f.name).join(', ')
    dropError.value = `Unsupported file type(s): ${names}. Please use .csv, .xlsx, or .xls files.`
    return
  }
  fileType.value = detected
  setFileInputFiles(csvFileInput.value, allFiles)
  await processFiles(allFiles)
}

// --- PK/FK auto-suggest ---

function onFkTableChange(index) {
  fkColumnSelections[index] = ''
  delete inferredFk[index]
}

function onFkManualChange(index) {
  delete inferredFk[index]
}

// When a PK is set on table at index pkIndex, check every other table for
// a column whose name matches the PK (case-insensitive). If found and the
// other table's FK fields are not already manually set, auto-fill them.
function autoSuggestFk(pkIndex) {
  const suggestions = computeAutoSuggestions(
    pkFkTables.value, csvColumns, pkSelections, pkIndex
  )
  for (const [index, s] of Object.entries(suggestions)) {
    const i = Number(index)
    // Don't override a manually-set FK
    if (fkSelections[i]) continue
    fkSelections[i] = s.fk
    fkTableSelections[i] = s.fkTable
    fkColumnSelections[i] = s.fkColumn
    inferredFk[i] = true
  }
}

// Clear auto-suggested FK fields when a PK is removed, so stale suggestions
// don't persist after the user changes their mind.
function clearAutoSuggestedFk(pkIndex) {
  const pkTableName = pkFkTables.value[pkIndex]
  if (!pkTableName) return
  const indices = computeClearIndices(fkTableSelections, pkTableName)
  for (const index of indices) {
    fkSelections[index] = ''
    fkTableSelections[index] = ''
    fkColumnSelections[index] = ''
    delete inferredFk[index]
  }
}

watch(pkSelections, () => {
  for (const index of Object.keys(pkSelections)) {
    if (pkSelections[index]) {
      autoSuggestFk(Number(index))
    } else {
      clearAutoSuggestedFk(Number(index))
    }
  }
}, { deep: true })

// --- Lifecycle ---

async function loadExistingGraphData() {
  try {
    const { data } = await api.get('/get-existing-graph-structure')
    existingGraphStructure.value = data
  } catch (e) {
    console.error('Error loading existing graph structure:', e)
  }
}

function onFormSubmit() {
  // Native browser form POST. Flask returns a redirect to the next page;
  // the legacy /data-submission page will take over from here until that
  // view is ported in a follow-up.
  submitting.value = true
}

function submitWithoutData(e) {
  // Bypass file validation and just POST the form. The native POST will
  // navigate the browser to Flask's redirect target.
  e.preventDefault()
  e.target.closest('form').submit()
}

onMounted(async () => {
  await refreshDataExists()
  if (graphExists.value) loadExistingGraphData()
})
</script>

<template>
  <div
    @dragenter.prevent="onPageDragEnter"
    @dragover.prevent="onPageDragOver"
    @dragleave.prevent="onPageDragLeave"
    @drop.prevent="onPageDrop"
  >
    <div
      v-if="pageDragActive"
      class="drop-overlay"
    >
      <div class="drop-overlay-content card shadow">
        <div class="card-body text-center py-4 px-5">
          <i class="fas fa-cloud-upload-alt fa-2x mb-3 d-block text-primary" />
          <h5 class="card-title mb-1">
            Drop files anywhere to upload
          </h5>
          <p class="text-muted small mb-0">
            CSV and Excel files will be auto-detected
          </p>
        </div>
      </div>
    </div>
    <h1><i class="fas fa-cookie-bite" /> Ingest your data</h1>
    <hr>
    <p>
      First, you have to ensure that the graph database contains data and ontology graphs.<br>
      You can achieve this by submitting your data for conversion using Flyover.
    </p>
    <hr>
    <div
      v-if="dropError"
      class="alert alert-danger d-flex align-items-center"
      role="alert"
    >
      <i class="fas fa-exclamation-circle me-2" />
      <span class="flex-grow-1">{{ dropError }}</span>
      <button
        type="button"
        class="btn btn-sm btn-link text-danger p-0 ms-2 lh-1"
        aria-label="Close"
        @click="dropError = ''"
      >
        <i class="fas fa-times" />
      </button>
    </div>
    <form
      method="POST"
      action="/upload"
      enctype="multipart/form-data"
      @submit="onFormSubmit"
    >
      <div class="card mb-4">
        <div class="card-header bg-light">
          <h5 class="mb-0">
            <i class="fas fa-database me-2" /> Data Source Type
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted mb-3">Start by selecting your data source, or drag &amp; drop files anywhere on this page:</p>
          <div class="row">
            <div class="col-md-3 mb-3 mb-md-0">
              <div
                class="form-check card h-100 p-3 border source-tile"
                :class="{ 'selected-source': fileType === 'CSV', 'drag-over': dragActiveTile === 'CSV' }"
                @dragover.prevent
                @dragenter.prevent="onTileDragEnter('CSV')"
                @dragleave.prevent="onTileDragLeave('CSV')"
                @drop.stop.prevent="onTileDrop('CSV', $event)"
              >
                <input
                  id="CSV"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="CSV"
                  class="form-check-input source-tile-radio"
                >
                <label
                  for="CSV"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-file-csv fa-2x mb-2 d-block text-primary" />
                  <strong>CSV Files</strong>
                  <small class="d-block text-muted">Upload one or more CSV files, or drag &amp; drop here</small>
                </label>
              </div>
            </div>
            <div class="col-md-3 mb-3 mb-md-0">
              <div
                class="form-check card h-100 p-3 border source-tile"
                :class="{ 'selected-source': fileType === 'Excel', 'drag-over': dragActiveTile === 'Excel' }"
                @dragover.prevent
                @dragenter.prevent="onTileDragEnter('Excel')"
                @dragleave.prevent="onTileDragLeave('Excel')"
                @drop.stop.prevent="onTileDrop('Excel', $event)"
              >
                <input
                  id="Excel"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="Excel"
                  class="form-check-input source-tile-radio"
                >
                <label
                  for="Excel"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-file-excel fa-2x mb-2 d-block text-success" />
                  <strong>Excel Files</strong>
                  <small class="d-block text-muted">Upload Excel files, or drag &amp; drop here</small>
                </label>
              </div>
            </div>
            <div class="col-md-3 mb-3 mb-md-0">
              <div
                class="form-check card h-100 p-3 border source-tile"
                :class="{ 'selected-source': fileType === 'Postgres' }"
              >
                <input
                  id="Postgres"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="Postgres"
                  class="form-check-input source-tile-radio"
                >
                <label
                  for="Postgres"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-database fa-2x mb-2 d-block text-info" />
                  <strong>PostgreSQL</strong>
                  <small class="d-block text-muted">Connect to a PostgreSQL database</small>
                </label>
              </div>
            </div>
            <div class="col-md-3">
              <div
                class="form-check card h-100 p-3 border source-tile"
                :class="{ 'selected-source': fileType === 'Other' }"
                @mouseenter="showOtherTooltip = true"
                @mouseleave="showOtherTooltip = false"
                @focus="showOtherTooltip = true"
                @blur="showOtherTooltip = false"
              >
                <input
                  id="Other"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="Other"
                  class="form-check-input source-tile-radio"
                >
                <label
                  for="Other"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-file-alt fa-2x mb-2 d-block text-secondary" />
                  <strong>Other</strong>
                  <small class="d-block text-muted">Prefer a different source type?</small>
                </label>
                <div
                  v-if="showOtherTooltip"
                  class="bootstrap-tooltip"
                  role="tooltip"
                >
                  Missing a source type? Please open an issue on the
                  <a
                    href="https://github.com/MaastrichtU-CDS/Flyover/issues"
                    target="_blank"
                    class="text-decoration-none text-white fw-bold"
                  >Flyover repo</a>.
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div
        v-show="fileType && fileType !== 'Other'"
        class="card mb-4"
      >
        <div class="card-header bg-light">
          <h5 class="mb-0">
            <i class="fas fa-sliders me-2" /> Specify Source Information
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted small mb-2">Provide the details for your selected source type:</p>
          <hr class="mt-0 mb-3">
          <div v-show="fileType === 'CSV' || fileType === 'Excel'">
            <div class="d-flex align-items-center flex-wrap" style="gap: 0;">
              <div class="input-group" style="min-width: 200px; max-width: 300px; flex: 1 1 auto; margin-right: 0.5rem;">
                <input
                  id="csvPath"
                  type="text"
                  name="csvPath"
                  :value="csvPath"
                  placeholder="No files selected"
                  readonly
                  class="form-control"
                >
                <div class="input-group-append">
                  <button
                    type="button"
                    class="btn btn-outline-secondary"
                    @click="triggerFileInput"
                  >
                    <i class="fas fa-folder-open" /> Browse
                  </button>
                </div>
              </div>
              <div
                v-show="fileType === 'CSV'"
                style="display: flex; align-items: center; gap: 0.5rem;"
              >
                <label
                  for="csv_separator_sign"
                  class="form-label small mb-0 text-nowrap"
                >Separator sign</label>
                <select
                  id="csv_separator_sign"
                  v-model="csvSeparatorSign"
                  name="csv_separator_sign"
                  class="form-control form-control-sm"
                  style="width: auto;"
                >
                  <option value=",">
                    Comma (,)
                  </option>
                  <option value=";">
                    Semicolon (;)
                  </option>
                  <option value="	">
                    Tab
                  </option>
                  <option value="|">
                    Pipe (|)
                  </option>
                </select>
                <label
                  for="csv_decimal_sign"
                  class="form-label small mb-0 text-nowrap"
                >Decimal sign</label>
                <select
                  id="csv_decimal_sign"
                  v-model="csvDecimalSign"
                  name="csv_decimal_sign"
                  class="form-control form-control-sm"
                  style="width: auto;"
                >
                  <option value=".">
                    Period (.)
                  </option>
                  <option value=",">
                    Comma (,)
                  </option>
                </select>
              </div>
            </div>
            <input
              id="csvFile"
              ref="csvFileInput"
              type="file"
              name="csvFile"
              style="display: none"
              multiple
              :accept="fileType === 'Excel' ? '.xlsx,.xls' : '.csv'"
              @change="handleFileChange"
            >
            <small class="form-text text-muted mt-2 d-block">
              <span v-if="fileType === 'CSV'">
                Supports multiple CSV files. Each file will be treated as a separate table.
              </span>
              <span v-else-if="fileType === 'Excel'">
                Supports Excel files (.xlsx, .xls). Each sheet will be treated as a separate table.
              </span>
            </small>
          </div>
          <div v-show="fileType === 'Postgres'">
            <div class="row">
              <div class="col-md-6 mb-2">
                <label
                  for="username"
                  class="form-label small"
                >Username:</label>
                <input
                  id="username"
                  v-model="pgUsername"
                  type="text"
                  name="username"
                  class="form-control form-control-sm"
                  :class="{ 'is-invalid': pgFieldError('username') }"
                  placeholder="Enter username"
                  @keydown="preventBlockedKey('username')"
                  @paste="stripBlockedOnPaste('username')"
                  @blur="pgFieldsTouched.username = true"
                >
                <div
                  v-if="pgFieldError('username')"
                  class="invalid-feedback d-block"
                >
                  {{ pgFieldError('username') }}
                </div>
              </div>
              <div class="col-md-6 mb-2">
                <label
                  for="password"
                  class="form-label small"
                >Password:</label>
                <input
                  id="password"
                  v-model="pgPassword"
                  type="password"
                  name="password"
                  class="form-control form-control-sm"
                  :class="{ 'is-invalid': pgFieldError('password') }"
                  placeholder="Enter password"
                  @keydown="preventBlockedKey('password')"
                  @paste="stripBlockedOnPaste('password')"
                  @blur="pgFieldsTouched.password = true"
                >
                <div
                  v-if="pgFieldError('password')"
                  class="invalid-feedback d-block"
                >
                  {{ pgFieldError('password') }}
                </div>
              </div>
              <div class="col-md-6 mb-2">
                <label
                  for="POSTGRES_URL"
                  class="form-label small"
                >URL:</label>
                <input
                  id="POSTGRES_URL"
                  v-model="pgUrl"
                  type="text"
                  name="POSTGRES_URL"
                  class="form-control form-control-sm"
                  :class="{ 'is-invalid': pgUrlError }"
                  placeholder="e.g. localhost:5432"
                  @keydown="preventBlockedKey('url')"
                  @paste="stripBlockedOnPaste('url')"
                  @blur="pgUrlTouched = true"
                >
                <div
                  v-if="pgUrlError"
                  class="invalid-feedback d-block"
                >
                  {{ pgUrlError }}
                </div>
              </div>
              <div class="col-md-6 mb-2">
                <label
                  for="POSTGRES_DB"
                  class="form-label small"
                >Database:</label>
                <input
                  id="POSTGRES_DB"
                  v-model="pgDb"
                  type="text"
                  name="POSTGRES_DB"
                  class="form-control form-control-sm"
                  :class="{ 'is-invalid': pgFieldError('db') }"
                  placeholder="Enter database name"
                  @keydown="preventBlockedKey('db')"
                  @paste="stripBlockedOnPaste('db')"
                  @blur="pgFieldsTouched.db = true"
                >
                <div
                  v-if="pgFieldError('db')"
                  class="invalid-feedback d-block"
                >
                  {{ pgFieldError('db') }}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-show="fileType === 'CSV' || fileType === 'Excel'">
        <div
          v-show="showPkFkSection"
          class="mt-4"
        >
          <hr>
          <div class="alert alert-info">
            <strong><i class="fas fa-info-circle" /> Multiple Tables Detected</strong><br>
            To establish relationships between your data tables, you can optionally
            specify Primary Keys (PK) and Foreign Keys (FK) for each table.
          </div>
          <div
            v-for="(tableName, index) in pkFkTables"
            :key="tableName"
            class="card mb-3"
          >
            <div class="card-header bg-light">
              <h6 class="mb-0">
                <i class="fas fa-table" /> {{ tableName }}
                <small class="text-white-50">
                  ({{ getFileColumns(tableName).length }} columns detected)
                </small>
                <span
                  v-if="inferredFk[index]"
                  class="badge bg-warning text-white ms-2 align-middle"
                >
                  <i class="fas fa-lightbulb" /> Inferred — please verify
                </span>
              </h6>
            </div>
            <div class="card-body">
              <div class="row">
                <div class="col-md-6">
                  <div class="form-group">
                    <label
                      :for="`pk_${index}`"
                      class="font-weight-bold"
                    >
                      <i class="fas fa-key text-warning" /> Primary Key:
                    </label>
                    <select
                      :id="`pk_${index}`"
                      v-model="pkSelections[index]"
                      :name="`pk_${index}`"
                      class="form-control"
                    >
                      <option value="">
                        -- No Primary Key --
                      </option>
                      <option
                        v-for="col in getFileColumns(tableName)"
                        :key="col"
                        :value="col"
                      >
                        {{ col }}
                      </option>
                    </select>
                  </div>
                </div>
                <div class="col-md-6">
                  <div class="form-group">
                    <label
                      :for="`fk_${index}`"
                      class="font-weight-bold"
                    >
                      <i class="fas fa-link text-info" /> Foreign Key:
                    </label>
                    <select
                      :id="`fk_${index}`"
                      v-model="fkSelections[index]"
                      :name="`fk_${index}`"
                      class="form-control"
                      :class="{ 'inferred-select': inferredFk[index] }"
                      @change="onFkManualChange(index)"
                    >
                      <option value="">
                        -- No Foreign Key --
                      </option>
                      <option
                        v-for="col in getFileColumns(tableName)"
                        :key="col"
                        :value="col"
                      >
                        {{ col }}
                      </option>
                    </select>
                  </div>
                </div>
              </div>
              <div
                v-show="fkSelections[index]"
                class="row"
              >
                <div class="col-md-6">
                  <div class="form-group">
                    <label
                      :for="`fkTable_${index}`"
                      class="font-weight-bold"
                    >
                      <i class="fas fa-arrow-right text-success" /> References Table:
                    </label>
                    <select
                      :id="`fkTable_${index}`"
                      v-model="fkTableSelections[index]"
                      :name="`fkTable_${index}`"
                      class="form-control"
                      @change="onFkTableChange(index)"
                    >
                      <option value="">
                        -- Select Referenced Table --
                      </option>
                      <option
                        v-for="otherTable in getOtherTables(tableName)"
                        :key="otherTable"
                        :value="otherTable"
                      >
                        {{ otherTable }}
                      </option>
                    </select>
                  </div>
                </div>
                <div class="col-md-6">
                  <div class="form-group">
                    <label
                      :for="`fkColumn_${index}`"
                      class="font-weight-bold"
                    >
                      <i class="fas fa-arrow-right text-success" /> References Column:
                    </label>
                    <select
                      :id="`fkColumn_${index}`"
                      v-model="fkColumnSelections[index]"
                      :name="`fkColumn_${index}`"
                      class="form-control"
                      :class="{ 'inferred-select': inferredFk[index] }"
                      @change="onFkManualChange(index)"
                    >
                      <option value="">
                        -- Select Referenced Column --
                      </option>
                      <option
                        v-for="col in getFileColumns(fkTableSelections[index])"
                        :key="col"
                        :value="col"
                      >
                        {{ col }}
                      </option>
                    </select>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div
          v-if="graphExists"
          v-show="showDataLinkingSection"
          class="mt-4"
        >
          <hr>
          <div class="alert alert-info">
            <strong><i class="fas fa-link" /> Link Your New Data to Existing Graph Data</strong><br>
            <p>
              This feature allows you to connect your new data with data that already
              exists in the graph database.
            </p>
          </div>

          <div class="form-check mb-3">
            <input
              id="enableDataLinking"
              v-model="enableDataLinking"
              class="form-check-input"
              type="checkbox"
              name="enableDataLinking"
            >
            <label
              class="form-check-label"
              for="enableDataLinking"
            >
              <strong>Enable Data Linking</strong>
            </label>
          </div>

          <div v-show="enableDataLinking">
            <div class="card">
              <div class="card-header">
                <h6 class="mb-0">
                  Configure Data Links
                </h6>
              </div>
              <div class="card-body">
                <div class="row">
                  <div class="col-md-6">
                    <h6>New Data (Data you're uploading now)</h6>
                    <div class="form-group">
                      <label for="newTableName">Select Data File:</label>
                      <select
                        id="newTableName"
                        v-model="newTableName"
                        name="newTableName"
                        class="form-control"
                      >
                        <option value="">
                          -- Select the table you want to link --
                        </option>
                        <option
                          v-for="table in pkFkTables"
                          :key="table"
                          :value="table"
                        >
                          {{ table }}
                        </option>
                      </select>
                    </div>
                    <div class="form-group">
                      <label for="newColumnName">Select Linking Column:</label>
                      <select
                        id="newColumnName"
                        v-model="newColumnName"
                        name="newColumnName"
                        class="form-control"
                      >
                        <option value="">
                          -- Select the column containing identifiers --
                        </option>
                        <option
                          v-for="col in newTableColumns"
                          :key="col"
                          :value="col"
                        >
                          {{ col }}
                        </option>
                      </select>
                    </div>
                  </div>
                  <div class="col-md-6">
                    <h6>Existing Graph Data (Data already in the graph database)</h6>
                    <div class="form-group">
                      <label for="existingTableName">Select Existing Table:</label>
                      <select
                        id="existingTableName"
                        v-model="existingTableName"
                        name="existingTableName"
                        class="form-control"
                      >
                        <option value="">
                          -- Select the existing data table to link to --
                        </option>
                        <option
                          v-for="table in existingTables"
                          :key="table"
                          :value="table"
                        >
                          {{ table }}
                        </option>
                      </select>
                    </div>
                    <div class="form-group">
                      <label for="existingColumnName">Select Matching Column:</label>
                      <select
                        id="existingColumnName"
                        v-model="existingColumnName"
                        name="existingColumnName"
                        class="form-control"
                      >
                        <option value="">
                          -- Select the column to match against --
                        </option>
                        <option
                          v-for="col in existingTableColumns"
                          :key="col"
                          :value="col"
                        >
                          {{ col }}
                        </option>
                      </select>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <br>
          </div>
          <div
            v-if="crossGraphLinkError"
            class="alert alert-warning mt-2"
          >
            <i class="fas fa-exclamation-triangle me-1" />{{ crossGraphLinkError }}
          </div>
          <input
            id="crossGraphLinkData"
            type="hidden"
            name="crossGraphLinkData"
            :value="crossGraphLinkDataJson"
          >
        </div>
      </div>



      <input
        id="pkFkData"
        type="hidden"
        name="pkFkData"
        :value="pkFkDataJson"
      >

      <button
        type="submit"
        class="btn btn-primary"
        :disabled="!isFormValid || submitting"
        :title="submitButtonTitle"
      >
        <i
          class="fas"
          :class="submitButtonIcon"
        />{{ submitButtonLabel }}
      </button>

      <div class="mt-4">
        <div class="alert alert-info-highlight py-2">
          <i class="fas fa-info-circle" />
          <strong>Directly uploading your graph data</strong><br>
          <div class="mt-1 ms-4">
            <p class="mb-1">
              If you have already converted your data using Flyover, you can also
              directly upload the files in the RDF store interface. In that case you can
              skip this data ingest step and proceed to describing and/or annotating
              your data.
            </p>
          </div>
        </div>
      </div>

      <template v-if="graphExists">
        <hr>
        <p>
          <i class="fas fa-exclamation-triangle me-1" />
          <i><b>The associated RDF store already contains a data graph.</b></i><br>
          <i>It is possible to add more data to this graph, for that you can use the data
            upload available above.</i><br>
          <i>It is, however, possible to describe the existing data without having to add
            new, for that please use the options below.</i>
        </p>
        <i>The existing data graph can also be removed through the RDF store interface.</i>
        <br><br>
        <button
          type="submit"
          class="btn btn-primary"
          @click="submitWithoutData"
        >
          <i class="fas fa-fast-forward" /> Proceed without adding data
        </button>
        <br><br>
      </template>
    </form>
  </div>
</template>

<style scoped>
.source-tile {
  position: relative;
  cursor: pointer;
}

.source-tile label {
  cursor: pointer;
}

.source-tile-radio {
  position: absolute;
  top: 0.5rem;
  right: 0.5rem;
  margin: 0;
  z-index: 1;
}

.selected-source {
  background-color: var(--bs-primary-bg-subtle, #cfe2ff);
  border-color: var(--bs-primary, #0d6efd);
  box-shadow: 0 0 0 1px var(--bs-primary, #0d6efd);
}

.drag-over {
  border-color: var(--bs-success, #198754);
  box-shadow: 0 0 0 2px var(--bs-success, #198754);
  background-color: var(--bs-success-bg-subtle, #d1e7dd);
}

.drop-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(13, 110, 253, 0.08);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9999;
  pointer-events: none;
}

.drop-overlay-content {
  border: 2px dashed var(--bs-primary, #0d6efd);
  pointer-events: none;
}

.bootstrap-tooltip {
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  padding: 0.5rem 0.75rem 0.6rem;
  background-color: rgba(0, 0, 0, 0.9);
  color: #fff;
  border-radius: 0.375rem;
  font-size: 0.875rem;
  white-space: normal;
  text-align: center;
  z-index: 1080;
  line-height: 1.4;
  max-width: 280px;
}

.bootstrap-tooltip::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border-width: 0.4rem 0.4rem 0;
  border-style: solid;
  border-color: rgba(0, 0, 0, 0.9) transparent transparent;
}

.inferred-select {
  border-color: var(--bs-warning, #ffc107);
  background-color: var(--bs-warning-bg-subtle, #fff3cd);
}
</style>
