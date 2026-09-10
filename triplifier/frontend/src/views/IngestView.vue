<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import JSZip from 'jszip'
import api from '@/services/api'
import { useNavigation } from '@/composables/useNavigation'

const { dataExists: graphExists, refreshDataExists } = useNavigation()

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

function isValidPgUrl(url) {
  if (!url) return false
  // Accept host:port (e.g. localhost:5432) or a full URL with a scheme.
  if (/^https?:\/\//.test(url)) {
    try {
      new URL(url)
      return true
    } catch {
      return false
    }
  }
  // host:port — validate host (hostname or IP) and numeric port
  return /^[a-zA-Z0-9._-]+:\d+$/.test(url)
}

// Characters that must be blocked per Postgres field.
// @ — hijacks connection-string parsing (postgresql://user:pass@host)
// \n \r — inject new lines into the backend .properties file via raw f-string
// = — injects key-value pairs in .properties file format
// / — changes the path in jdbc:postgresql://{url}/{db}
// Password only blocks newlines — psycopg2.connect() handles everything else.
const PG_BLOCKED_CHARS = {
  username: ['@', '\n', '\r', '='],
  password: ['\n', '\r'],
  url: ['@', '\n', '\r'],
  db: ['@', '\n', '\r', '=', '/'],
}

function pgBlockedCharsFor(field) {
  return PG_BLOCKED_CHARS[field] || []
}

function pgBlockedCharsLabel(field) {
  const chars = pgBlockedCharsFor(field)
  const printable = chars
    .filter((c) => c !== '\n' && c !== '\r')
    .map((c) => `"${c}"`)
  const parts = []
  if (printable.length) parts.push(printable.join(', '))
  if (chars.includes('\n')) parts.push('line breaks')
  return parts.join(' and ')
}

function preventBlockedKey(field) {
  return (e) => {
    if (pgBlockedCharsFor(field).includes(e.key)) e.preventDefault()
  }
}

function stripBlockedOnPaste(field) {
  return (e) => {
    const text = (e.clipboardData || window.clipboardData).getData('text')
    const blocked = pgBlockedCharsFor(field)
    if (blocked.some((c) => text.includes(c))) {
      e.preventDefault()
      const stripped = [...blocked].reduce(
        (s, c) => s.replaceAll(c, ''),
        text
      )
      document.execCommand('insertText', false, stripped)
    }
  }
}

function pgFieldError(field) {
  if (!pgFieldsTouched[field]) return ''
  const value = { username: pgUsername, password: pgPassword, db: pgDb }[field]?.value
  if (!value) return ''
  const blocked = pgBlockedCharsFor(field)
  const found = blocked.filter((c) => value.includes(c))
  if (found.length) {
    return `The following are not allowed: ${pgBlockedCharsLabel(field)}.`
  }
  return ''
}

function pgHasBlockedChar(field) {
  const value = { username: pgUsername, password: pgPassword, db: pgDb }[field]?.value
  if (!value) return false
  return pgBlockedCharsFor(field).some((c) => value.includes(c))
}

const pgUrlError = computed(() => {
  if (!pgUrlTouched.value) return ''
  if (pgUrl.value.includes('@') || /[\n\r]/.test(pgUrl.value)) {
    return `The following are not allowed: ${pgBlockedCharsLabel('url')}.`
  }
  if (!pgUrl.value) return 'URL is required.'
  if (!isValidPgUrl(pgUrl.value)) return 'Enter a valid host:port (e.g. localhost:5432) or a full URL.'
  return ''
})

const pkSelections = reactive({})
const fkSelections = reactive({})
const fkTableSelections = reactive({})
const fkColumnSelections = reactive({})

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

function tableNameOf(fileName) {
  return fileName.replace('.csv', '').replace('.xlsx', '').replace('.xls', '')
}

function getFileColumns(tableName) {
  if (!tableName) return []
  return csvColumns[tableName] || []
}

function getOtherTables(currentName) {
  return pkFkTables.value.filter((t) => t !== currentName)
}

function validatePkFkRelationships() {
  if (!showPkFkSection.value) return true
  let valid = true
  pkFkTables.value.forEach((_, index) => {
    const fk = fkSelections[index] || ''
    const fkTable = fkTableSelections[index] || ''
    if (fk && fkTable) {
      const refIdx = pkFkTables.value.findIndex((t) => t === fkTable)
      if (refIdx !== -1) {
        const refPk = pkSelections[refIdx] || ''
        if (!refPk) valid = false
      }
    }
  })
  return valid
}

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
      !pgHasBlockedChar('username') &&
      !pgHasBlockedChar('password') &&
      !pgHasBlockedChar('url') &&
      !pgHasBlockedChar('db'))
  return basic && validatePkFkRelationships()
})

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

const pkFkDataJson = computed(() => {
  if (!showPkFkSection.value) return ''
  const data = []
  pkFkTables.value.forEach((tableName, index) => {
    const pk = pkSelections[index] || ''
    const fk = fkSelections[index] || ''
    const fkTable = fkTableSelections[index] || ''
    const fkColumn = fkColumnSelections[index] || ''
    if (pk || fk) {
      data.push({
        fileName: tableName,
        primaryKey: pk || null,
        foreignKey: fk || null,
        foreignKeyTable: fkTable || null,
        foreignKeyColumn: fkColumn || null,
      })
    }
  })
  return JSON.stringify(data)
})

const crossGraphLinkDataJson = computed(() => {
  if (!enableDataLinking.value) return ''
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

function detectSeparator(line) {
  const seps = [',', ';', '\t', '|']
  let best = ','
  let max = 0
  for (const s of seps) {
    const c = line.split(s).length - 1
    if (c > max) {
      max = c
      best = s
    }
  }
  return best
}

function parseCSVLine(line, sep) {
  const out = []
  let cur = ''
  let q = false
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (ch === '"') q = !q
    else if (ch === sep && !q) {
      out.push(cur.trim())
      cur = ''
    } else cur += ch
  }
  out.push(cur.trim())
  return out.map((h) => h.replace(/"/g, '').trim()).filter(Boolean)
}

function readCSVColumns(file) {
  return new Promise((resolve) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const lines = e.target.result.split('\n')
        if (!lines.length) return resolve([])
        const sep = detectSeparator(lines[0])
        resolve(parseCSVLine(lines[0], sep))
      } catch {
        resolve([])
      }
    }
    reader.onerror = () => resolve([])
    reader.readAsText(file.slice(0, 1024))
  })
}

// Read sheet names and column headers from an .xlsx file using JSZip.
// Returns a list of { name, columns } entries, one per sheet.
// Column reading from the raw XML is complex; we read the first row
// of each sheet (inline strings only). If that fails we fall back to
// an empty column list — the PK/FK UI degrades gracefully.
async function readExcelSheetInfo(file) {
  try {
    const zip = await JSZip.loadAsync(file)
    const workbookXml = await zip.file('xl/workbook.xml')?.async('string')
    if (!workbookXml) return []
    // Parse sheet names from <sheet name="..."> elements
    const sheetNames = []
    const sheetRegex = /<sheet\s+[^>]*name="([^"]+)"/g
    let match
    while ((match = sheetRegex.exec(workbookXml)) !== null) {
      sheetNames.push(match[1])
    }
    if (!sheetNames.length) return []

    // Try to read column headers from each sheet.
    // The relationship file maps rId -> sheet file path.
    const relsXml = await zip.file('xl/_rels/workbook.xml.rels')?.async('string')
    const sheetPaths = []
    if (relsXml) {
      const relRegex = /<Relationship\s+[^>]*Id="([^"]+)"[^>]*Target="([^"]+)"/g
      const rels = []
      while ((match = relRegex.exec(relsXml)) !== null) {
        rels.push({ id: match[1], target: match[2] })
      }
      // Also need the sheet -> rId mapping from workbook.xml
      const sheetRelRegex = /<sheet\s+[^>]*name="([^"]+)"[^>]*r:id="([^"]+)"/g
      let relMatch
      while ((relMatch = sheetRelRegex.exec(workbookXml)) !== null) {
        const rel = rels.find((r) => r.id === relMatch[2])
        if (rel) {
          sheetPaths.push({
            name: relMatch[1],
            path: rel.target.startsWith('/') ? rel.target.slice(1) : `xl/${rel.target}`,
          })
        }
      }
    }

    const result = []
    for (let i = 0; i < sheetNames.length; i++) {
      const sheetInfo = sheetPaths[i]
      let columns = ['col1', 'col2', 'col3'] // fallback
      if (sheetInfo) {
        try {
          const sheetXml = await zip.file(sheetInfo.path)?.async('string')
          if (sheetXml) {
            // Extract inline string cells from the first row
            const rowMatch = sheetXml.match(/<row\s+r="1"[^>]*>([\s\S]*?)<\/row>/)
            if (rowMatch) {
              const cellRegex = /<t>([^<]+)<\/t>/g
              const cols = []
              let cellMatch
              while ((cellMatch = cellRegex.exec(rowMatch[1])) !== null) {
                cols.push(cellMatch[1])
              }
              if (cols.length) columns = cols
            }
          }
        } catch { /* use fallback */ }
      }
      result.push({ name: sheetNames[i], columns })
    }
    return result
  } catch {
    return []
  }
}

function resetPkFk() {
  for (const k of Object.keys(pkSelections)) delete pkSelections[k]
  for (const k of Object.keys(fkSelections)) delete fkSelections[k]
  for (const k of Object.keys(fkTableSelections)) delete fkTableSelections[k]
  for (const k of Object.keys(fkColumnSelections)) delete fkColumnSelections[k]
}

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
        const tableName = base
        tables.push(tableName)
        csvColumns[tableName] = []
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
  } else {
    // For CSV, each file is a table.
    pkFkTables.value = Array.from(files).map((f) => f.name)

    // Visibility depends only on file counts, not on the column reads. Set it
    // before awaiting so the multi-file UI appears immediately and tests don't
    // race the FileReader.onload macrotask.
    showPkFkSection.value = files.length > 1

    const cols = await Promise.all(
      Array.from(files).map((f) => readCSVColumns(f))
    )
    Array.from(files).forEach((f, i) => {
      csvColumns[f.name] = cols[i]
    })
  }

  showDataLinkingSection.value = graphExists.value && files.length > 0
}

async function handleFileChange(e) {
  await processFiles(e.target.files)
}

function setFileInputFiles(files) {
  if (typeof DataTransfer === 'undefined' || !csvFileInput.value) return
  const dt = new DataTransfer()
  for (const file of files) dt.items.add(file)
  csvFileInput.value.files = dt.files
}

function filterDroppedFiles(fileList, type) {
  const exts = type === 'Excel' ? ['.xlsx', '.xls'] : ['.csv']
  return Array.from(fileList).filter((f) =>
    exts.some((ext) => f.name.toLowerCase().endsWith(ext))
  )
}

function detectFileType(files) {
  const all = Array.from(files)
  const csvCount = all.filter((f) => f.name.toLowerCase().endsWith('.csv')).length
  const excelCount = all.filter(
    (f) => f.name.toLowerCase().endsWith('.xlsx') || f.name.toLowerCase().endsWith('.xls')
  ).length
  if (csvCount === all.length) return 'CSV'
  if (excelCount === all.length) return 'Excel'
  return null
}

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
  const dropped = filterDroppedFiles(e.dataTransfer.files, type)
  if (!dropped.length) {
    const exts = type === 'Excel' ? '.xlsx or .xls' : '.csv'
    dropError.value = `Please drop only ${exts} files on the ${type} tile.`
    return
  }
  fileType.value = type
  setFileInputFiles(dropped)
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
  const detected = detectFileType(allFiles)
  if (!detected) {
    const names = allFiles.map((f) => f.name).join(', ')
    dropError.value = `Unsupported file type(s): ${names}. Please use .csv, .xlsx, or .xls files.`
    return
  }
  fileType.value = detected
  setFileInputFiles(allFiles)
  await processFiles(allFiles)
}

function onFkTableChange(index) {
  fkColumnSelections[index] = ''
}

// When a PK is set on table at index pkIndex, check every other table for
// a column whose name matches the PK (case-insensitive). If found and the
// other table's FK fields are not already manually set, auto-fill them.
function autoSuggestFk(pkIndex) {
  const pkColumn = pkSelections[pkIndex]
  if (!pkColumn) return
  const pkTableName = pkFkTables.value[pkIndex]
  if (!pkTableName) return

  const pkLower = pkColumn.toLowerCase()

  pkFkTables.value.forEach((tableName, index) => {
    if (index === pkIndex) return
    // Don't override a manually-set FK
    if (fkSelections[index]) return

    const cols = csvColumns[tableName]
    if (!cols || !cols.length) return

    // Look for a case-insensitive exact match first, then a loose match
    // (contains the PK name or vice-versa).
    let match = cols.find((c) => c.toLowerCase() === pkLower)
    if (!match) {
      match = cols.find(
        (c) => c.toLowerCase().includes(pkLower) || pkLower.includes(c.toLowerCase())
      )
    }
    if (match) {
      fkSelections[index] = match
      fkTableSelections[index] = pkTableName
      fkColumnSelections[index] = pkColumn
    }
  })
}

// Clear auto-suggested FK fields when a PK is removed, so stale suggestions
// don't persist after the user changes their mind.
function clearAutoSuggestedFk(pkIndex) {
  const pkTableName = pkFkTables.value[pkIndex]
  if (!pkTableName) return

  pkFkTables.value.forEach((_, index) => {
    if (index === pkIndex) return
    if (fkTableSelections[index] === pkTableName) {
      fkSelections[index] = ''
      fkTableSelections[index] = ''
      fkColumnSelections[index] = ''
    }
  })
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
                <small class="text-muted">
                  ({{ getFileColumns(tableName).length }} columns detected)
                </small>
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
</style>
