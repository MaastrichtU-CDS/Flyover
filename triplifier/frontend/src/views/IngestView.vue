<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import api from '@/services/api'
import { useNavigation } from '@/composables/useNavigation'

const { dataExists: graphExists, refreshDataExists } = useNavigation()

const fileType = ref('CSV')
const csvFiles = ref([])
const csvColumns = reactive({})
const csvPath = ref('')
const csvSeparatorSign = ref('')
const csvDecimalSign = ref('')

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

const pkSelections = reactive({})
const fkSelections = reactive({})
const fkTableSelections = reactive({})
const fkColumnSelections = reactive({})

const showPkFkSection = ref(false)
const showDataLinkingSection = ref(false)
const csvFileInput = ref(null)
const submitting = ref(false)

const newTableColumns = computed(() => {
  if (!newTableName.value) return []
  const file = csvFiles.value.find(
    (f) => tableNameOf(f.name) === newTableName.value
  )
  return file ? csvColumns[file.name] || [] : []
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

function getFileColumns(fileName) {
  if (!fileName) return []
  return csvColumns[fileName] || []
}

function getOtherFiles(currentName) {
  return csvFiles.value.filter((f) => f.name !== currentName)
}

function validatePkFkRelationships() {
  if (!showPkFkSection.value) return true
  let valid = true
  csvFiles.value.forEach((file, index) => {
    const fk = fkSelections[index] || ''
    const fkTable = fkTableSelections[index] || ''
    if (fk && fkTable) {
      const refIdx = csvFiles.value.findIndex((f) => f.name === fkTable)
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
      pgDb.value)
  return basic && validatePkFkRelationships()
})

const submitButtonTitle = computed(() => {
  if (isFormValid.value) return ''
  if (!validatePkFkRelationships()) {
    return 'Please select primary keys for all tables that are referenced by foreign keys'
  }
  return ''
})

const pkFkDataJson = computed(() => {
  if (!showPkFkSection.value) return ''
  const data = []
  csvFiles.value.forEach((file, index) => {
    const pk = pkSelections[index] || ''
    const fk = fkSelections[index] || ''
    const fkTable = fkTableSelections[index] || ''
    const fkColumn = fkColumnSelections[index] || ''
    if (pk || fk) {
      data.push({
        fileName: file.name,
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

function resetPkFk() {
  for (const k of Object.keys(pkSelections)) delete pkSelections[k]
  for (const k of Object.keys(fkSelections)) delete fkSelections[k]
  for (const k of Object.keys(fkTableSelections)) delete fkTableSelections[k]
  for (const k of Object.keys(fkColumnSelections)) delete fkColumnSelections[k]
}

function triggerFileInput() {
  csvFileInput.value?.click()
}

async function handleFileChange(e) {
  const input = e.target
  csvFiles.value = []
  for (const k of Object.keys(csvColumns)) delete csvColumns[k]
  resetPkFk()

  const paths = []
  for (let i = 0; i < input.files.length; i++) {
    paths.push(input.files[i].name)
    csvFiles.value.push(input.files[i])
  }
  csvPath.value = paths.join(', ')

  // Visibility depends only on file counts, not on the column reads. Set it
  // before awaiting so the multi-file UI appears immediately and tests don't
  // race the FileReader.onload macrotask.
  showPkFkSection.value = input.files.length > 1
  showDataLinkingSection.value = graphExists.value && input.files.length > 0

  const cols = await Promise.all(
    Array.from(input.files).map((f) => readCSVColumns(f))
  )
  cols.forEach((c, i) => {
    csvColumns[input.files[i].name] = c
  })
}

function onFkTableChange(index) {
  fkColumnSelections[index] = ''
}

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
  <div>
    <h1><i class="fas fa-cookie-bite" /> Ingest your data</h1>
    <hr>
    <p>
      First, you have to ensure that the graph database contains data and ontology graphs.<br>
      You can achieve this by submitting your data for conversion using Flyover.
    </p>
    <hr>
    <header>Start by selecting your data source:</header>

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
          <div v-show="fileType === 'CSV' || fileType === 'Excel'" class="mb-3">
            <div class="input-group input-group-sm">
              <input
                id="csvPath"
                type="text"
                name="csvPath"
                :value="csvPath"
                placeholder="No files selected"
                readonly
                class="form-control form-control-sm"
              >
              <button
                type="button"
                class="btn btn-primary btn-sm"
                @click="triggerFileInput"
              >
                <i class="fas fa-folder-open me-1" /> Browse
              </button>
            </div>
            <small class="form-text text-muted mt-1 d-block">
              <span v-if="fileType === 'CSV'">
                Supports multiple CSV files. Each file will be treated as a separate table.
              </span>
              <span v-else-if="fileType === 'Excel'">
                Supports Excel files (.xlsx, .xls). Each sheet will be treated as a separate table.
              </span>
            </small>
          </div>
          <div class="row">
            <div class="col-md-3 mb-3 mb-md-0">
              <div class="form-check card h-100 p-3 border">
                <input
                  id="CSV"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="CSV"
                  class="form-check-input"
                >
                <label
                  for="CSV"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-file-csv fa-2x mb-2 d-block text-primary" />
                  <strong>CSV Files</strong>
                  <small class="d-block text-muted">Upload one or more CSV files</small>
                </label>
                <div
                  v-show="fileType === 'CSV'"
                  class="mt-3"
                >
                  <div class="row">
                    <div class="col-6">
                      <label
                        for="csv_separator_sign"
                        class="form-label small"
                      >Separator:</label>
                      <select
                        id="csv_separator_sign"
                        v-model="csvSeparatorSign"
                        name="csv_separator_sign"
                        class="form-select form-select-sm"
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
                    </div>
                    <div class="col-6">
                      <label
                        for="csv_decimal_sign"
                        class="form-label small"
                      >Decimal:</label>
                      <select
                        id="csv_decimal_sign"
                        v-model="csvDecimalSign"
                        name="csv_decimal_sign"
                        class="form-select form-select-sm"
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
                </div>
              </div>
            </div>
            <div class="col-md-3 mb-3 mb-md-0">
              <div class="form-check card h-100 p-3 border">
                <input
                  id="Excel"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="Excel"
                  class="form-check-input"
                >
                <label
                  for="Excel"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-file-excel fa-2x mb-2 d-block text-success" />
                  <strong>Excel Files</strong>
                  <small class="d-block text-muted">Upload Excel files with multiple sheets</small>
                </label>
              </div>
            </div>
            <div class="col-md-3">
              <div class="form-check card h-100 p-3 border">
                <input
                  id="Postgres"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="Postgres"
                  class="form-check-input"
                >
                <label
                  for="Postgres"
                  class="form-check-label d-block"
                >
                  <i class="fas fa-database fa-2x mb-2 d-block text-info" />
                  <strong>PostgreSQL</strong>
                  <small class="d-block text-muted">Connect to a PostgreSQL database</small>
                </label>
                <div
                  v-show="fileType === 'Postgres'"
                  class="mt-3"
                >
                  <div class="row">
                    <div class="col-6 mb-2">
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
                        placeholder="Enter username"
                      >
                    </div>
                    <div class="col-6 mb-2">
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
                        placeholder="Enter password"
                      >
                    </div>
                    <div class="col-6 mb-2">
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
                        placeholder="e.g., localhost:5432"
                      >
                    </div>
                    <div class="col-6 mb-2">
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
                        placeholder="Enter database name"
                      >
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <div class="col-md-3">
              <div class="form-check card h-100 p-3 border">
                <input
                  id="Other"
                  v-model="fileType"
                  type="radio"
                  name="fileType"
                  value="Other"
                  class="form-check-input"
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
                  v-show="fileType === 'Other'"
                  class="mt-3"
                >
                  <small class="form-text text-muted d-block">
                    Nothing to see here just yet. <a
                      href="https://github.com/MaastrichtU-CDS/Flyover/issues"
                      target="_blank"
                      class="text-decoration-none"
                    >Let us know!</a>
                  </small>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-show="fileType === 'CSV' || fileType === 'Excel'">
        <hr>
        <div class="card mb-4">
          <div class="card-header bg-light">
            <h5 class="mb-0">
              <i class="fas fa-upload me-2" /> File Upload
            </h5>
          </div>
          <div class="card-body">
            <p class="text-muted mb-3">
              Please select the {{ fileType === 'CSV' ? 'CSV' : 'Excel' }} file(s) you would like to process
            </p>
            <div class="input-group">
              <input
                id="csvPath"
                type="text"
                name="csvPath"
                :value="csvPath"
                placeholder="No files selected"
                readonly
                class="form-control"
              >
              <button
                type="button"
                class="btn btn-primary"
                @click="triggerFileInput"
              >
                <i class="fas fa-folder-open me-1" /> Browse Files
              </button>
            </div>
            <input
              id="csvFile"
              ref="csvFileInput"
              type="file"
              name="csvFile"
              style="display: none"
              multiple
              accept=".csv,.xlsx,.xls"
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
        </div>


        <div
          v-show="showPkFkSection"
          class="mt-4"
        >
          <hr>
          <div class="alert alert-info">
            <strong><i class="fas fa-info-circle" /> Multiple CSV Files Detected</strong><br>
            To establish relationships between your data files, you can optionally
            specify Primary Keys (PK) and Foreign Keys (FK) for each file.
          </div>
          <div
            v-for="(file, index) in csvFiles"
            :key="file.name"
            class="card mb-3"
          >
            <div class="card-header bg-light">
              <h6 class="mb-0">
                <i class="fas fa-table" /> {{ file.name }}
                <small class="text-muted">
                  ({{ getFileColumns(file.name).length }} columns detected)
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
                        v-for="col in getFileColumns(file.name)"
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
                        v-for="col in getFileColumns(file.name)"
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
                        v-for="otherFile in getOtherFiles(file.name)"
                        :key="otherFile.name"
                        :value="otherFile.name"
                      >
                        {{ otherFile.name }}
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
                          -- Select the CSV file you want to link --
                        </option>
                        <option
                          v-for="file in csvFiles"
                          :key="file.name"
                          :value="tableNameOf(file.name)"
                        >
                          {{ tableNameOf(file.name) }}
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
          :class="submitting ? 'fa-cookie' : 'fa-play'"
        />
        {{ submitting ? ' Processing...' : ' Submit Files' }}
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
