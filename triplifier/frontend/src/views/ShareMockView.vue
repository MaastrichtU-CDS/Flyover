<script setup>
import { computed, onMounted, ref } from 'vue'
import JSZip from 'jszip'
import api from '@/services/api'
import * as db from '@/lib/db'
import { useStatusStore } from '@/stores/status'

const status = useStatusStore()

const fileInput = ref(null)
const selectedFileName = ref('')
const storedJsonldData = ref(null)
const detectedJsonldData = ref(null)
const generatedData = ref(null)

const sampleCount = ref(100)
const randomSeed = ref('')
const databaseId = ref('')
const tableId = ref('')

const uploading = ref(false)
const generating = ref(false)
const uploadProgress = ref(0)
const generationProgress = ref(0)

function getSemanticMapStats(data) {
  let dbCount = 0
  let tableCount = 0
  let columnCount = 0
  if (data?.databases) {
    for (const dbData of Object.values(data.databases)) {
      dbCount++
      if (dbData?.tables) {
        for (const tableData of Object.values(dbData.tables)) {
          tableCount++
          if (tableData?.columns) {
            columnCount += Object.keys(tableData.columns).length
          }
        }
      }
    }
  } else if (data?.variable_info) {
    columnCount = Object.keys(data.variable_info).length
  }
  return { dbCount, tableCount, columnCount }
}

const stats = computed(() => {
  const map = storedJsonldData.value || detectedJsonldData.value
  if (!map) return null
  return getSemanticMapStats(map)
})

const databaseOptions = computed(() =>
  storedJsonldData.value?.databases
    ? Object.keys(storedJsonldData.value.databases)
    : []
)

const tableOptions = computed(() => {
  if (!storedJsonldData.value || !databaseId.value) return []
  return Object.keys(
    storedJsonldData.value.databases?.[databaseId.value]?.tables || {}
  )
})

function onFilePicked(e) {
  const f = e.target.files?.[0]
  selectedFileName.value = f ? f.name : ''
}

async function handleFileUpload() {
  const file = fileInput.value?.files?.[0]
  if (!file) return
  uploading.value = true
  uploadProgress.value = 25
  try {
    const text = await file.text()
    uploadProgress.value = 50
    const mappingData = JSON.parse(text)
    uploadProgress.value = 75
    if (!mappingData?.databases && !mappingData?.variable_info) {
      throw new Error('Invalid JSON-LD structure: missing databases or variable_info')
    }
    await db.saveData('metadata', {
      key: 'semantic_map',
      data: mappingData,
      timestamp: new Date().toISOString(),
    })
    uploadProgress.value = 100
    storedJsonldData.value = mappingData
    detectedJsonldData.value = null
    if (databaseOptions.value.length === 1) {
      databaseId.value = databaseOptions.value[0]
    }
  } catch (e) {
    status.error(`Error processing JSON-LD file: ${e.message || e}`)
  } finally {
    uploading.value = false
    setTimeout(() => {
      uploadProgress.value = 0
    }, 500)
  }
}

function useStoredJsonld() {
  if (detectedJsonldData.value) {
    storedJsonldData.value = detectedJsonldData.value
    detectedJsonldData.value = null
    if (databaseOptions.value.length === 1) {
      databaseId.value = databaseOptions.value[0]
    }
  }
}

function dataToCsv(data) {
  if (!data?.length) return ''
  const headers = Object.keys(data[0] || {})
  const lines = [headers.join(',')]
  for (const row of data) {
    const cells = headers.map((h) => {
      const v = row[h]
      if (v == null) return ''
      let cell = String(v).replace(/"/g, '""')
      if (cell.includes(',') || cell.includes('"') || cell.includes('\n')) {
        cell = `"${cell}"`
      }
      return cell
    })
    lines.push(cells.join(','))
  }
  return lines.join('\r\n')
}

function downloadCsv(rows, filename) {
  const csv = dataToCsv(rows)
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 100)
}

async function downloadAllAsZip() {
  if (!generatedData.value) return
  const zip = new JSZip()
  const entries = Object.entries(generatedData.value).sort(([a], [b]) =>
    a.localeCompare(b)
  )
  for (const [tableKey, tableData] of entries) {
    const [dbId, tId] = tableKey.split('.')
    zip.file(`${dbId}_${tId}_mock.csv`, dataToCsv(tableData))
  }
  const blob = await zip.generateAsync({ type: 'blob' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'mock_data_export.zip'
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 100)
}

async function generateMockData() {
  if (!storedJsonldData.value) {
    status.error('Please upload or select a JSON-LD semantic map first.')
    return
  }
  const n = parseInt(sampleCount.value, 10)
  if (Number.isNaN(n) || n < 1 || n > 10000) {
    status.error('Please enter a valid number of samples (1-10,000).')
    return
  }
  generating.value = true
  generationProgress.value = 25
  try {
    const { data: result } = await api.post('/api/generate-mock-data', {
      jsonld_map: storedJsonldData.value,
      num_rows: n,
      random_seed: randomSeed.value ? parseInt(randomSeed.value, 10) : null,
      database_id: databaseId.value || null,
      table_id: tableId.value || null,
    })
    if (!result.success) throw new Error(result.error || 'Failed to generate mock data')
    generationProgress.value = 75
    generatedData.value = result.data
    setTimeout(() => {
      generationProgress.value = 100
      setTimeout(() => {
        generationProgress.value = 0
      }, 500)
    }, 200)
  } catch (e) {
    status.error(`Error generating mock data: ${e.message || e}`)
    generationProgress.value = 0
  } finally {
    generating.value = false
  }
}

const generatedTableEntries = computed(() => {
  if (!generatedData.value) return []
  return Object.entries(generatedData.value)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([tableKey, tableData]) => {
      const [dbId, tId] = tableKey.split('.')
      return { tableKey, dbId, tableId: tId, tableData }
    })
})

onMounted(async () => {
  try {
    const r = await db.getData('metadata', 'semantic_map')
    if (r?.data) {
      detectedJsonldData.value = r.data
    }
  } catch (e) {
    console.error('IndexedDB read failed:', e)
  }
})
</script>

<template>
  <div>
    <h1><i class="fas fa-file-export"></i> Generate Mock Data</h1>
    <hr />
    <p style="line-height: 1.5; margin: 0">
      Generate anonymous mock data based on your semantic mapping and data structure.
      This allows you to share realistic test data without privacy concerns.
    </p>
    <br />

    <div class="card mb-4">
      <div class="card-header">
        <h5 class="mb-0">
          <i class="fas fa-database"></i> Mock Data Generation
        </h5>
      </div>
      <div class="card-body">
        <p class="text-muted">
          This feature generates synthetic data that maintains the structure and
          statistical properties of your original dataset while ensuring complete
          anonymity.
        </p>

        <hr />

        <div v-if="!storedJsonldData">
          <h6><i class="fas fa-sitemap"></i> JSON-LD Semantic Map</h6>
          <p class="text-muted mb-3">
            To generate mock data, you need a Flyover semantic mapping in JSON-LD
            format.
          </p>

          <div v-if="detectedJsonldData" class="alert alert-warning">
            <i class="fas fa-info-circle"></i>
            <strong>Existing semantic map detected!</strong><br />
            You already have a semantic map stored in your browser from a previous
            session. You can use the stored map or upload a new JSON-LD file to
            overwrite it.
            <br />
            <small class="text-muted" v-if="stats"
              >Current semantic map: {{ stats.dbCount }}
              {{ stats.dbCount === 1 ? 'database' : 'databases' }},
              {{ stats.tableCount }}
              {{ stats.tableCount === 1 ? 'table' : 'tables' }},
              {{ stats.columnCount }}
              {{ stats.columnCount === 1 ? 'column' : 'columns' }}.</small
            >
          </div>

          <div class="mb-4">
            <div class="custom-file mb-3">
              <input
                ref="fileInput"
                type="file"
                class="form-control"
                accept=".jsonld"
                @change="onFilePicked"
              />
            </div>
            <button
              type="button"
              class="btn btn-outline-primary"
              :disabled="!selectedFileName || uploading"
              @click="handleFileUpload"
            >
              <i v-if="uploading" class="fas fa-spinner fa-spin"></i>
              <i v-else class="fas fa-upload"></i>
              Upload JSON-LD
            </button>
            <button
              v-if="detectedJsonldData"
              type="button"
              class="btn btn-outline-secondary"
              @click="useStoredJsonld"
            >
              <i class="fas fa-database"></i> Use Stored JSON-LD
            </button>

            <div v-if="uploadProgress > 0" class="progress mt-3">
              <div
                class="progress-bar"
                role="progressbar"
                :style="{ width: `${uploadProgress}%` }"
              >
                {{ uploadProgress }}%
              </div>
            </div>
          </div>
        </div>

        <div v-if="storedJsonldData" class="alert alert-success">
          <i class="fas fa-check-circle"></i>
          <strong>JSON-LD semantic map loaded successfully!</strong>
          <span v-if="stats">
            ({{ stats.dbCount }}
            {{ stats.dbCount === 1 ? 'database' : 'databases' }},
            {{ stats.tableCount }}
            {{ stats.tableCount === 1 ? 'table' : 'tables' }},
            {{ stats.columnCount }}
            {{ stats.columnCount === 1 ? 'column' : 'columns' }})
          </span>
        </div>

        <div v-if="storedJsonldData">
          <hr />
          <h6><i class="fas fa-cogs"></i> Generation Options</h6>

          <div class="form-group">
            <label for="sampleCount"><strong>Number of Samples to Generate</strong></label>
            <input
              id="sampleCount"
              v-model="sampleCount"
              type="number"
              class="form-control"
              min="1"
              max="10000"
            />
            <small class="form-text text-muted"
              >Enter the number of rows you want in your mock dataset (1–10,000).</small
            >
          </div>

          <div class="form-group">
            <label for="randomSeed"><strong>Random Seed (Optional)</strong></label>
            <input
              id="randomSeed"
              v-model="randomSeed"
              type="number"
              class="form-control"
              placeholder="Leave empty for random results"
            />
          </div>

          <div class="form-group">
            <label><strong>Database and Table Selection</strong></label>
            <div class="row">
              <div class="col-md-6">
                <label for="databaseSelect">Database</label>
                <select
                  id="databaseSelect"
                  v-model="databaseId"
                  class="form-control"
                  @change="tableId = ''"
                >
                  <option value="">-- All Databases --</option>
                  <option v-for="d in databaseOptions" :key="d" :value="d">
                    {{ d }}
                  </option>
                </select>
              </div>
              <div class="col-md-6">
                <label for="tableSelect">Table</label>
                <select id="tableSelect" v-model="tableId" class="form-control">
                  <option value="">-- All Tables --</option>
                  <option v-for="t in tableOptions" :key="t" :value="t">
                    {{ t }}
                  </option>
                </select>
              </div>
            </div>
          </div>

          <button
            type="button"
            class="btn btn-primary mt-3"
            :disabled="generating"
            @click="generateMockData"
          >
            <i v-if="generating" class="fas fa-spinner fa-spin"></i>
            <i v-else class="fas fa-magic"></i>
            Generate Mock Data
          </button>

          <div v-if="generationProgress > 0" class="progress mt-3">
            <div
              class="progress-bar"
              role="progressbar"
              :style="{ width: `${generationProgress}%` }"
            >
              {{ generationProgress }}%
            </div>
          </div>
        </div>

        <div v-if="generatedData" class="mt-4">
          <hr />
          <h6><i class="fas fa-check-double"></i> Generation Results</h6>
          <div class="alert alert-success">
            <i class="fas fa-check-circle"></i>
            <strong>Mock data generated successfully!</strong>
            {{ generatedTableEntries.length }} table(s) generated with
            {{ sampleCount }} samples each.
          </div>

          <div class="form-group">
            <label><strong>Download mock data</strong></label>
            <div class="d-flex flex-wrap gap-2">
              <button
                v-if="generatedTableEntries.length > 1"
                type="button"
                class="btn btn-primary mb-3"
                @click="downloadAllAsZip"
              >
                <i class="fas fa-file-archive"></i> Download all as ZIP
              </button>
              <div v-if="generatedTableEntries.length > 1" class="w-100"></div>
              <button
                v-for="entry in generatedTableEntries"
                :key="entry.tableKey"
                type="button"
                class="btn btn-outline-secondary"
                :title="`Download ${entry.dbId}.${entry.tableId} as CSV`"
                @click="downloadCsv(entry.tableData, `${entry.dbId}_${entry.tableId}_mock.csv`)"
              >
                <i class="fas fa-download"></i> {{ entry.dbId }}<br />{{
                  entry.tableId
                }}
              </button>
            </div>
          </div>

          <div class="alert alert-info mt-3 info-purple">
            <i class="fas fa-info-circle"></i>
            <strong>Data Format Notes:</strong>
            <ul class="mb-0">
              <li>Categorical variables use values from your local mappings</li>
              <li>Continuous variables generate random numbers (1-100 range)</li>
              <li>
                Identifier columns use sequential IDs (ID_00001, ID_00002, etc.)
              </li>
              <li>Missing values are included at ~10% probability where applicable</li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <RouterLink to="/share" class="btn btn-light">
      <i class="fas fa-arrow-left"></i> Return to Share options
    </RouterLink>
    <br /><br />
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
