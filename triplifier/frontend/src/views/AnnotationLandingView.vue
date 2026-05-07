<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import api from '@/services/api'
import * as db from '@/lib/db'
import { useStatusStore } from '@/stores/status'
import { useNavigation } from '@/composables/useNavigation'

const router = useRouter()
const status = useStatusStore()
const { refreshDataExists } = useNavigation()

const checking = ref(true)
const dataExists = ref(false)

const rdfStoreDatabases = ref([])
const existingJsonldWarning = ref(false)
const existingJsonldDatabases = ref('-')
const showUploadInfo = ref(true)
const uploadProcessing = ref(false)
const selectedFileName = ref('')
const jsonFile = ref(null)
const uploadStatus = ref({ message: '', type: '' })
const continueToReview = ref(false)
const fileInput = ref(null)

const indexedDbSection = ref({ show: false, continueToReview: false, errorHtml: '' })
const indexedDbMatchInfo = ref('')

const uploadButtonState = computed(() => {
  if (uploadProcessing.value) {
    return {
      text: '<i class="fas fa-spinner fa-spin"></i> Uploading...',
      disabled: true,
    }
  }
  return {
    text: '<i class="fas fa-upload"></i> Upload JSON-LD for Annotation',
    disabled: !selectedFileName.value,
  }
})

const uploadStatusHtml = computed(() => {
  if (!uploadStatus.value.message) return ''
  const t = uploadStatus.value.type || 'success'
  const alertClass =
    t === 'error' ? 'alert-danger' : t === 'info' ? 'alert-info' : 'alert-success'
  const icon =
    t === 'error' ? 'exclamation-triangle' : t === 'info' ? 'info-circle' : 'check-circle'
  return `<div class="alert ${alertClass} alert-compact"><i class="fas fa-${icon}"></i> ${uploadStatus.value.message}</div>`
})

function escapeHtml(text) {
  if (!text) return ''
  const div = document.createElement('div')
  div.textContent = text
  return div.innerHTML
}

function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = (e) => resolve(e.target.result)
    reader.onerror = () => reject(new Error('Failed to read file'))
    reader.readAsText(file)
  })
}

function extractJsonLdTables(data) {
  const tables = []
  if (data?.databases) {
    for (const [, dbData] of Object.entries(data.databases)) {
      if (dbData?.tables) {
        for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
          const tableName =
            typeof tableData === 'object' && tableData.sourceFile
              ? tableData.sourceFile
              : tableKey
          tables.push(tableName)
        }
      }
    }
  } else if (data?.database_name) {
    tables.push(data.database_name)
  }
  return tables
}

function findMatchingDatabase(mapDbName, list) {
  if (!mapDbName) return null
  for (const d of list) {
    if (d === mapDbName) return d
    const a = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName
    const b = d.endsWith('.csv') ? d.slice(0, -4) : d
    if (a === b) return d
  }
  return null
}

function generateDatabaseComparisonHtml(jsonldTables, rdfStoreList) {
  const matching = []
  const nonMatchingJsonld = []
  const nonMatchingRdfStore = [...rdfStoreList]

  for (const jt of jsonldTables) {
    const match = findMatchingDatabase(jt, rdfStoreList)
    if (match) {
      matching.push({ jsonld: jt, rdf_store: match })
      const idx = nonMatchingRdfStore.indexOf(match)
      if (idx > -1) nonMatchingRdfStore.splice(idx, 1)
    } else {
      nonMatchingJsonld.push(jt)
    }
  }

  let html = ''
  if (matching.length) {
    html += `<div class="text-success mb-2 text-compact"><i class="fas fa-check-circle"></i> <strong>${matching.length}</strong> data source(s) ready for annotation</div>`
  }
  if (nonMatchingJsonld.length) {
    html += `<div class="text-warning mb-2 text-compact"><i class="fas fa-exclamation-triangle"></i> <strong>Not in RDF store:</strong> ${nonMatchingJsonld.map(escapeHtml).join(', ')}</div>`
  }
  if (nonMatchingRdfStore.length) {
    html += `<div class="text-muted mb-2 text-compact"><i class="fas fa-info-circle"></i> <strong>Other data in RDF store:</strong> ${nonMatchingRdfStore.map(escapeHtml).join(', ')}</div>`
  }
  return { html, hasMatches: matching.length > 0 }
}

async function fetchAndStoreRdfStoreDatabases() {
  try {
    const { data } = await api.get('/api/rdf-store-databases')
    if (data?.success && data.databases?.length) {
      rdfStoreDatabases.value = data.databases
      await db.saveData('metadata', {
        key: 'rdf_store_databases',
        data: data.databases,
        timestamp: new Date().toISOString(),
      })
      return data.databases
    }
    return []
  } catch {
    return []
  }
}

async function checkIndexedDbSemanticMap() {
  try {
    rdfStoreDatabases.value = await fetchAndStoreRdfStoreDatabases()
    const result = await db.getData('metadata', 'semantic_map')
    if (result?.data) {
      indexedDbSection.value.show = true
      const tables = extractJsonLdTables(result.data)
      const cmp = generateDatabaseComparisonHtml(tables, rdfStoreDatabases.value)
      indexedDbMatchInfo.value = cmp.html
      if (cmp.hasMatches) {
        indexedDbSection.value.continueToReview = true
        indexedDbSection.value.errorHtml = ''
      } else {
        indexedDbSection.value.continueToReview = false
        indexedDbSection.value.errorHtml = `
          <div class="alert alert-danger mt-3 alert-compact">
            <i class="fas fa-times-circle"></i>
            <strong>Cannot proceed:</strong> No matching data sources found between your semantic map and the RDF store.
          </div>`
      }
      existingJsonldWarning.value = true
      existingJsonldDatabases.value = tables.join(', ') || 'Unknown'
    } else {
      existingJsonldWarning.value = false
    }
  } catch (e) {
    console.error('IndexedDB semantic map check failed:', e)
  }
}

function updateAnnotationJsonPath(e) {
  const f = e.target.files?.[0] || null
  selectedFileName.value = f ? f.name : ''
  jsonFile.value = f
}

function showUploadStatus(message, type) {
  uploadStatus.value = { message, type: type || 'success' }
}

async function handleJsonUpload() {
  if (!jsonFile.value) {
    showUploadStatus('Please select a JSON-LD file to upload.', 'error')
    return
  }
  uploadProcessing.value = true
  uploadStatus.value = { message: '', type: '' }
  try {
    const text = await readFileAsText(jsonFile.value)
    let json
    try {
      json = JSON.parse(text)
    } catch {
      showUploadStatus('Invalid JSON-LD file format. Please check your file.', 'error')
      uploadProcessing.value = false
      return
    }
    const formData = new FormData()
    formData.append('annotationJsonFile', jsonFile.value)
    const res = await fetch('/upload-annotation-json', {
      method: 'POST',
      body: formData,
    })
    const result = await res.json()

    if (res.ok && result.success) {
      try {
        await db.saveData('metadata', {
          key: 'semantic_map',
          data: json,
          timestamp: new Date().toISOString(),
        })
      } catch (e) {
        console.error('Failed to save JSON-LD to IndexedDB:', e)
      }
      let msg = '<strong>JSON-LD file uploaded successfully!</strong><br>'
      if (result.matching_databases?.length) {
        msg += `<i class="fas fa-check-circle text-success"></i> ${result.matching_databases.length} data source(s) matched and ready for annotation.<br>`
      }
      if (result.non_matching_jsonld?.length) {
        msg += `<i class="fas fa-exclamation-triangle text-warning"></i> ${result.non_matching_jsonld.length} data source(s) in semantic map not found in RDF store.<br>`
      }
      showUploadStatus(msg, 'success')
      showUploadInfo.value = false
      existingJsonldWarning.value = false
      indexedDbSection.value.show = false
      continueToReview.value = true
    } else {
      let err = result.error || 'Failed to upload JSON-LD file.'
      if (result.rdf_store_databases?.length) {
        err += '<br><br><strong>Data available in RDF store:</strong><ul>'
        result.rdf_store_databases.forEach((d) => {
          err += `<li>${escapeHtml(d)}</li>`
        })
        err += '</ul>'
      }
      if (result.jsonld_databases?.length) {
        err += '<strong>Data sources in uploaded semantic map:</strong><ul>'
        result.jsonld_databases.forEach((d) => {
          err += `<li>${escapeHtml(d)}</li>`
        })
        err += '</ul>'
      }
      showUploadStatus(err, 'error')
    }
  } catch (e) {
    showUploadStatus(`Failed to upload file: ${e.message || e}`, 'error')
  } finally {
    uploadProcessing.value = false
  }
}

function goToAnnotationReview() {
  router.push('/annotate/review')
}

onMounted(async () => {
  try {
    const { data } = await api.get('/api/check-graph-exists')
    dataExists.value = !!data?.exists
    refreshDataExists()
  } catch {
    dataExists.value = false
  } finally {
    checking.value = false
  }
  if (dataExists.value) {
    await checkIndexedDbSemanticMap()
  }
})
</script>

<template>
  <div>
    <h1><i class="fas fa-tags"></i> Semantic Annotation</h1>
    <hr />
    <p>
      This step allows you to apply semantic annotations to your data using standardised
      ontologies, making your data more F.A.I.R. (Findable, Accessible, Interoperable,
      and Reusable) and increase semantic interoperability.
    </p>

    <div v-if="!checking && !dataExists" class="alert alert-warning">
      <h5><i class="fas fa-exclamation-triangle"></i> Prerequisites Required</h5>
      <p>To use the annotation features, you need to have completed the previous steps:</p>
      <ul>
        <li><strong>Step 1 — Ingest:</strong> Upload and process your data</li>
        <li><strong>Step 2 — Describe:</strong> Provide metadata and descriptions</li>
      </ul>
      <RouterLink to="/ingest" class="btn btn-primary">
        <i class="fas fa-arrow-left"></i> Go to Ingest Step
      </RouterLink>
    </div>

    <div v-if="dataExists">
      <div class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0">
            <i class="fas fa-project-diagram"></i> Option 1: Upload Finalised Flyover
            Semantic Map (JSON-LD) for Direct Annotation
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            If you have a finalised semantic map JSON-LD file with data descriptions and
            semantic mappings, you can upload it directly for annotation processing.
          </p>

          <div v-if="existingJsonldWarning" class="alert alert-warning alert-compact">
            <i class="fas fa-exclamation-triangle"></i>
            <strong>Existing semantic map detected!</strong><br />
            You already have a semantic map stored in your browser from a previous session.
            Uploading a new JSON-LD file will <strong>overwrite</strong> the existing
            semantic map.
            <br /><small class="text-muted"
              >Current semantic map data source(s):
              <span>{{ existingJsonldDatabases }}</span></small
            >
          </div>

          <div
            v-if="uploadStatus.message"
            class="alert-compact"
            v-html="uploadStatusHtml"
          ></div>

          <div v-if="showUploadInfo">
            <div class="alert alert-info-highlight">
              <i class="fas fa-info-circle"></i>
              <strong>What should the JSON-LD contain?</strong><br />
              Your JSON-LD file should include data variable definitions, types, and any
              pre-existing semantic mappings that you want to enhance with additional
              annotations.
            </div>

            <form id="jsonUploadForm" @submit.prevent="handleJsonUpload">
              <div class="form-group">
                <label for="annotationJsonPath">JSON-LD File for Annotation:</label>
                <div class="input-group">
                  <input
                    id="annotationJsonPath"
                    type="text"
                    class="form-control"
                    :value="selectedFileName"
                    placeholder="Select JSON-LD file..."
                    readonly
                  />
                  <div class="input-group-append">
                    <button
                      type="button"
                      class="btn btn-outline-secondary"
                      @click="fileInput?.click()"
                    >
                      <i class="fas fa-folder-open"></i> Browse
                    </button>
                  </div>
                </div>
                <input
                  ref="fileInput"
                  type="file"
                  style="display: none"
                  accept=".jsonld"
                  @change="updateAnnotationJsonPath"
                />
                <small class="form-text text-muted">
                  Upload a JSON-LD file containing your data descriptions and variable
                  mappings.
                </small>
              </div>
            </form>
          </div>

          <div class="form-group mb-0">
            <button
              v-if="!continueToReview"
              type="submit"
              form="jsonUploadForm"
              class="btn btn-success"
              :disabled="uploadButtonState.disabled"
              v-html="uploadButtonState.text"
            ></button>
            <button v-else type="button" class="btn btn-primary" @click="goToAnnotationReview">
              <i class="fas fa-arrow-right"></i> Proceed to Annotation Review
            </button>
          </div>
        </div>
      </div>

      <div class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0"><i class="fas fa-edit"></i> Option 2: Describe Existing Data</h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            Use the data that has already been uploaded and describe it in the "Describe"
            steps.
          </p>
          <RouterLink to="/describe" class="btn btn-primary">
            <i class="fas fa-fast-backward"></i> Start Describing your data
          </RouterLink>
        </div>
      </div>

      <div v-if="indexedDbSection.show" class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0">
            <i class="fas fa-history"></i> Option 3: Continue with Existing Semantic Map
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            A semantic map from your previous session was found in your browser storage.
          </p>
          <div v-if="indexedDbMatchInfo" class="mb-3" v-html="indexedDbMatchInfo"></div>
          <RouterLink
            v-if="indexedDbSection.continueToReview"
            to="/annotate/review"
            class="btn btn-primary"
          >
            <i class="fas fa-arrow-right"></i> Continue to Annotation Review
          </RouterLink>
          <div v-if="indexedDbSection.errorHtml" v-html="indexedDbSection.errorHtml"></div>
        </div>
      </div>
    </div>

    <hr />
  </div>
</template>
