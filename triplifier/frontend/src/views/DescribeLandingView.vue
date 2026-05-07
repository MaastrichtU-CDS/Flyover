<script setup>
import { onMounted, ref } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import { useStatusStore } from '@/stores/status'
import { useNavigation } from '@/composables/useNavigation'

const status = useStatusStore()
const { refreshDataExists } = useNavigation()

const dataExists = ref(true)
const checking = ref(true)
const semanticMapVisible = ref(true)
const continueDisabled = ref(true)
const fileInput = ref(null)
const selectedFileName = ref('')
const uploading = ref(false)

async function readFileAsText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = (e) => resolve(e.target.result)
    reader.onerror = () => reject(new Error('Failed to read file'))
    reader.readAsText(file)
  })
}

function onFilePicked(e) {
  const f = e.target.files?.[0]
  selectedFileName.value = f ? f.name : ''
}

async function uploadSemanticMap() {
  const file = fileInput.value?.files?.[0]
  if (!file) {
    status.error('Please select a semantic map JSON-LD file to upload.')
    return
  }
  uploading.value = true
  try {
    const text = await readFileAsText(file)
    const jsonld = JSON.parse(text)
    await db.saveData('metadata', {
      key: 'semantic_map',
      data: jsonld,
      filename: file.name,
      timestamp: new Date().toISOString(),
    })
    semanticMapVisible.value = false
    status.success(
      'Global semantic map uploaded successfully. This will guide the semantic mapping process and help standardise your data annotations.'
    )
    continueDisabled.value = false
  } catch (e) {
    status.error(`Failed to save semantic map file: ${e.message || e}`)
  } finally {
    uploading.value = false
  }
}

function skipSemanticMap() {
  semanticMapVisible.value = false
  status.info(
    'Semantic map upload skipped. You can proceed to describe your data manually.'
  )
  continueDisabled.value = false
}

function continueToDescribe() {
  // The describe-variables page is still the legacy Jinja flow until
  // its SFC port lands; navigate out of the SPA on submit.
  window.location.href = '/describe_variables'
}

onMounted(async () => {
  try {
    const { data } = await api.get('/api/check-graph-exists')
    dataExists.value = !!data?.exists
    if (!dataExists.value) continueDisabled.value = true
    refreshDataExists()
  } catch {
    dataExists.value = false
  } finally {
    checking.value = false
  }
})
</script>

<template>
  <div>
    <h1><i class="fas fa-clipboard-check" /> Data submission finalised</h1>
    <hr>

    <p v-if="dataExists">
      Data uploaded successfully. You can now describe your variables.
    </p>

    <div
      v-if="!checking && !dataExists"
      class="alert alert-warning"
    >
      <i class="fas fa-exclamation-triangle" />
      <strong>No Data Found.</strong> You cannot proceed with the describe step as no
      data has been uploaded yet. Please go back to the Ingest step to upload your
      data first.
      <br><br>
      <RouterLink
        to="/ingest"
        class="btn btn-primary"
      >
        <i class="fas fa-arrow-left" /> Go to Ingest Step
      </RouterLink>
    </div>

    <div
      v-if="dataExists && semanticMapVisible"
      class="card mb-4"
    >
      <div class="card-header">
        <h5 class="mb-0">
          <i class="fas fa-project-diagram" /> Optional: Upload Flyover Semantic Map
          Template
        </h5>
      </div>
      <div class="card-body">
        <p class="text-muted">
          You can optionally upload a Flyover semantic map JSON-LD file to guide the
          mapping process. This will help provide better semantic annotations and
          standardised mappings for your data variables. You can find an example
          template
          <a
            href="https://github.com/MaastrichtU-CDS/Flyover/blob/main/example_data/mapping_template.jsonld"
            target="_blank"
            rel="noopener"
          >here</a>.
        </p>

        <div class="alert alert-info info-purple">
          <i class="fas fa-info-circle" />
          <strong>What is a Global Semantic Map?</strong><br>
          A global semantic map defines standardised variable definitions, data types,
          and value mappings that can be consistently applied across different
          datasets to ensure semantic interoperability.
        </div>

        <form @submit.prevent="uploadSemanticMap">
          <div class="form-group">
            <label for="semanticMapPath">Global Semantic Map JSON-LD File:</label>
            <div class="input-group">
              <input
                type="text"
                class="form-control"
                :value="selectedFileName"
                placeholder="Select semantic map JSON-LD file..."
                readonly
              >
              <div class="input-group-append">
                <button
                  type="button"
                  class="btn btn-outline-secondary"
                  @click="fileInput?.click()"
                >
                  <i class="fas fa-folder-open" /> Browse
                </button>
              </div>
            </div>
            <input
              ref="fileInput"
              type="file"
              style="display: none"
              accept=".jsonld"
              @change="onFilePicked"
            >
            <small class="form-text text-muted">
              The JSON-LD file should follow the Flyover semantic mapping schema with
              @context, schema, and databases sections.
            </small>
          </div>

          <div class="form-group">
            <button
              type="submit"
              class="btn btn-success"
              :disabled="!selectedFileName || uploading"
            >
              <i
                v-if="uploading"
                class="fas fa-spinner fa-spin"
              />
              <i
                v-else
                class="fas fa-upload"
              />
              {{ uploading ? ' Saving...' : ' Upload Semantic Map' }}
            </button>
            <button
              type="button"
              class="btn btn-secondary"
              :disabled="uploading"
              @click="skipSemanticMap"
            >
              <i class="fas fa-forward" /> Skip
            </button>
          </div>
        </form>
      </div>
    </div>

    <p>
      <button
        type="button"
        class="btn btn-primary"
        :disabled="continueDisabled"
        @click="continueToDescribe"
      >
        <i class="fas fa-play" /> Click here to describe the data
      </button>
    </p>
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
