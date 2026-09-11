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
const validationErrors = ref([])
const validationWarnings = ref([])

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
  validationErrors.value = []
  validationWarnings.value = []
  try {
    const text = await readFileAsText(file)
    let jsonld
    try {
      jsonld = JSON.parse(text)
    } catch (e) {
      validationErrors.value = [
        {
          path: '(file)',
          severity: 'error',
          message: `Invalid JSON: ${e.message || e}`,
          suggestion: 'Check for missing commas, brackets, or quotes.',
        },
      ]
      return
    }

    // Validate against the schema and check that every localColumn exists in
    // the loaded CSV. Gate IDB save on the result so the user sees a clear
    // problem list here instead of broken UI two screens later.
    // Schema errors block upload, column warnings do not.
    let validation
    try {
      const { data } = await api.post('/api/v1/validate-mapping', jsonld)
      validation = data
    } catch (e) {
      const payload = e?.response?.data
      if (payload?.validation_errors) {
        validation = { 
          valid: false, 
          validation_errors: payload.validation_errors, 
          validation_warnings: [],
          cleaned_mapping: payload.cleaned_mapping
        }
      } else {
        status.error(
          `Validation request failed: ${payload?.error || e.message || e}`
        )
        return
      }
    }

    // Block upload only on schema errors (validation.valid === false)
    // Allow upload with warnings (validation_warnings may be present)
    if (!validation.valid) {
      validationErrors.value = validation.validation_errors || []
      return
    }

    // Store warnings if present (non-blocking)
    validationWarnings.value = validation.validation_warnings || []

    // Use cleaned mapping if available, otherwise use the original
    const mappingToSave = validation.cleaned_mapping || jsonld
    console.log(validation.cleaned_mapping)

    await db.saveData('metadata', {
      key: 'semantic_map',
      data: mappingToSave,
      filename: file.name,
      timestamp: new Date().toISOString(),
    })
    
    // If there are warnings, keep the card visible so users can see the warnings in the box
    // If no warnings, hide the card
    const hasWarnings = validationWarnings.value.length > 0
    if (hasWarnings) {
      semanticMapVisible.value = true
    } else {
      semanticMapVisible.value = false
      status.success(
        'Global semantic map uploaded successfully. This will guide the semantic mapping process and help standardise your data annotations.'
      )
    }
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

function escapeHtml(text) {
  if (!text) return ''
  const div = document.createElement('div')
  div.textContent = text
  return div.innerHTML
}

function formatWarnings() {
  if (validationWarnings.value.length === 0) return ''
  
  const warning = validationWarnings.value[0]
  // message contains trusted HTML from backend (<br> for line breaks)
  return `<i class="fas fa-exclamation-triangle"></i> ${warning.message}`
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

          <div
            v-if="validationErrors.length"
            class="alert alert-danger validation-errors"
          >
            <strong>
              <i class="fas fa-exclamation-triangle" />
              The semantic map could not be loaded ({{ validationErrors.length }}
              issue{{ validationErrors.length === 1 ? '' : 's' }}):
            </strong>
            <ul>
              <li
                v-for="(err, idx) in validationErrors"
                :key="idx"
              >
                <code>{{ err.path }}</code> — {{ err.message }}
                <div
                  v-if="err.suggestion"
                  class="suggestion"
                >
                  <i class="fas fa-lightbulb" /> {{ err.suggestion }}
                </div>
              </li>
            </ul>
          </div>

          <div
            v-if="validationWarnings.length"
            class="alert alert-warning alert-compact validation-warnings"
            v-html="formatWarnings()"
          />

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

.validation-errors {
  margin-top: 1rem;
}
.validation-errors ul {
  margin-top: 0.5rem;
  padding-left: 1.25rem;
}
.validation-errors code {
  background: rgba(0, 0, 0, 0.05);
  padding: 1px 4px;
  border-radius: 3px;
}
.validation-errors .suggestion {
  font-size: 0.85em;
  opacity: 0.85;
  margin-top: 0.15rem;
}

.validation-warnings {
  margin-top: 1rem;
}
</style>
