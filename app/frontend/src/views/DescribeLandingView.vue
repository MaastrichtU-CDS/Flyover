<script setup>
import { computed, onMounted, ref, shallowRef } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import api from '@/services/api'
import * as db from '@/lib/db'
import { loadPage } from '@/lib/browser'
import { compareWithStore } from '@/lib/jsonld'
import { mapDetails, projectName, projectVersion } from '@/lib/project'
import { errorMessage, useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'
import { useNavigation } from '@/composables/useNavigation'

const status = useStatusStore()
const suggestions = useSuggestionsStore()
const project = useProjectStore()
const { refreshDataExists } = useNavigation()
const route = useRoute()
const router = useRouter()

// What brought the user here: an ingest that just finished ('done'), one
// that went on without new data ('skipped'), or anything else (''). The
// ingest's redirect says so; the address drops it at once, so a reload or
// a later visit says nothing of an ingest.
const INGEST_OUTCOMES = ['done', 'skipped']
const arrival = INGEST_OUTCOMES.includes(route.query.ingest) ? route.query.ingest : ''

const dataExists = ref(true)
const checking = ref(true)
// Whether the map and the store's datasets were read: until then the page
// shows neither the map's card nor the upload, so neither flickers.
const loaded = ref(false)
// The datasets in the store Flyover works on, by name, and whether the
// store could list them.
const storeDatasets = ref([])
const storeListed = ref(false)
const storeDatasetsText = computed(() => {
  const count = storeDatasets.value.length
  return `The RDF store holds ${count} dataset${count === 1 ? '' : 's'}: ${storeDatasets.value.join(', ')}.`
})
// The semantic map this browser holds (its metadata row), if any: the open
// project's working copy, or a map uploaded in a step.
const held = shallowRef(null)
const heldMap = computed(() => {
  const row = held.value
  if (!row?.data) return null
  if (project.project) {
    return {
      ofProject: true,
      name: project.name,
      version: project.version,
      fileName: project.project.fileName || row.filename || '',
    }
  }
  const details = { ...mapDetails(row.data), fileName: row.filename || '' }
  return {
    ofProject: false,
    name: projectName(details, 'Unnamed semantic map'),
    version: projectVersion(details),
    fileName: details.fileName,
  }
})
// The held map's data sources against the store's datasets.
const sources = computed(() =>
  held.value?.data ? compareWithStore(held.value.data, storeDatasets.value) : null
)
const skipped = ref(false)
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

    // Which project the map belongs to comes first: opening it may move
    // Flyover to another store, which the column check below must use.
    let plan
    try {
      plan = await project.planUpload(jsonld, file.name)
    } catch (e) {
      status.error(`The map could not be checked: ${errorMessage(e)}`)
      return
    }
    if (plan.invalid) {
      validationErrors.value = plan.invalid.validation_errors?.length
        ? plan.invalid.validation_errors
        : [{ path: '(map)', severity: 'error', message: plan.invalid.error }]
      return
    }
    if (!plan.go) {
      status.info(`The upload was set aside: Flyover keeps working on ${project.name}.`)
      return
    }
    if (plan.storeChanged) {
      await checkDataExists()
      if (!dataExists.value) {
        status.info(
          "Flyover now works on the project's RDF store, which holds no data yet. Ingest the data first."
        )
        return
      }
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

    if (plan.opened) {
      // The project's working copy, as loaded: cleaned of absent columns.
      await project.adoptWorkingCopy(mappingToSave, { fileName: file.name })
    } else {
      await db.saveData('metadata', {
        key: 'semantic_map',
        data: mappingToSave,
        filename: file.name,
        timestamp: new Date().toISOString(),
      })
    }
    // A new map resets the describe fields, so earlier suggestion reviews
    // no longer apply to them.
    await suggestions.resetMarks()

    // The map's card now shows the new map, and the upload form is ready
    // for another; its warnings, if any, stay under the form.
    await Promise.all([loadHeldMap(), plan.storeChanged ? loadStoreDatasets() : null])
    clearPickedFile()
    if (!validationWarnings.value.length) {
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

function clearPickedFile() {
  selectedFileName.value = ''
  if (fileInput.value) fileInput.value.value = ''
}

function skipSemanticMap() {
  skipped.value = true
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
  // A full page load: lib/jsonld keeps the last map it read for the tab,
  // so the describe pages must start from the map this browser holds now
  // (or from none), not from one read earlier.
  loadPage('/describe/variables')
}

// Whether the store Flyover works on holds data: on arrival, and again when
// an upload moved Flyover to a project's store.
async function checkDataExists() {
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
}

async function loadStoreDatasets() {
  try {
    const { data } = await api.get('/api/rdf-store-databases')
    storeDatasets.value = [...(data?.databases || [])].sort((a, b) => a.localeCompare(b))
    storeListed.value = true
  } catch {
    storeDatasets.value = []
    storeListed.value = false
  }
}

async function loadHeldMap() {
  try {
    held.value = (await db.getData('metadata', 'semantic_map')) || null
  } catch {
    held.value = null
  }
}

// Drop the ingest's word from the address: it was said once.
function forgetArrival() {
  if (!('ingest' in route.query)) return
  const query = { ...route.query }
  delete query.ingest
  router.replace({ query, hash: route.hash })
}

onMounted(async () => {
  forgetArrival()
  await checkDataExists()
  if (dataExists.value) await Promise.all([loadStoreDatasets(), loadHeldMap()])
  loaded.value = true
})
</script>

<template>
  <div>
    <h1><i class="fas fa-edit" /> Data Description</h1>
    <hr>
    <p>
      In this step you describe the variables in your data: what each one means, its
      data type, and its units and categories. A semantic map can guide the
      descriptions with the variables a project has agreed on.
    </p>

    <div
      v-if="dataExists && arrival === 'done'"
      class="alert alert-success alert-compact ingest-outcome"
    >
      <i class="fas fa-check-circle" /> Data uploaded successfully. You can now describe
      your variables.
    </div>
    <div
      v-else-if="dataExists && arrival === 'skipped'"
      class="alert alert-info alert-compact ingest-outcome"
    >
      <i class="fas fa-info-circle" /> No new data was added. You can describe the data
      already in the RDF store.
    </div>
    <p
      v-else-if="dataExists && storeDatasets.length"
      class="text-muted store-datasets"
    >
      {{ storeDatasetsText }}
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
      v-if="loaded && dataExists && heldMap"
      class="card mb-4 held-map"
    >
      <div class="card-header">
        <h5 class="mb-0">
          <i class="fas fa-project-diagram" />
          {{
            heldMap.ofProject
              ? "Continue with the project's semantic map"
              : 'Continue with the semantic map in this browser'
          }}
        </h5>
      </div>
      <div class="card-body">
        <!-- The spaces sit inside the spans: the template drops those between tags. -->
        <p class="held-map-name mb-2">
          <strong>{{ heldMap.name }}</strong>
          <span v-if="heldMap.version">{{ ` · ${heldMap.version}` }}</span>
          <span
            v-if="heldMap.fileName && heldMap.fileName !== heldMap.name"
            class="text-muted"
          >{{ ` (${heldMap.fileName})` }}</span>
        </p>

        <div class="held-map-sources mb-3">
          <template v-if="storeListed">
            <div
              v-if="!sources.matched.length && !sources.missing.length"
              class="sources-none text-muted text-compact mb-2"
            >
              <i class="fas fa-info-circle" /> The map names no data sources yet: its
              schema guides the descriptions.
            </div>
            <div
              v-else-if="!sources.matched.length"
              class="sources-none text-warning text-compact mb-2"
            >
              <i class="fas fa-exclamation-triangle" /> None of the map's data sources
              is in the RDF store ({{ sources.missing.join(', ') }}): only its schema
              guides the descriptions.
            </div>
            <template v-else>
              <div class="sources-ready text-success text-compact mb-2">
                <i class="fas fa-check-circle" />
                <strong>{{ sources.matched.length }}</strong>
                data source{{ sources.matched.length === 1 ? '' : 's' }} ready to
                describe: {{ sources.matched.join(', ') }}
              </div>
              <div
                v-if="sources.missing.length"
                class="sources-missing text-warning text-compact mb-2"
              >
                <i class="fas fa-exclamation-triangle" />
                <strong>Not in the RDF store:</strong> {{ sources.missing.join(', ') }}
              </div>
            </template>
            <div
              v-if="sources.others.length"
              class="sources-other text-muted text-compact mb-2"
            >
              <i class="fas fa-info-circle" />
              <strong>Other data in the RDF store:</strong> {{ sources.others.join(', ') }}
            </div>
          </template>
          <div
            v-else
            class="text-muted text-compact mb-2"
          >
            <i class="fas fa-info-circle" /> Flyover could not list the datasets in the
            RDF store to compare them with the map.
          </div>
        </div>

        <button
          type="button"
          class="btn btn-primary"
          :disabled="uploading"
          @click="continueToDescribe"
        >
          <i class="fas fa-arrow-right" /> Describe your data with this map
        </button>
      </div>
    </div>

    <div
      v-if="loaded && dataExists && (heldMap || !skipped)"
      class="card mb-4 map-upload"
    >
      <div class="card-header">
        <h5
          v-if="heldMap"
          class="mb-0"
        >
          <i class="fas fa-file-upload" /> Or upload another semantic map
        </h5>
        <h5
          v-else
          class="mb-0"
        >
          <i class="fas fa-project-diagram" /> Optional: Upload Flyover Semantic Map
          Template
        </h5>
      </div>
      <div class="card-body">
        <p
          v-if="heldMap"
          class="text-muted"
        >
          A map you upload here replaces the one above.
          <template v-if="heldMap.ofProject">
            Flyover asks first when it belongs to another project, names another store,
            or would replace the descriptions you added.
          </template>
        </p>
        <template v-else>
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
        </template>

        <form @submit.prevent="uploadSemanticMap">
          <div class="form-group">
            <label for="semanticMapPath">Global Semantic Map JSON-LD File:</label>
            <div class="input-group">
              <input
                id="semanticMapPath"
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
              v-if="!heldMap"
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

    <p v-if="loaded && dataExists && !heldMap">
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
