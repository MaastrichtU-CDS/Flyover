<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import api from '@/services/api'
import * as db from '@/lib/db'

const router = useRouter()
const PAGE_SIZE = 10

const loading = ref(true)
const noDataMessage = ref('')
const semanticMapData = ref(null)
const rdfStoreDatabases = ref([])
const annotatedTableVariables = ref({})
const nonMatchingJsonld = ref([])
const nonMatchingRdfStore = ref([])
const databasePages = reactive({})
const expandedDatabases = reactive({})
const selectedTables = reactive({})
const annotationProcessing = ref(false)
const annotationButtonText = ref('Start Annotation Process')

function escapeHtml(text) {
  if (!text) return ''
  const div = document.createElement('div')
  div.textContent = text
  return div.innerHTML
}

function toTitleCase(k) {
  return k.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

function findMatchingDatabase(mapDbName) {
  if (!mapDbName) return rdfStoreDatabases.value[0] || null
  for (const d of rdfStoreDatabases.value) {
    if (d === mapDbName) return d
    const a = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName
    const b = d.endsWith('.csv') ? d.slice(0, -4) : d
    if (a === b) return d
  }
  return null
}

function extractJsonLdTables(data) {
  const tables = []
  if (data?.databases) {
    for (const [, dbData] of Object.entries(data.databases)) {
      if (dbData?.tables) {
        for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
          tables.push(
            typeof tableData === 'object' && tableData.sourceFile
              ? tableData.sourceFile
              : tableKey
          )
        }
      }
    }
  } else if (data?.database_name) {
    tables.push(data.database_name)
  }
  return tables
}

function transformJsonLdToVariableInfoByTable(data, matchingTables) {
  const result = {}
  const schemaVariables = data.schema?.variables || {}
  const databases = data.databases || {}
  const tableToRdfStore = {}
  for (const m of matchingTables) tableToRdfStore[m.jsonld] = m.rdf_store

  for (const [, dbData] of Object.entries(databases)) {
    if (!dbData.tables) continue
    for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
      const tableName = tableData.sourceFile || tableKey
      if (!tableToRdfStore[tableName]) continue
      const rdfStoreName = tableToRdfStore[tableName]
      const variableInfo = {}
      if (!tableData.columns) continue

      for (const [, colData] of Object.entries(tableData.columns)) {
        if (!colData.localColumn) continue
        if (!colData.mapsTo) continue
        const varName = colData.mapsTo.split('/').pop()
        if (!varName) continue

        const varSchema = schemaVariables[varName] || {}
        const localCols = Array.isArray(colData.localColumn)
          ? colData.localColumn
          : [colData.localColumn]
        const filtered = localCols.filter((c) => c)
        if (!filtered.length) continue

        const localDef = filtered.join(', ')
        const localMappings = colData.localMappings || {}

        variableInfo[varName] = {
          predicate: varSchema.predicate || null,
          class: varSchema.class || null,
          data_type: varSchema.dataType || null,
          local_definition: localDef,
          value_mapping: null,
        }

        if (varSchema.valueMapping?.terms) {
          const terms = {}
          let any = false
          for (const [termKey, termData] of Object.entries(
            varSchema.valueMapping.terms
          )) {
            const localTermValue = localMappings[termKey]
            if (localTermValue == null) continue
            const localTerm = Array.isArray(localTermValue)
              ? localTermValue.join(', ')
              : localTermValue
            terms[termKey] = {
              target_class: termData.targetClass || null,
              local_term: localTerm,
            }
            any = true
          }
          if (any) variableInfo[varName].value_mapping = { terms }
        }
      }

      if (Object.keys(variableInfo).length) {
        result[tableName] = { variableInfo, rdfStoreName }
      }
    }
  }
  return result
}

function showNoDataWarning(message) {
  loading.value = false
  if (message) {
    noDataMessage.value =
      '<i class="fas fa-exclamation-triangle"></i> ' +
      '<strong>Cannot proceed with annotation</strong><br>' +
      message +
      '<br><br>' +
      '<a href="/describe" class="btn btn-primary"><i class="fas fa-backward"></i> Go to Describe</a> ' +
      '<a href="/ingest" class="btn btn-secondary"><i class="fas fa-fast-backward"></i> Go to Ingest</a> ' +
      '<a href="/" class="btn btn-light"><i class="fas fa-home"></i> Return to Home</a>'
  } else {
    noDataMessage.value =
      '<i class="fas fa-exclamation-triangle"></i> ' +
      '<strong>No semantic map found</strong><br>' +
      'Please ensure you have completed the describe workflow and have a semantic map in your browser storage.' +
      '<br><br>' +
      '<a href="/describe" class="btn btn-primary">Go to Describe</a> ' +
      '<a href="/" class="btn btn-light">Return to Home</a>'
  }
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
      return true
    }
    return false
  } catch {
    return false
  }
}

async function loadRdfStoreDatabasesFromIndexedDB() {
  try {
    const r = await db.getData('metadata', 'rdf_store_databases')
    if (r?.data?.length) {
      rdfStoreDatabases.value = r.data
      return true
    }
  } catch {
    /* ignore */
  }
  return false
}

function processAnnotationData(data) {
  const jsonldTables = extractJsonLdTables(data)
  const matching = []
  const nonJl = []
  const nonRdf = [...rdfStoreDatabases.value]

  for (const t of jsonldTables) {
    const m = findMatchingDatabase(t)
    if (m) {
      matching.push({ jsonld: t, rdf_store: m })
      const i = nonRdf.indexOf(m)
      if (i > -1) nonRdf.splice(i, 1)
    } else {
      nonJl.push(t)
    }
  }
  nonMatchingJsonld.value = nonJl
  nonMatchingRdfStore.value = nonRdf

  if (!matching.length) {
    showNoDataWarning(
      '<strong>Cannot proceed with annotation</strong><br>' +
        'None of the data sources in the semantic map match data in the RDF store.<br><br>' +
        '<strong>Data sources in semantic map:</strong> ' +
        escapeHtml(jsonldTables.join(', ') || 'None') +
        '<br><strong>Data in RDF store:</strong> ' +
        escapeHtml(rdfStoreDatabases.value.join(', ') || 'None')
    )
    return
  }

  const tableVars = transformJsonLdToVariableInfoByTable(data, matching)
  if (!Object.keys(tableVars).length) {
    showNoDataWarning(
      'No variables with local column definitions found in the semantic map.'
    )
    return
  }

  const annotated = {}
  let total = 0
  for (const [tableName, td] of Object.entries(tableVars)) {
    const av = {}
    for (const [varName, varData] of Object.entries(td.variableInfo)) {
      if (!varData.predicate || !varData.class || !varData.local_definition) continue
      av[varName] = varData
    }
    if (Object.keys(av).length) {
      annotated[tableName] = { variables: av, rdfStoreName: td.rdfStoreName }
      total += Object.keys(av).length
    }
  }

  if (!total) {
    showNoDataWarning(
      'No variables are ready for annotation. Please ensure variables have local definitions, predicates, and classes.'
    )
    return
  }

  for (const [tableName, td] of Object.entries(annotated)) {
    databasePages[td.rdfStoreName] = 1
    expandedDatabases[td.rdfStoreName] = false
    selectedTables[tableName] = true
  }
  annotatedTableVariables.value = annotated
  loading.value = false
}

async function load() {
  try {
    let ok = await fetchAndStoreRdfStoreDatabases()
    if (!ok) ok = await loadRdfStoreDatabasesFromIndexedDB()
    if (!ok) {
      showNoDataWarning(
        'No databases found in the data store. Please complete the Ingest step first.'
      )
      return
    }
    const r = await db.getData('metadata', 'semantic_map')
    if (!r?.data) {
      showNoDataWarning()
      return
    }
    semanticMapData.value = r.data
    processAnnotationData(r.data)
  } catch (e) {
    showNoDataWarning(`An error occurred while loading: ${e.message || e}`)
  }
}

function totalPages(dbName, variables) {
  return Math.ceil(Object.keys(variables).length / PAGE_SIZE)
}

function currentPageVariables(dbName, variables) {
  const page = databasePages[dbName] || 1
  const entries = Object.entries(variables)
  return Object.fromEntries(entries.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE))
}

function changePage(database, direction) {
  const cur = databasePages[database] || 1
  const next = cur + direction
  for (const td of Object.values(annotatedTableVariables.value)) {
    if (td.rdfStoreName === database) {
      const p = totalPages(database, td.variables)
      if (next >= 1 && next <= p) databasePages[database] = next
      break
    }
  }
}

function toggleDatabase(name) {
  expandedDatabases[name] = !expandedDatabases[name]
}

function toggleTableSelection(tableName) {
  selectedTables[tableName] = !selectedTables[tableName]
}

function hasValueMapping(varInfo) {
  return (
    varInfo.value_mapping &&
    varInfo.value_mapping.terms &&
    Object.keys(varInfo.value_mapping.terms).length > 0
  )
}

const startBtnIcon = computed(() =>
  annotationProcessing.value ? 'fa-spinner fa-spin' : 'fa-play'
)

function createFilteredSemanticMap(selectedTableNames) {
  if (!semanticMapData.value || !selectedTableNames.length) return semanticMapData.value

  const selectedSet = new Set(selectedTableNames)
  const filteredMap = JSON.parse(JSON.stringify(semanticMapData.value))
  
  if (!filteredMap.databases) return filteredMap

  removeUnselectedTables(filteredMap, selectedSet)

  return filteredMap
}

function removeUnselectedTables(filteredMap, selectedSet) {
  for (const dbKey in filteredMap.databases) {
    const db = filteredMap.databases[dbKey]
    if (!db.tables) continue
    
    for (const tableKey in db.tables) {
      const tableName = db.tables[tableKey].sourceFile || tableKey
      if (!selectedSet.has(tableName) && !selectedSet.has(tableKey)) {
        delete db.tables[tableKey]
      }
    }
    
    if (Object.keys(db.tables).length === 0) {
      delete filteredMap.databases[dbKey]
    }
  }
}

function validateTableSelection() {
  if (!Object.values(selectedTables).some(selected => selected)) {
    alert('Please select at least one table to annotate.')
    return false
  }
  return true
}

function getSelectedTableNames() {
  return Object.entries(selectedTables)
    .filter(([tableName, isSelected]) => isSelected)
    .map(([tableName]) => tableName)
}

function validateFilteredMapHasTables(filteredSemanticMap) {
  const hasTables = Object.values(filteredSemanticMap.databases || {}).some(db => 
    Object.keys(db.tables || {}).length > 0
  )
  if (!hasTables) {
    alert('No tables selected for annotation')
    return false
  }
  return true
}

function resetAnnotationProcessingState() {
  annotationProcessing.value = false
  annotationButtonText.value = 'Start Annotation Process'
}

async function submitFullSemanticMap() {
  const submitRes = await fetch('/submit-indexeddb-semantic-map', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(semanticMapData.value),
  })
  const submit = await submitRes.json()
  
  if (!submitRes.ok || !submit.success) {
    let msg = submit.error || 'Failed to submit semantic map.'
    if (submit.validation_errors?.length) {
      msg += '\n\nValidation errors:\n' + submit.validation_errors.map((e) => `- ${e.message}`).join('\n')
    }
    alert(msg)
    return false
  }
  return true
}

async function startAnnotationWithFilteredMap(filteredSemanticMap) {
  const annRes = await fetch('/start-annotation', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(filteredSemanticMap),
  })
  const ann = await annRes.json()
  
  if (!ann.success) {
    alert(`Error: ${ann.error}`)
    return false
  }
  return true
}

async function startAnnotationProcess() {
  if (!semanticMapData.value) {
    alert('No semantic map data available. Please reload the page.')
    return
  }
  
  if (!validateTableSelection()) return
  
  annotationProcessing.value = true
  annotationButtonText.value = 'Submitting semantic map...'
  try {
    const selectedTableNames = getSelectedTableNames()
    const filteredSemanticMap = createFilteredSemanticMap(selectedTableNames)
    
    if (!validateFilteredMapHasTables(filteredSemanticMap)) {
      resetAnnotationProcessingState()
      return
    }

    // Send FULL semantic map to preserve original for Share page
    const submitSuccess = await submitFullSemanticMap()
    if (!submitSuccess) {
      resetAnnotationProcessingState()
      return
    }
    
    annotationButtonText.value = 'Processing Annotations...'
    
    const annotationSuccess = await startAnnotationWithFilteredMap(filteredSemanticMap)
    if (annotationSuccess) {
      router.push('/annotate/verify')
    } else {
      resetAnnotationProcessingState()
    }
  } catch (e) {
    console.error(e)
    alert('An error occurred while processing. Please try again.')
    resetAnnotationProcessingState()
  }
}

onMounted(load)
</script>

<template>
  <div>
    <h1><i class="fas fa-search" /> Review Annotation Data</h1>
    <hr>
    <p>
      Please review the variable mappings below before proceeding with the annotation
      process. If information is missing or incorrect, you can adjust the local
      definitions through the describe data pages.
    </p>

    <div
      v-if="loading"
      class="loading-spinner"
    >
      <i class="fas fa-spinner fa-spin" />
      <p>Loading semantic map from browser storage...</p>
    </div>

    <div
      v-else-if="noDataMessage"
      class="alert alert-warning"
      v-html="noDataMessage"
    />

    <div v-else>
      <div
        v-if="nonMatchingJsonld.length || nonMatchingRdfStore.length"
        class="mb-4"
      >
        <div
          v-if="nonMatchingJsonld.length"
          class="alert alert-warning alert-compact"
        >
          <i class="fas fa-exclamation-triangle" />
          <strong>Will not be annotated</strong> (not in the RDF store):
          {{ nonMatchingJsonld.join(', ') }}
        </div>
        <div
          v-if="nonMatchingRdfStore.length"
          class="alert alert-info alert-compact"
        >
          <i class="fas fa-info-circle" />
          <strong>Other data in RDF store</strong> (no mapping provided):
          {{ nonMatchingRdfStore.join(', ') }}
        </div>
      </div>

      <form class="form-horizontal">
        <div
          v-for="(dbData, dbName) in annotatedTableVariables"
          :key="dbName"
        >
          <div class="database-header">
            <h2 class="database-heading">
              <i class="fas fa-database" /> {{ dbData.rdfStoreName }}
              <span 
                class="table-selector"
                @click.stop="toggleTableSelection(dbName)"
                :class="{ selected: selectedTables[dbName] }"
              >
                <i class="fas" :class="selectedTables[dbName] ? 'fa-check' : 'fa-times'" />
                <span class="selector-tooltip">
                  {{ selectedTables[dbName] ? 'Deselect for annotation' : 'Select for annotation' }}
                </span>
              </span>
            </h2>
            <button
              type="button"
              class="toggle-button"
              :class="{ open: expandedDatabases[dbData.rdfStoreName] }"
              @click="toggleDatabase(dbData.rdfStoreName)"
            >
            <span class="toggle-text">{{
              expandedDatabases[dbData.rdfStoreName] ? 'Show less' : 'Show more'
            }}</span>
            <i
              class="fas"
              :class="
                expandedDatabases[dbData.rdfStoreName]
                  ? 'fa-chevron-down'
                  : 'fa-chevron-up'
              "
            />
            </button>
          </div>

          <div
            class="content"
            :class="{
              active: expandedDatabases[dbData.rdfStoreName],
              hidden: !expandedDatabases[dbData.rdfStoreName],
            }"
          >
            <div class="database-summary">
              <strong>Database Summary:</strong>
              {{ Object.keys(dbData.variables).length }} variable(s) ready for annotation
            </div>
            <div class="variables-container">
              <div
                v-for="(varInfo, varName) in currentPageVariables(
                  dbData.rdfStoreName,
                  dbData.variables
                )"
                :key="varName"
                class="variable-card"
              >
                <div class="variable-header">
                  <div class="variable-name">
                    {{ varName }}
                  </div>
                  <div
                    class="status-badge"
                    :class="
                      varInfo.local_definition
                        ? 'status-annotated'
                        : 'status-unannotated'
                    "
                  >
                    <i
                      class="fas"
                      :class="
                        varInfo.local_definition
                          ? 'fa-check-circle'
                          : 'fa-exclamation-triangle'
                      "
                    />
                    {{ varInfo.local_definition ? 'Described' : 'Undescribed' }}
                  </div>
                </div>
                <div class="variable-details">
                  <div class="detail-row">
                    <div class="detail-label">
                      Local Definition:
                    </div>
                    <div class="detail-value">
                      {{ varInfo.local_definition || 'Not specified' }}
                    </div>
                  </div>
                  <div class="detail-row">
                    <div class="detail-label">
                      Predicate:
                    </div>
                    <div class="detail-value">
                      <code>{{ varInfo.predicate || '' }}</code>
                    </div>
                  </div>
                  <div class="detail-row">
                    <div class="detail-label">
                      Class:
                    </div>
                    <div class="detail-value">
                      <code>{{ varInfo.class || '' }}</code>
                    </div>
                  </div>
                  <div
                    v-if="varInfo.data_type"
                    class="detail-row"
                  >
                    <div class="detail-label">
                      Data Type:
                    </div>
                    <div class="detail-value">
                      {{ varInfo.data_type }}
                    </div>
                  </div>
                  <div
                    v-if="hasValueMapping(varInfo)"
                    class="detail-row"
                  >
                    <div class="detail-label">
                      Value Mapping:
                    </div>
                    <div class="detail-value">
                      <div class="value-mapping-section">
                        <div
                          v-for="(termInfo, termKey) in varInfo.value_mapping.terms"
                          :key="termKey"
                          class="value-mapping-item"
                        >
                          <span class="term-name">{{ toTitleCase(termKey) }}</span>
                          <span
                            :class="
                              termInfo.local_term === ''
                                ? 'local-value empty-value'
                                : 'local-value'
                            "
                          >
                            {{
                              termInfo.local_term === ''
                                ? '(empty)'
                                : termInfo.local_term
                            }}
                          </span>
                          <span class="target-class">
                            <code>{{ termInfo.target_class || '' }}</code>
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div
              v-if="totalPages(dbData.rdfStoreName, dbData.variables) > 1"
              class="pagination-controls"
            >
              <button
                type="button"
                :disabled="(databasePages[dbData.rdfStoreName] || 1) <= 1"
                @click="changePage(dbData.rdfStoreName, -1)"
              >
                &#x2190;
              </button>
              <span class="page-indicator">
                Page
                <span class="current-page">{{
                  databasePages[dbData.rdfStoreName] || 1
                }}</span>
                of
                <span class="total-pages">{{
                  totalPages(dbData.rdfStoreName, dbData.variables)
                }}</span>
              </span>
              <button
                type="button"
                :disabled="
                  (databasePages[dbData.rdfStoreName] || 1) >=
                    totalPages(dbData.rdfStoreName, dbData.variables)
                "
                @click="changePage(dbData.rdfStoreName, 1)"
              >
                &#x2192;
              </button>
            </div>
          </div>
          <hr>
        </div>
      </form>

      <button
        class="btn btn-primary"
        :disabled="annotationProcessing"
        @click="startAnnotationProcess"
      >
        <i
          class="fas"
          :class="startBtnIcon"
        />
        {{ ' ' }}{{ annotationButtonText }}
      </button>
      <RouterLink
        to="/describe/variable-details"
        class="btn btn-light"
      >
        <i class="fas fa-backward" /> Back to Describe Variable Details
      </RouterLink>
      <br>

      <div class="mt-4">
        <div class="alert alert-info-highlight py-2">
          <i class="fas fa-info-circle" />
          <strong>Annotating your data</strong><br>
          <div class="mt-1 ms-4">
            <p class="mb-1">
              Annotation data can always be adapted without having to re-upload your data.
              You can do this by removing the annotation graph in the RDF store interface
              and simply redoing the annotation process.
            </p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.database-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 8px;
}

.database-heading {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0;
  padding: 0;
  flex: 1;
}

.table-selector {
  position: relative;
  cursor: pointer;
  color: #6c757d;
  font-size: 0.75em;
  padding: 2px 4px;
  border-radius: 3px;
  transition: all 0.2s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.table-selector:hover {
  color: #495057;
  background-color: #f8f9fa;
}

.table-selector.selected {
  color: #28a745;
}

.table-selector.selected:hover {
  color: #218838;
  background-color: #f8f9fa;
}

.selector-tooltip {
  visibility: hidden;
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  margin-bottom: 6px;
  padding: 0.25rem 0.5rem;
  background-color: rgba(0, 0, 0, 0.9);
  color: #fff;
  border-radius: 0.25rem;
  font-size: 0.875rem;
  white-space: nowrap;
  z-index: 1080;
  line-height: 1.5;
  opacity: 0;
  transition: opacity 0.3s ease, visibility 0.3s ease;
}

.selector-tooltip::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border-width: 0.4rem 0.4rem 0;
  border-style: solid;
  border-color: rgba(0, 0, 0, 0.9) transparent transparent;
}

.table-selector:hover .selector-tooltip {
  visibility: visible;
  opacity: 1;
}

.toggle-button {
  margin-left: auto;
  white-space: nowrap;
}
</style>
