<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'

const DEFAULT_CATEGORY_OPTIONS = [
  { value: 'Yes', label: 'Yes' },
  { value: 'No', label: 'No' },
  { value: 'Male', label: 'Male sex' },
  { value: 'Female', label: 'Female sex' },
  { value: 'Primary Education', label: 'Primary education' },
  { value: 'Secondary Education', label: 'Secondary education' },
  { value: 'Tertiary Education', label: 'Tertiary education' },
  { value: 'Missing', label: 'Missing value' },
  { value: 'Other', label: 'Other' },
]

const descriptiveInfo = ref(null)
const descriptiveInfoDetails = ref(null)
const preselectedValues = ref({})
const expandedDatabases = reactive({})
const expandedVariables = reactive({})
const isProcessing = ref(false)
const loadingIconIsPen = ref(false)
const mapperLoaded = ref(false)
let _loadingInterval = null

// per-input form state — keyed by stable composite keys
const continuousUnits = reactive({}) // `${db}_${var}` -> unit
const continuousMissing = reactive({}) // `${db}_${var}` -> missing notation
const categorySelections = reactive({}) // `${db}_${var}_${value}` -> selected option
const categoryComments = reactive({}) // `${db}_${var}_${value}` -> comment
const previousSelections = reactive({}) // tracks the last value for updateCategoryMapping

function buildContinuousVariable(database, displayName, dbIdx, itemIdx) {
  const m = displayName.match(/\(or "([^"]+)"\)/)
  const localVariable = m
    ? m[1]
    : displayName.toLowerCase().replace(/ /g, '_')
  const isMissing = displayName.startsWith('Missing Description')
  const displayLabel = displayName.replace(' (or "', '<br>(or "')
  return {
    type: 'continuous',
    displayName,
    displayLabel,
    localVariable,
    isMissing,
    dbIdx,
    itemIdx,
    unitKey: `${database}_${localVariable}`,
  }
}

function buildCategoricalVariable(database, varName, categories, dbIdx, itemIdx) {
  const m = varName.match(/\(or "([^"]+)"\)/)
  const localVariable = m
    ? m[1]
    : varName.toLowerCase().replace(/ /g, '_')
  const globalVarName = varName.split(' (or')[0].toLowerCase().replace(/ /g, '_')
  const isMissing = varName.startsWith('Missing Description')
  const displayLabel = varName.replace(' (or "', '<br>(or "')

  const categoryOptions = jsonld.getCategoryOptionsForVariable(database, globalVarName)
  const localMappings = jsonld.getLocalMappingsForVariable(
    database,
    localVariable,
    globalVarName
  )

  const valueToTermKey = {}
  for (const [termKey, values] of Object.entries(localMappings)) {
    if (Array.isArray(values)) {
      for (const v of values) {
        if (v != null) valueToTermKey[String(v).trim()] = termKey
      }
    } else if (values != null) {
      valueToTermKey[String(values).trim()] = termKey
    }
  }

  const processedCategories = []
  for (const c of categories) {
    const value = c.value !== undefined ? c.value : ''
    const count = c.count || 0
    const safeValue = String(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;')
    const displayValue = value !== '' ? value : 'Empty cells'

    const termKey = valueToTermKey[String(value).trim()]
    const preselectedValue = termKey
      ? termKey.charAt(0).toUpperCase() + termKey.slice(1).replace(/_/g, ' ')
      : ''

    // Pre-seed reactive selections from the local mappings, but only once
    const selKey = `${database}_${localVariable}_${value}`
    if (!(selKey in categorySelections) && preselectedValue) {
      categorySelections[selKey] = preselectedValue
      previousSelections[selKey] = preselectedValue
    }
    // Pre-seed from server preselectedValues
    const backendKey = `${database}_${localVariable}_category_"${value}"`
    if (preselectedValues.value?.[backendKey] && !categorySelections[selKey]) {
      categorySelections[selKey] = preselectedValues.value[backendKey]
      previousSelections[selKey] = preselectedValues.value[backendKey]
    }

    processedCategories.push({
      value,
      count,
      safeValue,
      displayValue,
      preselectedValue,
      key: selKey,
      backendKey,
      backendCommentKey: `comment_${database}_${localVariable}_category_"${value}"`,
      backendCountKey: `count_${database}_${localVariable}_category_"${value}"`,
    })
  }

  return {
    type: 'categorical',
    displayName: varName,
    displayLabel,
    localVariable,
    globalVarName,
    isMissing,
    dbIdx,
    itemIdx,
    categoryOptions,
    categories: processedCategories,
  }
}

const parsedDatabases = computed(() => {
  if (!descriptiveInfoDetails.value) return []
  // depend on mapperLoaded so we recompute after the mapping arrives
  void mapperLoaded.value
  const result = []
  let dbIdx = 0
  for (const [database, variables] of Object.entries(descriptiveInfoDetails.value)) {
    dbIdx++
    if (!variables?.length) continue
    const dbEntry = { name: database, dbIdx, variables: [] }
    let itemIdx = 0
    for (const variable of variables) {
      if (typeof variable === 'string') {
        itemIdx++
        dbEntry.variables.push(
          buildContinuousVariable(database, variable, dbIdx, itemIdx)
        )
      } else if (typeof variable === 'object') {
        for (const [varName, categories] of Object.entries(variable)) {
          itemIdx++
          dbEntry.variables.push(
            buildCategoricalVariable(database, varName, categories, dbIdx, itemIdx)
          )
        }
      }
    }
    result.push(dbEntry)
  }
  return result
})

function toggleDatabase(name) {
  expandedDatabases[name] = !expandedDatabases[name]
}

function toggleVariable(database, varIdx) {
  if (!expandedVariables[database]) expandedVariables[database] = {}
  expandedVariables[database][varIdx] = !expandedVariables[database][varIdx]
}

function isVariableExpanded(database, varIdx) {
  return !!expandedVariables[database]?.[varIdx]
}

async function onCategoryChange(database, localVariable, globalVariable, categoryValue, key) {
  const selectedOption = categorySelections[key]
  const previousOption = previousSelections[key]
  previousSelections[key] = selectedOption
  try {
    await jsonld.updateCategoryMapping(
      database,
      localVariable,
      globalVariable,
      String(categoryValue),
      selectedOption,
      previousOption
    )
  } catch (e) {
    console.error('Failed to update category mapping:', e)
  }
}

const loadingIconClass = computed(() =>
  loadingIconIsPen.value ? 'fa-pen' : 'fa-edit'
)

function startLoadingAnimation() {
  isProcessing.value = true
  loadingIconIsPen.value = false
  _loadingInterval = setInterval(() => {
    loadingIconIsPen.value = !loadingIconIsPen.value
  }, 1000)
}

async function onFormSubmit() {
  // Persist updated descriptive_info to IndexedDB before the native POST.
  try {
    const updated = JSON.parse(JSON.stringify(descriptiveInfo.value || {}))
    for (const dbEntry of parsedDatabases.value) {
      for (const v of dbEntry.variables) {
        if (v.type === 'continuous') {
          const unit = continuousUnits[v.unitKey]
          if (unit && updated[dbEntry.name]?.[v.localVariable]) {
            updated[dbEntry.name][v.localVariable].units = unit
          }
        } else if (v.type === 'categorical') {
          for (const c of v.categories) {
            const description = categorySelections[c.key]
            if (description && updated[dbEntry.name]?.[v.localVariable]) {
              const comment = categoryComments[c.key] || 'No comment provided'
              const count = c.count != null ? c.count : 'No count available'
              updated[dbEntry.name][v.localVariable][`Category: ${c.value}`] =
                `Category ${c.value}: ${description}, comment: ${comment}, count: ${count}`
            }
          }
        }
      }
    }
    await db.saveData('metadata', {
      key: 'descriptive_info',
      data: updated,
      timestamp: new Date().toISOString(),
    })
  } catch (e) {
    console.error('Failed to update descriptive_info in IndexedDB:', e)
  }
  startLoadingAnimation()
}

onMounted(async () => {
  try {
    const { data } = await api.get('/api/v1/describe-variable-details-state')
    descriptiveInfo.value = data.descriptive_info || {}
    descriptiveInfoDetails.value = data.descriptive_info_details || {}
    preselectedValues.value = data.preselected_values || {}
    await db.saveData('metadata', {
      key: 'descriptive_info',
      data: descriptiveInfo.value,
      timestamp: new Date().toISOString(),
    })
    await db.saveData('metadata', {
      key: 'descriptive_info_details',
      data: descriptiveInfoDetails.value,
      timestamp: new Date().toISOString(),
    })
    await jsonld.loadFromIndexedDB()
    mapperLoaded.value = true
  } catch (e) {
    console.error('Failed to load variable details state:', e)
  }
})
</script>

<template>
  <div>
    <h1><i class="fas fa-pencil-ruler" /> Describe categories and units</h1>
    <hr>
    <p>
      Please provide more information for the categorical and continuous variables that
      were defined in the variable description page.
    </p>

    <form
      class="form-horizontal"
      method="POST"
      action="/end"
      @submit="onFormSubmit"
    >
      <hr>

      <div>
        <div
          v-for="dbEntry in parsedDatabases"
          :key="dbEntry.name"
          class="database-section"
        >
          <h2 class="database-heading">
            <i class="fas fa-database" /> {{ dbEntry.name }}
          </h2>
          <button
            type="button"
            class="toggle-button"
            :class="{ open: expandedDatabases[dbEntry.name] }"
            @click="toggleDatabase(dbEntry.name)"
          >
            <span class="toggle-text">
              {{ expandedDatabases[dbEntry.name] ? 'Show less' : 'Show more' }}
            </span>
            <i
              class="fas"
              :class="
                expandedDatabases[dbEntry.name]
                  ? 'fa-chevron-down'
                  : 'fa-chevron-up'
              "
            />
          </button>

          <div
            class="content variables-container"
            :class="{
              active: expandedDatabases[dbEntry.name],
              hidden: !expandedDatabases[dbEntry.name],
            }"
          >
            <template
              v-for="(variable, varIdx) in dbEntry.variables"
              :key="`${dbEntry.name}_${varIdx}`"
            >
              <div
                v-if="variable.type === 'continuous'"
                class="variable-row"
              >
                <div
                  class="variable-label"
                  v-html="
                    variable.isMissing
                      ? `<span class='missing-description'>${variable.displayLabel}</span>`
                      : variable.displayLabel
                  "
                />
                <div class="variable-controls">
                  <input
                    v-model="continuousUnits[variable.unitKey]"
                    type="text"
                    :name="`${dbEntry.name}_${variable.localVariable}`"
                    placeholder="Type unit here"
                    class="form-control"
                  >
                  <input
                    v-model="continuousMissing[variable.unitKey]"
                    type="text"
                    :name="`${dbEntry.name}_${variable.localVariable}_notation_missing_or_unspecified`"
                    placeholder="Type missing value notation here"
                    class="form-control"
                  >
                </div>
              </div>

              <template v-if="variable.type === 'categorical'">
                <div class="variable-row">
                  <div
                    class="variable-label"
                    v-html="
                      variable.isMissing
                        ? `<span class='missing-description'>${variable.displayLabel}</span>`
                        : variable.displayLabel
                    "
                  />
                  <div class="variable-controls">
                    <button
                      type="button"
                      class="item-toggle-button"
                      :class="{ open: isVariableExpanded(dbEntry.name, varIdx) }"
                      @click="toggleVariable(dbEntry.name, varIdx)"
                    />
                  </div>
                </div>

                <div
                  class="toggle-content categorical-section"
                  :class="{
                    active: isVariableExpanded(dbEntry.name, varIdx),
                    hidden: !isVariableExpanded(dbEntry.name, varIdx),
                  }"
                >
                  <template
                    v-for="cat in variable.categories"
                    :key="cat.key"
                  >
                    <div class="category-item">
                      <div class="category-label">
                        {{ cat.displayValue }} (counted: {{ cat.count }})
                      </div>
                      <div class="category-controls">
                        <select
                          v-model="categorySelections[cat.key]"
                          class="form-control category-select"
                          :name="cat.backendKey"
                          @change="
                            onCategoryChange(
                              dbEntry.name,
                              variable.localVariable,
                              variable.globalVarName,
                              cat.value,
                              cat.key
                            )
                          "
                        >
                          <option value="">
                            Description
                          </option>
                          <template v-if="variable.categoryOptions.length > 0">
                            <option
                              v-for="opt in variable.categoryOptions"
                              :key="opt"
                              :value="opt"
                            >
                              {{ opt }}
                            </option>
                            <option value="Other">
                              Other
                            </option>
                          </template>
                          <template v-else>
                            <option
                              v-for="opt in DEFAULT_CATEGORY_OPTIONS"
                              :key="opt.value"
                              :value="opt.value"
                            >
                              {{ opt.label }}
                            </option>
                          </template>
                        </select>
                        <input
                          v-model="categoryComments[cat.key]"
                          type="text"
                          class="form-control"
                          :name="cat.backendCommentKey"
                          placeholder="If other, please specify"
                          :disabled="categorySelections[cat.key] !== 'Other'"
                        >
                      </div>
                    </div>
                    <input
                      type="hidden"
                      :name="cat.backendCountKey"
                      :value="cat.count"
                    >
                  </template>
                </div>
              </template>
            </template>
          </div>
          <hr>
        </div>
      </div>

      <p>
        <button
          type="submit"
          class="btn btn-primary"
          :disabled="isProcessing"
          :class="{ processing: isProcessing }"
        >
          <template v-if="!isProcessing">
            <i class="fas fa-play" /> Submit
          </template>
          <template v-else>
            <i
              class="fas loading-icon"
              :class="loadingIconClass"
            />
            Processing descriptions...
          </template>
        </button>
        <RouterLink
          to="/describe/variables"
          class="btn btn-light"
        >
          <i class="fas fa-backward" /> Back to Describe Variables
        </RouterLink>
      </p>
    </form>

    <div class="mt-4">
      <div class="alert alert-info py-2 info-purple">
        <i class="fas fa-info-circle" />
        <strong>Reference Guide</strong><br>
        <div class="mt-1 ms-4">
          <strong>Data Types:</strong>
          <div class="row g-1 mt-1">
            <div class="col-md-6">
              <i class="fas fa-tags me-2 ref-guide-icon" />
              <strong>Categorical</strong> — select the categories that best describe
              the listed values.
            </div>
            <div class="col-md-6">
              <i class="fas fa-chart-line me-2 ref-guide-icon" />
              <strong>Continuous</strong> — specify the unit and missing value notation.
            </div>
          </div>
          <p class="mb-0 mt-2">
            <i class="fas fa-exclamation-triangle" />
            Variables that are by definition standardised (e.g. ICD-10, EORTC-QLQ-C30,
            EuroQoL EQ5D) <u>do not have to be described</u>.
          </p>
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
