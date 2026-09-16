<script setup>
import { computed, onMounted, ref } from 'vue'
import api from '@/services/api'
import { useStatusStore } from '@/stores/status'

const status = useStatusStore()

const variables = ref([]) // { fullName, displayName, database, status, message }
const search = ref('')
const filter = ref('all')
const currentPage = ref(1)
const itemsPerPage = 10

const filteredVariables = computed(() =>
  variables.value.filter((v) => {
    const matchesSearch = v.displayName.toLowerCase().includes(search.value.toLowerCase())
    const matchesStatus = filter.value === 'all' || v.status === filter.value
    return matchesSearch && matchesStatus
  })
)

const totalPages = computed(() =>
  Math.max(1, Math.ceil(filteredVariables.value.length / itemsPerPage))
)

const pageVariables = computed(() => {
  const start = (currentPage.value - 1) * itemsPerPage
  return filteredVariables.value.slice(start, start + itemsPerPage)
})

function changePage(d) {
  const next = currentPage.value + d
  if (next >= 1 && next <= totalPages.value) currentPage.value = next
}

function statusIcon(s) {
  if (s === 'success') return 'fa-check-circle success'
  if (s === 'error') return 'fa-times-circle error'
  if (s === 'skipped') return 'fa-minus-circle skipped'
  if (s === 'undescribed') return 'fa-minus-circle skipped'
  return 'fa-spinner spinner'
}

async function verifyOne(v) {
  try {
    const { data } = await api.post('/verify-annotation-ask', { variable: v.fullName })
    if (data.success && typeof data.valid !== 'undefined') {
      v.status = data.valid ? 'success' : 'error'
      v.message = data.valid ? 'Successfully annotated' : 'Annotation failed'
    } else {
      v.status = 'error'
      v.message = `Error: ${data.error || 'Unknown error'}`
    }
  } catch {
    v.status = 'error'
    v.message = 'Network error'
  }
}

onMounted(async () => {
  try {
    const { data } = await api.get('/api/v1/annotation-verify-state')

    // Build the set of databases that were actually included in the annotation
    // run (i.e. appear in annotation_status). Databases absent from this set
    // were intentionally skipped (filtered out) and should not be shown as
    // failed — they simply were not annotated.
    const annotationStatus = data.annotation_status || {}
    const annotatedDatabases = new Set(
      Object.keys(annotationStatus).map((key) => key.split('.')[0])
    )
    const anyAnnotationRan = Object.keys(annotationStatus).length > 0

    const described = Object.keys(data.variable_data || {}).map((fullName) => {
      const db = fullName.split('.')[0]
      // If an annotation run happened but this database was not part of it,
      // mark the variable as skipped rather than pending (verification would
      // fail for the wrong reason and mislead the user).
      const wasSkipped = anyAnnotationRan && !annotatedDatabases.has(db)
      return {
        fullName,
        displayName: `${fullName.split('.').slice(-1)[0]} (${db})`,
        database: db,
        status: wasSkipped ? 'skipped' : 'pending',
        message: wasSkipped ? 'Not annotated' : 'Checking annotation...',
      }
    })
    const seen = new Set()
    const undescribed = (data.unannotated_variables || [])
      .map((n) => n.split('.').slice(-1)[0])
      .filter((short) => {
        if (seen.has(short)) return false
        seen.add(short)
        return true
      })
      .map((short) => ({
        fullName: short,
        displayName: short,
        database: '',
        status: 'undescribed',
        message: 'Not described',
      }))
    variables.value = [...described, ...undescribed]
    // Iterate via the reactive proxy — mutating the originals in `described`
    // bypasses Vue's set-trap and the UI would stay on "Checking annotation…"
    for (const v of variables.value) {
      if (v.status === 'pending') verifyOne(v)
    }
  } catch (e) {
    status.error(`Could not load verification data: ${e.message || e}`)
  }
})
</script>

<template>
  <div>
    <h1><i class="fas fa-clipboard-check" /> Annotation Verification</h1>
    <hr>
    <p>Checking annotation status for all variables below.</p>

    <div class="filter-section">
      <div class="row align-items-center">
        <div class="col-md-6">
          <div class="input-group">
            <span class="input-group-text"><i class="fas fa-search" /></span>
            <input
              v-model="search"
              type="text"
              class="form-control"
              placeholder="Search variables..."
              @input="currentPage = 1"
            >
          </div>
        </div>
        <div class="col-md-6">
          <select
            v-model="filter"
            class="form-select"
            @change="currentPage = 1"
          >
            <option value="all">
              All Variables
            </option>
            <option value="success">
              Successfully Annotated
            </option>
            <option value="error">
              Failed
            </option>
            <option value="skipped">
              Not Annotated
            </option>
            <option value="pending">
              Pending
            </option>
            <option value="undescribed">
              Not Described
            </option>
          </select>
        </div>
      </div>
    </div>

    <div id="all-variables-list">
      <div
        v-for="v in pageVariables"
        :key="v.fullName"
        class="variable-item"
        :class="{ unannotated: v.status === 'undescribed' || v.status === 'skipped' }"
      >
        <span class="variable-name">{{ v.displayName }}</span>
        <div class="status-indicator">
          <span class="status-message">{{ v.message }}</span>
          <span class="status-icon">
            <i
              class="fas"
              :class="statusIcon(v.status)"
            />
          </span>
        </div>
      </div>
    </div>

    <div
      v-if="totalPages > 1"
      class="pagination-controls"
    >
      <button
        type="button"
        :disabled="currentPage <= 1"
        @click="changePage(-1)"
      >
        &#x2190;
      </button>
      <span class="page-indicator">Page <span>{{ currentPage }}</span> of <span>{{ totalPages }}</span></span>
      <button
        type="button"
        :disabled="currentPage >= totalPages"
        @click="changePage(1)"
      >
        &#x2192;
      </button>
    </div>

    <hr>
    <RouterLink
      to="/share"
      class="btn btn-primary"
    >
      <i class="fas fa-play" /> Proceed to Share
    </RouterLink>
    <br><br>
  </div>
</template>

<style scoped>
.success { color: #28a745; }
.error   { color: #dc3545; }
.skipped { color: #6c757d; }
.spinner { color: #6c757d; }
</style>
