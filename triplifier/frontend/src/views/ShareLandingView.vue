<script setup>
import { ref, onMounted } from 'vue'
import api from '@/services/api'
import { useStatusStore } from '@/stores/status'

const status = useStatusStore()

const hasSemanticMap = ref(false)
const hasOntology = ref(true)

async function downloadSemanticMap() {
  try {
    const res = await api.get('/downloadSemanticMap', { responseType: 'blob' })
    const blob = new Blob([res.data])
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'semantic_map.jsonld'
    document.body.appendChild(a)
    a.click()
    a.remove()
    URL.revokeObjectURL(url)
  } catch (e) {
    status.error(`Could not download semantic map: ${e.message || e}`)
  }
}

onMounted(async () => {
  try {
    const { data } = await api.get('/api/check-graph-exists')
    hasOntology.value = !!data?.exists
  } catch {
    hasOntology.value = false
  }
})
</script>

<template>
  <div>
    <br>
    <h1><i class="fas fa-mail-forward" /> Share</h1>
    <hr>
    <p style="line-height: 1.5; margin: 0">
      Share metadata about your data in a privacy-aware manner using the options below.
    </p>
    <br>

    <div
      v-if="hasSemanticMap"
      id="semanticMapSection"
    >
      <div class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0">
            <i class="fas fa-project-diagram" /> Download your local semantic mapping
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            Given you have previously uploaded a Flyover semantic map JSON-LD file, you can
            now download your local semantic mapping.
          </p>
          <button
            class="btn btn-outline-secondary"
            @click="downloadSemanticMap"
          >
            <i class="fas fa-download" /> Download local semantic map
          </button>
        </div>
      </div>
      <br>
    </div>

    <div
      v-if="hasOntology"
      id="ontologySection"
    >
      <div class="card mb-4">
        <div class="card-header">
          <h5 class="mb-0">
            <i class="fas fa-file-pen" /> Download your local ontology file
          </h5>
        </div>
        <div class="card-body">
          <p class="text-muted">
            Ran into variables you could not directly map to the global mapping, or have
            not used a global map at all? In that case we recommend that you download your
            local ontology.
          </p>
          <a
            href="/downloadOntology"
            download="local_ontology.owl"
          >
            <button class="btn btn-outline-secondary">
              <i class="fas fa-download" /> Download local ontology
            </button>
          </a>
        </div>
      </div>
      <br>
    </div>

    <div id="shareMoreSection">
      <div class="row">
        <div class="col-md-6 mb-4">
          <div class="card h-100">
            <div class="card-header">
              <h5 class="mb-0">
                <i class="fas fa-file-export" /> Generate Mock Data
              </h5>
            </div>
            <div class="card-body d-flex flex-column">
              <p class="text-muted flex-grow-1">
                Generate anonymous mock data based on your semantic mapping and data
                structure. This allows you to share realistic test data without privacy
                concerns.
              </p>
              <RouterLink
                to="/share/mock"
                class="btn btn-outline-secondary mt-3"
              >
                <i class="fas fa-arrow-right" /> Generate Mock Data
              </RouterLink>
            </div>
          </div>
        </div>

        <div class="col-md-6 mb-4">
          <div class="card h-100">
            <div class="card-header">
              <h5 class="mb-0">
                <i class="fas fa-cloud-upload-alt" /> Publish to Repository
              </h5>
            </div>
            <div class="card-body d-flex flex-column">
              <p class="text-muted flex-grow-1">
                Publish your semantically interoperable data to external repositories.
                Share your metadata and mappings with the broader research community.
              </p>
              <RouterLink
                to="/share/publish"
                class="btn btn-outline-secondary mt-3"
              >
                <i class="fas fa-arrow-right" /> Publish Data
              </RouterLink>
            </div>
          </div>
        </div>
      </div>
      <br>
    </div>

    <RouterLink
      to="/"
      class="btn btn-light"
    >
      <i class="fas fa-home" /> Return to Home
    </RouterLink>
    <br><br>
  </div>
</template>
