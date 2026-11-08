<script setup>
import { onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import AppNav from '@/components/AppNav.vue'
import AppFooter from '@/components/AppFooter.vue'
import MapUploadDialog from '@/components/MapUploadDialog.vue'
import ProjectPicker from '@/components/ProjectPicker.vue'
import ProjectStatusBar from '@/components/ProjectStatusBar.vue'
import StatusBanner from '@/components/StatusBanner.vue'
import StoreWait from '@/components/StoreWait.vue'
import WorkflowSoonDialog from '@/components/WorkflowSoonDialog.vue'
import { useStoreGate } from '@/composables/useStoreGate'
import { useProjectStore } from '@/stores/project'

const project = useProjectStore()
const route = useRoute()
// A page that needs the store waits for it in its place, or says above it
// why the store cannot be reached (see useStoreGate).
const { waitingFor, trouble } = useStoreGate()

onMounted(() => {
  project.init()
})

// A tab that comes to a page of the RDF workflow without having entered it
// (from the home page, or by a link) is asked for the project first (the
// page waits meanwhile; see useStoreGate). Only a change of page asks, or
// the project store becoming ready: closing the pop-up does not, nor does
// leaving the workflow (Exit RDF leaves it a moment before the home page
// shows). Until the router has settled on the first page, nothing is asked.
watch(
  () => [route.name, route.path, project.ready],
  ([name, , ready]) => {
    if (!name || !ready) return
    if (
      route.meta?.workflow === 'rdf' &&
      project.workflow !== 'rdf' &&
      !project.picker.open
    ) {
      project.openPicker('enter')
    }
  },
  { immediate: true }
)

// A tab that comes back to the home page, by Exit RDF or otherwise, has left
// the workflow, and is asked again when it enters it again.
watch(
  () => [route.name, route.meta?.workflow, project.workflow],
  ([name, workflow, inWorkflow]) => {
    if (name && !workflow && inWorkflow) project.exitWorkflow()
  },
  { immediate: true }
)
</script>

<template>
  <div class="app-shell">
    <ProjectStatusBar />
    <AppNav />
    <main class="app-main">
      <div class="container container-flyover">
        <StatusBanner />
        <div
          v-if="trouble && !waitingFor"
          class="alert alert-warning alert-compact store-trouble"
          role="alert"
        >
          <i class="fas fa-exclamation-triangle" /> {{ trouble }}
        </div>
        <StoreWait
          v-if="waitingFor"
          :reason="waitingFor"
        />
        <!-- Opens afresh once the tab enters a workflow: the page may have
             opened under the pop-up, before the choice it reads. -->
        <RouterView
          v-else
          :key="project.entries"
        />
      </div>
    </main>
    <AppFooter />
    <ProjectPicker />
    <MapUploadDialog />
    <WorkflowSoonDialog />
  </div>
</template>
