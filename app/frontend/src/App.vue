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

// The home page asks for the project before it goes to the RDF workflow
// (see useWorkflowEntry). A tab that comes to a page of the workflow without
// having entered it, by a link, is asked there, and the page waits meanwhile
// (see useStoreGate). Only a change of page asks, or the project store
// becoming ready: closing the pop-up does not, nor does leaving the workflow
// (Exit RDF leaves it a moment before the home page shows). Until the router
// has settled on the first page, nothing is asked.
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
// the workflow, and is asked again when it enters it again. Only arriving
// there leaves: a tab that chose on the home page enters the workflow a
// moment before it goes to the workflow's page.
watch(
  () => [route.name, route.meta?.workflow],
  ([name, workflow]) => {
    if (name && !workflow && project.workflow) project.exitWorkflow()
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
        <RouterView v-else />
      </div>
    </main>
    <AppFooter />
    <ProjectPicker />
    <MapUploadDialog />
    <WorkflowSoonDialog />
  </div>
</template>
