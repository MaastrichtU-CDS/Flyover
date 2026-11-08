<script setup>
import { onMounted } from 'vue'
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
// A page that needs the store waits for it in its place, or says above it
// why the store cannot be reached (see useStoreGate).
const { waitingFor, trouble } = useStoreGate()

onMounted(() => {
  project.init()
})
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
