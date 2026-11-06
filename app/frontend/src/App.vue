<script setup>
import { onMounted } from 'vue'
import AppNav from '@/components/AppNav.vue'
import AppFooter from '@/components/AppFooter.vue'
import ProjectStatusBar from '@/components/ProjectStatusBar.vue'
import StatusBanner from '@/components/StatusBanner.vue'
import { useProjectStore } from '@/stores/project'

const project = useProjectStore()

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
        <RouterView />
      </div>
    </main>
    <AppFooter />
  </div>
</template>
