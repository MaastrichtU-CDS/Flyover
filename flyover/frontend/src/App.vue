<script setup>
import AppNav from '@/components/AppNav.vue'
import AppFooter from '@/components/AppFooter.vue'
import StatusBanner from '@/components/StatusBanner.vue'
</script>

<template>
  <div class="app-shell">
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
