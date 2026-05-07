<script setup>
import AppNav from '@/components/AppNav.vue'
import StatusBanner from '@/components/StatusBanner.vue'
</script>

<template>
  <AppNav />
  <StatusBanner />
  <main class="container">
    <RouterView />
  </main>
</template>
