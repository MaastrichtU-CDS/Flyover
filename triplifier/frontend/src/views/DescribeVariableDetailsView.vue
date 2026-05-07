<script setup>
defineProps({ name: { type: String, default: '' } })
</script>

<template>
  <section>
    <h1>Variable: {{ name }}</h1>
    <p class="placeholder">Variable details — to be migrated in Phase 3.</p>
  </section>
</template>

<style scoped>
.placeholder { color: #888; font-style: italic; }
</style>
