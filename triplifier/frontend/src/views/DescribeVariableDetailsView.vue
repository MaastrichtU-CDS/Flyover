<script setup>
import MigrationStub from '@/components/MigrationStub.vue'
// Detail data is collected from /describe_variable_details (legacy) — that's a
// single page covering all variables, not a per-variable URL.
</script>

<template>
  <MigrationStub
    title="Describe categories and units"
    title-icon="fa-pencil-ruler"
    legacy-href="/describe_variable_details"
    legacy-label="Open legacy variable details"
  />
</template>
