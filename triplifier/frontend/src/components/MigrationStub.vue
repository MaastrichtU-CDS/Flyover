<script setup>
defineProps({
  title: { type: String, required: true },
  titleIcon: { type: String, default: 'fa-tools' },
  legacyHref: { type: String, required: true },
  legacyLabel: { type: String, default: 'Open legacy implementation' },
})
</script>

<template>
  <div>
    <h1><i class="fas" :class="titleIcon"></i>&nbsp;{{ title }}</h1>
    <hr />
    <div class="alert alert-info">
      <i class="fas fa-info-circle"></i>
      <strong>Migration in progress.</strong>
      This page is part of the Vue 3 SPA but its full interactive features are still being
      ported. The legacy implementation remains available below until the port lands.
    </div>
    <a :href="legacyHref" class="btn btn-primary">
      <i class="fas fa-external-link-alt"></i>&nbsp;{{ legacyLabel }}
    </a>
    <br /><br />
  </div>
</template>
