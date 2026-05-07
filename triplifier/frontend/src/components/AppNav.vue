<script setup>
import { RouterLink } from 'vue-router'
</script>

<template>
  <nav class="app-nav">
    <RouterLink to="/">Home</RouterLink>
    <RouterLink to="/ingest">Ingest</RouterLink>
    <RouterLink to="/describe">Describe</RouterLink>
    <RouterLink to="/annotate">Annotate</RouterLink>
    <RouterLink to="/share">Share</RouterLink>
  </nav>
</template>

<style scoped>
.app-nav {
  display: flex;
  gap: 1rem;
  padding: 1rem;
  border-bottom: 1px solid #ddd;
}
.app-nav a {
  text-decoration: none;
  color: #333;
}
.app-nav a.router-link-active {
  font-weight: bold;
  color: #0066cc;
}
</style>
