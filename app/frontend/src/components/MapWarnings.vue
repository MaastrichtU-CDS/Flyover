<script setup>
// The warnings of a semantic map (`map_warnings` in the answers of the
// routes that open a map). They never block: each says what it means for the
// annotation. Rendered as text, as their messages carry the names of tables
// and columns from an uploaded file.
import { computed, ref } from 'vue'

const SHOWN_AT_FIRST = 5

const props = defineProps({
  warnings: { type: Array, default: () => [] },
})

const showAll = ref(false)
const count = computed(() => props.warnings.length)
const shown = computed(() =>
  showAll.value ? props.warnings : props.warnings.slice(0, SHOWN_AT_FIRST)
)
const heading = computed(() =>
  count.value === 1
    ? 'The semantic map has 1 warning.'
    : `The semantic map has ${count.value} warnings.`
)
</script>

<template>
  <div
    v-if="count"
    class="alert alert-warning alert-compact map-warnings"
  >
    <p class="mb-1">
      <i class="fas fa-exclamation-triangle" /> <strong>{{ heading }}</strong>
      You can go on; each one says what it means for the annotation.
    </p>
    <ul class="mb-0">
      <li
        v-for="(warning, index) in shown"
        :key="index"
        class="map-warning"
      >
        {{ warning.message }}
        <div
          v-if="warning.suggestion"
          class="map-warning-suggestion"
        >
          {{ warning.suggestion }}
        </div>
      </li>
    </ul>
    <button
      v-if="count > SHOWN_AT_FIRST && !showAll"
      type="button"
      class="btn btn-link btn-sm p-0 mt-1 map-warnings-more"
      @click="showAll = true"
    >
      {{ `Show all ${count} warnings` }}
    </button>
  </div>
</template>

<style scoped>
.map-warnings ul {
  padding-left: 1.25rem;
}
.map-warning + .map-warning {
  margin-top: 0.25rem;
}
.map-warning-suggestion {
  font-size: 0.85em;
  opacity: 0.85;
}
</style>
