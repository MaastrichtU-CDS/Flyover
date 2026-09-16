<script setup>
/**
 * SuggestionStatusBar — the bar above the describe form that shows suggestion
 * job status, tier activity, and a "clear all" button.
 *
 * Extracted from the LLM branch's inline `.llm-status-bar` markup and adapted
 * to show the tier-aware /status shape instead of a single provider. The
 * purple left-border + faint purple fill aesthetic is kept verbatim.
 *
 * Slots:
 *   actions — per-section buttons (e.g. "Suggest this section first").
 *
 * Props:
 *   phaseState — the store's variables or values reactive object.
 *   tiers — the /status tiers dict.
 *   compute — the /status compute mode.
 *   unreviewedCount — number of applied-but-unreviewed fields.
 */

import { computed } from 'vue'

const props = defineProps({
  phaseState: { type: Object, required: true },
  tiers: { type: Object, default: () => ({}) },
  compute: { type: String, default: 'host' },
  unreviewedCount: { type: Number, default: 0 },
})

defineEmits(['clear-all'])

const activeTiers = computed(() => {
  return Object.entries(props.tiers)
    .filter(([_, info]) => info?.state === 'active')
    .map(([n]) => `tier ${n}`)
    .join(', ')
})

const inactiveTiers = computed(() => {
  const inactive = Object.entries(props.tiers)
    .filter(([_, info]) => info?.state === 'inactive')
    .map(([n]) => `tier ${n}`)
    .join(', ')
  if (!inactive) return ''
  const reasons = Object.entries(props.tiers)
    .filter(([_, info]) => info?.state === 'inactive')
    .map(([_, info]) => info?.reason)
    .filter(Boolean)
  return reasons[0] || `tiers ${inactive} inactive`
})

const tierNote = computed(() => {
  if (activeTiers.value && inactiveTiers.value) {
    return `${activeTiers.value} active · ${inactiveTiers.value}`
  }
  if (activeTiers.value) {
    return `${activeTiers.value} active`
  }
  return inactiveTiers.value || 'all tiers inactive'
})

const progressText = computed(() => {
  const done = props.phaseState.progress?.done ?? 0
  const total = props.phaseState.progress?.total ?? 0
  return `${done} of ${total}`
})

const unavailableMessage = computed(() => {
  switch (props.phaseState.reason) {
    case 'no_semantic_map':
      return 'Upload a semantic map to enable suggestions'
    case 'nothing_to_suggest':
      return 'All items are already mapped — nothing to suggest'
    default:
      return 'Suggestions are not available'
  }
})
</script>

<template>
  <div
    v-if="phaseState.status !== 'idle'"
    class="suggestion-status-bar"
  >
    <i class="fas fa-lightbulb" />
    <span v-if="phaseState.status === 'running'">
      <i class="fas fa-spinner fa-spin" />
      Suggestions: {{ progressText }} variables
    </span>
    <span v-else-if="phaseState.status === 'done'">
      Suggestions ready — review the highlighted fields
    </span>
    <span v-else-if="phaseState.status === 'failed'">
      Suggestions unavailable — fill in fields manually
    </span>
    <span v-else-if="phaseState.status === 'unavailable'">
      {{ unavailableMessage }}
    </span>
    <span class="suggestion-tier-note">{{ tierNote }}</span>
    <button
      v-if="unreviewedCount"
      type="button"
      class="btn btn-sm btn-outline-secondary suggestion-clear-all"
      @click="$emit('clear-all')"
    >
      Clear all suggestions
    </button>
    <slot name="actions" />
  </div>
</template>

<style scoped>
.suggestion-status-bar {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.5rem;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: rgba(118, 75, 162, 0.08);
  border-radius: 4px;
  font-size: 0.9em;
}

.suggestion-tier-note {
  color: #777;
  font-size: 0.85em;
}

.suggestion-clear-all {
  margin-left: auto;
}
</style>
