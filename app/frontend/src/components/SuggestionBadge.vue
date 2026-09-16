<script setup>
/**
 * SuggestionBadge — the pill that appears next to a column label or category
 * value when a mapping suggestion is available.
 *
 * Extracted from the LLM branch's inline `.llm-badge` markup and made
 * source-agnostic: the icon and tier label come from the record's `source`
 * and `tier` fields, so the same component renders identically for alias,
 * value_regex, string, and (future) llm sources.
 *
 * Props:
 *   suggestion — the store record dict (status, match, confidence, reason,
 *     source, tier, alternatives).
 *   applied — whether the user has accepted this suggestion.
 *   touched — whether the user subsequently edited the field.
 *   showDismiss — render the × dismiss button (default true).
 *
 * Emits:
 *   dismiss — the user clicked ×.
 *   accept — the user clicked the badge body (explicit accept action).
 */

import { computed } from 'vue'
import { SOURCE_ICONS } from '@/stores/suggestions'

const props = defineProps({
  suggestion: { type: Object, required: true },
  applied: { type: Boolean, default: false },
  touched: { type: Boolean, default: false },
  showDismiss: { type: Boolean, default: true },
})

const emit = defineEmits(['dismiss', 'accept'])

const sourceIcon = computed(() => {
  return SOURCE_ICONS[props.suggestion.source] || 'fa-lightbulb'
})

const tierLabel = computed(() => {
  const tier = props.suggestion.tier
  return tier ? `tier ${tier}` : ''
})

const confidencePct = computed(() => {
  return Math.round((props.suggestion.confidence || 0) * 100)
})

const hasAlternatives = computed(() => {
  return (props.suggestion.alternatives || []).length > 0
})

const tooltipText = computed(() => {
  return props.suggestion.reason || 'Mapping suggestion'
})
</script>

<template>
  <span
    v-if="touched"
    class="suggestion-badge confirmed"
    :title="tooltipText"
  >
    <i class="fas fa-check" /> reviewed
  </span>
  <span
    v-else
    class="suggestion-badge"
    :class="{ applied }"
    :title="tooltipText"
    @click.stop="emit('accept')"
  >
    <i class="fas" :class="sourceIcon" />
    {{ confidencePct }}%
    <span
      v-if="hasAlternatives"
      class="suggestion-alternatives"
      :title="`${hasAlternatives} alternative(s) available`"
    >
      <i class="fas fa-list" />
    </span>
    <button
      v-if="showDismiss"
      type="button"
      class="suggestion-dismiss"
      title="Dismiss this suggestion"
      @click.stop="emit('dismiss')"
    >
      &times;
    </button>
  </span>
</template>

<style scoped>
.suggestion-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  margin-left: 0.5rem;
  padding: 0.1rem 0.45rem;
  border-radius: 999px;
  font-size: 0.75em;
  background: rgba(118, 75, 162, 0.12);
  color: rgb(90, 60, 130);
  border: 1px dashed rgba(118, 75, 162, 0.6);
  cursor: pointer;
  transition: background 0.15s;
}

.suggestion-badge:hover {
  background: rgba(118, 75, 162, 0.18);
}

.suggestion-badge.applied {
  border-style: solid;
  border-color: rgba(40, 140, 80, 0.6);
  background: rgba(40, 140, 80, 0.1);
  color: rgb(30, 110, 60);
}

.suggestion-badge.confirmed {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  margin-left: 0.5rem;
  padding: 0.1rem 0.45rem;
  border-radius: 999px;
  font-size: 0.75em;
  border-style: solid;
  border-color: rgba(40, 140, 80, 0.6);
  background: rgba(40, 140, 80, 0.1);
  color: rgb(30, 110, 60);
  cursor: default;
}

.suggestion-alternatives {
  display: inline-flex;
  align-items: center;
  margin-left: 0.1rem;
  opacity: 0.7;
  font-size: 0.85em;
}

.suggestion-dismiss {
  border: none;
  background: none;
  padding: 0 0.1rem;
  line-height: 1;
  font-size: 1.1em;
  color: inherit;
  cursor: pointer;
}
</style>
