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
 *   itemLabel — what the progress counter counts ("variables" or
 *     "values"); the details page describes values, not variables.
 */

import { computed, ref } from 'vue'

const TIER_DESCRIPTIONS = {
  1: 'regex and string matching (tier 1)',
  2: 'transformer embeddings (tier 2)',
  3: 'LLM suggestions (tier 3)',
}

const props = defineProps({
  phaseState: { type: Object, required: true },
  tiers: { type: Object, default: () => ({}) },
  compute: { type: String, default: 'host' },
  unreviewedCount: { type: Number, default: 0 },
  itemLabel: { type: String, default: 'variables' },
})

const emit = defineEmits(['clear-all', 'show-coachmark'])

// The tier note ("regex and string matching (tier 1) enabled · …") is
// noise until asked for: it stays hidden until the user opens it with
// the help link, on both describe pages. Opening it also re-triggers
// the review callout.
const helpOpen = ref(false)

function toggleHelp() {
  helpOpen.value = !helpOpen.value
  if (helpOpen.value) emit('show-coachmark')
}

const activeTiers = computed(() => {
  return Object.entries(props.tiers)
    .filter(([_, info]) => info?.state === 'active')
    .map(([n]) => TIER_DESCRIPTIONS[n] || `tier ${n}`)
    .join(', ')
})

const inactiveTiers = computed(() => {
  const inactive = Object.entries(props.tiers)
    .filter(([_, info]) => info?.state === 'inactive')
    .map(([n]) => TIER_DESCRIPTIONS[n] || `tier ${n}`)
    .join(', ')
  if (!inactive) return ''
  return inactive
})

const tierNote = computed(() => {
  if (activeTiers.value && inactiveTiers.value) {
    return `${activeTiers.value} enabled · ${inactiveTiers.value} disabled — see FLYOVER_SUGGESTION_TIERS in docker-compose.yml`
  }
  if (activeTiers.value) {
    return `${activeTiers.value} enabled`
  }
  return inactiveTiers.value
    ? `${inactiveTiers.value} disabled — see FLYOVER_SUGGESTION_TIERS in docker-compose.yml`
    : 'all tiers inactive'
})

const progressText = computed(() => {
  const done = props.phaseState.progress?.done ?? 0
  const total = props.phaseState.progress?.total ?? 0
  return `${done} of ${total}`
})

const hasMatches = computed(() => {
  const entries = Object.values(props.phaseState.byKey || {})
  return entries.some((e) => e?.match)
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
  <!-- A small grid: icon, status message and actions on the first row,
       the tier note on its own full-width row underneath. Sharing one
       flex row made the message and the long tier note wrap into each
       other on normal widths. -->
  <div
    v-if="phaseState.status !== 'idle'"
    class="suggestion-status-bar"
  >
    <i class="fas fa-lightbulb suggestion-status-icon" />
    <div class="suggestion-status-message">
      <template v-if="phaseState.status === 'running'">
        <i class="fas fa-spinner fa-spin" />
        Suggestions: {{ progressText }} {{ itemLabel }}
      </template>
      <template v-else-if="phaseState.status === 'done'">
        <template v-if="hasMatches">
          Suggestions ready — review the highlighted fields
        </template>
        <template v-else>
          No matches found — fill in fields manually
        </template>
      </template>
      <template v-else-if="phaseState.status === 'failed'">
        Suggestions unavailable — fill in fields manually
      </template>
      <template v-else-if="phaseState.status === 'unavailable'">
        {{ unavailableMessage }}
      </template>
    </div>
    <div class="suggestion-status-actions">
      <button
        v-if="hasMatches"
        type="button"
        class="btn btn-sm btn-link suggestion-help-link"
        :aria-expanded="helpOpen ? 'true' : 'false'"
        :title="helpOpen ? null : 'Show the explanation of the suggestion review flow again'"
        @click="toggleHelp"
      >
        {{ helpOpen ? 'Hide' : 'How do suggestions work?' }}
      </button>
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
    <div
      v-if="helpOpen && tierNote"
      class="suggestion-tier-note"
    >
      {{ tierNote }}
    </div>
  </div>
</template>

<style scoped>
.suggestion-status-bar {
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: center;
  column-gap: 0.75rem;
  row-gap: 0.15rem;
  padding: 0.6rem 0.9rem;
  margin-bottom: 0.5rem;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: rgba(118, 75, 162, 0.08);
  border-radius: 4px;
  font-size: 0.9rem;
}

.suggestion-status-icon {
  color: rgb(118, 75, 162);
}

.suggestion-status-message {
  min-width: 0;
  font-weight: 500;
}

.suggestion-status-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  white-space: nowrap;
}

/* Full width under the message (aligned with it, not with the icon). */
.suggestion-tier-note {
  grid-column: 2 / -1;
  color: #6c757d;
  font-size: 0.8rem;
}

.suggestion-tier-note::first-letter {
  text-transform: uppercase;
}

.suggestion-help-link {
  padding: 0 0.25rem;
  font-size: 0.85rem;
  color: #764ba2;
  text-decoration: none;
}

.suggestion-help-link:hover {
  text-decoration: underline;
}

/* Narrow screens: the actions drop to their own row under the message. */
@media (max-width: 767.98px) {
  .suggestion-status-bar {
    grid-template-columns: auto 1fr;
  }

  .suggestion-status-actions {
    grid-column: 2;
    flex-wrap: wrap;
    white-space: normal;
  }
}
</style>
