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
 *   coachmark — show the first-visit review callout anchored to this pill.
 *   coachmarkCopy — { title, body } copy for that callout.
 *
 * Emits:
 *   dismiss — the user clicked ×.
 *   accept — the user clicked the accept button.
 *   coachmark-close — the user acknowledged the callout.
 */

import { computed } from 'vue'
import { SOURCE_ICONS } from '@/stores/suggestions'
import SuggestionCoachmark from '@/components/SuggestionCoachmark.vue'

const props = defineProps({
  suggestion: { type: Object, required: true },
  applied: { type: Boolean, default: false },
  touched: { type: Boolean, default: false },
  showDismiss: { type: Boolean, default: true },
  coachmark: { type: Boolean, default: false },
  coachmarkCopy: {
    type: Object,
    default: () => ({ title: '', body: '' }),
  },
})

const emit = defineEmits(['dismiss', 'accept', 'coachmark-close'])

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
  const reason = props.suggestion.reason || 'Mapping suggestion'
  if (props.applied && !props.touched) {
    return `${reason} — click to confirm or change the dropdown`
  }
  return reason
})

// Screen-reader labels: the pill's visible text is just an icon and a
// percentage, so each button needs an explicit, self-describing label.
const acceptLabel = computed(() => {
  const match = props.suggestion.match
  const item = props.suggestion.item
  const pct = props.confidencePct
  if (match && item) return `Accept suggestion: map '${item}' to '${match}' (${pct}%)`
  return `Accept suggestion (${pct}%)`
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
  >
    <!-- Accept and dismiss are two sibling buttons so both are keyboard
         reachable; a button cannot nest inside another button, so the
         pill body itself is no longer a clickable span. -->
    <button
      type="button"
      class="suggestion-accept"
      :aria-label="acceptLabel"
      :title="tooltipText"
      @click.stop="emit('accept')"
    >
      <i
        class="fas"
        :class="sourceIcon"
      />
      {{ confidencePct }}%
      <span
        v-if="hasAlternatives"
        class="suggestion-alternatives"
        :title="`${suggestion.alternatives.length} alternative(s) available`"
      >
        <i class="fas fa-list" />
      </span>
    </button>
    <button
      v-if="showDismiss"
      type="button"
      class="suggestion-dismiss"
      aria-label="Dismiss this suggestion"
      title="Dismiss this suggestion"
      @click.stop="emit('dismiss')"
    >
      &times;
    </button>
    <!-- First-visit review cue: anchored under this pill. The root span is
         position: relative so the callout needs no positioning library. -->
    <SuggestionCoachmark
      v-if="coachmark"
      :title="coachmarkCopy.title"
      :body="coachmarkCopy.body"
      @close="emit('coachmark-close')"
    />
  </span>
</template>

<style scoped>
.suggestion-badge {
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 0;
  margin-left: 0.5rem;
  padding: 0;
  border-radius: 999px;
  font-size: 0.75em;
  background: rgba(118, 75, 162, 0.12);
  color: rgb(90, 60, 130);
  border: 1px dashed rgba(118, 75, 162, 0.6);
  transition: background 0.15s;
}

.suggestion-badge:hover {
  background: rgba(118, 75, 162, 0.18);
}

/* An applied-but-unreviewed suggestion keeps the dashed purple "needs
 * review" look (WS1.3): only a reviewed field turns green. The `applied`
 * class stays on the element for tooltips and tests, it just no longer
 * restyles the pill. */

.suggestion-accept {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.1rem 0.45rem 0.1rem 0.35rem;
  border: none;
  border-radius: 999px 0 0 999px;
  background: none;
  color: inherit;
  font-size: inherit;
  cursor: pointer;
}

.suggestion-accept:focus-visible,
.suggestion-dismiss:focus-visible {
  outline: 2px solid rgba(118, 75, 162, 0.9);
  outline-offset: 1px;
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
  padding: 0.1rem 0.4rem 0.1rem 0.1rem;
  line-height: 1;
  font-size: 1.1em;
  color: inherit;
  cursor: pointer;
}
</style>
