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
 *     source, tier, alternatives). A record with no match of its own but
 *     with alternatives (a conflict loser, decision D2) renders an
 *     alternatives-only pill: nothing to accept, just the choices it kept.
 *   applied — whether the user has accepted this suggestion.
 *   touched — whether the user subsequently edited the field.
 *   showDismiss — render the × dismiss button (default true).
 *   coachmark — show the first-visit review callout anchored to this pill.
 *   coachmarkCopy — { title, body } copy for that callout.
 *
 * Emits:
 *   dismiss — the user clicked ×.
 *   accept — the user clicked the accept button.
 *   apply-alternative — the user picked one of the alternative matches
 *     from the popover; the payload is the alternative record dict.
 *   coachmark-close — the user acknowledged the callout.
 *
 * Slots:
 *   retry — reserved for tier 3 (a "retry suggestion" affordance);
 *     unused for now.
 */

import { computed, onBeforeUnmount, ref, watch } from 'vue'
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

const emit = defineEmits([
  'dismiss',
  'accept',
  'apply-alternative',
  'coachmark-close',
])

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

// Only real choices for the user: a non-null alternative that differs
// from the record's own match. Abstains and duplicates of the winner are
// filtered out by the backend merge; keep the guard here so a stale
// record cannot resurrect the noise.
const alternatives = computed(() => {
  const own = props.suggestion.match
  return (props.suggestion.alternatives || []).filter(
    (alt) => alt?.match && alt.match !== own
  )
})

// A conflict loser (D2): the backend nulled its match but kept the
// contested variable as an alternative. There is nothing to accept, so
// the pill shows only the alternatives (the reason names the winner).
const alternativesOnly = computed(
  () => !props.suggestion.match && alternatives.value.length > 0
)

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
  const pct = confidencePct.value
  if (match && item) return `Accept suggestion: map '${item}' to '${match}' (${pct}%)`
  return `Accept suggestion (${pct}%)`
})

// Alternatives popover: CSS-only, positioned under the pill (the root is
// position: relative), toggled by its own button so it stays outside the
// accept button (buttons cannot nest). The Escape listener is attached
// only while the popover is open — a page can render hundreds of badges.
const showAlternatives = ref(false)

function toggleAlternatives() {
  showAlternatives.value = !showAlternatives.value
}

function applyAlternative(alt) {
  showAlternatives.value = false
  emit('apply-alternative', alt)
}

function onKeydown(e) {
  if (e.key === 'Escape' && showAlternatives.value) showAlternatives.value = false
}

watch(showAlternatives, (open) => {
  if (typeof document === 'undefined') return
  if (open) document.addEventListener('keydown', onKeydown)
  else document.removeEventListener('keydown', onKeydown)
})

onBeforeUnmount(() => {
  if (typeof document !== 'undefined') {
    document.removeEventListener('keydown', onKeydown)
  }
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
    :class="{ applied, 'alternatives-only': alternativesOnly }"
  >
    <!-- Accept, alternatives, and dismiss are sibling buttons so all are
         keyboard reachable; a button cannot nest inside another button,
         so the pill body itself is no longer a clickable span. -->
    <button
      v-if="!alternativesOnly"
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
        v-if="tierLabel"
        class="suggestion-tier"
      >{{ tierLabel }}</span>
    </button>
    <button
      v-if="alternatives.length"
      type="button"
      class="suggestion-alternatives"
      :aria-expanded="showAlternatives ? 'true' : 'false'"
      :aria-label="`${alternatives.length} alternative match${alternatives.length === 1 ? '' : 'es'} available — show them`"
      :title="alternativesOnly ? tooltipText : `${alternatives.length} alternative match${alternatives.length === 1 ? '' : 'es'} available`"
      @click.stop="toggleAlternatives"
    >
      <i class="fas fa-list" />
      <template v-if="alternativesOnly">
        {{ alternatives.length }} alternative{{ alternatives.length === 1 ? '' : 's' }}
      </template>
    </button>
    <div
      v-if="showAlternatives"
      class="suggestion-alternatives-popover"
    >
      <button
        v-for="alt in alternatives"
        :key="alt.match"
        type="button"
        class="alternative-entry"
        :aria-label="`Apply alternative: map to '${alt.match}' (${alt.source}, ${Math.round((alt.confidence || 0) * 100)}%)`"
        @click.stop="applyAlternative(alt)"
      >
        <i
          class="fas"
          :class="SOURCE_ICONS[alt.source] || 'fa-lightbulb'"
        />
        <span class="alternative-match">{{ alt.match }}</span>
        <span class="alternative-confidence">{{ Math.round((alt.confidence || 0) * 100) }}%</span>
      </button>
    </div>
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
    <slot name="retry" />
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
  padding: 0.1rem 0.35rem 0.1rem 0.35rem;
  border: none;
  border-radius: 999px 0 0 999px;
  background: none;
  color: inherit;
  font-size: inherit;
  cursor: pointer;
}

.suggestion-accept:focus-visible,
.suggestion-dismiss:focus-visible,
.suggestion-alternatives:focus-visible,
.alternative-entry:focus-visible {
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

.suggestion-tier {
  opacity: 0.7;
  font-size: 0.85em;
}

.suggestion-alternatives {
  display: inline-flex;
  align-items: center;
  padding: 0.1rem 0.15rem;
  border: none;
  background: none;
  opacity: 0.7;
  font-size: 0.85em;
  color: inherit;
  cursor: pointer;
}

.suggestion-badge.alternatives-only .suggestion-alternatives {
  gap: 0.3rem;
  opacity: 1;
}

.suggestion-alternatives-popover {
  position: absolute;
  top: calc(100% + 8px);
  left: 0;
  z-index: 1070;
  min-width: 200px;
  max-width: 280px;
  padding: 0.25rem;
  border-radius: 0.25rem;
  background-color: rgba(0, 0, 0, 0.9);
  color: #fff;
  font-size: 0.9em;
  text-align: left;
  box-shadow: 0 5px 15px rgba(0, 0, 0, 0.25);
}

.alternative-entry {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  width: 100%;
  padding: 0.25rem;
  border: none;
  border-radius: 0.2rem;
  background: none;
  color: inherit;
  font-size: inherit;
  text-align: left;
  cursor: pointer;
}

.alternative-entry:hover {
  background: rgba(255, 255, 255, 0.15);
}

.alternative-match {
  flex: 1;
  overflow-wrap: anywhere;
}

.alternative-confidence {
  opacity: 0.75;
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
