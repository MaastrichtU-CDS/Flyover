<script setup>
/**
 * SuggestionCoachmark — the first-visit callout that explains the
 * suggestion review flow (WS2).
 *
 * Non-modal: it never blocks clicking the pill or the dropdown
 * underneath, never steals focus on page load, and closes on "Got it"
 * or Escape (the views additionally close it when the anchored
 * suggestion itself is accepted or dismissed).
 *
 * Props:
 *   title — bold heading of the callout.
 *   body — explanation text.
 *   confirmLabel — label of the acknowledgement button (default "Got it").
 *
 * Emits:
 *   close — the user acknowledged the callout (button or Escape).
 *
 * The callout appears ~300 ms after mount so it does not flash during
 * layout, is announced once through a polite live region, and respects
 * prefers-reduced-motion. The look follows the app's CSS-only
 * .bootstrap-tooltip (dark bubble with an arrow); there is no
 * Bootstrap JS in the app.
 */

import { onBeforeUnmount, onMounted, ref, useId } from 'vue'

const props = defineProps({
  title: { type: String, required: true },
  body: { type: String, required: true },
  confirmLabel: { type: String, default: 'Got it' },
})

const emit = defineEmits(['close'])

const titleId = useId()

// Hidden until the short delay passed; announced through the live region
// only once visible so screen readers do not read it during layout.
const visible = ref(false)
const announced = ref('')
let _showTimer = null
let _announceTimer = null

function close() {
  emit('close')
}

function onKeydown(e) {
  if (e.key === 'Escape' && visible.value) close()
}

onMounted(() => {
  document.addEventListener('keydown', onKeydown)
  _showTimer = setTimeout(() => {
    visible.value = true
    _announceTimer = setTimeout(() => {
      announced.value = `${props.title}. ${props.body}`
    }, 50)
  }, 300)
})

onBeforeUnmount(() => {
  document.removeEventListener('keydown', onKeydown)
  clearTimeout(_showTimer)
  clearTimeout(_announceTimer)
})
</script>

<template>
  <div>
    <span
      class="coachmark-live-region"
      aria-live="polite"
    >{{ announced }}</span>
    <div
      v-show="visible"
      class="suggestion-coachmark"
      role="dialog"
      aria-modal="false"
      :aria-labelledby="titleId"
    >
      <strong
        :id="titleId"
        class="coachmark-title"
      >{{ title }}</strong>
      <p class="coachmark-body">
        {{ body }}
      </p>
      <button
        type="button"
        class="btn btn-sm btn-light coachmark-confirm"
        @click.stop="close"
      >
        {{ confirmLabel }}
      </button>
    </div>
  </div>
</template>

<style scoped>
/* Visually hidden but available to screen readers. */
.coachmark-live-region {
  position: absolute;
  width: 1px;
  height: 1px;
  margin: -1px;
  padding: 0;
  border: 0;
  clip: rect(0 0 0 0);
  overflow: hidden;
  white-space: nowrap;
}

.suggestion-coachmark {
  position: absolute;
  top: calc(100% + 10px);
  left: 0;
  z-index: 1080;
  /* The callout sits inside a small bold pill or a large bold h2, so it
     sizes itself in rem and resets the text styles it would inherit:
     with em units it came out tiny next to a pill and huge in a heading.
     max-content lets it grow past its (narrow) anchor up to max-width. */
  width: max-content;
  max-width: min(300px, calc(100vw - 2rem));
  padding: 0.6rem 0.8rem;
  border-radius: 0.375rem;
  background-color: rgba(0, 0, 0, 0.9);
  color: #fff;
  font-size: 0.875rem;
  font-weight: 400;
  font-style: normal;
  line-height: 1.45;
  letter-spacing: normal;
  text-transform: none;
  white-space: normal;
  text-align: left;
  box-shadow: 0 5px 15px rgba(0, 0, 0, 0.25);
  transition: opacity 0.2s ease-in;
}

/* Upward arrow, mirroring .bootstrap-tooltip's downward one. */
.suggestion-coachmark::before {
  content: '';
  position: absolute;
  bottom: 100%;
  left: 14px;
  border-width: 0 0.4rem 0.4rem;
  border-style: solid;
  border-color: transparent transparent rgba(0, 0, 0, 0.9);
}

.coachmark-title {
  display: block;
  margin-bottom: 0.2rem;
  font-size: 0.9375rem;
  font-weight: 600;
}

.coachmark-body {
  margin: 0 0 0.5rem;
}

.coachmark-confirm {
  font-size: 0.8125rem;
  padding: 0.15rem 0.6rem;
}

@media (prefers-reduced-motion: reduce) {
  .suggestion-coachmark {
    transition: none;
  }
}
</style>
