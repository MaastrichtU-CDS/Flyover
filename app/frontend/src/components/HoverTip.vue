<script setup>
import { ref, useId } from 'vue'

// A tooltip in the style of the home page's step tiles (.bootstrap-tooltip
// in assets/main.css), for what it wraps. It shows while the pointer is
// over it or the keyboard focus is inside it, and Escape hides it. The slot
// receives `describedby`, the tooltip's id while it shows, for a focusable
// element inside to point aria-describedby at.

const props = defineProps({
  text: { type: String, default: '' },
  // above | below
  placement: { type: String, default: 'above' },
  // start | center | end: which edge of the anchor it lines up with
  align: { type: String, default: 'center' },
  // Let longer text wrap (and keep its line breaks).
  wrap: { type: Boolean, default: false },
})

const id = useId()
const shown = ref(false)

function show() {
  if (props.text) shown.value = true
}

function hide() {
  shown.value = false
}
</script>

<template>
  <span
    class="hover-tip"
    @mouseenter="show"
    @mouseleave="hide"
    @focusin="show"
    @focusout="hide"
    @keydown.esc="hide"
  >
    <slot :describedby="shown ? id : undefined" />
    <span
      v-if="shown"
      :id="id"
      role="tooltip"
      class="bootstrap-tooltip"
      :class="[
        placement === 'below' ? 'is-below' : '',
        align !== 'center' ? `is-${align}` : '',
        { 'is-wrapping': wrap },
      ]"
    >{{ text }}</span>
  </span>
</template>

<style scoped>
.hover-tip {
  position: relative;
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  gap: inherit;
}
</style>
