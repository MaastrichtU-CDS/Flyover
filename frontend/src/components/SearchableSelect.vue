<script setup>
// Searchable replacement for large native <select> elements. Type-to-filter
// with capped rendering (large option lists made the native dropdowns take
// seconds to open), and a Teleported panel positioned from the trigger's
// bounding rect — inside overflow/scroll containers an absolutely-positioned
// panel would be clipped or misplaced. A hidden input carries the selected
// value under `name` so the surrounding native form POST keeps working.
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'

const MAX_RENDERED = 100

const props = defineProps({
  modelValue: { type: String, default: '' },
  options: { type: Array, default: () => [] },
  name: { type: String, default: '' },
  inputId: { type: String, default: '' },
  placeholder: { type: String, default: 'Select…' },
  disabledOption: { type: Function, default: null },
})

const emit = defineEmits(['update:modelValue', 'change'])

const open = ref(false)
const query = ref('')
const activeIndex = ref(-1)
const rootEl = ref(null)
const inputEl = ref(null)
const panelEl = ref(null)
const panelStyle = ref({})

const normalisedOptions = computed(() =>
  props.options.map((opt) =>
    typeof opt === 'string' ? { value: opt, label: opt } : opt
  )
)

const filteredOptions = computed(() => {
  const q = query.value.trim().toLowerCase()
  if (!q) return normalisedOptions.value
  return normalisedOptions.value.filter(
    (opt) =>
      opt.label.toLowerCase().includes(q) || opt.value.toLowerCase().includes(q)
  )
})

const renderedOptions = computed(() => filteredOptions.value.slice(0, MAX_RENDERED))
const hiddenCount = computed(
  () => Math.max(0, filteredOptions.value.length - MAX_RENDERED)
)

const selectedLabel = computed(() => {
  const match = normalisedOptions.value.find((o) => o.value === props.modelValue)
  return match ? match.label : props.modelValue
})

const displayText = computed(() => (open.value ? query.value : selectedLabel.value))

function isDisabled(opt) {
  return props.disabledOption ? !!props.disabledOption(opt.value) : false
}

function positionPanel() {
  const trigger = rootEl.value
  if (!trigger) return
  const rect = trigger.getBoundingClientRect()
  const panelMax = 280
  const openUp =
    window.innerHeight - rect.bottom < panelMax && rect.top > panelMax
  panelStyle.value = {
    position: 'fixed',
    left: `${rect.left}px`,
    width: `${rect.width}px`,
    ...(openUp
      ? { bottom: `${window.innerHeight - rect.top + 2}px` }
      : { top: `${rect.bottom + 2}px` }),
  }
}

function openPanel() {
  if (open.value) return
  open.value = true
  query.value = ''
  activeIndex.value = renderedOptions.value.findIndex(
    (o) => o.value === props.modelValue
  )
  nextTick(positionPanel)
  document.addEventListener('mousedown', onDocumentMousedown)
  window.addEventListener('scroll', positionPanel, true)
  window.addEventListener('resize', positionPanel)
}

function closePanel() {
  if (!open.value) return
  open.value = false
  query.value = ''
  activeIndex.value = -1
  document.removeEventListener('mousedown', onDocumentMousedown)
  window.removeEventListener('scroll', positionPanel, true)
  window.removeEventListener('resize', positionPanel)
}

function onDocumentMousedown(event) {
  if (rootEl.value?.contains(event.target)) return
  if (panelEl.value?.contains(event.target)) return
  closePanel()
}

function select(opt) {
  if (isDisabled(opt)) return
  closePanel()
  if (opt.value !== props.modelValue) {
    emit('update:modelValue', opt.value)
    emit('change', opt.value)
  }
  inputEl.value?.blur()
}

function clear() {
  closePanel()
  if (props.modelValue !== '') {
    emit('update:modelValue', '')
    emit('change', '')
  }
}

function onInput(event) {
  if (!open.value) openPanel()
  query.value = event.target.value
  activeIndex.value = renderedOptions.value.length ? 0 : -1
}

function moveActive(delta) {
  if (!open.value) {
    openPanel()
    return
  }
  const count = renderedOptions.value.length
  if (!count) return
  let next = activeIndex.value + delta
  for (let i = 0; i < count; i++) {
    const wrapped = (next + count) % count
    if (!isDisabled(renderedOptions.value[wrapped])) {
      activeIndex.value = wrapped
      scrollActiveIntoView()
      return
    }
    next = wrapped + delta
  }
}

function scrollActiveIntoView() {
  nextTick(() => {
    panelEl.value
      ?.querySelector('.searchable-select-option.active')
      ?.scrollIntoView({ block: 'nearest' })
  })
}

function onKeydown(event) {
  switch (event.key) {
    case 'ArrowDown':
      event.preventDefault()
      moveActive(1)
      break
    case 'ArrowUp':
      event.preventDefault()
      moveActive(-1)
      break
    case 'Enter':
      event.preventDefault()
      if (open.value && activeIndex.value >= 0) {
        select(renderedOptions.value[activeIndex.value])
      }
      break
    case 'Escape':
      closePanel()
      inputEl.value?.blur()
      break
    case 'Tab':
      closePanel()
      break
  }
}

watch(filteredOptions, () => {
  if (activeIndex.value >= renderedOptions.value.length) {
    activeIndex.value = renderedOptions.value.length ? 0 : -1
  }
})

onBeforeUnmount(closePanel)
</script>

<template>
  <div
    ref="rootEl"
    class="searchable-select"
    role="combobox"
    :aria-expanded="open"
  >
    <input
      v-if="name"
      type="hidden"
      :name="name"
      :value="modelValue"
    >
    <input
      :id="inputId"
      ref="inputEl"
      type="text"
      class="form-control searchable-select-input"
      autocomplete="off"
      :placeholder="placeholder"
      :value="displayText"
      @focus="openPanel"
      @click="openPanel"
      @input="onInput"
      @keydown="onKeydown"
    >
    <button
      v-if="modelValue && !open"
      type="button"
      class="searchable-select-clear"
      title="Clear selection"
      tabindex="-1"
      @click="clear"
    >
      &times;
    </button>
    <span
      class="searchable-select-caret"
      aria-hidden="true"
    >&#9662;</span>

    <Teleport to="body">
      <div
        v-if="open"
        ref="panelEl"
        class="searchable-select-panel"
        :style="panelStyle"
        role="listbox"
      >
        <div
          v-for="(opt, index) in renderedOptions"
          :key="opt.value"
          class="searchable-select-option"
          :class="{
            active: index === activeIndex,
            selected: opt.value === modelValue,
            disabled: isDisabled(opt),
          }"
          role="option"
          :aria-selected="opt.value === modelValue"
          @mousedown.prevent="select(opt)"
          @mousemove="activeIndex = index"
        >
          {{ opt.label }}
        </div>
        <div
          v-if="!renderedOptions.length"
          class="searchable-select-empty"
        >
          No matches
        </div>
        <div
          v-if="hiddenCount"
          class="searchable-select-more"
        >
          {{ hiddenCount }} more — keep typing to narrow down
        </div>
      </div>
    </Teleport>
  </div>
</template>

<style scoped>
.searchable-select {
  position: relative;
  display: inline-block;
  width: 100%;
}

.searchable-select-input {
  width: 100%;
  padding-right: 2.6rem;
  text-overflow: ellipsis;
}

.searchable-select-clear {
  position: absolute;
  right: 1.5rem;
  top: 50%;
  transform: translateY(-50%);
  border: none;
  background: none;
  color: #888;
  font-size: 1rem;
  line-height: 1;
  cursor: pointer;
  padding: 0 0.2rem;
}

.searchable-select-caret {
  position: absolute;
  right: 0.6rem;
  top: 50%;
  transform: translateY(-50%);
  color: #888;
  pointer-events: none;
  font-size: 0.8rem;
}
</style>

<style>
/* Panel styles are unscoped: the panel is Teleported to <body>, outside the
   component's scoped-attribute subtree. */
.searchable-select-panel {
  z-index: 2000;
  max-height: 280px;
  overflow-y: auto;
  background: #fff;
  border: 1px solid #ced4da;
  border-radius: 4px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.searchable-select-option {
  padding: 0.35rem 0.75rem;
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.searchable-select-option.active {
  background: rgba(102, 126, 234, 0.15);
}

.searchable-select-option.selected {
  font-weight: 600;
}

.searchable-select-option.disabled {
  color: #aaa;
  cursor: not-allowed;
}

.searchable-select-empty,
.searchable-select-more {
  padding: 0.35rem 0.75rem;
  color: #888;
  font-style: italic;
}
</style>
