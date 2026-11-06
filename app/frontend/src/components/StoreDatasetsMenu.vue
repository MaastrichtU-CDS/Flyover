<script setup>
import { computed, nextTick, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { reloadPage } from '@/lib/browser'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'
import { useStoreDatasetsStore } from '@/stores/storeDatasets'

// The status bar's store, as a button that opens a small panel under it:
// the datasets in the store Flyover works on, with their rows and progress,
// and the deletes (one dataset, every dataset, every annotation). Each
// delete asks first and offers the TriG file of what goes. Escape or a
// click elsewhere closes the panel; a step page then reloads, if anything
// went, so it shows the store as it now is.

const datasetsStore = useStoreDatasetsStore()
const projectStore = useProjectStore()
const status = useStatusStore()
const route = useRoute()
const { open, loading, busy, error, datasets, confirming, anyAnnotated } =
  storeToRefs(datasetsStore)
const { store, project } = storeToRefs(projectStore)

const root = ref(null)
const toggleButton = ref(null)
const panel = ref(null)

// While a delete runs, its button reads "Deleting" with the dots counting
// up, one to three, so it is plain that something is happening.
const dots = ref(1)
let dotsTimer = null

watch(busy, (isBusy) => {
  clearInterval(dotsTimer)
  dotsTimer = null
  dots.value = 1
  if (isBusy) dotsTimer = setInterval(() => (dots.value = (dots.value % 3) + 1), 400)
})

const repository = computed(() => store.value?.repository || 'the store')

const number = new Intl.NumberFormat('en-GB')

function rowsLabel(rows) {
  return `${number.format(rows)} ${rows === 1 ? 'row' : 'rows'}`
}

const asked = computed(() => {
  const what = confirming.value
  if (!what) return null
  if (what.kind === 'dataset') {
    const dataset = datasets.value.find((d) => d.name === what.name)
    return {
      question: `Delete ${what.name}?`,
      consequence:
        `Its data${dataset ? ` (${rowsLabel(dataset.rows)})` : ''}, its describe answers, ` +
        'its annotations and inferences, and its links to other datasets leave the store.' +
        (project.value ? " The open project's map stays as it is." : ''),
      download: 'Download it first (.trig)',
      confirm: `Delete ${what.name}`,
    }
  }
  if (what.kind === 'all') {
    const count = datasets.value.length
    return {
      question: 'Delete every dataset?',
      consequence:
        `All ${count} ${count === 1 ? 'dataset' : 'datasets'} in ${repository.value}, with their ` +
        'describe answers, annotations and links, leave the store. Anything else the ' +
        'repository holds stays.',
      download: 'Download them first (.trig)',
      confirm: 'Delete every dataset',
    }
  }
  return {
    question: 'Delete every annotation?',
    consequence:
      'The annotations of every dataset, and the inferences materialised from them, ' +
      'leave the store. The data and the describe answers stay.',
    download: 'Download them first (.trig)',
    confirm: 'Delete every annotation',
  }
})

function describe(gone) {
  if (gone.some((g) => g.kind === 'all')) return 'every dataset'
  const names = gone.filter((g) => g.kind === 'dataset').map((g) => g.name)
  const parts = []
  if (names.length) parts.push(names.join(', '))
  if (gone.some((g) => g.kind === 'annotations')) parts.push('every annotation')
  return parts.join(' and ')
}

function close({ returnFocus = false } = {}) {
  const gone = datasetsStore.close()
  if (returnFocus) toggleButton.value?.focus()
  if (!gone.length) return
  const message = `Deleted ${describe(gone)} from ${repository.value}.`
  // A step page shows what it read of the store: it reads it again.
  if (route.path !== '/') {
    projectStore.noticeAfterReload(message)
    reloadPage()
    return
  }
  status.success(message)
}

function toggle() {
  if (open.value) close()
  else datasetsStore.show()
}

// While a delete runs, the panel stays: closing it then would lose what
// the delete is about to report.
function onPointerDown(event) {
  if (busy.value) return
  if (root.value && !root.value.contains(event.target)) close()
}

function onKeydown(event) {
  if (event.key !== 'Escape' || busy.value) return
  if (confirming.value) {
    datasetsStore.cancel()
    return
  }
  close({ returnFocus: true })
}

watch(open, async (isOpen) => {
  if (typeof document === 'undefined') return
  if (isOpen) {
    document.addEventListener('pointerdown', onPointerDown)
    document.addEventListener('keydown', onKeydown)
    await nextTick()
    panel.value?.querySelector('button, a[href]')?.focus()
  } else {
    document.removeEventListener('pointerdown', onPointerDown)
    document.removeEventListener('keydown', onKeydown)
  }
})

onBeforeUnmount(() => {
  clearInterval(dotsTimer)
  document.removeEventListener('pointerdown', onPointerDown)
  document.removeEventListener('keydown', onKeydown)
})
</script>

<template>
  <span
    ref="root"
    class="store-menu"
  >
    <button
      id="statusbar-store-button"
      ref="toggleButton"
      type="button"
      class="store-menu-toggle"
      aria-controls="store-datasets-panel"
      :aria-expanded="open ? 'true' : 'false'"
      @click="toggle"
    >
      <slot />
      <i
        class="fas fa-caret-down store-menu-caret"
        aria-hidden="true"
      />
    </button>

    <div
      v-if="open"
      id="store-datasets-panel"
      ref="panel"
      class="store-panel"
      role="dialog"
      :aria-label="`Datasets in ${repository}`"
    >
      <div class="store-panel-header">
        <span class="store-panel-title">
          <i
            class="fas fa-database"
            aria-hidden="true"
          /> Datasets in {{ repository }}
        </span>
        <button
          type="button"
          class="store-panel-close"
          aria-label="Close"
          :disabled="busy"
          @click="close({ returnFocus: true })"
        >
          &times;
        </button>
      </div>

      <div class="store-panel-body">
        <div
          v-if="error"
          class="alert alert-danger store-panel-error"
          role="alert"
        >
          <i class="fas fa-exclamation-triangle" /> {{ error }}
          <button
            v-if="!confirming"
            type="button"
            class="btn btn-link btn-sm p-0 align-baseline"
            @click="datasetsStore.load()"
          >
            Try again
          </button>
        </div>

        <div
          v-if="asked"
          class="store-panel-confirm"
        >
          <p class="store-panel-question">
            <strong>{{ asked.question }}</strong>
          </p>
          <p class="store-panel-consequence">
            {{ asked.consequence }}
          </p>
          <span
            class="sr-only"
            aria-live="polite"
          >{{ busy ? `${asked.confirm.replace(/^Delete/, 'Deleting')}…` : '' }}</span>
          <div class="store-panel-actions">
            <a
              class="btn btn-sm btn-outline-secondary"
              :href="datasetsStore.exportUrl(confirming)"
              download
            >
              <i class="fas fa-download" /> {{ asked.download }}
            </a>
            <button
              type="button"
              class="btn btn-sm btn-danger store-panel-confirm-button"
              :disabled="busy"
              :aria-busy="busy ? 'true' : 'false'"
              @click="datasetsStore.confirm()"
            >
              <template v-if="busy">
                <i
                  class="fas fa-trash-can fa-shake"
                  aria-hidden="true"
                /> Deleting<span
                  class="store-panel-dots"
                  aria-hidden="true"
                >{{ '.'.repeat(dots) }}</span>
              </template>
              <template v-else>
                <i
                  class="fas fa-trash-can"
                  aria-hidden="true"
                /> {{ asked.confirm }}
              </template>
            </button>
            <button
              type="button"
              class="btn btn-sm btn-link"
              :disabled="busy"
              @click="datasetsStore.cancel()"
            >
              Cancel
            </button>
          </div>
        </div>

        <template v-else>
          <p
            v-if="loading"
            class="store-panel-note"
          >
            <i class="fas fa-spinner fa-spin" /> Looking at the store…
          </p>
          <p
            v-else-if="!datasets.length && !error"
            class="store-panel-note"
          >
            No datasets in this store yet.
          </p>
          <ul
            v-else
            class="store-panel-list"
          >
            <li
              v-for="dataset in datasets"
              :key="dataset.name"
              class="store-panel-item"
            >
              <div class="store-panel-item-text">
                <span class="store-panel-name">{{ dataset.name }}</span>{{ ' ' }}
                <span class="store-panel-meta">
                  {{ rowsLabel(dataset.rows) }} ·
                  <template v-if="dataset.described">
                    <i class="fas fa-check" /> Described
                  </template>
                  <template v-else>Not described yet</template> ·
                  <template v-if="dataset.annotated">
                    <i class="fas fa-check" /> Annotated
                  </template>
                  <template v-else>Not annotated yet</template>
                </span>
              </div>
              <button
                type="button"
                class="btn btn-sm btn-outline-danger store-panel-delete"
                :aria-label="`Delete ${dataset.name}`"
                @click="datasetsStore.ask('dataset', dataset.name)"
              >
                <i
                  class="fas fa-trash-can"
                  aria-hidden="true"
                />
              </button>
            </li>
          </ul>
        </template>
      </div>

      <div
        v-if="!confirming"
        class="store-panel-footer"
      >
        <RouterLink
          to="/ingest"
          class="btn btn-sm btn-link store-panel-add"
          @click="close()"
        >
          <i class="fas fa-plus" /> Add a dataset
        </RouterLink>
        <!-- Each shows only when there is something for it to delete. -->
        <button
          v-if="!loading && anyAnnotated"
          type="button"
          class="btn btn-sm btn-outline-secondary"
          @click="datasetsStore.ask('annotations')"
        >
          Delete all annotations
        </button>
        <button
          v-if="!loading && datasets.length"
          type="button"
          class="btn btn-sm btn-outline-danger"
          @click="datasetsStore.ask('all')"
        >
          Delete every dataset
        </button>
      </div>
    </div>
  </span>
</template>

<style scoped>
.store-menu {
  position: relative;
  display: inline-flex;
  align-items: center;
}

.store-menu-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  padding: 0;
  border: 0;
  background: none;
  color: inherit;
  font: inherit;
  cursor: pointer;
}

.store-menu-toggle:hover {
  text-decoration: underline;
  text-underline-offset: 2px;
}

.store-menu-toggle:focus-visible {
  outline: 2px solid rgba(255, 255, 255, 0.85);
  outline-offset: 2px;
  border-radius: 2px;
}

.store-menu-caret {
  font-size: 0.75em;
  opacity: 0.85;
}

/* A small window under the strip, in the pop-ups' style. */
.store-panel {
  position: absolute;
  top: calc(100% + 0.45rem);
  left: 0;
  z-index: 1085;
  display: flex;
  flex-direction: column;
  width: min(28rem, calc(100vw - 2rem));
  max-height: min(70vh, 32rem);
  overflow: hidden;
  background: #fff;
  color: #212529;
  border-radius: 8px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
  font-size: 0.85rem;
  line-height: 1.4;
  text-align: left;
}

.store-panel-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: #fff;
}

.store-panel-title {
  flex: 1;
  font-weight: 600;
}

.store-panel-close {
  border: none;
  background: none;
  color: #fff;
  font-size: 1.25rem;
  line-height: 1;
  cursor: pointer;
}

/* The list scrolls when the store holds many datasets. */
.store-panel-body {
  flex: 1;
  overflow-y: auto;
  padding: 0.5rem 0.75rem;
}

.store-panel-error {
  padding: 0.4rem 0.6rem;
  margin-bottom: 0.5rem;
  font-size: 0.85em;
}

.store-panel-note {
  margin: 0.25rem 0;
  color: #6c757d;
}

.store-panel-list {
  margin: 0;
  padding: 0;
  list-style: none;
}

.store-panel-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.4rem 0;
  border-bottom: 1px solid #e9ecef;
}

.store-panel-item:last-child {
  border-bottom: none;
}

.store-panel-item-text {
  display: flex;
  flex: 1;
  flex-direction: column;
  min-width: 0;
}

.store-panel-name {
  font-weight: 600;
  overflow-wrap: anywhere;
}

.store-panel-meta {
  color: #6c757d;
  font-size: 0.9em;
}

.store-panel-meta .fa-check {
  color: #28a745;
}

.store-panel-delete {
  flex-shrink: 0;
}

/* A delete under way stays in full colour, so it reads as working rather
   than as unavailable; it still takes no clicks. */
.store-panel-confirm-button[aria-busy='true'] {
  opacity: 1;
  cursor: progress;
}

.store-panel-dots {
  display: inline-block;
  width: 1.1em;
  text-align: left;
}

.store-panel-question {
  margin-bottom: 0.35rem;
}

.store-panel-consequence {
  margin-bottom: 0.6rem;
  color: #495057;
}

.store-panel-actions,
.store-panel-footer {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.4rem;
}

.store-panel-footer {
  padding: 0.5rem 0.75rem;
  border-top: 1px solid #dee2e6;
}

.store-panel-add {
  margin-right: auto;
  padding-left: 0;
}
</style>
