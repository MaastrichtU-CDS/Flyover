<script setup>
import { computed, nextTick, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { reloadPage } from '@/lib/browser'
import { projectName, projectVersion } from '@/lib/project'
import { errorMessage, useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'

// The pop-up that asks which project to work on. Flyover shows it when it
// opens without a project (unless this tab chose to work without one), and
// "Switch project" in the status bar opens it again. At start-up it can only
// be left by choosing; opened from the status bar it can be cancelled.

const projectStore = useProjectStore()
const status = useStatusStore()
const route = useRoute()
const { picker, project, configuredStore, store, name: openName } = storeToRefs(projectStore)

const dialog = ref(null)
const fileInput = ref(null)
const url = ref('')
// choose | confirm-open | confirm-leave
const step = ref('choose')
const busy = ref(false)
const dragging = ref(false)
const error = ref('')
const validationErrors = ref([])
// The map waiting to be opened: { map, fileName, sourceUrl, check, kind }.
// Kept shallow: the map goes to IndexedDB and the server as it was read.
const pending = shallowRef(null)

const isOpen = computed(() => picker.value.open)
const canCancel = computed(() => picker.value.mode === 'switch')
// The store working without a project uses.
const standardStore = computed(() => configuredStore.value || (project.value ? null : store.value))

const pendingName = computed(() =>
  pending.value ? projectName({ ...pending.value.check.project }) : ''
)
const pendingVersion = computed(() =>
  pending.value ? projectVersion(pending.value.check.project) : ''
)

function clearErrors() {
  error.value = ''
  validationErrors.value = []
}

function reset() {
  step.value = 'choose'
  busy.value = false
  dragging.value = false
  url.value = ''
  pending.value = null
  clearErrors()
}

function focusables() {
  return Array.from(
    dialog.value?.querySelectorAll(
      'button:not([disabled]), input:not([disabled]):not([type="file"]), a[href], [tabindex]:not([tabindex="-1"])'
    ) || []
  )
}

async function focusFirst() {
  await nextTick()
  focusables()[0]?.focus()
}

function onKeydown(event) {
  if (event.key === 'Escape') {
    event.preventDefault()
    if (canCancel.value && !busy.value) cancel()
    return
  }
  if (event.key !== 'Tab') return
  // Keep the focus inside the pop-up while it is open.
  const items = focusables()
  if (!items.length) return
  const first = items[0]
  const last = items[items.length - 1]
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault()
    last.focus()
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault()
    first.focus()
  }
}

watch(
  isOpen,
  (open) => {
    if (typeof document === 'undefined') return
    if (open) {
      reset()
      document.addEventListener('keydown', onKeydown)
      focusFirst()
    } else {
      document.removeEventListener('keydown', onKeydown)
    }
  },
  { immediate: true }
)

onBeforeUnmount(() => document.removeEventListener('keydown', onKeydown))

watch(step, () => focusFirst())

function cancel() {
  if (canCancel.value) projectStore.closePicker()
}

// After a change: a step page reloads so it reads the project and the store
// afresh; the message waits for the reload. Home has nothing to reload.
function finish(message) {
  projectStore.closePicker()
  if (route.path !== '/') {
    projectStore.noticeAfterReload(message)
    reloadPage()
    return
  }
  status.success(message)
}

function openedMessage() {
  const label = [projectStore.name, projectStore.version].filter(Boolean).join(' · ')
  const repository = store.value?.repository
  return `Flyover now works on the project ${label}${repository ? `, on the RDF store's repository ${repository}` : ''}.`
}

function withoutProjectMessage() {
  const repository = standardStore.value?.repository
  return `Flyover now works without a project: the standard RDF flow${repository ? ` on the configured RDF store's repository ${repository}` : ''}.`
}

async function needsConfirmation(kind) {
  if (kind === 'same-project') return projectStore.isEdited()
  if (kind === 'no-project') return projectStore.hasHeldMap()
  return true
}

async function consider(map, origin) {
  busy.value = true
  try {
    const check = await projectStore.checkMap(map, origin.fileName)
    if (!check?.valid) {
      error.value = check?.error || 'This map cannot be opened as a project.'
      validationErrors.value = check?.validation_errors || []
      return
    }
    const kind = projectStore.classify(check)
    pending.value = { map, ...origin, check, kind }
    if (await needsConfirmation(kind)) {
      step.value = 'confirm-open'
      return
    }
    await openPending()
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    busy.value = false
  }
}

async function chooseFile(file) {
  if (!file) return
  clearErrors()
  let map
  try {
    map = JSON.parse(await file.text())
  } catch (e) {
    error.value = `${file.name} is not valid JSON: ${e.message || e}`
    return
  }
  if (!map || typeof map !== 'object' || Array.isArray(map)) {
    error.value = `${file.name} is not a JSON-LD map.`
    return
  }
  await consider(map, { fileName: file.name, sourceUrl: '' })
}

function onFilePicked(event) {
  const file = event.target.files?.[0]
  event.target.value = ''
  chooseFile(file)
}

function onDrop(event) {
  dragging.value = false
  chooseFile(event.dataTransfer?.files?.[0])
}

async function loadFromUrl() {
  clearErrors()
  const address = url.value.trim()
  if (!address) return
  busy.value = true
  let fetched
  try {
    fetched = await projectStore.fetchMap(address)
  } catch (e) {
    error.value = e.message
    busy.value = false
    return
  }
  busy.value = false
  await consider(fetched.mapping, { fileName: fetched.fileName, sourceUrl: fetched.sourceUrl })
}

async function openPending() {
  busy.value = true
  try {
    const { map, fileName, sourceUrl } = pending.value
    await projectStore.openProject(map, { fileName, sourceUrl })
    finish(openedMessage())
  } catch (e) {
    step.value = 'choose'
    error.value = errorMessage(e)
    validationErrors.value = e?.response?.data?.validation_errors || []
  } finally {
    busy.value = false
  }
}

async function chooseWithoutProject() {
  clearErrors()
  if (project.value) {
    step.value = 'confirm-leave'
    return
  }
  busy.value = true
  try {
    const { storeChanged } = await projectStore.continueWithoutProject()
    if (storeChanged) finish(withoutProjectMessage())
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    busy.value = false
  }
}

async function leave() {
  busy.value = true
  try {
    await projectStore.leaveProject()
    finish(withoutProjectMessage())
  } catch (e) {
    step.value = 'choose'
    error.value = errorMessage(e)
  } finally {
    busy.value = false
  }
}

async function download() {
  if (!(await projectStore.downloadWorkingCopy())) {
    status.warning('There is no map in this browser to download.')
  }
}

function back() {
  pending.value = null
  step.value = 'choose'
}
</script>

<template>
  <div
    v-if="isOpen"
    class="project-picker"
    role="dialog"
    aria-modal="true"
    aria-labelledby="project-picker-title"
  >
    <div
      class="project-picker-backdrop"
      @click="cancel"
    />
    <div
      ref="dialog"
      class="project-picker-dialog"
    >
      <div class="project-picker-header">
        <h5
          id="project-picker-title"
          class="project-picker-title"
        >
          <i
            class="fas fa-folder-open"
            aria-hidden="true"
          /> Choose a project
        </h5>
        <button
          v-if="canCancel"
          type="button"
          class="project-picker-close"
          aria-label="Close"
          :disabled="busy"
          @click="cancel"
        >
          &times;
        </button>
      </div>

      <div class="project-picker-body">
        <template v-if="step === 'choose'">
          <p class="project-picker-intro">
            A project is defined by its JSON-LD semantic map: the general schema every site
            of the project shares, and your site's own part. Flyover then works on the RDF
            store the map names.
          </p>
          <p
            v-if="project"
            class="project-picker-current"
          >
            Open now: <strong>{{ openName }}</strong>
          </p>

          <div class="card mb-3">
            <div class="card-header bg-light">
              <h6 class="mb-0">
                <i class="fas fa-project-diagram" /> Open a project
              </h6>
            </div>
            <div class="card-body">
              <div
                class="card p-3 border text-center project-drop"
                :class="{ 'drag-over': dragging }"
                @dragover.prevent
                @dragenter.prevent="dragging = true"
                @dragleave.prevent="dragging = false"
                @drop.prevent="onDrop"
              >
                <div class="mb-2">
                  <i class="fas fa-file-code fa-2x text-primary" />
                </div>
                <strong>Drop the project's JSON-LD map here</strong>
                <small class="d-block text-muted mb-2">or choose the file on your computer</small>
                <div>
                  <button
                    type="button"
                    class="btn btn-outline-secondary btn-sm"
                    :disabled="busy"
                    @click="fileInput?.click()"
                  >
                    <i class="fas fa-folder-open" /> Browse
                  </button>
                </div>
                <input
                  ref="fileInput"
                  type="file"
                  class="d-none"
                  accept=".jsonld,.json,application/ld+json,application/json"
                  data-testid="project-file-input"
                  @change="onFilePicked"
                >
              </div>

              <form
                class="mt-3"
                @submit.prevent="loadFromUrl"
              >
                <label
                  for="project-map-url"
                  class="small text-muted mb-1"
                >Or load the map from a web address:</label>
                <div class="input-group input-group-sm">
                  <input
                    id="project-map-url"
                    v-model="url"
                    type="url"
                    class="form-control"
                    placeholder="https://…/project-map.jsonld"
                    :disabled="busy"
                  >
                  <div class="input-group-append">
                    <button
                      type="submit"
                      class="btn btn-outline-primary"
                      :disabled="busy || !url.trim()"
                    >
                      <i class="fas fa-cloud-download-alt" /> Load
                    </button>
                  </div>
                </div>
              </form>

              <div
                v-if="busy"
                class="text-muted small mt-2"
              >
                <i class="fas fa-spinner fa-spin" /> Checking the map…
              </div>

              <div
                v-if="error"
                class="alert alert-danger mt-3 mb-0 project-picker-errors"
                role="alert"
              >
                <strong><i class="fas fa-exclamation-triangle" /> {{ error }}</strong>
                <ul v-if="validationErrors.length">
                  <li
                    v-for="(item, index) in validationErrors"
                    :key="index"
                  >
                    <code>{{ item.path }}</code> — {{ item.message }}
                    <div
                      v-if="item.suggestion"
                      class="suggestion"
                    >
                      <i class="fas fa-lightbulb" /> {{ item.suggestion }}
                    </div>
                  </li>
                </ul>
              </div>
            </div>
          </div>

          <div class="card">
            <div class="card-header bg-light">
              <h6 class="mb-0">
                <i class="fas fa-database" /> Continue without a project
              </h6>
            </div>
            <div class="card-body">
              <div class="mb-2 project-picker-standard">
                <p class="mb-1">
                  Work in the <strong>standard RDF flow</strong>, on Flyover's configured RDF
                  store, as Flyover always has. You can still upload a map in the steps, and
                  choose a project later from the bar at the top.
                </p>
                <p
                  v-if="standardStore"
                  class="small text-muted mb-0 project-picker-store"
                >
                  <i class="fas fa-database" /> Repository
                  <strong>{{ standardStore.repository }}</strong> ·
                  <span class="project-picker-url">{{ standardStore.url }}</span>
                </p>
              </div>
              <button
                type="button"
                class="btn btn-secondary"
                :disabled="busy"
                @click="chooseWithoutProject"
              >
                <i class="fas fa-forward" /> Continue without a project
              </button>
            </div>
          </div>
        </template>

        <template v-else-if="step === 'confirm-open' && pending">
          <h6 class="project-picker-question">
            <i class="fas fa-exchange-alt" /> Open {{ pendingName }}<template v-if="pendingVersion">
              · {{ pendingVersion }}
            </template>?
          </h6>
          <p>
            Flyover will work on the RDF store's repository
            <strong>{{ pending.check.store.repository }}</strong> at
            <code>{{ pending.check.store.url }}</code>.
          </p>
          <p
            v-if="pending.check.store.note"
            class="small text-muted"
          >
            {{ pending.check.store.note }}
          </p>
          <div class="alert alert-warning">
            <template v-if="pending.kind === 'same-project'">
              This map belongs to the open project. It replaces the project's working copy,
              with the describe answers added since it was loaded.
            </template>
            <template v-else-if="pending.kind === 'other-store'">
              This map belongs to the open project, but it names another RDF store. Flyover
              switches to that store; the data in the current one stays where it is. The map
              replaces the project's working copy.
            </template>
            <template v-else-if="pending.kind === 'other-project'">
              It replaces the open project, <strong>{{ openName }}</strong>, and its working
              copy in this browser.
            </template>
            <template v-else>
              It replaces the map this browser holds, which a step uploaded.
            </template>
            Download that map first to keep it.
          </div>
          <div class="project-picker-actions">
            <button
              type="button"
              class="btn btn-outline-secondary"
              :disabled="busy"
              @click="download"
            >
              <i class="fas fa-download" /> Download the current map
            </button>
            <button
              type="button"
              class="btn btn-primary"
              :disabled="busy"
              @click="openPending"
            >
              <i class="fas fa-folder-open" /> Open the project
            </button>
            <button
              type="button"
              class="btn btn-link"
              :disabled="busy"
              @click="back"
            >
              Back
            </button>
          </div>
        </template>

        <template v-else-if="step === 'confirm-leave'">
          <h6 class="project-picker-question">
            <i class="fas fa-sign-out-alt" /> Leave {{ openName }}?
          </h6>
          <p>
            Flyover goes back to the standard RDF flow, on its configured RDF
            store<template v-if="standardStore">
              — repository <strong>{{ standardStore.repository }}</strong>
            </template>.
          </p>
          <div class="alert alert-warning">
            The project's map leaves this browser. Download it first to keep the describe
            answers it holds.
          </div>
          <div class="project-picker-actions">
            <button
              type="button"
              class="btn btn-outline-secondary"
              :disabled="busy"
              @click="download"
            >
              <i class="fas fa-download" /> Download the current map
            </button>
            <button
              type="button"
              class="btn btn-primary"
              :disabled="busy"
              @click="leave"
            >
              <i class="fas fa-sign-out-alt" /> Leave the project
            </button>
            <button
              type="button"
              class="btn btn-link"
              :disabled="busy"
              @click="back"
            >
              Back
            </button>
          </div>
        </template>
      </div>

      <div
        v-if="canCancel && step === 'choose'"
        class="project-picker-footer"
      >
        <button
          type="button"
          class="btn btn-sm btn-link"
          :disabled="busy"
          @click="cancel"
        >
          Cancel
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.project-picker {
  position: fixed;
  inset: 0;
  z-index: 1090;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.project-picker-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
}

.project-picker-dialog {
  position: relative;
  display: flex;
  flex-direction: column;
  width: min(44rem, 100%);
  max-height: calc(100vh - 2rem);
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
  font-size: 0.9rem;
  overflow: hidden;
}

/* The app's own gradient, as on the step bar. */
.project-picker-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem 1rem;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: #fff;
}

.project-picker-title {
  flex: 1;
  margin: 0;
  font-size: 1.1rem;
}

.project-picker-close {
  border: none;
  background: none;
  color: #fff;
  font-size: 1.4rem;
  line-height: 1;
  cursor: pointer;
}

.project-picker-body {
  overflow: auto;
  padding: 1rem;
}

.project-picker-intro {
  font-size: 0.85em;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: rgba(118, 75, 162, 0.06);
  border-radius: 4px;
  padding: 0.6rem 0.8rem;
}

.project-picker-current {
  margin-bottom: 0.75rem;
}

.project-drop {
  cursor: default;
}

.project-drop.drag-over {
  border-color: var(--bs-success, #198754);
  box-shadow: 0 0 0 2px var(--bs-success, #198754);
  background-color: var(--bs-success-bg-subtle, #d1e7dd);
}

.project-picker-errors ul {
  margin: 0.5rem 0 0;
  padding-left: 1.25rem;
}

.project-picker-errors code {
  background: rgba(0, 0, 0, 0.05);
  padding: 1px 4px;
  border-radius: 3px;
}

.project-picker-errors .suggestion {
  font-size: 0.85em;
  opacity: 0.85;
}

.project-picker-url {
  overflow-wrap: anywhere;
}

.project-picker-question {
  font-size: 1rem;
  margin-bottom: 0.75rem;
}

.project-picker-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  align-items: center;
}

.project-picker-footer {
  display: flex;
  justify-content: flex-end;
  padding: 0.4rem 1rem;
  border-top: 1px solid #dee2e6;
}
</style>
