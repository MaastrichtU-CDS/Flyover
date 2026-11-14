<script setup>
import { computed, ref, shallowRef, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useModalFocus } from '@/composables/useModalFocus'
import { reloadPage } from '@/lib/browser'
import { useWorkflowEntry } from '@/composables/useWorkflowEntry'
import {
  ASSUMED_RDF_NOTE,
  DEFAULT_TARGET_STANDARD,
  projectName,
  projectVersion,
  targetStandard,
} from '@/lib/project'
import { workflowById } from '@/lib/workflows'
import { errorMessage, useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'

// The pop-up that asks which project to work on. It opens when a tab enters
// a workflow, with the project this browser remembers as its first choice
// when that project is the workflow's: on the home page, which goes on to
// the workflow once the tab has chosen, or over a page of the workflow a
// link led to. "Switch project" in the status bar opens it again inside the
// workflow. Every choice is for the pop-up's workflow. Cancelled while
// entering, it leaves the user on, or takes them back to, the home page.

const projectStore = useProjectStore()
const status = useStatusStore()
const route = useRoute()
const router = useRouter()
const {
  picker,
  project,
  configuredStore,
  store,
  name: openName,
  version: openVersion,
} = storeToRefs(projectStore)

const dialog = ref(null)
const fileInput = ref(null)
const url = ref('')
// 'choose' (the choices), 'open' (a project's map, from a file or a web
// address), 'confirm-open', 'confirm-leave' or 'other-standard' (the map is
// for another workflow).
const step = ref('choose')
const busy = ref(false)
const dragging = ref(false)
const error = ref('')
const validationErrors = ref([])
// The map waiting to be opened: { map, fileName, sourceUrl, check, kind }.
// Kept shallow: the map goes to IndexedDB and the server as it was read.
const pending = shallowRef(null)
// The maps an address offers to choose from (a Zenodo record that holds
// several): { message, items: [{ name, url, size }] }.
const choices = shallowRef(null)

const isOpen = computed(() => picker.value.open)
const entering = computed(() => picker.value.mode === 'enter')
// The project this browser remembers, offered first when entering the
// workflow its map targets.
const rememberedLabel = computed(() =>
  [openName.value, openVersion.value].filter(Boolean).join(' · ')
)
// The store working without a project uses.
const standardStore = computed(() => configuredStore.value || (project.value ? null : store.value))

const pendingName = computed(() =>
  pending.value ? projectName({ ...pending.value.check.project }) : ''
)
const pendingVersion = computed(() =>
  pending.value ? projectVersion(pending.value.check.project) : ''
)

// The workflow the pop-up is for (see openPicker): its maps target its
// standard; a map for another is that workflow's.
const { enter } = useWorkflowEntry()
const currentWorkflow = computed(
  () => workflowById(picker.value.workflow) || workflowById(DEFAULT_TARGET_STANDARD)
)
const rememberedFits = computed(
  () => Boolean(project.value) && targetStandard(project.value) === currentWorkflow.value.id
)
// What the workflow and its store are called, from the workflow list.
const workflowLabel = computed(() => currentWorkflow.value.label)
const storeLabel = computed(() => currentWorkflow.value.storeLabel || 'store')
const pendingWorkflow = computed(() => workflowById(pending.value?.standard) || {})

function clearErrors() {
  error.value = ''
  validationErrors.value = []
}

function showOpen() {
  clearErrors()
  step.value = 'open'
}

function showChoices() {
  clearErrors()
  choices.value = null
  step.value = 'choose'
}

function reset() {
  step.value = 'choose'
  busy.value = false
  dragging.value = false
  url.value = ''
  pending.value = null
  choices.value = null
  clearErrors()
}

const { focusFirst } = useModalFocus(dialog, isOpen, {
  onEscape: cancel,
  onOpen: reset,
})

watch(step, () => focusFirst())
// Another address makes the last one's choices stale.
watch(url, () => {
  choices.value = null
})

// Cancelled while entering a workflow, the pop-up leaves it for the home
// page; opened to switch, it leaves everything as it was. A choice under
// way finishes first.
function cancel() {
  if (busy.value) return
  if (entering.value && route.meta?.workflow) {
    projectStore.exitWorkflow()
    router.push('/')
    return
  }
  projectStore.closePicker()
}

// After a choice; `message`, if any, says what changed. Entering, nothing
// reloads: the pop-up goes on to the workflow's page it was opened for, and
// a page waits until the tab has entered (see useStoreGate). Switching, a
// step page reloads so it reads the project and the store afresh; the
// message waits for the reload. The workflow's home page reads neither, so
// it does not reload.
function finish(message) {
  const { mode, target } = picker.value
  projectStore.closePicker()
  if (mode === 'enter') {
    if (message) status.success(message)
    if (target && route.path !== target) router.push(target)
    return
  }
  if (route.path !== workflowById(route.meta?.workflow)?.to) {
    if (message) projectStore.noticeAfterReload(message)
    reloadPage()
    return
  }
  if (message) status.success(message)
}

function openedMessage() {
  const label = [projectStore.name, projectStore.version].filter(Boolean).join(' · ')
  const repository = store.value?.repository
  const opened = `Flyover now works on the project ${label}${repository ? `, on the ${storeLabel.value}'s repository ${repository}` : ''}.`
  // A map that names no standard is taken as RDF, and the user hears so.
  return project.value?.targetStandardDeclared === false ? `${opened} ${ASSUMED_RDF_NOTE}` : opened
}

function withoutProjectMessage() {
  const repository = standardStore.value?.repository
  const where = repository ? ` on the configured ${storeLabel.value}'s repository ${repository}` : ''
  return `Flyover now works without a project: the standard ${workflowLabel.value} flow${where}.`
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
    const standard = targetStandard(check.project)
    if (standard !== currentWorkflow.value.id) {
      pending.value = { map, ...origin, check, kind: 'other-standard', standard }
      step.value = 'other-standard'
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
  choices.value = null
  const address = url.value.trim()
  if (!address) return
  await loadAddress(address)
}

// Fetch the map at `address` and consider it. An address that offers
// several maps lists them to choose from instead.
async function loadAddress(address) {
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
  if (fetched?.choices?.length) {
    choices.value = { message: fetched.message || '', items: fetched.choices }
    return
  }
  await consider(fetched.mapping, { fileName: fetched.fileName, sourceUrl: fetched.sourceUrl })
}

async function loadChoice(item) {
  clearErrors()
  await loadAddress(item.url)
}

// A file's size as the choices show it: "860 B", "4.2 kB", "48 kB".
function formatSize(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return ''
  if (bytes < 1000) return `${bytes} B`
  const units = ['kB', 'MB', 'GB']
  let value = bytes / 1000
  let unit = 0
  while (value >= 999.5 && unit < units.length - 1) {
    value /= 1000
    unit += 1
  }
  return `${value < 9.95 ? value.toFixed(1) : Math.round(value)} ${units[unit]}`
}

async function continueWithRemembered() {
  clearErrors()
  busy.value = true
  try {
    await projectStore.continueWithProject({ workflow: currentWorkflow.value.id })
    finish(openedMessage())
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    busy.value = false
  }
}

async function openPending() {
  busy.value = true
  try {
    const { map, fileName, sourceUrl } = pending.value
    await projectStore.openProject(map, {
      fileName,
      sourceUrl,
      workflow: currentWorkflow.value.id,
    })
    finish(openedMessage())
  } catch (e) {
    step.value = 'open'
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
  const wasEntering = entering.value
  try {
    const { storeChanged } = await projectStore.continueWithoutProject({
      workflow: currentWorkflow.value.id,
    })
    // Entering, the tab goes on to the workflow either way; switching, only
    // another store calls for a fresh read.
    if (storeChanged || wasEntering) finish(storeChanged ? withoutProjectMessage() : '')
  } catch (e) {
    error.value = errorMessage(e)
  } finally {
    busy.value = false
  }
}

async function leave() {
  busy.value = true
  try {
    await projectStore.leaveProject({ workflow: currentWorkflow.value.id })
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

// The map is for another workflow: go there as from the home page (for now,
// its note that it is under development, over this pop-up).
function switchWorkflow() {
  const other = workflowById(pending.value?.standard)
  if (other) enter(other)
}

// From a question, back to where it was asked: the map to open, or the
// choices.
function back() {
  step.value = pending.value ? 'open' : 'choose'
  pending.value = null
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
          <div class="project-picker-intro">
            <p>All sites in a project share one JSON-LD, with a common schema and a part for each site.</p>
            <p>Choose one of the options below; you only need to create a new JSON-LD once per project.</p>
          </div>
          <p
            v-if="project && !entering"
            class="project-picker-current"
          >
            Open now: <strong>{{ openName }}</strong>
          </p>

          <!-- The project this browser remembers, first: a row in the
               choices' style, the whole row the button. -->
          <button
            v-if="entering && rememberedFits"
            id="project-picker-continue"
            type="button"
            class="project-picker-option project-picker-remembered"
            :disabled="busy"
            @click="continueWithRemembered"
          >
            <span class="project-picker-option-icon"><i
              class="fas fa-play"
              aria-hidden="true"
            /></span>
            <span class="project-picker-remembered-text">
              <strong class="project-picker-option-title">Continue with {{ rememberedLabel }}</strong>
              <span class="project-picker-option-text">The project this browser remembers</span>
            </span>
          </button>

          <!-- The choices first; opening a project then asks for its map. -->
          <div class="project-picker-options">
            <button
              id="project-picker-open"
              type="button"
              class="project-picker-option"
              :disabled="busy"
              @click="showOpen"
            >
              <span class="project-picker-option-icon"><i
                class="fas fa-folder-open"
                aria-hidden="true"
              /></span>
              <strong class="project-picker-option-title">Open a project</strong>
              <span class="project-picker-option-text">From its JSON-LD file, or a web address</span>
            </button>
            <button
              id="project-picker-create"
              type="button"
              class="project-picker-option"
              disabled
            >
              <span class="project-picker-option-icon"><i
                class="fas fa-pen-ruler"
                aria-hidden="true"
              /></span>
              <strong class="project-picker-option-title">Create a JSON-LD</strong>
              <span class="badge project-picker-soon">Under development</span>
              <span class="project-picker-option-text">Describe your data model once, and share it with your
                partners</span>
            </button>
            <button
              id="project-picker-without"
              type="button"
              class="project-picker-option project-picker-standard"
              :title="standardStore?.url || ''"
              :disabled="busy"
              @click="chooseWithoutProject"
            >
              <span class="project-picker-option-icon"><i
                class="fas fa-forward"
                aria-hidden="true"
              /></span>
              <strong class="project-picker-option-title">Continue without a project</strong>
              <span class="project-picker-option-text">The standard {{ workflowLabel }} flow, on Flyover's
                configured {{ storeLabel }}<template v-if="standardStore">{{ ' (repository ' }}<strong>{{ standardStore.repository }}</strong>)</template></span>
            </button>
          </div>
        </template>

        <template v-else-if="step === 'open'">
          <!-- One root, so the step leaves the page in one piece. -->
          <div class="project-picker-open">
            <button
              type="button"
              class="btn btn-link btn-sm p-0 mb-2 project-picker-back"
              :disabled="busy"
              @click="showChoices"
            >
              <i
                class="fas fa-arrow-left"
                aria-hidden="true"
              /> Back to the choices
            </button>
            <h6 class="project-picker-step-title">
              Open a project
            </h6>
            <p class="small text-muted mb-2">
              Flyover then works on the {{ storeLabel }} that the project's map names.
            </p>
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
                  aria-describedby="project-map-url-help"
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
              <small
                id="project-map-url-help"
                class="form-text text-muted"
              >
                The file's own address, its page on Codeberg, GitHub, GitLab or a similar
                site, or its Zenodo record or DOI.
              </small>
            </form>

            <div
              v-if="choices"
              class="mt-3 project-picker-choices"
            >
              <p
                id="project-map-choices-label"
                class="small mb-1"
              >
                {{ choices.message }} Choose the one to open:
              </p>
              <div
                class="list-group"
                role="group"
                aria-labelledby="project-map-choices-label"
              >
                <button
                  v-for="item in choices.items"
                  :key="item.url"
                  type="button"
                  class="list-group-item list-group-item-action project-picker-choice"
                  :disabled="busy"
                  @click="loadChoice(item)"
                >
                  <span class="project-picker-choice-name">
                    <i class="fas fa-file-code" /> {{ item.name }}
                  </span>
                  <!-- The space keeps name and size apart for screen readers. -->
                  <span
                    v-if="formatSize(item.size)"
                    class="small text-muted"
                  >{{ ' ' + formatSize(item.size) }}</span>
                </button>
              </div>
            </div>

            <div
              v-if="busy"
              class="text-muted small mt-2"
            >
              <i class="fas fa-spinner fa-spin" /> Checking the map…
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
            Flyover will work on the {{ storeLabel }}'s repository
            <strong>{{ pending.check.store.repository }}</strong> at
            <code>{{ pending.check.store.url }}</code>.
          </p>
          <div class="alert alert-warning">
            <template v-if="pending.kind === 'same-project'">
              This map belongs to the open project. It replaces the project's working copy,
              with the describe answers added since it was loaded.
            </template>
            <template v-else-if="pending.kind === 'other-store'">
              This map belongs to the open project, but it names another {{ storeLabel }}. Flyover
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

        <template v-else-if="step === 'other-standard' && pending">
          <h6 class="project-picker-question">
            <i
              class="fas fa-right-left"
              aria-hidden="true"
            /> This JSON-LD is for the {{ pendingWorkflow.label }} workflow
          </h6>
          <p>
            <strong>{{ pendingName }}</strong> targets {{ pendingWorkflow.standardName }}, so the
            {{ currentWorkflow.label }} workflow cannot open it. Switch to the
            {{ pendingWorkflow.label }} workflow, or choose a different JSON-LD.
          </p>
          <div class="project-picker-actions">
            <button
              type="button"
              class="btn btn-primary"
              :disabled="busy"
              @click="switchWorkflow"
            >
              <i class="fas fa-right-left" /> Switch to {{ pendingWorkflow.label }}
            </button>
            <button
              type="button"
              class="btn btn-link"
              :disabled="busy"
              @click="back"
            >
              Choose a different JSON-LD
            </button>
          </div>
        </template>

        <template v-else-if="step === 'confirm-leave'">
          <h6 class="project-picker-question">
            <i class="fas fa-sign-out-alt" /> Leave {{ openName }}?
          </h6>
          <p>
            Flyover goes back to the standard {{ workflowLabel }} flow, on its configured
            {{ storeLabel }}<template v-if="standardStore">
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

        <div
          v-if="error && (step === 'choose' || step === 'open')"
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

      <div
        v-if="step === 'choose' || step === 'open'"
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

/* What a project is, and what to do: in the dialog's own size and colour,
   not as small print. */
.project-picker-intro {
  margin-bottom: 1rem;
  color: #343a40;
  line-height: 1.5;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  background: rgba(118, 75, 162, 0.07);
  border-radius: 4px;
  padding: 0.7rem 0.9rem;
}

/* What a project is, then what to do, each on a line of its own. */
.project-picker-intro p {
  margin: 0;
}

.project-picker-intro p + p {
  margin-top: 0.3rem;
}

.project-picker-current {
  margin-bottom: 0.75rem;
}

/* The choices: three tiles side by side, one above the other on a phone. */
.project-picker-options {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
  gap: 0.75rem;
}

.project-picker-option {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.35rem;
  padding: 1rem 0.75rem 0.85rem;
  border: 1px solid #dee2e6;
  border-radius: 8px;
  background: #fff;
  color: inherit;
  font: inherit;
  text-align: center;
  cursor: pointer;
  transition: border-color 0.2s ease, box-shadow 0.2s ease;
}

.project-picker-option:hover:not(:disabled),
.project-picker-option:focus-visible {
  border-color: rgba(118, 75, 162, 0.75);
  box-shadow: 0 2px 8px rgba(118, 75, 162, 0.2);
  outline: none;
}

.project-picker-option:disabled {
  cursor: not-allowed;
}

.project-picker-option:disabled .project-picker-option-icon,
.project-picker-option:disabled .project-picker-option-title,
.project-picker-option:disabled .project-picker-option-text {
  opacity: 0.6;
}

.project-picker-option-icon {
  display: inline-flex;
  flex: none;
  align-items: center;
  justify-content: center;
  width: 2.25rem;
  height: 2.25rem;
  border-radius: 50%;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: #fff;
}

/* The old stylesheet gives every icon a right margin, which pushes it off
   the centre of its circle. */
.project-picker-option-icon i {
  margin: 0;
}

/* The remembered project: one row across the choices, its icon beside the
   text, in the choices' purple, as the first thing to pick. */
.project-picker-remembered {
  flex-direction: row;
  gap: 0.75rem;
  width: 100%;
  margin-bottom: 0.75rem;
  padding: 0.75rem 1rem;
  border-color: rgba(118, 75, 162, 0.45);
  background: rgba(118, 75, 162, 0.04);
  text-align: left;
}

.project-picker-remembered-text {
  display: flex;
  flex-direction: column;
  gap: 0.1rem;
}

.project-picker-option-title {
  font-size: 0.95rem;
}

.project-picker-option-text {
  color: #6c757d;
  font-size: 0.8rem;
  line-height: 1.35;
}

/* As on the home page's tiles of workflows under development, but under
   the title, so it never covers the icon. */
.project-picker-soon {
  background: rgba(118, 75, 162, 0.12);
  color: #764ba2;
  font-weight: 500;
}

.project-picker-step-title {
  margin-bottom: 0.25rem;
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

/* A record's maps: compact rows, the list scrolling when it is long. */
.project-picker-choices .list-group {
  max-height: 12rem;
  overflow-y: auto;
}

.project-picker-choice {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  padding: 0.4rem 0.75rem;
  font-size: 0.85rem;
}

.project-picker-choice-name {
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
