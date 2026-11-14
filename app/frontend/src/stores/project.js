import { defineStore } from 'pinia'
import { computed, ref, shallowRef } from 'vue'
import api from '@/services/api'
import * as db from '@/lib/db'
import {
  downloadName,
  fingerprint,
  newToken,
  projectName,
  projectVersion,
  sameProject,
  sameStore,
} from '@/lib/project'
import { refreshDataExists } from '@/composables/useNavigation'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'

// The open project, as the browser keeps it (see lib/project.js for what a
// project is). The browser holds the project's working copy, the map row
// the describe pages fill in, and a record of the project next to it; the
// server holds the map and works on the store the map names
// (/api/v1/project). A project survives reloads and restarts here, in
// IndexedDB. Choosing to work without one lasts for the tab
// (sessionStorage), so opening Flyover again asks again.

const MAP_KEY = 'semantic_map'
const PROJECT_KEY = 'project'
// Rows that describe one store's data; they go stale when the store changes.
const STORE_ROWS = [
  'column_info',
  'rdf_store_databases',
  'descriptive_info',
  'descriptive_info_details',
]
const WITHOUT_PROJECT = 'flyover.project.withoutProject'
const NOTICE = 'flyover.project.notice'
const CHANNEL = 'flyover-project'
// How often the status bar asks whether the store answers while the tab is
// visible. A hidden tab does not ask, and asks as soon as it is shown again
// when a check is due. Each check is one short request.
export const STORE_CHECK_INTERVAL_MS = 60_000

function now() {
  return new Date().toISOString()
}

function tabSetting(key) {
  try {
    return globalThis.sessionStorage?.getItem(key) ?? null
  } catch {
    return null
  }
}

function setTabSetting(key, value) {
  try {
    if (value == null) globalThis.sessionStorage?.removeItem(key)
    else globalThis.sessionStorage?.setItem(key, value)
  } catch {
    // Storage may be blocked; the choice then lasts until the next reload.
  }
}

async function readRow(key) {
  try {
    return await db.getData('metadata', key)
  } catch {
    return null
  }
}

// The message an API error carries, for the user.
export function errorMessage(error) {
  return error?.response?.data?.error || error?.message || String(error)
}

export const useProjectStore = defineStore('project', () => {
  const status = useStatusStore()

  const ready = ref(false)
  // The open project: the details the server reported, plus the
  // fingerprint of the working copy as it was loaded.
  const project = ref(null)
  // The store the app works on, and the one working without a project uses.
  const store = ref(null)
  const configuredStore = ref(null)
  // The store's last answer (state: unknown | reachable | unreachable) and
  // whether a check is under way.
  const storeStatus = ref({ state: 'unknown', reason: '', checking: false })
  const picker = ref({ open: false, mode: 'startup' })
  // The question an upload in a step raises, while it waits for the user:
  // { kind, check, fileName, edited } (see planUpload).
  const uploadQuestion = shallowRef(null)
  let answerUploadQuestion = null

  // The token of the project the server works on (null: none).
  let serverToken = null
  let channel = null
  let statusRequest = 0
  let lastStatusCheck = 0
  let statusTimer = null

  const name = computed(() => (project.value ? projectName(project.value) : ''))
  const version = computed(() => (project.value ? projectVersion(project.value) : ''))

  function applyServer(answer) {
    if (!answer) return
    store.value = answer.store ?? store.value
    configuredStore.value = answer.configuredStore ?? configuredStore.value
    serverToken = answer.project?.token ?? null
  }

  async function fetchServer() {
    try {
      const { data } = await api.get('/api/v1/project')
      return data
    } catch {
      return null
    }
  }

  async function putProject(map, details, token) {
    const { data } = await api.put('/api/v1/project', {
      mapping: map,
      fileName: details.fileName || '',
      sourceUrl: details.sourceUrl || '',
      token,
    })
    return data
  }

  // What the steps worked out for the previous map: the review marks.
  async function forgetMapState() {
    try {
      await useSuggestionsStore().resetMarks()
    } catch {
      // Marks are a convenience; a failure must not block the change.
    }
  }

  async function forgetStoreRows() {
    for (const key of STORE_ROWS) {
      try {
        await db.deleteData('metadata', key)
      } catch {
        // A row that cannot be deleted is refreshed by its page.
      }
    }
  }

  // The store's data changed under the app (datasets deleted): what the
  // browser cached of it goes, and the step bar asks again what is there.
  async function storeDataChanged() {
    await forgetStoreRows()
    refreshDataExists()
    refreshStoreStatus()
  }

  // Bring the browser and the server to the same project. With `settle`,
  // the browser's project is reopened on a server that lost it (a restart)
  // and a project this browser does not know is closed when this tab works
  // without one; without it (another tab changed things) nothing is sent.
  async function load({ settle = true } = {}) {
    const [record, held] = await Promise.all([readRow(PROJECT_KEY), readRow(MAP_KEY)])
    let mine = record?.data || null
    let server = await fetchServer()

    if (mine && !held?.data) {
      await db.deleteData('metadata', PROJECT_KEY).catch(() => {})
      mine = null
      status.warning(
        "The open project's map was no longer in this browser, so Flyover works without a project."
      )
    }
    if (settle && mine && server && server.project?.token !== mine.token) {
      try {
        server = await putProject(held.data, mine, mine.token)
      } catch (e) {
        if (e?.response?.status === 400) {
          // The map no longer opens (a stricter release): keep it as a map
          // without a project rather than lose the user's work.
          await db.deleteData('metadata', PROJECT_KEY).catch(() => {})
          mine = null
          status.warning(
            `Flyover could not reopen the project: ${errorMessage(e)} Its map is kept, without a project.`
          )
        } else {
          status.error(`Flyover could not reopen the project on the server: ${errorMessage(e)}`)
        }
      }
    }
    if (settle && !mine && server?.project && tabSetting(WITHOUT_PROJECT)) {
      try {
        const { data } = await api.delete('/api/v1/project')
        server = data
      } catch {
        // The server keeps its project; the status bar shows its store.
      }
    }
    project.value = mine
    applyServer(server)
  }

  function showNoticeFromBeforeReload() {
    const notice = tabSetting(NOTICE)
    if (notice) {
      setTabSetting(NOTICE, null)
      status.success(notice)
    }
  }

  // Say something after the page reloads (the reload drops the banner).
  function noticeAfterReload(text) {
    setTabSetting(NOTICE, text)
  }

  function announce() {
    try {
      channel?.postMessage({ type: 'project-changed' })
    } catch {
      // Other tabs catch up when they next load.
    }
  }

  function listenToOtherTabs() {
    if (channel || typeof BroadcastChannel === 'undefined') return
    channel = new BroadcastChannel(CHANNEL)
    channel.onmessage = async (event) => {
      if (event?.data?.type !== 'project-changed') return
      await load({ settle: false })
      if (project.value) {
        setTabSetting(WITHOUT_PROJECT, null)
        if (picker.value.open && picker.value.mode === 'startup') closePicker()
      }
      status.warning(
        project.value
          ? `The project was changed in another tab: Flyover now works on ${name.value}. Reload this page before you go on.`
          : 'The project was closed in another tab: Flyover now works without a project. Reload this page before you go on.'
      )
      refreshStoreStatus()
      refreshDataExists()
    }
  }

  async function init() {
    showNoticeFromBeforeReload()
    await load()
    ready.value = true
    if (!project.value && !tabSetting(WITHOUT_PROJECT)) openPicker('startup')
    listenToOtherTabs()
    refreshStoreStatus()
    startStatusChecks()
  }

  // Ask whether the store answers. Until the answer arrives, the last one
  // stays shown; only the newest of overlapping checks counts.
  async function refreshStoreStatus() {
    const request = ++statusRequest
    lastStatusCheck = Date.now()
    storeStatus.value = { ...storeStatus.value, checking: true }
    const before = storeStatus.value.state
    try {
      const { data } = await api.get('/api/v1/project/store-status')
      if (request !== statusRequest) return
      storeStatus.value = {
        state: data?.reachable ? 'reachable' : 'unreachable',
        reason: data?.reason || '',
        checking: false,
      }
      if (data?.store) store.value = data.store
    } catch {
      if (request !== statusRequest) return
      storeStatus.value = {
        state: 'unreachable',
        reason: 'Flyover could not check the store.',
        checking: false,
      }
    }
    // A store that came back (or went) changes which steps have data to work
    // on: the step bar asks again.
    if (before !== 'unknown' && storeStatus.value.state !== before) refreshDataExists()
  }

  function tabHidden() {
    return typeof document !== 'undefined' && document.visibilityState === 'hidden'
  }

  function checkIfDue() {
    if (!tabHidden() && Date.now() - lastStatusCheck >= STORE_CHECK_INTERVAL_MS) {
      refreshStoreStatus()
    }
  }

  // Check the store every minute while the tab is visible.
  function startStatusChecks() {
    if (statusTimer || typeof document === 'undefined') return
    statusTimer = setInterval(() => {
      if (!tabHidden()) refreshStoreStatus()
    }, STORE_CHECK_INTERVAL_MS)
    document.addEventListener('visibilitychange', checkIfDue)
  }

  function stopStatusChecks() {
    if (statusTimer) clearInterval(statusTimer)
    statusTimer = null
    if (typeof document !== 'undefined') {
      document.removeEventListener('visibilitychange', checkIfDue)
    }
  }

  // Everything that follows a change of project or store. The review marks
  // go only when the browser's map changed with it.
  async function settleChange(previousStore, { mapChanged = true } = {}) {
    if (mapChanged) await forgetMapState()
    const storeChanged = !sameStore(previousStore, store.value)
    if (storeChanged) await forgetStoreRows()
    announce()
    refreshStoreStatus()
    refreshDataExists()
    return { storeChanged }
  }

  // Open `map` as the project, replacing any open project and any map this
  // browser holds. Throws the API error when the server refuses the map.
  async function openProject(map, { fileName = '', sourceUrl = '' } = {}) {
    const previousStore = store.value
    const answer = await putProject(map, { fileName, sourceUrl }, newToken())
    const details = { ...answer.project, loadedFingerprint: fingerprint(map) }
    await db.saveData('metadata', { key: MAP_KEY, data: map, filename: fileName, timestamp: now() })
    await db.saveData('metadata', { key: PROJECT_KEY, data: details, timestamp: now() })
    setTabSetting(WITHOUT_PROJECT, null)
    project.value = details
    applyServer(answer)
    return settleChange(previousStore)
  }

  // Work without a project: the server goes back to the configured store
  // and the project's map leaves this browser.
  async function leaveProject() {
    const previousStore = store.value
    const { data } = await api.delete('/api/v1/project')
    await db.deleteData('metadata', MAP_KEY)
    await db.deleteData('metadata', PROJECT_KEY)
    setTabSetting(WITHOUT_PROJECT, '1')
    project.value = null
    applyServer(data)
    return settleChange(previousStore)
  }

  // The start-up choice to work without a project, for this tab. A project
  // the server still has open from elsewhere is closed.
  async function continueWithoutProject() {
    setTabSetting(WITHOUT_PROJECT, '1')
    let storeChanged = false
    if (serverToken) {
      const previousStore = store.value
      const { data } = await api.delete('/api/v1/project')
      applyServer(data)
      storeChanged = (await settleChange(previousStore, { mapChanged: false })).storeChanged
    }
    closePicker()
    return { storeChanged }
  }

  // Keep `map` as the working copy as loaded: a step cleaned it (columns
  // the data lacks) right after it was uploaded.
  async function adoptWorkingCopy(map, { fileName } = {}) {
    const held = await readRow(MAP_KEY)
    await db.saveData('metadata', {
      key: MAP_KEY,
      data: map,
      filename: fileName ?? held?.filename ?? project.value?.fileName ?? '',
      timestamp: now(),
    })
    if (project.value) {
      const updated = { ...project.value, loadedFingerprint: fingerprint(map) }
      await db.saveData('metadata', { key: PROJECT_KEY, data: updated, timestamp: now() })
      project.value = updated
    }
  }

  // Whether the working copy changed since it was loaded (describe answers).
  async function isEdited() {
    if (!project.value) return false
    const held = await readRow(MAP_KEY)
    return Boolean(held?.data) && fingerprint(held.data) !== project.value.loadedFingerprint
  }

  async function hasHeldMap() {
    return Boolean((await readRow(MAP_KEY))?.data)
  }

  // Save the working copy as a file, describe answers included.
  async function downloadWorkingCopy() {
    const held = await readRow(MAP_KEY)
    if (!held?.data) return false
    const blob = new Blob([JSON.stringify(held.data, null, 2)], { type: 'application/ld+json' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = downloadName(project.value || { fileName: held.filename })
    document.body.appendChild(link)
    link.click()
    link.remove()
    setTimeout(() => URL.revokeObjectURL(url), 0)
    return true
  }

  // What opening `map` would mean: valid or not, which project, which store.
  async function checkMap(map, fileName = '') {
    const { data } = await api.post('/api/v1/project/check', { mapping: map, fileName })
    return data
  }

  // A map from a web address, fetched by the server. Throws an Error with
  // the reason when it cannot be fetched.
  async function fetchMap(url) {
    try {
      const { data } = await api.post('/api/v1/project/fetch-map', { url })
      return data
    } catch (e) {
      throw new Error(errorMessage(e))
    }
  }

  // How a checked map relates to the open project.
  function classify(checked) {
    if (!project.value) return 'no-project'
    if (!sameProject(project.value, checked?.project)) return 'other-project'
    if (!sameStore(store.value, checked?.store)) return 'other-store'
    return 'same-project'
  }

  // Ask the user what to do with an uploaded map; resolves to their answer.
  function ask(question) {
    if (answerUploadQuestion) answerUploadQuestion('keep')
    return new Promise((resolve) => {
      answerUploadQuestion = resolve
      uploadQuestion.value = question
    })
  }

  // The dialog's answer: 'open' (open the map as the project) or 'keep'
  // (keep what Flyover works on now).
  function answerUpload(choice) {
    const resolve = answerUploadQuestion
    answerUploadQuestion = null
    uploadQuestion.value = null
    resolve?.(choice)
  }

  // A map a step's upload brings (describe landing, annotate landing, share
  // mock), checked against the open project before the step uses it.
  //
  // - Without a project, the map may belong to a project: Flyover offers to
  //   open it as one. Kept without a project (or not openable as a project),
  //   the step goes on as it always did.
  // - The open project's own map replaces the working copy at once, unless
  //   describe answers were added since it was loaded: then Flyover asks.
  // - Another project's map, or the open project's map naming another
  //   store, asks whether to switch or keep going with what is open.
  //
  // Resolves to { go, opened, storeChanged, invalid }: `go` false means
  // the user kept what they had and the step must not use the map; `opened`
  // means the map is now the project (and its working copy); `invalid`
  // carries the server's check when a project cannot take the map.
  async function planUpload(map, fileName = '') {
    let check
    try {
      check = await checkMap(map, fileName)
    } catch (e) {
      if (!project.value) return { go: true, opened: false }
      throw e
    }
    if (!project.value) {
      if (!check?.valid) return { go: true, opened: false }
      const choice = await ask({ kind: 'no-project', check, fileName, edited: false })
      if (choice !== 'open') return { go: true, opened: false }
      return { go: true, opened: true, ...(await openProject(map, { fileName })) }
    }
    if (!check?.valid) return { go: false, opened: false, invalid: check }
    const kind = classify(check)
    const edited = await isEdited()
    if (kind !== 'same-project' || edited) {
      const choice = await ask({ kind, check, fileName, edited })
      if (choice !== 'open') return { go: false, opened: false }
    }
    return { go: true, opened: true, ...(await openProject(map, { fileName })) }
  }

  function openPicker(mode = 'switch') {
    picker.value = { open: true, mode }
  }

  function closePicker() {
    picker.value = { ...picker.value, open: false }
  }

  return {
    ready,
    project,
    store,
    configuredStore,
    storeStatus,
    picker,
    uploadQuestion,
    name,
    version,
    init,
    load,
    refreshStoreStatus,
    stopStatusChecks,
    openProject,
    leaveProject,
    continueWithoutProject,
    adoptWorkingCopy,
    isEdited,
    hasHeldMap,
    downloadWorkingCopy,
    checkMap,
    fetchMap,
    classify,
    planUpload,
    answerUpload,
    openPicker,
    closePicker,
    noticeAfterReload,
    storeDataChanged,
  }
})
