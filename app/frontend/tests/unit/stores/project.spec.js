import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// The project store keeps the browser and the server on the same project:
// the browser holds the working copy and a record of the project in
// IndexedDB, the server the map and the store it names.

const rows = new Map()

vi.mock('@/lib/db', () => ({
  getData: vi.fn(async (_store, key) => (rows.has(key) ? structuredClone(rows.get(key)) : null)),
  saveData: vi.fn(async (_store, row) => {
    rows.set(row.key, structuredClone(row))
  }),
  deleteData: vi.fn(async (_store, key) => {
    rows.delete(key)
  }),
}))

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))

vi.mock('@/composables/useNavigation', () => ({
  refreshDataExists: vi.fn(async () => {}),
}))

import api from '@/services/api'
import { refreshDataExists } from '@/composables/useNavigation'
import { fingerprint } from '@/lib/project'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'

const CONFIGURED = {
  kind: 'rdf-store',
  repository: 'userRepo',
  url: 'http://rdf-store:7200/repositories/userRepo',
  source: 'configured',
  note: '',
}
const PROJECT_STORE = {
  kind: 'rdf-store',
  repository: 'aya',
  url: 'https://store.example.org/repositories/aya',
  source: 'project',
  note: '',
}
const MAP = {
  name: 'AYA mapping',
  version: '1.0.0',
  endpoint: 'https://store.example.org/repositories/aya/statements',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema' },
  databases: {},
}

function details(token, extra = {}) {
  return {
    token,
    name: 'AYA mapping',
    version: '1.0.0',
    description: '',
    schemaId: 'schema:aya/v1',
    schemaName: 'AYA schema',
    schemaVersion: '',
    fileName: 'aya.jsonld',
    sourceUrl: '',
    openedAt: '2026-11-06T10:00:00+00:00',
    ...extra,
  }
}

function serverWith(project, store = project ? PROJECT_STORE : CONFIGURED) {
  return { project, store, configuredStore: CONFIGURED }
}

// What the server answers: its project, the store status, a PUT echoing the
// token it was sent, a DELETE closing it.
function serve({ project = null, reachable = true } = {}) {
  let current = project
  api.get.mockImplementation(async (url) => {
    if (url === '/api/v1/project') return { data: serverWith(current) }
    if (url === '/api/v1/project/store-status') {
      return {
        data: {
          reachable,
          reason: reachable ? '' : 'The store cannot be reached from Flyover.',
          store: current ? PROJECT_STORE : CONFIGURED,
        },
      }
    }
    throw new Error(`unexpected GET ${url}`)
  })
  api.put.mockImplementation(async (url, body) => {
    current = details(body.token, {
      fileName: body.fileName,
      sourceUrl: body.sourceUrl,
      name: body.mapping.name,
      schemaId: body.mapping.schema['@id'],
      schemaName: body.mapping.schema.name,
    })
    return { data: serverWith(current) }
  })
  api.delete.mockImplementation(async () => {
    current = null
    return { data: serverWith(null) }
  })
}

function holdProject(token = 'browser-token', map = MAP) {
  rows.set('semantic_map', { key: 'semantic_map', data: structuredClone(map) })
  rows.set('project', {
    key: 'project',
    data: { ...details(token), loadedFingerprint: fingerprint(map) },
  })
}

class FakeChannel {
  static all = []
  constructor(name) {
    this.name = name
    this.sent = []
    this.onmessage = null
    FakeChannel.all.push(this)
  }
  postMessage(message) {
    this.sent.push(message)
  }
}

describe('the project store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    rows.clear()
    sessionStorage.clear()
    vi.clearAllMocks()
    FakeChannel.all = []
    vi.stubGlobal('BroadcastChannel', FakeChannel)
    serve()
  })

  afterEach(() => {
    useProjectStore().stopStatusChecks()
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  describe('when Flyover opens', () => {
    it('asks which project to work on when there is none', async () => {
      const project = useProjectStore()

      await project.init()

      expect(project.project).toBe(null)
      expect(project.picker).toEqual({ open: true, mode: 'startup' })
      expect(project.store).toEqual(CONFIGURED)
      expect(project.configuredStore).toEqual(CONFIGURED)
      expect(project.storeStatus).toEqual({ state: 'reachable', reason: '', checking: false })
    })

    it('does not ask again in a tab that chose to work without a project', async () => {
      sessionStorage.setItem('flyover.project.withoutProject', '1')
      const project = useProjectStore()

      await project.init()

      expect(project.picker.open).toBe(false)
    })

    it('reopens the project it still holds without asking', async () => {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()

      await project.init()

      expect(project.picker.open).toBe(false)
      expect(project.name).toBe('AYA mapping')
      expect(project.version).toBe('v1.0.0')
      expect(project.store).toEqual(PROJECT_STORE)
      expect(api.put).not.toHaveBeenCalled()
    })

    it('hands the project back to a server that lost it, under the same token', async () => {
      // A restart: the server works without a project, the browser has one.
      holdProject('t1')
      const project = useProjectStore()

      await project.init()

      expect(api.put).toHaveBeenCalledWith('/api/v1/project', {
        mapping: MAP,
        fileName: 'aya.jsonld',
        sourceUrl: '',
        token: 't1',
      })
      expect(project.store).toEqual(PROJECT_STORE)
    })

    it('sends the working copy as it is now, describe answers included', async () => {
      holdProject('t1')
      const answered = { ...structuredClone(MAP), databases: { a: { tables: {} } } }
      rows.get('semantic_map').data = answered
      const project = useProjectStore()

      await project.init()

      expect(api.put.mock.calls[0][1].mapping).toEqual(answered)
    })

    it('keeps the map, without a project, when the server no longer opens it', async () => {
      holdProject('t1')
      api.put.mockRejectedValue({
        response: { status: 400, data: { error: 'The map is not a valid Flyover semantic map.' } },
      })
      const project = useProjectStore()

      await project.init()

      expect(project.project).toBe(null)
      expect(rows.has('project')).toBe(false)
      expect(rows.has('semantic_map')).toBe(true)
      expect(project.picker.open).toBe(true)
      expect(useStatusStore().messages[0].text).toContain('could not reopen the project')
    })

    it('forgets a project whose map is no longer in the browser', async () => {
      holdProject('t1')
      rows.delete('semantic_map')
      const project = useProjectStore()

      await project.init()

      expect(project.project).toBe(null)
      expect(rows.has('project')).toBe(false)
      expect(api.put).not.toHaveBeenCalled()
    })

    it('closes a project the server holds for someone else when this tab works without one', async () => {
      sessionStorage.setItem('flyover.project.withoutProject', '1')
      serve({ project: details('elsewhere') })
      const project = useProjectStore()

      await project.init()

      expect(api.delete).toHaveBeenCalledWith('/api/v1/project')
      expect(project.store).toEqual(CONFIGURED)
    })

    it('leaves such a project to the pop-up when the tab has not chosen yet', async () => {
      serve({ project: details('elsewhere') })
      const project = useProjectStore()

      await project.init()

      expect(api.delete).not.toHaveBeenCalled()
      expect(project.picker.open).toBe(true)
    })

    it('says what it had to say before the page reloaded', async () => {
      sessionStorage.setItem('flyover.project.notice', 'Flyover now works on AYA mapping.')
      const project = useProjectStore()

      await project.init()

      expect(useStatusStore().messages.map((m) => m.text)).toContain(
        'Flyover now works on AYA mapping.'
      )
      expect(sessionStorage.getItem('flyover.project.notice')).toBe(null)
    })
  })

  describe('opening a project', () => {
    it('sends the map with a new token and keeps it as the working copy', async () => {
      const project = useProjectStore()
      await project.init()

      const result = await project.openProject(MAP, { fileName: 'aya.jsonld' })

      const body = api.put.mock.calls[0][1]
      expect(body.mapping).toEqual(MAP)
      expect(body.token).toMatch(/^[0-9a-f]{32}$/)
      expect(rows.get('semantic_map').data).toEqual(MAP)
      expect(rows.get('semantic_map').filename).toBe('aya.jsonld')
      expect(rows.get('project').data.token).toBe(body.token)
      expect(rows.get('project').data.loadedFingerprint).toBe(fingerprint(MAP))
      expect(project.name).toBe('AYA mapping')
      expect(result).toEqual({ storeChanged: true })
    })

    it('forgets the review marks and what it knew of the previous store', async () => {
      rows.set('column_info', { key: 'column_info', data: {} })
      rows.set('rdf_store_databases', { key: 'rdf_store_databases', data: [] })
      const suggestions = useSuggestionsStore()
      const reset = vi.spyOn(suggestions, 'resetMarks')
      const project = useProjectStore()
      await project.init()

      await project.openProject(MAP)

      expect(reset).toHaveBeenCalled()
      expect(rows.has('column_info')).toBe(false)
      expect(rows.has('rdf_store_databases')).toBe(false)
      expect(refreshDataExists).toHaveBeenCalled()
    })

    it('keeps what it knew of the store when the store stays the same', async () => {
      rows.set('column_info', { key: 'column_info', data: {} })
      api.put.mockImplementation(async (url, body) => ({
        data: { ...serverWith(details(body.token)), store: CONFIGURED },
      }))
      const project = useProjectStore()
      await project.init()

      const result = await project.openProject(MAP)

      expect(result).toEqual({ storeChanged: false })
      expect(rows.has('column_info')).toBe(true)
    })

    it('tells the other tabs', async () => {
      const project = useProjectStore()
      await project.init()

      await project.openProject(MAP)

      expect(FakeChannel.all[0].sent).toEqual([{ type: 'project-changed' }])
    })

    it('changes nothing in the browser when the server refuses the map', async () => {
      api.put.mockRejectedValue({ response: { status: 400, data: { error: 'nope' } } })
      const project = useProjectStore()
      await project.init()

      await expect(project.openProject(MAP)).rejects.toBeTruthy()
      expect(rows.has('semantic_map')).toBe(false)
      expect(project.project).toBe(null)
    })

    it('ends the tab\'s choice to work without a project', async () => {
      sessionStorage.setItem('flyover.project.withoutProject', '1')
      const project = useProjectStore()
      await project.init()

      await project.openProject(MAP)

      expect(sessionStorage.getItem('flyover.project.withoutProject')).toBe(null)
    })
  })

  describe('leaving a project', () => {
    it('closes it on the server and removes its map from the browser', async () => {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()
      await project.init()

      const result = await project.leaveProject()

      expect(api.delete).toHaveBeenCalledWith('/api/v1/project')
      expect(rows.has('semantic_map')).toBe(false)
      expect(rows.has('project')).toBe(false)
      expect(project.project).toBe(null)
      expect(project.store).toEqual(CONFIGURED)
      expect(sessionStorage.getItem('flyover.project.withoutProject')).toBe('1')
      expect(result).toEqual({ storeChanged: true })
    })
  })

  describe('continuing without a project', () => {
    it('remembers the choice for the tab and closes the pop-up', async () => {
      const project = useProjectStore()
      await project.init()

      await project.continueWithoutProject()

      expect(sessionStorage.getItem('flyover.project.withoutProject')).toBe('1')
      expect(project.picker.open).toBe(false)
      expect(api.delete).not.toHaveBeenCalled()
    })

    it('keeps the map and the review marks this browser holds', async () => {
      rows.set('semantic_map', { key: 'semantic_map', data: MAP })
      serve({ project: details('elsewhere') })
      const reset = vi.spyOn(useSuggestionsStore(), 'resetMarks')
      const project = useProjectStore()
      await project.init()

      const result = await project.continueWithoutProject()

      expect(api.delete).toHaveBeenCalled()
      expect(rows.get('semantic_map').data).toEqual(MAP)
      expect(reset).not.toHaveBeenCalled()
      expect(result).toEqual({ storeChanged: true })
    })
  })

  describe('the working copy', () => {
    it('counts as edited once a describe answer changed it', async () => {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()
      await project.init()

      expect(await project.isEdited()).toBe(false)
      rows.get('semantic_map').data.databases = { a: { tables: {} } }
      expect(await project.isEdited()).toBe(true)
    })

    it('is as loaded again after a step adopts a cleaned copy', async () => {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()
      await project.init()
      const cleaned = { ...structuredClone(MAP), databases: { a: {} } }

      await project.adoptWorkingCopy(cleaned)

      expect(rows.get('semantic_map').data).toEqual(cleaned)
      expect(rows.get('semantic_map').filename).toBe('aya.jsonld')
      expect(await project.isEdited()).toBe(false)
    })

    it('is never edited without a project', async () => {
      rows.set('semantic_map', { key: 'semantic_map', data: MAP })
      const project = useProjectStore()
      await project.init()

      expect(await project.isEdited()).toBe(false)
      expect(await project.hasHeldMap()).toBe(true)
    })

    it('downloads as a file named after the project\'s file', async () => {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()
      await project.init()
      const clicked = []
      vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function () {
        clicked.push(this.download)
      })
      URL.createObjectURL = vi.fn(() => 'blob:map')
      URL.revokeObjectURL = vi.fn()

      expect(await project.downloadWorkingCopy()).toBe(true)
      expect(clicked).toEqual(['aya.jsonld'])
    })

    it('has nothing to download without a map', async () => {
      const project = useProjectStore()
      await project.init()

      expect(await project.downloadWorkingCopy()).toBe(false)
    })
  })

  describe('a map an upload brings', () => {
    async function withOpenProject() {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()
      await project.init()
      return project
    }

    it('is checked by the server', async () => {
      api.post.mockResolvedValue({ data: { valid: true } })
      const project = useProjectStore()

      await project.checkMap(MAP, 'aya.jsonld')

      expect(api.post).toHaveBeenCalledWith('/api/v1/project/check', {
        mapping: MAP,
        fileName: 'aya.jsonld',
      })
    })

    it('belongs to no project when none is open', async () => {
      const project = useProjectStore()
      await project.init()

      expect(project.classify({ project: details('x'), store: PROJECT_STORE })).toBe('no-project')
    })

    it('belongs to the open project when its schema is the same', async () => {
      const project = await withOpenProject()
      const checked = { project: details('', { name: 'Another name' }), store: PROJECT_STORE }

      expect(project.classify(checked)).toBe('same-project')
    })

    it('belongs to another project when its schema differs', async () => {
      const project = await withOpenProject()
      const checked = { project: details('', { schemaId: 'schema:other/v1' }), store: PROJECT_STORE }

      expect(project.classify(checked)).toBe('other-project')
    })

    it('points at another store when the schema is the same but the store is not', async () => {
      const project = await withOpenProject()

      expect(project.classify({ project: details(''), store: CONFIGURED })).toBe('other-store')
    })
  })

  describe('a map uploaded in a step', () => {
    const OTHER = {
      ...MAP,
      name: 'Other mapping',
      schema: { '@id': 'schema:other/v1', name: 'Other schema' },
    }

    // The server's check: valid, with the map's project and the store
    // its endpoint names (`store`, the AYA store by default).
    function checks(store = PROJECT_STORE) {
      api.post.mockImplementation(async (url, body) => {
        if (url !== '/api/v1/project/check') throw new Error(url)
        const map = body.mapping
        return {
          data: {
            valid: true,
            project: {
              ...details(''),
              name: map.name,
              schemaId: map.schema['@id'],
              schemaName: map.schema.name,
            },
            store,
          },
        }
      })
    }

    async function openStore({ withProject = true } = {}) {
      if (withProject) {
        holdProject('t1')
        serve({ project: details('t1') })
      }
      const project = useProjectStore()
      await project.init()
      return project
    }

    it('without a project, offers to open it as one and goes on as before when kept', async () => {
      checks()
      const project = await openStore({ withProject: false })

      const planned = project.planUpload(MAP, 'aya.jsonld')
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))
      expect(project.uploadQuestion.kind).toBe('no-project')
      expect(project.uploadQuestion.fileName).toBe('aya.jsonld')
      project.answerUpload('keep')

      expect(await planned).toEqual({ go: true, opened: false })
      expect(api.put).not.toHaveBeenCalled()
      expect(project.uploadQuestion).toBe(null)
    })

    it('without a project, opens it as the project when the user says so', async () => {
      checks()
      const project = await openStore({ withProject: false })

      const planned = project.planUpload(MAP, 'aya.jsonld')
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))
      project.answerUpload('open')

      expect(await planned).toEqual({ go: true, opened: true, storeChanged: true })
      expect(project.name).toBe('AYA mapping')
      expect(rows.get('semantic_map').data).toEqual(MAP)
    })

    it('without a project, leaves a map no project can take to the step, as before', async () => {
      api.post.mockResolvedValue({ data: { valid: false, error: 'not a project map' } })
      const project = await openStore({ withProject: false })

      expect(await project.planUpload(MAP)).toEqual({ go: true, opened: false })
      expect(project.uploadQuestion).toBe(null)
    })

    it('without a project, goes on as before when the check cannot be made', async () => {
      api.post.mockRejectedValue(new Error('offline'))
      const project = await openStore({ withProject: false })

      expect(await project.planUpload(MAP)).toEqual({ go: true, opened: false })
    })

    it('replaces the open project\'s unedited working copy without asking', async () => {
      checks()
      const project = await openStore()
      const filledIn = { ...structuredClone(MAP), name: 'Centre A AYA', databases: { a: {} } }

      const planned = await project.planUpload(filledIn, 'centre_a.jsonld')

      expect(planned).toEqual({ go: true, opened: true, storeChanged: false })
      expect(project.uploadQuestion).toBe(null)
      expect(rows.get('semantic_map').data).toEqual(filledIn)
      expect(project.name).toBe('Centre A AYA')
    })

    it('asks before replacing a working copy with describe answers in it', async () => {
      checks()
      const project = await openStore()
      rows.get('semantic_map').data.databases = { answered: {} }

      const planned = project.planUpload({ ...MAP, name: 'Centre A AYA' })
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))
      expect(project.uploadQuestion).toMatchObject({ kind: 'same-project', edited: true })
      project.answerUpload('keep')

      expect(await planned).toEqual({ go: false, opened: false })
      expect(rows.get('semantic_map').data.databases).toEqual({ answered: {} })
      expect(api.put).not.toHaveBeenCalled()
    })

    it('asks before switching to another project, and switches when told', async () => {
      checks()
      const project = await openStore()

      const planned = project.planUpload(OTHER, 'other.jsonld')
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))
      expect(project.uploadQuestion.kind).toBe('other-project')
      project.answerUpload('open')

      expect(await planned).toMatchObject({ go: true, opened: true })
      expect(project.name).toBe('Other mapping')
    })

    it('asks before moving the open project to another store', async () => {
      checks(CONFIGURED)
      const project = await openStore()

      const planned = project.planUpload(MAP)
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))
      expect(project.uploadQuestion.kind).toBe('other-store')
      project.answerUpload('keep')

      expect(await planned).toEqual({ go: false, opened: false })
      expect(project.store).toEqual(PROJECT_STORE)
    })

    it('hands the step the reasons when the open project cannot take the map', async () => {
      const refusal = { valid: false, error: 'Flyover cannot use the store the map\'s endpoint names.' }
      api.post.mockResolvedValue({ data: refusal })
      const project = await openStore()

      expect(await project.planUpload(MAP)).toEqual({ go: false, opened: false, invalid: refusal })
    })

    it('keeps what is open when a second question replaces an unanswered one', async () => {
      checks()
      const project = await openStore()
      const first = project.planUpload(OTHER)
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))

      const second = project.planUpload(OTHER)
      expect(await first).toEqual({ go: false, opened: false })
      await vi.waitFor(() => expect(project.uploadQuestion).not.toBe(null))
      project.answerUpload('keep')
      expect(await second).toEqual({ go: false, opened: false })
    })
  })

  describe('a map from a web address', () => {
    it('is fetched by the server', async () => {
      api.post.mockResolvedValue({ data: { mapping: MAP, fileName: 'aya.jsonld' } })
      const project = useProjectStore()

      const fetched = await project.fetchMap('https://example.org/aya.jsonld')

      expect(api.post).toHaveBeenCalledWith('/api/v1/project/fetch-map', {
        url: 'https://example.org/aya.jsonld',
      })
      expect(fetched.fileName).toBe('aya.jsonld')
    })

    it('fails with the server\'s reason', async () => {
      api.post.mockRejectedValue({
        response: { status: 502, data: { error: 'The address answered with HTTP 404.' } },
      })
      const project = useProjectStore()

      await expect(project.fetchMap('https://example.org/x')).rejects.toThrow(
        'The address answered with HTTP 404.'
      )
    })
  })

  describe('the store status', () => {
    it('reads as unreachable with the reason the server gives', async () => {
      serve({ reachable: false })
      const project = useProjectStore()

      await project.init()

      expect(project.storeStatus).toEqual({
        state: 'unreachable',
        reason: 'The store cannot be reached from Flyover.',
        checking: false,
      })
    })

    it('reads as unreachable when the check itself fails', async () => {
      const project = useProjectStore()
      await project.init()
      api.get.mockRejectedValue(new Error('offline'))

      await project.refreshStoreStatus()

      expect(project.storeStatus.state).toBe('unreachable')
    })

    it('keeps only the latest answer when checks overlap', async () => {
      const project = useProjectStore()
      await project.init()
      let answerFirst
      api.get.mockImplementationOnce(
        () => new Promise((resolve) => (answerFirst = resolve))
      )
      api.get.mockResolvedValueOnce({ data: { reachable: false, reason: 'down', store: CONFIGURED } })

      const first = project.refreshStoreStatus()
      await project.refreshStoreStatus()
      answerFirst({ data: { reachable: true, reason: '', store: CONFIGURED } })
      await first

      expect(project.storeStatus).toEqual({ state: 'unreachable', reason: 'down', checking: false })
    })

    it('keeps showing the last answer while a new check is under way', async () => {
      const project = useProjectStore()
      await project.init()
      let answer
      api.get.mockImplementationOnce(() => new Promise((resolve) => (answer = resolve)))

      const checking = project.refreshStoreStatus()

      expect(project.storeStatus).toEqual({ state: 'reachable', reason: '', checking: true })
      answer({ data: { reachable: false, reason: 'down', store: CONFIGURED } })
      await checking
      expect(project.storeStatus).toEqual({ state: 'unreachable', reason: 'down', checking: false })
    })
  })

  describe('a store that comes back', () => {
    it('makes the step bar ask again which steps have data', async () => {
      serve({ reachable: false })
      const project = useProjectStore()
      await project.init()
      refreshDataExists.mockClear()

      serve({ reachable: true })
      await project.refreshStoreStatus()

      expect(project.storeStatus.state).toBe('reachable')
      expect(refreshDataExists).toHaveBeenCalledTimes(1)
    })

    it('leaves the step bar alone while nothing changed', async () => {
      const project = useProjectStore()
      await project.init()
      refreshDataExists.mockClear()

      await project.refreshStoreStatus()

      expect(refreshDataExists).not.toHaveBeenCalled()
    })
  })

  describe('checking the store again', () => {
    function statusChecks() {
      return api.get.mock.calls.filter(([url]) => url === '/api/v1/project/store-status').length
    }

    function setVisibility(state) {
      Object.defineProperty(document, 'visibilityState', { value: state, configurable: true })
      document.dispatchEvent(new Event('visibilitychange'))
    }

    beforeEach(() => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] })
      setVisibility('visible')
    })

    afterEach(() => {
      setVisibility('visible')
    })

    it('happens every minute while the tab is visible', async () => {
      const project = useProjectStore()
      await project.init()
      expect(statusChecks()).toBe(1)

      await vi.advanceTimersByTimeAsync(59_000)
      expect(statusChecks()).toBe(1)
      await vi.advanceTimersByTimeAsync(1_000)
      expect(statusChecks()).toBe(2)
      await vi.advanceTimersByTimeAsync(60_000)
      expect(statusChecks()).toBe(3)
    })

    it('pauses in a hidden tab, and catches up when it is shown again', async () => {
      const project = useProjectStore()
      await project.init()
      setVisibility('hidden')

      await vi.advanceTimersByTimeAsync(180_000)
      expect(statusChecks()).toBe(1)

      setVisibility('visible')
      await vi.advanceTimersByTimeAsync(0)
      expect(statusChecks()).toBe(2)
    })

    it('does not ask again on showing the tab when the last check is recent', async () => {
      const project = useProjectStore()
      await project.init()

      setVisibility('hidden')
      await vi.advanceTimersByTimeAsync(10_000)
      setVisibility('visible')
      await vi.advanceTimersByTimeAsync(0)

      expect(statusChecks()).toBe(1)
    })

    it('starts once, however often Flyover starts', async () => {
      const project = useProjectStore()
      await project.init()
      await project.init()

      await vi.advanceTimersByTimeAsync(60_000)

      expect(statusChecks()).toBe(3)
    })

    it('stops when told', async () => {
      const project = useProjectStore()
      await project.init()
      project.stopStatusChecks()

      await vi.advanceTimersByTimeAsync(120_000)
      setVisibility('visible')

      expect(statusChecks()).toBe(1)
    })
  })

  describe('another tab', () => {
    it('opening a project closes this tab\'s start-up pop-up and says so', async () => {
      const project = useProjectStore()
      await project.init()
      expect(project.picker.open).toBe(true)
      // The other tab opened a project: IndexedDB and the server have it.
      holdProject('t9')
      serve({ project: details('t9') })

      await FakeChannel.all[0].onmessage({ data: { type: 'project-changed' } })

      expect(project.picker.open).toBe(false)
      expect(project.name).toBe('AYA mapping')
      expect(api.put).not.toHaveBeenCalled()
      expect(useStatusStore().messages.at(-1).text).toContain('changed in another tab')
    })

    it('closing the project is followed without sending anything', async () => {
      holdProject('t1')
      serve({ project: details('t1') })
      const project = useProjectStore()
      await project.init()
      rows.clear()
      serve()

      await FakeChannel.all[0].onmessage({ data: { type: 'project-changed' } })

      expect(project.project).toBe(null)
      expect(api.delete).not.toHaveBeenCalled()
      expect(useStatusStore().messages.at(-1).text).toContain('closed in another tab')
    })
  })
})
