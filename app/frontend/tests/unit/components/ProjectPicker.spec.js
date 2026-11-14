import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { reactive } from 'vue'

// The pop-up that asks which project to work on: as a tab enters the RDF
// workflow, with the project this browser remembers offered first (cancelled,
// it goes back to the home page), and from the status bar to switch. Opening
// a map or leaving a project that would replace work in this browser asks
// first.

const rows = new Map()
const route = reactive({ path: '/describe', meta: { workflow: 'rdf' } })
const router = { push: vi.fn() }

vi.mock('vue-router', () => ({ useRoute: () => route, useRouter: () => router }))
vi.mock('@/lib/browser', () => ({ reloadPage: vi.fn() }))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async (_s, key) => (rows.has(key) ? structuredClone(rows.get(key)) : null)),
  saveData: vi.fn(async (_s, row) => {
    rows.set(row.key, structuredClone(row))
  }),
  deleteData: vi.fn(async (_s, key) => {
    rows.delete(key)
  }),
}))
vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/composables/useNavigation', () => ({ refreshDataExists: vi.fn(async () => {}) }))

import api from '@/services/api'
import { reloadPage } from '@/lib/browser'
import { fingerprint } from '@/lib/project'
import ProjectPicker from '@/components/ProjectPicker.vue'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'
import { useWorkflowsStore } from '@/stores/workflows'

const CONFIGURED = {
  kind: 'rdf-store',
  repository: 'userRepo',
  url: 'http://rdf-store:7200/repositories/userRepo',
  source: 'configured',
  note: '',
}
const AYA_STORE = {
  kind: 'rdf-store',
  repository: 'aya',
  url: 'https://store.example.org/repositories/aya',
  source: 'project',
  note: '',
}
// Maps that say they target RDF, as maps for the RDF workflow should.
const AYA = {
  name: 'AYA mapping',
  version: '1.0.0',
  targetStandard: 'rdf',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema' },
  databases: {},
}
const OTHER = {
  name: 'Other mapping',
  version: '',
  targetStandard: 'rdf',
  schema: { '@id': 'schema:other/v1', name: 'Other schema' },
  databases: {},
}
const OMOP = { ...OTHER, name: 'OMOP mapping', targetStandard: 'omop' }

// As the server reports a map: its standard is RDF unless it names one.
function detailsOf(map, extra = {}) {
  return {
    name: map.name,
    version: map.version,
    description: '',
    schemaId: map.schema['@id'],
    schemaName: map.schema.name,
    schemaVersion: '',
    targetStandard: map.targetStandard || 'rdf',
    targetStandardDeclared: Boolean(map.targetStandard),
    fileName: 'map.jsonld',
    sourceUrl: '',
    ...extra,
  }
}

// The server: a project or none, a check that accepts every map as a
// project on the AYA store, a PUT that opens it, a DELETE that closes it.
function serve({ open = null } = {}) {
  let current = open
  const answer = () => ({
    project: current,
    store: current ? AYA_STORE : CONFIGURED,
    configuredStore: CONFIGURED,
  })
  api.get.mockImplementation(async (url) => {
    if (url === '/api/v1/project') return { data: answer() }
    return { data: { reachable: true, reason: '', store: answer().store } }
  })
  api.post.mockImplementation(async (url, body) => {
    if (url === '/api/v1/project/check') {
      return { data: { valid: true, project: detailsOf(body.mapping), store: AYA_STORE } }
    }
    if (url === '/api/v1/project/fetch-map') {
      return { data: { mapping: AYA, fileName: 'aya.jsonld', sourceUrl: body.url } }
    }
    if (url === '/api/v1/project/store-start') {
      return { data: { state: 'starting', reachable: false, reason: '', store: answer().store } }
    }
    throw new Error(url)
  })
  api.put.mockImplementation(async (_url, body) => {
    current = { ...detailsOf(body.mapping, { fileName: body.fileName }), token: body.token }
    return { data: answer() }
  })
  api.delete.mockImplementation(async () => {
    current = null
    return { data: answer() }
  })
}

// The project this browser remembers; the server still has it, unless
// `serverLost` (a restart).
function holdOpenProject(map = AYA, { serverLost = false } = {}) {
  rows.set('semantic_map', { key: 'semantic_map', data: structuredClone(map) })
  rows.set('project', {
    key: 'project',
    data: { ...detailsOf(map), token: 't1', loadedFingerprint: fingerprint(map) },
  })
  serve({ open: serverLost ? null : { ...detailsOf(map), token: 't1' } })
}

// The pop-up, opened as App.vue opens it: 'enter' as the tab enters the RDF
// workflow, 'switch' from the status bar inside it.
async function mountPicker(mode = 'enter') {
  const project = useProjectStore()
  await project.init()
  if (mode === 'switch') project.enterWorkflow('rdf')
  project.openPicker(mode)
  const wrapper = mount(ProjectPicker, { attachTo: document.body })
  await flushPromises()
  return { wrapper, project }
}

function fileOf(map, name = 'map.jsonld') {
  return new File([typeof map === 'string' ? map : JSON.stringify(map)], name, {
    type: 'application/ld+json',
  })
}

// The step that asks for a project's map, from the choices.
async function openStep(wrapper) {
  if (wrapper.find('#project-map-url').exists()) return
  await wrapper.find('#project-picker-open').trigger('click')
}

async function pick(wrapper, file) {
  await openStep(wrapper)
  const input = wrapper.find('input[type="file"]')
  Object.defineProperty(input.element, 'files', { value: [file], configurable: true })
  await input.trigger('change')
  await new Promise((resolve) => setTimeout(resolve, 0))
  await flushPromises()
}

function button(wrapper, label) {
  return wrapper.findAll('button').find((b) => b.text().includes(label))
}

describe('ProjectPicker', () => {
  let wrapper

  beforeEach(() => {
    setActivePinia(createPinia())
    rows.clear()
    sessionStorage.clear()
    vi.clearAllMocks()
    vi.stubGlobal('BroadcastChannel', undefined)
    route.path = '/describe'
    route.meta = { workflow: 'rdf' }
    serve()
  })

  afterEach(() => {
    wrapper?.unmount()
    wrapper = undefined
    useProjectStore().stopStatusChecks()
    vi.unstubAllGlobals()
  })

  describe('entering the RDF workflow', () => {
    it('asks which project to work on, and goes back to the home page when cancelled', async () => {
      let project
      ;({ wrapper, project } = await mountPicker())

      const dialog = wrapper.find('[role="dialog"]')
      expect(dialog.exists()).toBe(true)
      expect(dialog.attributes('aria-modal')).toBe('true')
      expect(wrapper.find('#project-picker-title').text()).toBe('Choose a project')
      expect(wrapper.find('.project-picker-close').exists()).toBe(true)

      await button(wrapper, 'Cancel').trigger('click')

      expect(project.picker.open).toBe(false)
      expect(project.workflow).toBe(null)
      expect(router.push).toHaveBeenCalledWith('/')
      expect(api.delete).not.toHaveBeenCalled()
    })

    it('goes back to the home page on Escape, a click beside it, or its close button', async () => {
      const leave = [
        () => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' })),
        () => wrapper.find('.project-picker-backdrop').trigger('click'),
        () => wrapper.find('.project-picker-close').trigger('click'),
      ]
      for (const how of leave) {
        setActivePinia(createPinia())
        router.push.mockClear()
        let project
        ;({ wrapper, project } = await mountPicker())

        await how()
        await flushPromises()

        expect(project.picker.open).toBe(false)
        expect(router.push).toHaveBeenCalledWith('/')
        wrapper.unmount()
      }
      wrapper = undefined
    })

    it('lets a choice under way finish before anything closes it', async () => {
      let project
      ;({ wrapper, project } = await mountPicker())
      let answer
      api.delete.mockImplementationOnce(
        () => new Promise((resolve) => {
          answer = resolve
        })
      )

      await button(wrapper, 'Continue without a project').trigger('click')
      await wrapper.find('.project-picker-backdrop').trigger('click')
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
      await flushPromises()

      expect(project.picker.open).toBe(true)
      expect(router.push).not.toHaveBeenCalled()

      answer({ data: { project: null, store: CONFIGURED, configuredStore: CONFIGURED } })
      await flushPromises()

      expect(project.picker.open).toBe(false)
      expect(project.workflow).toBe('rdf')
      expect(router.push).not.toHaveBeenCalled()
    })

    it('opens on three choices, creating a JSON-LD still under development', async () => {
      ;({ wrapper } = await mountPicker())

      const titles = wrapper.findAll('.project-picker-option-title').map((t) => t.text())
      expect(titles).toEqual(['Open a project', 'Create a JSON-LD', 'Continue without a project'])
      // The map is asked for once the user chooses to open a project.
      expect(wrapper.find('input[type="file"]').exists()).toBe(false)
      expect(wrapper.find('#project-map-url').exists()).toBe(false)
      const create = wrapper.find('#project-picker-create')
      expect(create.element.disabled).toBe(true)
      expect(create.find('.project-picker-soon').text()).toBe('Under development')
      expect(create.find('.project-picker-option-text').text().replace(/\s+/g, ' ')).toBe(
        'Describe your data model once, and share it with your partners'
      )
    })

    it('says what a shared JSON-LD is for, and asks to pick one of the options', async () => {
      ;({ wrapper } = await mountPicker())

      // What a project is, then what to do, each on a line of its own.
      expect(wrapper.findAll('.project-picker-intro p').map((line) => line.text())).toEqual([
        'All sites in a project share one JSON-LD, with a common schema and a part for each site.',
        'Choose one of the options below; you only need to create a new JSON-LD once per project.',
      ])
    })

    it('asks for the map once the user chooses to open a project, with a way back', async () => {
      ;({ wrapper } = await mountPicker())

      await wrapper.find('#project-picker-open').trigger('click')
      expect(wrapper.find('input[type="file"]').exists()).toBe(true)
      expect(wrapper.find('#project-map-url').exists()).toBe(true)
      expect(wrapper.find('.project-picker-options').exists()).toBe(false)

      await button(wrapper, 'Back to the choices').trigger('click')
      expect(wrapper.find('.project-picker-options').exists()).toBe(true)
    })

    it('says that going without a project is the standard RDF flow on the configured store', async () => {
      ;({ wrapper } = await mountPicker())

      const standard = wrapper.find('.project-picker-standard')
      expect(standard.find('.project-picker-option-text').text().replace(/\s+/g, ' ')).toBe(
        "The standard RDF flow, on Flyover's configured RDF store (repository userRepo)"
      )
      // Its address, for whoever wants it, on hover.
      expect(standard.attributes('title')).toBe('http://rdf-store:7200/repositories/userRepo')
    })

    it('continues without a project, in the workflow, without reloading', async () => {
      let project
      ;({ wrapper, project } = await mountPicker())

      await button(wrapper, 'Continue without a project').trigger('click')
      await flushPromises()

      expect(project.picker.open).toBe(false)
      expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
      expect(project.workflow).toBe('rdf')
      expect(api.delete).toHaveBeenCalledWith('/api/v1/project', { params: { workflow: 'rdf' } })
      expect(router.push).not.toHaveBeenCalled()
      expect(reloadPage).not.toHaveBeenCalled()
      expect(useStatusStore().messages).toEqual([])
    })

    it('opens a dropped map as the project, and says so without reloading', async () => {
      let project
      ;({ wrapper, project } = await mountPicker())

      await pick(wrapper, fileOf(AYA, 'aya.jsonld'))

      expect(api.post).toHaveBeenCalledWith('/api/v1/project/check', {
        mapping: AYA,
        fileName: 'aya.jsonld',
      })
      expect(api.put.mock.calls[0][1]).toMatchObject({ mapping: AYA, fileName: 'aya.jsonld' })
      expect(project.picker.open).toBe(false)
      expect(project.workflow).toBe('rdf')
      expect(reloadPage).not.toHaveBeenCalled()
      expect(useStatusStore().messages[0].text).toBe(
        "Flyover now works on the project AYA mapping · v1.0.0, on the RDF store's repository aya."
      )
    })

    it('offers no project first when the browser remembers none', async () => {
      ;({ wrapper } = await mountPicker())

      expect(wrapper.find('.project-picker-remembered').exists()).toBe(false)
      expect(wrapper.find('.project-picker-current').exists()).toBe(false)
    })

    describe('with a project this browser remembers', () => {
      it('offers it first, and continues with it on a server that still has it', async () => {
        holdOpenProject()
        let project
        ;({ wrapper, project } = await mountPicker())

        // A row above the choices, the whole row the button.
        const remembered = wrapper.find('.project-picker-remembered')
        expect(remembered.attributes('id')).toBe('project-picker-continue')
        expect(remembered.find('.project-picker-option-title').text()).toBe(
          'Continue with AYA mapping · v1.0.0'
        )
        expect(remembered.find('.project-picker-option-text').text()).toBe(
          'The project this browser remembers'
        )
        // "Open now" is for switching; entering, the offer says it.
        expect(wrapper.find('.project-picker-current').exists()).toBe(false)

        await wrapper.find('#project-picker-continue').trigger('click')
        await flushPromises()

        // The server keeps its work on the project; its store has to run.
        expect(api.post).toHaveBeenCalledWith('/api/v1/project/store-start')
        expect(api.put).not.toHaveBeenCalled()
        expect(project.picker.open).toBe(false)
        expect(project.workflow).toBe('rdf')
        expect(reloadPage).not.toHaveBeenCalled()
        expect(useStatusStore().messages[0].text).toBe(
          "Flyover now works on the project AYA mapping · v1.0.0, on the RDF store's repository aya."
        )
      })

      it('offers it only as the tab enters the workflow its map targets', async () => {
        // An OMOP project (once OMOP opens) is not the RDF workflow's.
        holdOpenProject(OMOP)
        ;({ wrapper } = await mountPicker())

        expect(wrapper.find('#project-picker-continue').exists()).toBe(false)
        expect(wrapper.find('#project-picker-open').exists()).toBe(true)
      })

      it('opens it again on a server that lost it, under the same token', async () => {
        holdOpenProject(AYA, { serverLost: true })
        let project
        ;({ wrapper, project } = await mountPicker())

        await wrapper.find('#project-picker-continue').trigger('click')
        await flushPromises()

        expect(api.put.mock.calls[0][1]).toMatchObject({ mapping: AYA, token: 't1' })
        expect(project.picker.open).toBe(false)
        expect(project.workflow).toBe('rdf')
      })

      it('says why it could not continue with it, and stays open to choose another', async () => {
        holdOpenProject(AYA, { serverLost: true })
        api.put.mockRejectedValueOnce({
          response: { status: 400, data: { error: 'The map is not a valid Flyover semantic map.' } },
        })
        let project
        ;({ wrapper, project } = await mountPicker())

        await wrapper.find('#project-picker-continue').trigger('click')
        await flushPromises()

        expect(wrapper.find('.project-picker-errors').text()).toContain(
          'The map is not a valid Flyover semantic map.'
        )
        expect(project.picker.open).toBe(true)
        expect(project.workflow).toBe(null)
      })

      it('asks before leaving it to continue without a project, and reloads nothing', async () => {
        holdOpenProject()
        let project
        ;({ wrapper, project } = await mountPicker())

        await button(wrapper, 'Continue without a project').trigger('click')
        await flushPromises()
        expect(wrapper.find('.project-picker-question').text()).toBe('Leave AYA mapping?')
        expect(api.delete).not.toHaveBeenCalled()

        await button(wrapper, 'Leave the project').trigger('click')
        await flushPromises()

        expect(api.delete).toHaveBeenCalled()
        expect(project.project).toBe(null)
        expect(rows.has('semantic_map')).toBe(false)
        expect(project.workflow).toBe('rdf')
        expect(reloadPage).not.toHaveBeenCalled()
        expect(useStatusStore().messages[0].text).toBe(
          "Flyover now works without a project: the standard RDF flow on the configured RDF store's repository userRepo."
        )
      })
    })

    it('opens a map from a web address, fetched by the server', async () => {
      ;({ wrapper } = await mountPicker())
      await openStep(wrapper)

      await wrapper.find('#project-map-url').setValue('https://example.org/aya.jsonld')
      await wrapper.find('form').trigger('submit')
      await flushPromises()

      expect(api.post).toHaveBeenCalledWith('/api/v1/project/fetch-map', {
        url: 'https://example.org/aya.jsonld',
      })
      expect(api.put.mock.calls[0][1]).toMatchObject({
        sourceUrl: 'https://example.org/aya.jsonld',
        fileName: 'aya.jsonld',
      })
    })

    it("says a file's page on a code hosting site, or a Zenodo record, will do", async () => {
      ;({ wrapper } = await mountPicker())
      await openStep(wrapper)

      const field = wrapper.find('#project-map-url')
      const help = wrapper.find(`#${field.attributes('aria-describedby')}`)
      expect(help.text().replace(/\s+/g, ' ')).toBe(
        "The file's own address, its page on Codeberg, GitHub, GitLab or a similar " +
          'site, or its Zenodo record or DOI.'
      )
    })

    describe('a Zenodo record with several maps', () => {
      const DOI = 'https://doi.org/10.5281/zenodo.1486992'
      const FILES = 'https://zenodo.org/records/1486992/files'
      const CHOICES = [
        { name: 'mapping_centre_a.jsonld', url: `${FILES}/mapping_centre_a.jsonld`, size: 48210 },
        { name: 'mapping_template.jsonld', url: `${FILES}/mapping_template.jsonld`, size: 860 },
        { name: 'mapping_centre_b.jsonld', url: `${FILES}/mapping_centre_b.jsonld`, size: null },
      ]

      beforeEach(() => {
        const opening = api.post.getMockImplementation()
        api.post.mockImplementation(async (url, body) => {
          if (url === '/api/v1/project/fetch-map' && body.url === DOI) {
            return {
              data: {
                message: 'This Zenodo record holds 3 JSON-LD files.',
                choices: CHOICES,
                sourceUrl: DOI,
              },
            }
          }
          if (url === '/api/v1/project/fetch-map') {
            return {
              data: { mapping: AYA, fileName: body.url.split('/').pop(), sourceUrl: body.url },
            }
          }
          return opening(url, body)
        })
      })

      async function load(address) {
        await openStep(wrapper)
        await wrapper.find('#project-map-url').setValue(address)
        await wrapper.find('#project-map-url').element.form.dispatchEvent(new Event('submit'))
        await flushPromises()
      }

      function rows(list) {
        return list.findAll('.project-picker-choice').map((b) => b.text().replace(/\s+/g, ' '))
      }

      it('lists them with their sizes, and opens the one picked', async () => {
        ;({ wrapper } = await mountPicker())

        await load(DOI)

        const list = wrapper.find('.project-picker-choices')
        expect(list.text()).toContain('This Zenodo record holds 3 JSON-LD files. Choose the one to open:')
        expect(rows(list)).toEqual([
          'mapping_centre_a.jsonld 48 kB',
          'mapping_template.jsonld 860 B',
          'mapping_centre_b.jsonld',
        ])
        expect(api.put).not.toHaveBeenCalled()

        await list.findAll('.project-picker-choice')[0].trigger('click')
        await flushPromises()

        expect(api.post).toHaveBeenCalledWith('/api/v1/project/fetch-map', {
          url: `${FILES}/mapping_centre_a.jsonld`,
        })
        expect(api.put.mock.calls[0][1]).toMatchObject({
          fileName: 'mapping_centre_a.jsonld',
          sourceUrl: `${FILES}/mapping_centre_a.jsonld`,
        })
        expect(useProjectStore().picker.open).toBe(false)
      })

      it('forgets them once another address is typed', async () => {
        ;({ wrapper } = await mountPicker())
        await load(DOI)
        expect(wrapper.find('.project-picker-choices').exists()).toBe(true)

        await wrapper.find('#project-map-url').setValue('https://example.org/aya.jsonld')

        expect(wrapper.find('.project-picker-choices').exists()).toBe(false)
      })

      it('keeps them when the one picked cannot be loaded, to pick another', async () => {
        ;({ wrapper } = await mountPicker())
        await load(DOI)
        const opening = api.post.getMockImplementation()
        api.post.mockImplementation(async (url, body) => {
          if (url === '/api/v1/project/fetch-map' && body.url !== DOI) {
            throw { response: { status: 502, data: { error: 'The address answered with HTTP 404.' } } }
          }
          return opening(url, body)
        })

        await wrapper.findAll('.project-picker-choice')[1].trigger('click')
        await flushPromises()

        expect(wrapper.find('.project-picker-errors').text()).toContain(
          'The address answered with HTTP 404.'
        )
        expect(wrapper.findAll('.project-picker-choice')).toHaveLength(3)
        expect(api.put).not.toHaveBeenCalled()
      })
    })

    it('shows why a web address could not be loaded', async () => {
      ;({ wrapper } = await mountPicker())
      await openStep(wrapper)
      api.post.mockRejectedValueOnce({
        response: { status: 502, data: { error: 'The address answered with HTTP 404.' } },
      })

      await wrapper.find('#project-map-url').setValue('https://example.org/missing.jsonld')
      await wrapper.find('form').trigger('submit')
      await flushPromises()

      expect(wrapper.find('.project-picker-errors').text()).toContain(
        'The address answered with HTTP 404.'
      )
      expect(api.put).not.toHaveBeenCalled()
    })

    it('shows a file that is not JSON, and opens nothing', async () => {
      ;({ wrapper } = await mountPicker())

      await pick(wrapper, fileOf('{ "name": ', 'broken.jsonld'))

      expect(wrapper.find('.project-picker-errors').text()).toContain('broken.jsonld is not valid JSON')
      expect(api.post).not.toHaveBeenCalled()
    })

    it('lists why a map cannot be opened, as the describe page does', async () => {
      ;({ wrapper } = await mountPicker())
      api.post.mockResolvedValueOnce({
        data: {
          valid: false,
          error: 'The map is not a valid Flyover semantic map.',
          validation_errors: [
            { path: 'schema', message: "'schema' is required", suggestion: 'Add a schema.' },
          ],
        },
      })

      await pick(wrapper, fileOf({ name: 'x' }))

      const errors = wrapper.find('.project-picker-errors')
      expect(errors.text()).toContain('The map is not a valid Flyover semantic map.')
      expect(errors.find('code').text()).toBe('schema')
      expect(errors.text()).toContain('Add a schema.')
      expect(api.put).not.toHaveBeenCalled()
      expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    })

    it('asks before replacing a map a step uploaded, and offers to download it', async () => {
      rows.set('semantic_map', { key: 'semantic_map', data: OTHER, filename: 'other.jsonld' })
      ;({ wrapper } = await mountPicker())
      const project = useProjectStore()
      const download = vi.spyOn(project, 'downloadWorkingCopy').mockResolvedValue(true)

      await pick(wrapper, fileOf(AYA))

      expect(wrapper.find('.project-picker-question').text()).toMatch(/Open AYA mapping\s+· v1\.0\.0\s*\?/)
      expect(wrapper.text()).toContain('It replaces the map this browser holds')
      expect(api.put).not.toHaveBeenCalled()

      await button(wrapper, 'Download the current map').trigger('click')
      expect(download).toHaveBeenCalled()

      await button(wrapper, 'Open the project').trigger('click')
      await flushPromises()
      expect(api.put).toHaveBeenCalled()
    })

    it('goes back to the map to open, and on to the choices, opening nothing', async () => {
      rows.set('semantic_map', { key: 'semantic_map', data: OTHER })
      ;({ wrapper } = await mountPicker())

      await pick(wrapper, fileOf(AYA))
      await wrapper
        .findAll('.project-picker-actions button')
        .find((b) => b.text() === 'Back')
        .trigger('click')

      expect(wrapper.find('.project-picker-question').exists()).toBe(false)
      expect(wrapper.find('#project-map-url').exists()).toBe(true)
      await button(wrapper, 'Back to the choices').trigger('click')
      expect(button(wrapper, 'Continue without a project')).toBeTruthy()
      expect(api.put).not.toHaveBeenCalled()
    })

    it('keeps the focus inside the pop-up', async () => {
      ;({ wrapper } = await mountPicker())
      const buttons = wrapper.findAll('button').filter((b) => !b.element.disabled)
      const last = buttons.at(-1).element
      last.focus()

      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab' }))

      expect(document.activeElement).not.toBe(last)
      expect(wrapper.find('[role="dialog"]').element.contains(document.activeElement)).toBe(true)
    })
  })

  describe('a map for another workflow', () => {
    it('opens nothing, says which workflow it is for, and offers to switch', async () => {
      ;({ wrapper } = await mountPicker())

      await pick(wrapper, fileOf(OMOP, 'omop.jsonld'))

      expect(wrapper.find('.project-picker-question').text()).toBe(
        'This JSON-LD is for the OMOP workflow'
      )
      expect(wrapper.find('.project-picker-body p').text().replace(/\s+/g, ' ')).toBe(
        'OMOP mapping targets the OMOP common data model, so the RDF workflow cannot open ' +
          'it. Switch to the OMOP workflow, or choose a different JSON-LD.'
      )
      expect(api.put).not.toHaveBeenCalled()

      // OMOP is under development: switching shows its note, over the pop-up.
      await button(wrapper, 'Switch to OMOP').trigger('click')
      expect(useWorkflowsStore().soon.label).toBe('OMOP')
      expect(useProjectStore().picker.open).toBe(true)
    })

    it('goes back to choose a different JSON-LD', async () => {
      ;({ wrapper } = await mountPicker())
      await pick(wrapper, fileOf(OMOP, 'omop.jsonld'))

      await button(wrapper, 'Choose a different JSON-LD').trigger('click')

      expect(wrapper.find('.project-picker-question').exists()).toBe(false)
      expect(wrapper.find('#project-map-url').exists()).toBe(true)
    })

    it('is refused on the home page too, for the workflow the pop-up opens', async () => {
      route.path = '/'
      route.meta = {}
      const project = useProjectStore()
      await project.init()
      project.openPicker('enter', { workflow: 'rdf', target: '/rdf' })
      wrapper = mount(ProjectPicker, { attachTo: document.body })
      await flushPromises()

      await pick(wrapper, fileOf(OMOP, 'omop.jsonld'))

      expect(wrapper.find('.project-picker-question').text()).toBe(
        'This JSON-LD is for the OMOP workflow'
      )
      expect(router.push).not.toHaveBeenCalled()
    })
  })

  it("names the workflow it asks for, and that workflow's store", async () => {
    // Only RDF is built; nothing in the pop-up's words is RDF's own.
    route.path = '/'
    route.meta = {}
    const project = useProjectStore()
    await project.init()
    project.openPicker('enter', { workflow: 'omop', target: '/omop' })
    wrapper = mount(ProjectPicker, { attachTo: document.body })
    await flushPromises()

    const standard = wrapper.find('.project-picker-standard .project-picker-option-text')
    expect(standard.text().replace(/\s+/g, ' ')).toBe(
      "The standard OMOP flow, on Flyover's configured OMOP database (repository userRepo)"
    )
    await wrapper.find('#project-picker-open').trigger('click')
    expect(wrapper.text()).toContain(
      "Flyover then works on the OMOP database that the project's map names."
    )
  })

  it('makes every choice for the workflow it was opened for', async () => {
    // Only RDF is built, but nothing in the choice is RDF's own.
    route.path = '/'
    route.meta = {}
    const project = useProjectStore()
    await project.init()
    project.openPicker('enter', { workflow: 'csv', target: '/csv' })
    wrapper = mount(ProjectPicker, { attachTo: document.body })
    await flushPromises()

    await wrapper.find('#project-picker-without').trigger('click')
    await flushPromises()

    expect(api.delete).toHaveBeenCalledWith('/api/v1/project', { params: { workflow: 'csv' } })
    expect(project.workflow).toBe('csv')
    expect(router.push).toHaveBeenCalledWith('/csv')
  })

  it('says it takes a map naming no standard as RDF, once it opened it', async () => {
    ;({ wrapper } = await mountPicker())
    const { targetStandard: _, ...undeclared } = AYA

    await pick(wrapper, fileOf(undeclared, 'aya.jsonld'))

    expect(useStatusStore().messages[0].text).toBe(
      "Flyover now works on the project AYA mapping · v1.0.0, on the RDF store's repository aya. " +
        'Its JSON-LD names no target standard, so Flyover takes it as an RDF project: ' +
        'add "targetStandard": "rdf" to the JSON-LD to say so.'
    )
  })

  describe('opened on the home page', () => {
    // As the RDF tile, the start button and the navigation bar open it.
    async function mountOnHome() {
      route.path = '/'
      route.meta = {}
      const project = useProjectStore()
      await project.init()
      project.openPicker('enter', { workflow: 'rdf', target: '/rdf' })
      const mounted = mount(ProjectPicker, { attachTo: document.body })
      await flushPromises()
      return { wrapper: mounted, project }
    }

    it('goes on to the RDF landing once the tab continues without a project', async () => {
      let project
      ;({ wrapper, project } = await mountOnHome())

      await button(wrapper, 'Continue without a project').trigger('click')
      await flushPromises()

      expect(api.delete).toHaveBeenCalledWith('/api/v1/project', { params: { workflow: 'rdf' } })
      expect(project.workflow).toBe('rdf')
      expect(project.picker.open).toBe(false)
      expect(router.push).toHaveBeenCalledWith('/rdf')
      expect(reloadPage).not.toHaveBeenCalled()
    })

    it('goes on once a dropped map opened as the project, and says so', async () => {
      ;({ wrapper } = await mountOnHome())

      await pick(wrapper, fileOf(AYA, 'aya.jsonld'))

      expect(router.push).toHaveBeenCalledWith('/rdf')
      expect(useStatusStore().messages[0].text).toBe(
        "Flyover now works on the project AYA mapping · v1.0.0, on the RDF store's repository aya."
      )
    })

    it('goes on with the project this browser remembers', async () => {
      holdOpenProject()
      ;({ wrapper } = await mountOnHome())

      await wrapper.find('#project-picker-continue').trigger('click')
      await flushPromises()

      expect(api.post).toHaveBeenCalledWith('/api/v1/project/store-start')
      expect(router.push).toHaveBeenCalledWith('/rdf')
    })

    it('stays on the home page when cancelled, the tab outside the workflow', async () => {
      let project
      ;({ wrapper, project } = await mountOnHome())

      await button(wrapper, 'Cancel').trigger('click')

      expect(project.picker.open).toBe(false)
      expect(project.workflow).toBe(null)
      expect(router.push).not.toHaveBeenCalled()
      expect(api.delete).not.toHaveBeenCalled()
    })

    it('stays on the home page, still asking, when the server does not hear the choice', async () => {
      let project
      ;({ wrapper, project } = await mountOnHome())
      api.delete.mockRejectedValueOnce({
        response: { status: 503, data: { error: 'The server is busy.' } },
      })

      await button(wrapper, 'Continue without a project').trigger('click')
      await flushPromises()

      expect(wrapper.find('.project-picker-errors').text()).toContain('The server is busy.')
      expect(project.picker.open).toBe(true)
      expect(project.workflow).toBe(null)
      expect(router.push).not.toHaveBeenCalled()
    })
  })

  describe('from the status bar', () => {
    it('can be cancelled, with Escape too', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker('switch'))

      expect(wrapper.find('.project-picker-current').text()).toBe('Open now: AYA mapping')
      expect(wrapper.find('.project-picker-remembered').exists()).toBe(false)
      await button(wrapper, 'Cancel').trigger('click')
      expect(project.picker.open).toBe(false)

      project.openPicker('switch')
      await flushPromises()
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
      await flushPromises()
      expect(project.picker.open).toBe(false)
      // The tab stays where it was, in the workflow.
      expect(project.workflow).toBe('rdf')
      expect(router.push).not.toHaveBeenCalled()
    })

    it('says so instead of reloading on the RDF home page', async () => {
      route.path = '/rdf'
      holdOpenProject()
      ;({ wrapper } = await mountPicker('switch'))

      await pick(wrapper, fileOf(OTHER))
      await button(wrapper, 'Open the project').trigger('click')
      await flushPromises()

      expect(reloadPage).not.toHaveBeenCalled()
      expect(useStatusStore().messages[0].text).toContain('Flyover now works on the project Other mapping')
    })

    it('asks before switching to another project', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker('switch'))

      await pick(wrapper, fileOf(OTHER))

      expect(wrapper.text()).toContain('It replaces the open project, AYA mapping')
      await button(wrapper, 'Open the project').trigger('click')
      await flushPromises()
      expect(project.name).toBe('Other mapping')
      expect(reloadPage).toHaveBeenCalled()
    })

    it('replaces an unedited copy of the same project without asking', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker('switch'))

      await pick(wrapper, fileOf({ ...AYA, name: 'Centre A AYA mapping' }))

      expect(wrapper.find('.project-picker-question').exists()).toBe(false)
      expect(api.put).toHaveBeenCalled()
      expect(project.name).toBe('Centre A AYA mapping')
    })

    it('asks before replacing a copy of the same project with describe answers in it', async () => {
      holdOpenProject()
      rows.get('semantic_map').data.databases = { a: { tables: {} } }
      ;({ wrapper } = await mountPicker('switch'))

      await pick(wrapper, fileOf({ ...AYA, name: 'Centre A AYA mapping' }))

      expect(wrapper.text()).toContain('This map belongs to the open project')
      expect(api.put).not.toHaveBeenCalled()
    })

    it('asks before leaving the project, and says where Flyover goes', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker('switch'))

      await button(wrapper, 'Continue without a project').trigger('click')
      await flushPromises()

      expect(wrapper.find('.project-picker-question').text()).toBe('Leave AYA mapping?')
      expect(wrapper.text()).toContain('the standard RDF flow, on its configured RDF store')
      expect(wrapper.text()).toContain('repository userRepo')
      expect(api.delete).not.toHaveBeenCalled()

      await button(wrapper, 'Leave the project').trigger('click')
      await flushPromises()
      expect(api.delete).toHaveBeenCalled()
      expect(project.project).toBe(null)
      expect(rows.has('semantic_map')).toBe(false)
      expect(reloadPage).toHaveBeenCalled()
      expect(sessionStorage.getItem('flyover.project.notice')).toBe(
        "Flyover now works without a project: the standard RDF flow on the configured RDF store's repository userRepo."
      )
    })

    it('shows why the server refused to open a map, and keeps the open project', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker('switch'))
      api.put.mockRejectedValueOnce({
        response: { status: 400, data: { error: 'Flyover cannot use the store the map\'s endpoint names.', validation_errors: [] } },
      })

      await pick(wrapper, fileOf(OTHER))
      await button(wrapper, 'Open the project').trigger('click')
      await flushPromises()

      expect(wrapper.find('.project-picker-errors').text()).toContain('cannot use the store')
      expect(project.name).toBe('AYA mapping')
    })
  })
})
