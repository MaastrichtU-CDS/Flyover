import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { reactive } from 'vue'

// The pop-up that asks which project to work on: at start-up it can only be
// left by choosing; from the status bar it can be cancelled. Opening a map
// or leaving a project that would replace work in this browser asks first.

const rows = new Map()
const route = reactive({ path: '/describe' })

vi.mock('vue-router', () => ({ useRoute: () => route }))
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
const AYA = {
  name: 'AYA mapping',
  version: '1.0.0',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema' },
  databases: {},
}
const OTHER = {
  name: 'Other mapping',
  version: '',
  schema: { '@id': 'schema:other/v1', name: 'Other schema' },
  databases: {},
}

function detailsOf(map, extra = {}) {
  return {
    name: map.name,
    version: map.version,
    description: '',
    schemaId: map.schema['@id'],
    schemaName: map.schema.name,
    schemaVersion: '',
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

function holdOpenProject(map = AYA) {
  rows.set('semantic_map', { key: 'semantic_map', data: structuredClone(map) })
  rows.set('project', {
    key: 'project',
    data: { ...detailsOf(map), token: 't1', loadedFingerprint: fingerprint(map) },
  })
  serve({ open: { ...detailsOf(map), token: 't1' } })
}

async function mountPicker() {
  const project = useProjectStore()
  await project.init()
  const wrapper = mount(ProjectPicker, { attachTo: document.body })
  await flushPromises()
  return { wrapper, project }
}

function fileOf(map, name = 'map.jsonld') {
  return new File([typeof map === 'string' ? map : JSON.stringify(map)], name, {
    type: 'application/ld+json',
  })
}

async function pick(wrapper, file) {
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
    serve()
  })

  afterEach(() => {
    wrapper?.unmount()
    wrapper = undefined
    useProjectStore().stopStatusChecks()
    vi.unstubAllGlobals()
  })

  describe('at start-up', () => {
    it('asks which project to work on and can only be left by choosing', async () => {
      ;({ wrapper } = await mountPicker())

      const dialog = wrapper.find('[role="dialog"]')
      expect(dialog.exists()).toBe(true)
      expect(dialog.attributes('aria-modal')).toBe('true')
      expect(wrapper.find('#project-picker-title').text()).toBe('Choose a project')
      expect(wrapper.find('.project-picker-close').exists()).toBe(false)
      expect(button(wrapper, 'Cancel')).toBeUndefined()

      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
      await wrapper.find('.project-picker-backdrop').trigger('click')
      await flushPromises()
      expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    })

    it('says that going without a project is the standard RDF flow on the configured store', async () => {
      ;({ wrapper } = await mountPicker())

      const standard = wrapper.find('.project-picker-standard').text().replace(/\s+/g, ' ')
      expect(standard).toContain('standard RDF flow')
      expect(standard).toContain("Flyover's configured RDF store")
      expect(standard).toContain('Repository userRepo')
      expect(standard).toContain('http://rdf-store:7200/repositories/userRepo')
    })

    it('continues without a project for this tab, without reloading', async () => {
      let project
      ;({ wrapper, project } = await mountPicker())

      await button(wrapper, 'Continue without a project').trigger('click')
      await flushPromises()

      expect(project.picker.open).toBe(false)
      expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
      expect(sessionStorage.getItem('flyover.project.withoutProject')).toBe('1')
      expect(reloadPage).not.toHaveBeenCalled()
      expect(useStatusStore().messages).toEqual([])
    })

    it('opens a dropped map as the project and reloads the step page', async () => {
      let project
      ;({ wrapper, project } = await mountPicker())

      await pick(wrapper, fileOf(AYA, 'aya.jsonld'))

      expect(api.post).toHaveBeenCalledWith('/api/v1/project/check', {
        mapping: AYA,
        fileName: 'aya.jsonld',
      })
      expect(api.put.mock.calls[0][1]).toMatchObject({ mapping: AYA, fileName: 'aya.jsonld' })
      expect(project.picker.open).toBe(false)
      expect(reloadPage).toHaveBeenCalled()
      expect(sessionStorage.getItem('flyover.project.notice')).toBe(
        "Flyover now works on the project AYA mapping · v1.0.0, on the RDF store's repository aya."
      )
    })

    it('says so instead of reloading on the home page', async () => {
      route.path = '/'
      ;({ wrapper } = await mountPicker())

      await pick(wrapper, fileOf(AYA))

      expect(reloadPage).not.toHaveBeenCalled()
      expect(useStatusStore().messages[0].text).toContain('Flyover now works on the project')
    })

    it('opens a map from a web address, fetched by the server', async () => {
      ;({ wrapper } = await mountPicker())

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
        expect(reloadPage).toHaveBeenCalled()
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

    it('goes back to the choice without opening anything', async () => {
      rows.set('semantic_map', { key: 'semantic_map', data: OTHER })
      ;({ wrapper } = await mountPicker())

      await pick(wrapper, fileOf(AYA))
      await button(wrapper, 'Back').trigger('click')

      expect(wrapper.find('.project-picker-question').exists()).toBe(false)
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

  describe('from the status bar', () => {
    it('can be cancelled, with Escape too', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker())
      project.openPicker('switch')
      await flushPromises()

      expect(wrapper.find('.project-picker-current').text()).toBe('Open now: AYA mapping')
      await button(wrapper, 'Cancel').trigger('click')
      expect(project.picker.open).toBe(false)

      project.openPicker('switch')
      await flushPromises()
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
      await flushPromises()
      expect(project.picker.open).toBe(false)
    })

    it('asks before switching to another project', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker())
      project.openPicker('switch')
      await flushPromises()

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
      ;({ wrapper, project } = await mountPicker())
      project.openPicker('switch')
      await flushPromises()

      await pick(wrapper, fileOf({ ...AYA, name: 'Centre A AYA mapping' }))

      expect(wrapper.find('.project-picker-question').exists()).toBe(false)
      expect(api.put).toHaveBeenCalled()
      expect(project.name).toBe('Centre A AYA mapping')
    })

    it('asks before replacing a copy of the same project with describe answers in it', async () => {
      holdOpenProject()
      rows.get('semantic_map').data.databases = { a: { tables: {} } }
      let project
      ;({ wrapper, project } = await mountPicker())
      project.openPicker('switch')
      await flushPromises()

      await pick(wrapper, fileOf({ ...AYA, name: 'Centre A AYA mapping' }))

      expect(wrapper.text()).toContain('This map belongs to the open project')
      expect(api.put).not.toHaveBeenCalled()
    })

    it('asks before leaving the project, and says where Flyover goes', async () => {
      holdOpenProject()
      let project
      ;({ wrapper, project } = await mountPicker())
      project.openPicker('switch')
      await flushPromises()

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
      ;({ wrapper, project } = await mountPicker())
      project.openPicker('switch')
      await flushPromises()
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
