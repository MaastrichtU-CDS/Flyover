import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// A map uploaded on the annotate landing page is checked against the open
// project before the page sends it for annotation: set aside, it goes
// nowhere; opened as the project, the page's cleaned copy becomes the
// project's working copy.

vi.mock('vue-router', () => ({ useRouter: () => ({ push: vi.fn() }) }))
vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn(), put: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({
  useNavigation: () => ({ refreshDataExists: vi.fn(async () => {}) }),
  refreshDataExists: vi.fn(async () => {}),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import { useProjectStore } from '@/stores/project'
import AnnotationLandingView from '@/views/AnnotationLandingView.vue'

const MAP = {
  '@type': 'mapping:DataMapping',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema', variables: {} },
  databases: {},
}
const CLEANED = { ...MAP, databases: { cleaned: {} } }

const RouterLinkStub = { props: ['to'], template: '<a :href="String(to)"><slot /></a>' }

async function upload(wrapper) {
  const file = new File([JSON.stringify(MAP)], 'aya.jsonld', { type: 'application/ld+json' })
  const input = wrapper.find('input[type="file"]')
  Object.defineProperty(input.element, 'files', { value: [file], configurable: true })
  await input.trigger('change')
  await wrapper.find('form').trigger('submit')
}

function statusText(wrapper) {
  return wrapper.find('.alert-compact').exists() ? wrapper.find('.alert-compact').text() : ''
}

describe('AnnotationLandingView — an upload and the open project', () => {
  let project
  let fetchMock
  let wrapper

  beforeEach(async () => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    api.get.mockImplementation(async (url) =>
      url === '/api/check-graph-exists'
        ? { data: { exists: true } }
        : { data: { success: true, databases: [] } }
    )
    fetchMock = vi.fn(async () => ({
      ok: true,
      json: async () => ({ success: true, cleaned_mapping: CLEANED }),
    }))
    vi.stubGlobal('fetch', fetchMock)
    project = useProjectStore()
    project.project = { name: 'AYA mapping', schemaId: 'schema:aya/v1' }
    wrapper = mount(AnnotationLandingView, { global: { stubs: { RouterLink: RouterLinkStub } } })
    await flushPromises()
  })

  afterEach(() => {
    wrapper.unmount()
    vi.unstubAllGlobals()
  })

  it('sends a map the user set aside nowhere', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: false, opened: false })

    await upload(wrapper)
    await vi.waitFor(() => expect(statusText(wrapper)).toContain('set aside'))

    expect(project.planUpload).toHaveBeenCalledWith(MAP, 'aya.jsonld')
    expect(fetchMock).not.toHaveBeenCalled()
    expect(db.saveData).not.toHaveBeenCalled()
    expect(statusText(wrapper)).toContain('Flyover keeps working on AYA mapping')
  })

  it('lists why the open project cannot take the map, escaped', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({
      go: false,
      opened: false,
      invalid: {
        error: 'The map is not a valid Flyover semantic map.',
        validation_errors: [{ path: 'schema', message: '<b>schema</b> is required' }],
      },
    })

    await upload(wrapper)
    await vi.waitFor(() => expect(statusText(wrapper)).toContain('not a valid'))

    expect(wrapper.find('.alert-danger code').text()).toBe('schema')
    expect(wrapper.find('.alert-danger').html()).toContain('&lt;b&gt;schema&lt;/b&gt;')
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('keeps the cleaned copy of an opened project\'s map as its working copy', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: true, storeChanged: false })
    const adopt = vi.spyOn(project, 'adoptWorkingCopy').mockResolvedValue()

    await upload(wrapper)
    await vi.waitFor(() => expect(adopt).toHaveBeenCalled())

    expect(fetchMock).toHaveBeenCalledWith('/upload-annotation-json', expect.anything())
    expect(adopt).toHaveBeenCalledWith(CLEANED, { fileName: 'aya.jsonld' })
    expect(db.saveData).not.toHaveBeenCalledWith(
      'metadata',
      expect.objectContaining({ key: 'semantic_map' })
    )
  })

  it('saves the map as before when it stays without a project', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: false })

    await upload(wrapper)
    await vi.waitFor(() =>
      expect(db.saveData).toHaveBeenCalledWith(
        'metadata',
        expect.objectContaining({ key: 'semantic_map', data: CLEANED })
      )
    )
  })

  it('stops at a project store that holds no data yet', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: true, storeChanged: true })
    api.get.mockResolvedValue({ data: { exists: false } })

    await upload(wrapper)
    await vi.waitFor(() => expect(wrapper.text()).toContain('Prerequisites Required'))

    expect(fetchMock).not.toHaveBeenCalled()
  })
})
