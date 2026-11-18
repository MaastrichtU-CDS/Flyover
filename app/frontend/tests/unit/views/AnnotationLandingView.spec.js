import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { ref } from 'vue'

// Uploading a finalised semantic map on the annotate landing page: the map is
// opened, and its warnings are shown without stopping it.

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/composables/useNavigation', () => ({
  useNavigation: () => ({
    dataExists: ref(true),
    refreshDataExists: vi.fn(async () => {}),
    stepStates: ref([]),
    currentStep: ref(null),
  }),
}))

vi.mock('vue-router', () => ({
  useRouter: () => ({ push: vi.fn() }),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import AnnotationLandingView from '@/views/AnnotationLandingView.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

const MAP = {
  '@type': 'mapping:DataMapping',
  schema: { variables: {} },
  databases: { db: { tables: { patients: { sourceFile: 'patients', columns: {} } } } },
}

function answer(body) {
  return { ok: true, status: 200, json: async () => body }
}

async function uploadMap(wrapper) {
  const file = new File([JSON.stringify(MAP)], 'map.jsonld', { type: 'application/ld+json' })
  const input = wrapper.find('input[type="file"]').element
  Object.defineProperty(input, 'files', { value: [file], configurable: true })
  await wrapper.find('input[type="file"]').trigger('change')
  await wrapper.find('#jsonUploadForm').trigger('submit')
  // The page reads the file first, with a FileReader that resolves on a task
  // of its own: wait for the upload request that follows it. Every later
  // step is a promise the mocks resolve.
  await vi.waitFor(() => expect(fetch).toHaveBeenCalled())
  await flushPromises()
}

describe('AnnotationLandingView — uploading a semantic map', () => {
  beforeEach(() => {
    api.get.mockReset()
    api.get.mockImplementation(async (url) =>
      url === '/api/check-graph-exists'
        ? { data: { exists: true } }
        : { data: { success: true, databases: ['patients'] } }
    )
    db.saveData.mockClear()
    vi.stubGlobal('fetch', vi.fn())
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it("opens the map and shows its warnings", async () => {
    const mapWarning = {
      path: 'databases.db.tables.patients.columns.sex.localMappings',
      message: "Table 'patients', column 'sex': the code 'M' is listed under the terms 'male' and 'female'",
      suggestion: 'A cell holding this code gets the class of each of these terms.',
    }
    fetch.mockResolvedValue(
      answer({
        success: true,
        matching_databases: [{ jsonld: 'patients', rdf_store: 'patients' }],
        cleaned_mapping: MAP,
        map_warnings: [mapWarning],
      })
    )

    const wrapper = mount(AnnotationLandingView, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    await uploadMap(wrapper)

    expect(fetch).toHaveBeenCalledWith('/upload-annotation-json', expect.anything())
    expect(db.saveData).toHaveBeenCalledWith(
      'metadata',
      expect.objectContaining({ key: 'semantic_map', data: MAP })
    )
    expect(wrapper.text()).toContain('Proceed to Annotation Review')
    expect(wrapper.find('.map-warnings').text()).toContain(mapWarning.message)
  })

  it('shows no warnings for a map without any', async () => {
    fetch.mockResolvedValue(
      answer({ success: true, matching_databases: [], cleaned_mapping: MAP, map_warnings: [] })
    )

    const wrapper = mount(AnnotationLandingView, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    await uploadMap(wrapper)

    expect(wrapper.text()).toContain('Proceed to Annotation Review')
    expect(wrapper.find('.map-warnings').exists()).toBe(false)
  })
})
