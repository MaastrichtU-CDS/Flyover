import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// A map uploaded on the share mock page is checked against the open project
// before the page keeps it: set aside, nothing changes; kept without a
// project, it is saved as before; opened as the project, it already is the
// project's working copy.

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn(), put: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({ refreshDataExists: vi.fn(async () => {}) }))

import * as db from '@/lib/db'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'
import ShareMockView from '@/views/ShareMockView.vue'

const MAP = {
  '@type': 'mapping:DataMapping',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema', variables: {} },
  databases: { centre_a: { tables: { patients: { columns: {} } } } },
}

async function upload(wrapper) {
  const file = new File([JSON.stringify(MAP)], 'aya.jsonld', { type: 'application/ld+json' })
  const input = wrapper.find('input[type="file"]')
  Object.defineProperty(input.element, 'files', { value: [file], configurable: true })
  await input.trigger('change')
  const button = wrapper.findAll('button').find((b) => b.text().includes('Upload JSON-LD'))
  await button.trigger('click')
}

function savedMaps() {
  return db.saveData.mock.calls.map((c) => c[1]).filter((row) => row.key === 'semantic_map')
}

describe('ShareMockView — an upload and the open project', () => {
  let project
  let wrapper

  beforeEach(async () => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    project = useProjectStore()
    project.project = { name: 'AYA mapping', schemaId: 'schema:aya/v1' }
    wrapper = mount(ShareMockView)
    await flushPromises()
  })

  afterEach(() => wrapper.unmount())

  it('keeps nothing of a map the user set aside', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: false, opened: false })

    await upload(wrapper)
    await vi.waitFor(() => expect(useStatusStore().messages).toHaveLength(1))

    expect(project.planUpload).toHaveBeenCalledWith(MAP, 'aya.jsonld')
    expect(useStatusStore().messages[0].text).toBe(
      'The upload was set aside: Flyover keeps working on AYA mapping.'
    )
    expect(savedMaps()).toEqual([])
    expect(wrapper.find('#sampleCount').exists()).toBe(false)
  })

  it('says why the open project cannot take the map', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({
      go: false,
      opened: false,
      invalid: { error: 'Flyover cannot use the store the map\'s endpoint names.' },
    })

    await upload(wrapper)
    await vi.waitFor(() => expect(useStatusStore().messages).toHaveLength(1))

    expect(useStatusStore().messages[0]).toMatchObject({
      level: 'error',
      text: "This map cannot be opened as a project: Flyover cannot use the store the map's endpoint names.",
    })
    expect(savedMaps()).toEqual([])
  })

  it('saves the map as before when it stays without a project', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: false })

    await upload(wrapper)
    await vi.waitFor(() => expect(savedMaps()).toHaveLength(1))

    expect(savedMaps()[0].data).toEqual(MAP)
    await vi.waitFor(() => expect(wrapper.find('#sampleCount').exists()).toBe(true))
  })

  it('leaves an opened project\'s map where opening put it', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: true })

    await upload(wrapper)
    await vi.waitFor(() => expect(wrapper.find('#sampleCount').exists()).toBe(true))

    expect(savedMaps()).toEqual([])
  })
})
