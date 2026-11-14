import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { ref } from 'vue'
import { setActivePinia, createPinia } from 'pinia'

// Uploading a semantic map on the describe landing page starts the
// describe flow over: the fields are rebuilt from the uploaded map, so the
// suggestion review marks of the previous run must go with them.

const route = { query: {}, hash: '' }
const router = { replace: vi.fn(), push: vi.fn() }

vi.mock('vue-router', () => ({ useRoute: () => route, useRouter: () => router }))
vi.mock('@/lib/browser', () => ({ loadPage: vi.fn(), reloadPage: vi.fn() }))
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

import api from '@/services/api'
import * as db from '@/lib/db'
import { loadPage } from '@/lib/browser'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'
import DescribeLandingView from '@/views/DescribeLandingView.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

function mountLanding() {
  return mount(DescribeLandingView, {
    global: { stubs: { RouterLink: RouterLinkStub } },
  })
}

const EMPTY_MAP = {
  '@context': { schema: 'mapping:schema/' },
  '@type': 'mapping:SemanticMapping',
  schema: { variables: { age_at_diagnosis: {} } },
  databases: {},
}

async function uploadMap(wrapper, map) {
  const file = new File([JSON.stringify(map)], 'mapping_template.jsonld', {
    type: 'application/ld+json',
  })
  const input = wrapper.find('input[type="file"]').element
  Object.defineProperty(input, 'files', { value: [file], configurable: true })
  await wrapper.find('input[type="file"]').trigger('change')
  await wrapper.find('form').trigger('submit')
  // The upload reads the file on a later task, then checks the map against
  // the open project and the schema: wait until it has finished (the
  // button stops saving, or its card goes once the map is in).
  await vi.waitFor(() => {
    const submit = wrapper.find('button[type="submit"]')
    expect(submit.exists() && submit.text().includes('Saving')).toBe(false)
  })
  await flushPromises()
}

describe('DescribeLandingView — uploading a semantic map', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    api.post.mockReset()
    db.saveData.mockClear()
    api.get.mockResolvedValue({ data: { exists: true } })
    api.post.mockResolvedValue({ data: { valid: true, validation_warnings: [] } })
    // Without a project, kept so: the upload goes on as it always did.
    vi.spyOn(useProjectStore(), 'planUpload').mockResolvedValue({ go: true, opened: false })
  })

  it('forgets the previous run\'s suggestion reviews', async () => {
    // The user reviewed a suggestion, went back, and uploads the same map
    // again: the field is rebuilt empty, so "reviewed" must not survive.
    const store = useSuggestionsStore()
    store.setPhase('variables')
    store.markApplied('db_age')
    store.markUserTouched('db_age')
    expect(store.isTouched('db_age')).toBe(true)

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)

    // The map was saved, then the marks were reset and persisted empty.
    expect(db.saveData).toHaveBeenCalledWith(
      'metadata',
      expect.objectContaining({ key: 'semantic_map' }),
    )
    expect(store.isTouched('db_age')).toBe(false)
    expect(store.isApplied('db_age')).toBe(false)
    const savedMarks = db.saveData.mock.calls
      .map((c) => c[1])
      .filter((row) => row.key === 'suggestion_marks_variables')
      .at(-1)
    expect(savedMarks).toMatchObject({ applied: [], touched: [] })
  })

  it('keeps the reviews when the map fails validation', async () => {
    // Nothing was replaced, so the previous run's reviews still apply.
    api.post.mockResolvedValue({ data: { valid: false, validation_errors: [{ path: '$', message: 'bad' }] } })
    const store = useSuggestionsStore()
    store.setPhase('variables')
    store.markApplied('db_age')
    store.markUserTouched('db_age')

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)

    expect(store.isTouched('db_age')).toBe(true)
  })
})

describe('DescribeLandingView — an upload and the open project', () => {
  let project

  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    api.post.mockReset()
    db.saveData.mockClear()
    api.get.mockResolvedValue({ data: { exists: true } })
    api.post.mockResolvedValue({ data: { valid: true, validation_warnings: [] } })
    project = useProjectStore()
    project.project = { name: 'AYA mapping', schemaId: 'schema:aya/v1' }
  })

  function savedMaps() {
    return db.saveData.mock.calls.map((c) => c[1]).filter((row) => row.key === 'semantic_map')
  }

  it('sets aside a map the user chose not to switch to', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: false, opened: false })

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)
    // The file is read on a later task: wait for the upload to end.
    await vi.waitFor(() => expect(useStatusStore().messages).toHaveLength(1))

    expect(project.planUpload).toHaveBeenCalledWith(EMPTY_MAP, 'mapping_template.jsonld')
    expect(api.post).not.toHaveBeenCalledWith('/api/v1/validate-mapping', expect.anything())
    expect(savedMaps()).toEqual([])
    expect(useStatusStore().messages[0].text).toBe(
      'The upload was set aside: Flyover keeps working on AYA mapping.'
    )
  })

  it('lists why the open project cannot take the map', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({
      go: false,
      opened: false,
      invalid: {
        error: 'Flyover cannot use the store the map\'s endpoint names.',
        validation_errors: [{ path: 'endpoint', message: 'no /repositories/<name> part' }],
      },
    })

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)
    await vi.waitFor(() => expect(wrapper.find('.validation-errors').exists()).toBe(true))

    expect(wrapper.find('.validation-errors').text()).toContain('no /repositories/<name> part')
    expect(savedMaps()).toEqual([])
  })

  it('keeps the opened project\'s map, cleaned of absent columns, as its working copy', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: true, storeChanged: false })
    const adopt = vi.spyOn(project, 'adoptWorkingCopy').mockResolvedValue()
    const cleaned = { ...EMPTY_MAP, databases: { a: {} } }
    api.post.mockResolvedValue({
      data: { valid: true, validation_warnings: [], cleaned_mapping: cleaned },
    })

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)
    await vi.waitFor(() => expect(adopt).toHaveBeenCalled())
    await flushPromises()

    expect(adopt).toHaveBeenCalledWith(cleaned, { fileName: 'mapping_template.jsonld' })
    expect(savedMaps()).toEqual([])
    expect(wrapper.find('button.btn-primary').attributes('disabled')).toBeUndefined()
  })

  it('stops at a project store that holds no data yet', async () => {
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: true, storeChanged: true })
    const wrapper = mountLanding()
    await flushPromises()
    api.get.mockResolvedValue({ data: { exists: false } })

    await uploadMap(wrapper, EMPTY_MAP)
    await vi.waitFor(() =>
      expect(useStatusStore().messages.at(-1)?.text).toContain('holds no data yet')
    )
    await flushPromises()

    expect(api.post).not.toHaveBeenCalledWith('/api/v1/validate-mapping', expect.anything())
    expect(wrapper.text()).toContain('No Data Found')
    expect(useStatusStore().messages.at(-1).text).toContain('holds no data yet')
  })
})

describe('DescribeLandingView — what brought the user here', () => {
  let databases

  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    router.replace.mockClear()
    route.query = {}
    route.hash = ''
    databases = ['visits', 'patients']
    api.get.mockImplementation(async (url) =>
      url === '/api/rdf-store-databases'
        ? { data: { success: true, databases } }
        : { data: { exists: true } }
    )
  })

  it('says the data came in right after an ingest, once', async () => {
    route.query = { ingest: 'done' }

    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.ingest-outcome').text()).toContain('Data uploaded successfully')
    // The address drops the word, so a reload says nothing of the ingest.
    expect(router.replace).toHaveBeenCalledWith({ query: {}, hash: '' })
  })

  it('says no new data came in when the ingest went on without any', async () => {
    route.query = { ingest: 'skipped', other: 'kept' }

    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.ingest-outcome').text()).toContain('No new data was added')
    expect(wrapper.text()).not.toContain('Data uploaded successfully')
    expect(router.replace).toHaveBeenCalledWith({ query: { other: 'kept' }, hash: '' })
  })

  it('names the datasets in the store when the user came another way', async () => {
    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.ingest-outcome').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('Data uploaded successfully')
    expect(wrapper.find('.store-datasets').text()).toBe(
      'The RDF store holds 2 datasets: patients, visits.'
    )
    expect(router.replace).not.toHaveBeenCalled()
  })

  it('counts a single dataset as one', async () => {
    databases = ['patients']

    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.store-datasets').text()).toBe('The RDF store holds 1 dataset: patients.')
  })

  it('takes an unknown word in the address for no ingest', async () => {
    route.query = { ingest: 'yes' }

    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.ingest-outcome').exists()).toBe(false)
    expect(wrapper.find('.store-datasets').exists()).toBe(true)
    expect(router.replace).toHaveBeenCalledWith({ query: {}, hash: '' })
  })

  it('claims no ingest when the store turns out to hold no data', async () => {
    route.query = { ingest: 'done' }
    api.get.mockResolvedValue({ data: { exists: false } })

    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.ingest-outcome').exists()).toBe(false)
    expect(wrapper.text()).toContain('No Data Found')
    expect(api.get).not.toHaveBeenCalledWith('/api/rdf-store-databases')
  })
})

describe('DescribeLandingView — a map the browser holds', () => {
  const MAP = {
    name: 'Centre A - AYA mapping',
    version: '1.0.0',
    schema: { '@id': 'schema:aya/v1', name: 'AYA schema', variables: {} },
    databases: {
      centre_a: {
        tables: {
          patients: { sourceFile: 'patients.csv' },
          survaya: { sourceFile: 'survaya.csv' },
        },
      },
    },
  }
  let row
  let databases
  let project

  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    route.query = {}
    route.hash = ''
    row = { key: 'semantic_map', data: MAP, filename: 'aya.jsonld' }
    databases = ['patients', 'nki_t2']
    api.get.mockImplementation(async (url) =>
      url === '/api/rdf-store-databases'
        ? { data: { success: true, databases } }
        : { data: { exists: true } }
    )
    db.getData.mockImplementation(async (_store, key) => (key === 'semantic_map' ? row : null))
    project = useProjectStore()
  })

  afterEach(() => {
    // The other blocks expect a browser without a map.
    db.getData.mockImplementation(async () => null)
    db.saveData.mockImplementation(async () => {})
  })

  async function mounted() {
    const wrapper = mountLanding()
    await flushPromises()
    return wrapper
  }

  function card(wrapper) {
    return wrapper.find('.held-map')
  }

  it("offers to go on with the project's map, against the store's datasets", async () => {
    project.project = {
      name: 'Centre A - AYA mapping',
      version: '1.0.0',
      schemaId: 'schema:aya/v1',
      fileName: 'aya.jsonld',
    }
    const wrapper = await mounted()

    expect(card(wrapper).find('.card-header').text()).toBe(
      "Continue with the project's semantic map"
    )
    expect(card(wrapper).find('.held-map-name').text()).toBe(
      'Centre A - AYA mapping · v1.0.0 (aya.jsonld)'
    )
    const line = (name) => card(wrapper).find(`.sources-${name}`).text().replace(/\s+/g, ' ')
    expect(line('ready')).toBe('1 data source ready to describe: patients')
    expect(line('missing')).toBe('Not in the RDF store: survaya.csv')
    expect(line('other')).toBe('Other data in the RDF store: nki_t2')

    await card(wrapper).find('button').trigger('click')
    expect(loadPage).toHaveBeenCalledWith('/describe/variables')
  })

  it('puts the upload of another map below it, without Skip or the old way on', async () => {
    const wrapper = await mounted()

    const upload = wrapper.find('.map-upload')
    expect(upload.find('.card-header').text()).toBe('Or upload another semantic map')
    expect(upload.text()).toContain('A map you upload here replaces the one above.')
    expect(wrapper.findAll('button').map((b) => b.text())).not.toContain('Skip')
    expect(wrapper.text()).not.toContain('Click here to describe the data')
  })

  it('names a map held without a project after itself, or its file', async () => {
    const wrapper = await mounted()
    expect(card(wrapper).find('.card-header').text()).toBe(
      'Continue with the semantic map in this browser'
    )
    expect(card(wrapper).find('.held-map-name').text()).toBe(
      'Centre A - AYA mapping · v1.0.0 (aya.jsonld)'
    )
    wrapper.unmount()

    row = { key: 'semantic_map', data: { name: 'Site map' }, filename: 'site.jsonld' }
    const unversioned = await mounted()
    expect(card(unversioned).find('.held-map-name').text()).toBe('Site map (site.jsonld)')
    unversioned.unmount()

    row = { key: 'semantic_map', data: { databases: {} }, filename: 'template.jsonld' }
    const unnamed = await mounted()
    // The file name is the name, so it is not repeated.
    expect(card(unnamed).find('.held-map-name').text()).toBe('template.jsonld')
    unnamed.unmount()

    row = { key: 'semantic_map', data: { databases: {} } }
    const anonymous = await mounted()
    expect(card(anonymous).find('.held-map-name').text()).toBe('Unnamed semantic map')
  })

  it('lets a template without data sources guide the descriptions', async () => {
    row = { key: 'semantic_map', data: { schema: { variables: {} }, databases: {} }, filename: 't.jsonld' }
    const wrapper = await mounted()

    const sources = card(wrapper).find('.held-map-sources').text()
    expect(sources).toContain('The map names no data sources yet: its schema guides the descriptions.')
    expect(card(wrapper).find('button').attributes('disabled')).toBeUndefined()
  })

  it('says so when none of the map\'s data sources is in the store, and still goes on', async () => {
    databases = ['nki_t2']
    const wrapper = await mounted()

    const sources = card(wrapper).find('.held-map-sources').text()
    expect(sources).toMatch(
      /None of the map's data sources\s+is in the RDF store \(patients\.csv, survaya\.csv\): only its schema\s+guides the descriptions\./
    )
    expect(sources).not.toContain('ready to describe')
    expect(card(wrapper).find('button').attributes('disabled')).toBeUndefined()
  })

  it('compares nothing when the store could not list its datasets', async () => {
    api.get.mockImplementation(async (url) => {
      if (url === '/api/rdf-store-databases') throw new Error('down')
      return { data: { exists: true } }
    })
    const wrapper = await mounted()

    const sources = card(wrapper).find('.held-map-sources').text()
    expect(sources).toMatch(/Flyover could not list the datasets in the\s+RDF store/)
    expect(sources).not.toContain('Not in the RDF store')
  })

  it('shows the upload and Skip as before when the browser holds no map', async () => {
    row = null
    const wrapper = await mounted()

    expect(card(wrapper).exists()).toBe(false)
    expect(wrapper.find('.map-upload .card-header').text()).toMatch(
      /Optional: Upload Flyover Semantic Map\s+Template/
    )
    const skip = wrapper.findAll('button').find((b) => b.text() === 'Skip')
    await skip.trigger('click')
    expect(wrapper.find('.map-upload').exists()).toBe(false)
    const onward = wrapper.findAll('button').find((b) => b.text().includes('Click here to describe'))
    expect(onward.attributes('disabled')).toBeUndefined()
    await onward.trigger('click')
    expect(loadPage).toHaveBeenCalledWith('/describe/variables')
  })

  it('shows an uploaded map in the card at once, and clears the form for another', async () => {
    row = null
    vi.spyOn(project, 'planUpload').mockResolvedValue({ go: true, opened: false })
    api.post.mockResolvedValue({ data: { valid: true, validation_warnings: [] } })
    db.saveData.mockImplementation(async (_store, saved) => {
      if (saved.key === 'semantic_map') row = saved
    })
    const wrapper = await mounted()

    await uploadMap(wrapper, MAP)
    await vi.waitFor(() => expect(card(wrapper).exists()).toBe(true))

    expect(card(wrapper).find('.held-map-name').text()).toBe(
      'Centre A - AYA mapping · v1.0.0 (mapping_template.jsonld)'
    )
    expect(wrapper.find('.map-upload .card-header').text()).toBe('Or upload another semantic map')
    expect(wrapper.find('#semanticMapPath').element.value).toBe('')
    expect(wrapper.findAll('button').map((b) => b.text())).not.toContain('Skip')
  })

  it('waits for the map before it shows either card', async () => {
    let release
    db.getData.mockImplementation(
      (_store, key) =>
        new Promise((resolve) => {
          release = () => resolve(key === 'semantic_map' ? row : null)
        })
    )
    const wrapper = mountLanding()
    await flushPromises()

    expect(wrapper.find('.map-upload').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('Click here to describe the data')
    release()
    await flushPromises()
    expect(card(wrapper).exists()).toBe(true)
  })
})
