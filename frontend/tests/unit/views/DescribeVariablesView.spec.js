import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/lib/jsonld', () => ({
  loadFromIndexedDB: vi.fn(async () => {}),
  getMapping: vi.fn(() => null),
  formatToTitleCase: vi.fn((s) =>
    s == null ? '' : String(s).charAt(0).toUpperCase() + String(s).slice(1).replace(/_/g, ' ')
  ),
  getGlobalVariableNames: vi.fn(() => ['Biological Sex', 'Age', 'Other']),
  computePreselectionsForDatabases: vi.fn(() => ({
    preselectedDescriptions: {},
    preselectedDatatypes: {},
    descriptionToDatatype: {},
  })),
  updateMappingFromForm: vi.fn(async () => {}),
}))

import api from '@/services/api'
import * as jsonld from '@/lib/jsonld'
import DescribeVariablesView from '@/views/DescribeVariablesView.vue'
import SearchableSelect from '@/components/SearchableSelect.vue'

let pinia

function mountView() {
  return mount(DescribeVariablesView, { global: { plugins: [pinia] } })
}

// The description dropdown is a SearchableSelect; locate it by its form name.
function descSelect(w, name) {
  return w
    .findAllComponents(SearchableSelect)
    .find((c) => c.props('name') === name)
}

function descValue(w, name) {
  return descSelect(w, name)?.props('modelValue')
}

async function setDescription(w, name, value) {
  descSelect(w, name).vm.$emit('update:modelValue', value)
  await flushPromises()
}

describe('DescribeVariablesView — description uniqueness across rows', () => {
  beforeEach(() => {
    pinia = createPinia()
    setActivePinia(pinia)
    api.get.mockReset()
    api.post.mockReset()
    jsonld.computePreselectionsForDatabases.mockReset()
  })

  it('disables a preselected description on other rows in the same database when the dbName contains underscores', async () => {
    // Regression: selectedDescriptionsByDb used `key.split('_')[0]` to derive
    // the database name from a preselection key, truncating multi-segment names
    // like "synthetic_dutch_150" → "synthetic". That bucketed preselections
    // under the wrong key, so isDescriptionDisabled always returned false and
    // users could assign the same description to multiple variables.
    api.get.mockResolvedValue({
      data: { column_info: { synthetic_dutch_150: ['geslacht', 'leeftijd'] } },
    })
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      preselectedDescriptions: { synthetic_dutch_150_geslacht: 'Biological Sex' },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    })

    const w = mountView()
    await flushPromises()

    // The leeftijd select should NOT offer "Biological Sex" — it's already
    // bound to geslacht.
    const leeftijd = descSelect(w, 'ncit_comment_synthetic_dutch_150_leeftijd')
    expect(leeftijd).toBeTruthy()
    expect(leeftijd.props('disabledOption')('Biological Sex')).toBe(true)

    // Sanity check: an unrelated description stays enabled.
    expect(leeftijd.props('disabledOption')('Age')).toBe(false)
  })

  it('keeps the preselected description enabled on its own row', async () => {
    api.get.mockResolvedValue({
      data: { column_info: { synthetic_dutch_150: ['geslacht', 'leeftijd'] } },
    })
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      preselectedDescriptions: { synthetic_dutch_150_geslacht: 'Biological Sex' },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    })

    const w = mountView()
    await flushPromises()

    const geslacht = descSelect(w, 'ncit_comment_synthetic_dutch_150_geslacht')
    expect(geslacht.props('disabledOption')('Biological Sex')).toBe(false)
  })
})

describe('DescribeVariablesView — IndexedDB sync on form changes', () => {
  beforeEach(() => {
    pinia = createPinia()
    setActivePinia(pinia)
    api.get.mockReset()
    api.post.mockReset()
    jsonld.computePreselectionsForDatabases.mockReset()
    jsonld.updateMappingFromForm.mockReset()
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      preselectedDescriptions: {},
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    })
  })

  async function mountReady() {
    api.get.mockResolvedValue({
      data: { column_info: { patients: ['age', 'sex'] } },
    })
    const w = mountView()
    await flushPromises()
    return w
  }

  it('forwards a selection to updateMappingFromForm with a non-empty description', async () => {
    const w = await mountReady()
    expect(descSelect(w, 'ncit_comment_patients_age')).toBeTruthy()
    await setDescription(w, 'ncit_comment_patients_age', 'Age')
    expect(jsonld.updateMappingFromForm).toHaveBeenCalled()
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.patients_age).toMatchObject({
      database: 'patients',
      description: 'Age',
    })
  })

  it('forwards a deselect (description="") so the lib can purge the IDB entry', async () => {
    // Regression: previously, when the user cleared a description, the call
    // to updateMappingFromForm still happened — but the lib silently skipped
    // entries with empty descriptions, leaving the column orphaned in IDB.
    // This test guarantees the view at least sends the empty value through;
    // the lib-level tests guarantee the lib then removes it.
    const w = await mountReady()
    await setDescription(w, 'ncit_comment_patients_age', 'Age')
    jsonld.updateMappingFromForm.mockClear()

    await setDescription(w, 'ncit_comment_patients_age', '')

    expect(jsonld.updateMappingFromForm).toHaveBeenCalled()
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.patients_age).toMatchObject({
      database: 'patients',
      description: '',
    })
  })

  it('forwards a description change ("Other") so the lib can drop the stale column', async () => {
    const w = await mountReady()
    await setDescription(w, 'ncit_comment_patients_age', 'Age')
    jsonld.updateMappingFromForm.mockClear()

    await setDescription(w, 'ncit_comment_patients_age', 'Other')

    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.patients_age).toMatchObject({
      database: 'patients',
      description: 'Other',
    })
  })

  it('only sends the changed row, not the whole world, on a single edit', async () => {
    // Confirms partial-form syncs work: the lib must not blow away unrelated
    // entries in IDB just because they're not in the payload.
    const w = await mountReady()
    await setDescription(w, 'ncit_comment_patients_sex', 'Biological Sex')
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(Object.keys(payload)).toEqual(['patients_sex'])
  })
})

describe('DescribeVariablesView — LLM suggestion merging', () => {
  beforeEach(() => {
    pinia = createPinia()
    setActivePinia(pinia)
    api.get.mockReset()
    api.post.mockReset()
    jsonld.computePreselectionsForDatabases.mockReset()
    jsonld.updateMappingFromForm.mockReset()
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      preselectedDescriptions: {},
      preselectedDatatypes: {},
      descriptionToDatatype: { Age: 'continuous' },
    })
  })

  function wireApi({ suggestions = {}, status = 'done', enabled = true } = {}) {
    api.get.mockImplementation(async (url) => {
      if (url === '/api/v1/llm/status') {
        return { data: { enabled, model: 'llama3.2:3b', ollama: 'ready' } }
      }
      if (url === '/api/v1/llm/suggestions/variables') {
        return {
          data: {
            enabled,
            status,
            progress: { chunks_done: 1, chunks_total: 1 },
            error: null,
            suggestions,
          },
        }
      }
      return { data: { column_info: { patients: ['age', 'sex'] } } }
    })
    api.post.mockResolvedValue({ data: { status: 'started' } })
  }

  const AGE_SUGGESTION = {
    patients: {
      age: { status: 'done', variable_key: 'age', confidence: 0.9, reason: 'match' },
    },
  }

  it('fills an empty description and derives the datatype', async () => {
    wireApi({ suggestions: AGE_SUGGESTION })
    const w = mountView()
    await flushPromises()

    expect(descValue(w, 'ncit_comment_patients_age')).toBe('Age')
    const datatype = w.find('select[name="patients_age"]')
    expect(datatype.element.value).toBe('continuous')
    expect(w.find('.llm-badge').exists()).toBe(true)
    expect(w.find('.llm-badge').text()).toContain('90%')
  })

  it('renders zero LLM UI when the feature is disabled', async () => {
    wireApi({ enabled: false })
    const w = mountView()
    await flushPromises()

    expect(w.find('.llm-status-bar').exists()).toBe(false)
    expect(w.find('.llm-badge').exists()).toBe(false)
    const started = api.post.mock.calls.some(([url]) => url.includes('/start'))
    expect(started).toBe(false)
  })

  it('never overwrites a user-filled description', async () => {
    wireApi({ suggestions: {} })
    const w = mountView()
    await flushPromises()

    await setDescription(w, 'ncit_comment_patients_age', 'Biological Sex')

    const { useSuggestionsStore } = await import('@/stores/suggestions.js')
    const store = useSuggestionsStore()
    store.variables.byKey['patients_age'] = {
      status: 'done',
      database: 'patients',
      column: 'age',
      variableKey: 'age',
      display: 'Age',
      confidence: 0.9,
      reason: 'match',
    }
    await flushPromises()

    expect(descValue(w, 'ncit_comment_patients_age')).toBe('Biological Sex')
    expect(store.isApplied('patients_age')).toBe(false)
  })

  it('never overwrites a preselected description', async () => {
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      preselectedDescriptions: { patients_age: 'Biological Sex' },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    })
    wireApi({ suggestions: AGE_SUGGESTION })
    const w = mountView()
    await flushPromises()

    expect(descValue(w, 'ncit_comment_patients_age')).toBe('Biological Sex')
    expect(w.find('.llm-badge').exists()).toBe(false)
  })

  it('dismissing clears the field and blocks re-application', async () => {
    wireApi({ suggestions: AGE_SUGGESTION })
    const w = mountView()
    await flushPromises()

    await w.find('.llm-dismiss').trigger('click')
    expect(descValue(w, 'ncit_comment_patients_age')).toBe('')

    const { useSuggestionsStore } = await import('@/stores/suggestions.js')
    await useSuggestionsStore().refresh('variables')
    await flushPromises()
    expect(descValue(w, 'ncit_comment_patients_age')).toBe('')
    expect(w.find('.llm-badge').exists()).toBe(false)
  })

  it('editing an applied field turns the badge into a reviewed check', async () => {
    wireApi({ suggestions: AGE_SUGGESTION })
    const w = mountView()
    await flushPromises()
    expect(w.find('.llm-badge').text()).toContain('90%')

    const datatype = w.find('select[name="patients_age"]')
    await datatype.setValue('categorical')

    expect(w.find('.llm-badge').classes()).toContain('confirmed')
    expect(w.find('.llm-badge').text()).toContain('reviewed')
  })

  it('submit asks for confirmation while unreviewed AI fields exist', async () => {
    wireApi({ suggestions: AGE_SUGGESTION })
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false)
    const w = mountView()
    await flushPromises()

    const event = { preventDefault: vi.fn() }
    await w.find('form').trigger('submit', event)
    expect(confirmSpy).toHaveBeenCalledOnce()
    confirmSpy.mockRestore()
  })
})
