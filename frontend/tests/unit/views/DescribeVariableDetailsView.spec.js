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
  formatToTitleCase: vi.fn((s) =>
    s == null ? '' : String(s).charAt(0).toUpperCase() + String(s).slice(1).replace(/_/g, ' ')
  ),
  getCategoryOptionsForVariable: vi.fn(() => []),
  getLocalMappingsForVariable: vi.fn(() => ({})),
  updateCategoryMapping: vi.fn(async () => {}),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'
import DescribeVariableDetailsView from '@/views/DescribeVariableDetailsView.vue'
import SearchableSelect from '@/components/SearchableSelect.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

let pinia

function mountView() {
  return mount(DescribeVariableDetailsView, {
    global: { plugins: [pinia], stubs: { RouterLink: RouterLinkStub } },
  })
}

// Category dropdowns are SearchableSelect components; locate by form name.
function findCategorySelect(w, name) {
  return w
    .findAllComponents(SearchableSelect)
    .find((c) => c.props('name') === name)
}

async function setCategory(w, name, value) {
  const component = findCategorySelect(w, name)
  component.vm.$emit('update:modelValue', value)
  component.vm.$emit('change', value)
  await flushPromises()
}

const STATE = {
  descriptive_info: {
    patients: { age: { type: 'continuous' }, sex: { type: 'categorical' } },
  },
  descriptive_info_details: {
    patients: [
      'Age',
      { Sex: [{ value: 'M', count: 80 }, { value: 'F', count: 70 }] },
    ],
  },
  preselected_values: {},
}

describe('Frontend unit: DescribeVariableDetailsView', () => {
  beforeEach(() => {
    pinia = createPinia()
    setActivePinia(pinia)
    api.get.mockReset()
    api.post.mockReset()
    db.saveData.mockClear()
    jsonld.loadFromIndexedDB.mockClear()
    jsonld.updateCategoryMapping.mockClear()
    jsonld.getCategoryOptionsForVariable.mockReturnValue([])
    jsonld.getLocalMappingsForVariable.mockReturnValue({})
  })

  it('renders the form posting to /end', async () => {
    api.get.mockResolvedValue({ data: STATE })
    const w = mountView()
    await flushPromises()
    const form = w.find('form')
    expect(form.attributes('action')).toBe('/end')
    expect(form.attributes('method')).toBe('POST')
  })

  it('hydrates from the state endpoint and seeds IndexedDB on mount', async () => {
    api.get.mockResolvedValue({ data: STATE })
    mountView()
    await flushPromises()
    expect(api.get).toHaveBeenCalledWith('/api/v1/describe-variable-details-state')
    const saveKeys = db.saveData.mock.calls.map((c) => c[1].key)
    expect(saveKeys).toEqual(['descriptive_info', 'descriptive_info_details'])
    expect(jsonld.loadFromIndexedDB).toHaveBeenCalled()
  })

  it('renders one section per database with its variables', async () => {
    api.get.mockResolvedValue({ data: STATE })
    const w = mountView()
    await flushPromises()
    expect(w.findAll('.database-section')).toHaveLength(1)
    expect(w.text()).toContain('patients')
    expect(w.text()).toContain('Age')
    expect(w.text()).toContain('Sex')
  })

  it('writes updated continuous unit on submit', async () => {
    api.get.mockResolvedValue({ data: STATE })
    const w = mountView()
    await flushPromises()
    db.saveData.mockClear()

    const unitInput = w.find('input[name="patients_age"]')
    await unitInput.setValue('years')

    await w.find('form').trigger('submit.prevent')
    await flushPromises()

    expect(db.saveData).toHaveBeenCalled()
    const lastCall = db.saveData.mock.calls.at(-1)
    expect(lastCall[0]).toBe('metadata')
    expect(lastCall[1].key).toBe('descriptive_info')
    expect(lastCall[1].data.patients.age.units).toBe('years')
  })

  it('calls jsonld.updateCategoryMapping with correct args on category change', async () => {
    api.get.mockResolvedValue({ data: STATE })
    const w = mountView()
    await flushPromises()

    expect(findCategorySelect(w, 'patients_sex_category_"M"')).toBeTruthy()
    await setCategory(w, 'patients_sex_category_"M"', 'Male')

    expect(jsonld.updateCategoryMapping).toHaveBeenCalledTimes(1)
    const args = jsonld.updateCategoryMapping.mock.calls[0]
    expect(args[0]).toBe('patients') // database
    expect(args[1]).toBe('sex') // localVariable
    expect(args[2]).toBe('sex') // globalVarName
    expect(args[3]).toBe('M') // categoryValue
    expect(args[4]).toBe('Male') // selected
  })

  it('does not crash when /api/v1/describe-variable-details-state rejects', async () => {
    api.get.mockRejectedValue(new Error('boom'))
    const w = mountView()
    await flushPromises()
    expect(w.find('form').exists()).toBe(true)
    expect(w.findAll('.database-section')).toHaveLength(0)
  })
})

describe('Frontend unit: DescribeVariableDetailsView — LLM suggestion merging', () => {
  const SEX_SUGGESTION = {
    patients: {
      sex: {
        status: 'done',
        variable_key: 'biological_sex',
        values: {
          M: { term_key: 'male', confidence: 0.95, reason: 'M abbreviates male' },
        },
      },
    },
  }

  beforeEach(() => {
    pinia = createPinia()
    setActivePinia(pinia)
    api.get.mockReset()
    api.post.mockReset()
    db.saveData.mockClear()
    db.getData.mockResolvedValue(null)
    jsonld.updateCategoryMapping.mockClear()
    jsonld.getCategoryOptionsForVariable.mockReturnValue(['Male', 'Female'])
    jsonld.getLocalMappingsForVariable.mockReturnValue({})
  })

  function wireApi({ suggestions = {}, enabled = true, status = 'done' } = {}) {
    api.get.mockImplementation(async (url) => {
      if (url === '/api/v1/llm/status') {
        return { data: { enabled, model: 'llama3.2:3b', ollama: 'ready' } }
      }
      if (url === '/api/v1/llm/suggestions/values') {
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
      return { data: STATE }
    })
    api.post.mockResolvedValue({ data: { status: 'started' } })
  }

  it('applies an arriving suggestion through updateCategoryMapping', async () => {
    wireApi({ suggestions: SEX_SUGGESTION })
    const w = mountView()
    await flushPromises()

    const select = findCategorySelect(w, 'patients_sex_category_"M"')
    expect(select.props('modelValue')).toBe('Male')
    expect(w.find('.llm-badge').exists()).toBe(true)
    expect(w.find('.llm-badge').text()).toContain('95%')

    const args = jsonld.updateCategoryMapping.mock.calls.at(-1)
    expect(args.slice(0, 5)).toEqual(['patients', 'sex', 'sex', 'M', 'Male'])
  })

  it('does not overwrite a mapping the JSON-LD already preseeds', async () => {
    jsonld.getLocalMappingsForVariable.mockReturnValue({ female: ['M'] })
    wireApi({ suggestions: SEX_SUGGESTION })
    const w = mountView()
    await flushPromises()

    const select = findCategorySelect(w, 'patients_sex_category_"M"')
    expect(select.props('modelValue')).toBe('Female')
    expect(w.find('.llm-badge').exists()).toBe(false)
  })

  it('skips suggestions whose term is not among the variable options', async () => {
    jsonld.getCategoryOptionsForVariable.mockReturnValue(['Yes', 'No'])
    wireApi({ suggestions: SEX_SUGGESTION })
    const w = mountView()
    await flushPromises()

    const select = findCategorySelect(w, 'patients_sex_category_"M"')
    expect(select.props('modelValue')).toBe('')
  })

  it('dismissing clears the selection and blocks re-application', async () => {
    wireApi({ suggestions: SEX_SUGGESTION })
    const w = mountView()
    await flushPromises()

    await w.find('.llm-dismiss').trigger('click')
    await flushPromises()

    expect(findCategorySelect(w, 'patients_sex_category_"M"').props('modelValue')).toBe('')

    const { useSuggestionsStore } = await import('@/stores/suggestions.js')
    await useSuggestionsStore().refresh('values')
    await flushPromises()
    expect(findCategorySelect(w, 'patients_sex_category_"M"').props('modelValue')).toBe('')
  })

  it('renders zero LLM UI when the feature is disabled', async () => {
    wireApi({ enabled: false })
    const w = mountView()
    await flushPromises()

    expect(w.find('.llm-status-bar').exists()).toBe(false)
    const started = api.post.mock.calls.some(([url]) => url.includes('/start'))
    expect(started).toBe(false)
  })
})
