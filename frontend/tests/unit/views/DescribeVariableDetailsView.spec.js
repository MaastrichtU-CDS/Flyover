import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/lib/jsonld', () => ({
  loadFromIndexedDB: vi.fn(async () => {}),
  getCategoryOptionsForVariable: vi.fn(() => []),
  getLocalMappingsForVariable: vi.fn(() => ({})),
  updateCategoryMapping: vi.fn(async () => {}),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'
import DescribeVariableDetailsView from '@/views/DescribeVariableDetailsView.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

function mountView() {
  return mount(DescribeVariableDetailsView, {
    global: { stubs: { RouterLink: RouterLinkStub } },
  })
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
    api.get.mockReset()
    db.saveData.mockClear()
    jsonld.loadFromIndexedDB.mockClear()
    jsonld.updateCategoryMapping.mockClear()
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

    const selects = w.findAll('select.category-select')
    expect(selects.length).toBeGreaterThan(0)
    await selects[0].setValue('Male')

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
