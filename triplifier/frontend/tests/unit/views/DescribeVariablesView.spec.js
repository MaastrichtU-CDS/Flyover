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

function mountView() {
  return mount(DescribeVariablesView)
}

describe('DescribeVariablesView — description uniqueness across rows', () => {
  beforeEach(() => {
    api.get.mockReset()
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
    const leeftijdSelect = w.find('select[name="ncit_comment_synthetic_dutch_150_leeftijd"]')
    expect(leeftijdSelect.exists()).toBe(true)
    const sexOption = leeftijdSelect.find('option[value="Biological Sex"]')
    expect(sexOption.exists()).toBe(true)
    expect(sexOption.attributes('disabled')).toBeDefined()

    // Sanity check: an unrelated description stays enabled.
    const ageOption = leeftijdSelect.find('option[value="Age"]')
    expect(ageOption.exists()).toBe(true)
    expect(ageOption.attributes('disabled')).toBeUndefined()
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

    const geslachtSelect = w.find('select[name="ncit_comment_synthetic_dutch_150_geslacht"]')
    const sexOption = geslachtSelect.find('option[value="Biological Sex"]')
    expect(sexOption.attributes('disabled')).toBeUndefined()
  })
})
