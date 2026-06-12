import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

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
    setActivePinia(createPinia())
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

describe('DescribeVariablesView — IndexedDB sync on form changes', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
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
    const select = w.find('select[name="ncit_comment_patients_age"]')
    expect(select.exists()).toBe(true)
    await select.setValue('Age')
    await flushPromises()
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
    const select = w.find('select[name="ncit_comment_patients_age"]')
    await select.setValue('Age')
    await flushPromises()
    jsonld.updateMappingFromForm.mockClear()

    await select.setValue('')
    await flushPromises()

    expect(jsonld.updateMappingFromForm).toHaveBeenCalled()
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.patients_age).toMatchObject({
      database: 'patients',
      description: '',
    })
  })

  it('forwards a description change ("Other") so the lib can drop the stale column', async () => {
    const w = await mountReady()
    const select = w.find('select[name="ncit_comment_patients_age"]')
    await select.setValue('Age')
    await flushPromises()
    jsonld.updateMappingFromForm.mockClear()

    await select.setValue('Other')
    await flushPromises()

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
    const sexSelect = w.find('select[name="ncit_comment_patients_sex"]')
    await sexSelect.setValue('Biological Sex')
    await flushPromises()
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(Object.keys(payload)).toEqual(['patients_sex'])
  })
})

describe('DescribeVariablesView — JSON-LD/CSV mismatch handling', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    jsonld.computePreselectionsForDatabases.mockReset()
  })

  it('does not disable a global option when the only preselection points at a non-existent column', async () => {
    // Bug 2: when the JSON-LD references a localColumn that does not exist in
    // the loaded CSV, that preselection used to count as "used" and disabled
    // the matching global-variable option for every real column.
    api.get.mockResolvedValue({
      data: { column_info: { patients: ['age', 'sex'] } },
    })
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      // ghost: 'patients_geslacht' is not in column_info above
      preselectedDescriptions: { patients_geslacht: 'Biological Sex' },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    })

    const w = mountView()
    await flushPromises()

    const sexSelect = w.find('select[name="ncit_comment_patients_sex"]')
    expect(sexSelect.exists()).toBe(true)
    const sexOption = sexSelect.find('option[value="Biological Sex"]')
    expect(sexOption.exists()).toBe(true)
    expect(sexOption.attributes('disabled')).toBeUndefined()
  })

  it('drops orphan preselections at mount and emits a status warning', async () => {
    api.get.mockResolvedValue({
      data: { column_info: { patients: ['age', 'sex'] } },
    })
    jsonld.computePreselectionsForDatabases.mockReturnValue({
      preselectedDescriptions: {
        patients_age: 'Age',
        patients_ghost: 'Biological Sex',
      },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    })

    const { useStatusStore } = await import('@/stores/status.js')
    const status = useStatusStore()

    mountView()
    await flushPromises()

    const warnings = status.messages.filter((m) => m.level === 'warning')
    expect(warnings.length).toBe(1)
    expect(warnings[0].text).toContain('patients_ghost')
    // Real preselection survives, ghost does not
    expect(warnings[0].text).not.toContain('patients_age')
  })
})
