import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// ---------------------------------------------------------------------------
// Zero JSON-LD writes without an explicit review (WS1).
//
// The plan invariant: a suggestion becomes a mapping only through an
// explicit accept in the UI. The pre-fill watch may set the dropdown for
// display, but nothing may reach the JSON-LD writers
// (updateMappingFromForm / updateCategoryMapping) until the user reviews
// the field — and an accept must change only the accepted column.
//
// These tests run against the REAL Pinia suggestions store (only `api`
// and `db` are mocked) so records arrive through refresh() and the
// views' pre-fill watchers actually fire. The previous version mocked the
// whole store as a plain object, which was not reactive: the watcher
// never ran and the test passed vacuously.
// ---------------------------------------------------------------------------

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

// Keep the real jsonld implementation (the store and the views use many of
// its helpers) but spy on the two writers the invariant is about.
vi.mock('@/lib/jsonld', async (importOriginal) => {
  const actual = await importOriginal()
  return {
    ...actual,
    updateMappingFromForm: vi.fn(actual.updateMappingFromForm),
    updateCategoryMapping: vi.fn(actual.updateCategoryMapping),
  }
})

import api from '@/services/api'
import * as jsonld from '@/lib/jsonld'
import { useSuggestionsStore } from '@/stores/suggestions'
import DescribeVariablesView from '@/views/DescribeVariablesView.vue'
import DescribeVariableDetailsView from '@/views/DescribeVariableDetailsView.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

const STATUS = {
  enabled: true,
  compute: 'host',
  tiers: { 1: { state: 'active' } },
  threshold: 0.8,
  rules_version: 'test',
}

const VARIABLES_SNAPSHOT = {
  status: 'done',
  fingerprint: 'fp-variables-1',
  progress: { done: 2, total: 2 },
  error: null,
  records: {
    test_db_morph: {
      status: 'done',
      item: 'morph',
      match: 'tumour_morphology_icd_o',
      confidence: 0.92,
      reason: "Alias: column 'morph' in database 'christie'",
      source: 'alias',
      tier: 1,
      database: 'test_db',
      column: 'morph',
    },
    test_db_sex: {
      status: 'done',
      item: 'sex',
      match: 'biological_sex',
      confidence: 0.9,
      reason: "Alias: column 'sex' in database 'christie'",
      source: 'alias',
      tier: 1,
      database: 'test_db',
      column: 'sex',
    },
  },
}

const DETAILS_STATE = {
  descriptive_info: {
    patients: { sex: { type: 'categorical' } },
  },
  descriptive_info_details: {
    patients: [{ Sex: [{ value: 'M', count: 80 }, { value: 'F', count: 70 }] }],
  },
  preselected_values: {},
}

const VALUES_SNAPSHOT = {
  status: 'done',
  fingerprint: 'fp-values-1',
  progress: { done: 2, total: 2 },
  error: null,
  records: {
    patients_sex_M: {
      status: 'done',
      item: 'M',
      match: 'male',
      confidence: 0.95,
      reason: "Alias: value 'M' in database 'christie'",
      source: 'alias',
      tier: 1,
      database: 'patients',
      column: 'sex',
      value: 'M',
    },
    patients_sex_F: {
      status: 'done',
      item: 'F',
      match: 'female',
      confidence: 0.95,
      reason: "Alias: value 'F' in database 'christie'",
      source: 'alias',
      tier: 1,
      database: 'patients',
      column: 'sex',
      value: 'F',
    },
  },
}

// Route api.get by URL so the store's status/poll calls and the views'
// state calls can coexist in one mock.
function mockApiRoutes(routes) {
  api.get.mockImplementation(async (url) => {
    for (const [prefix, response] of routes) {
      if (url === prefix) return response
    }
    return { data: {} }
  })
  api.post.mockResolvedValue({ data: { status: 'started' } })
}

beforeEach(() => {
  setActivePinia(createPinia())
  api.get.mockReset()
  api.post.mockReset()
  jsonld.updateMappingFromForm.mockClear()
  jsonld.updateCategoryMapping.mockClear()
})

describe('DescribeVariablesView — zero writes without explicit review', () => {
  beforeEach(() => {
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: VARIABLES_SNAPSHOT }],
    ])
  })

  it('pre-fills the dropdown for display but never calls updateMappingFromForm', async () => {
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    // The real store landed the records and the pre-fill watch fired: both
    // fields are applied-but-unreviewed, so the review hint shows.
    const store = useSuggestionsStore()
    expect(store.isApplied('test_db_morph')).toBe(true)
    expect(store.isApplied('test_db_sex')).toBe(true)
    expect(wrapper.text()).toContain('2 suggestions need review')

    // Nothing was written: no user review happened, so the JSON-LD writer
    // must not have been called at all.
    expect(jsonld.updateMappingFromForm).not.toHaveBeenCalled()
  })

  it('persists only the accepted column when the user accepts one suggestion', async () => {
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges.length).toBe(2)

    // Accept the suggestion for 'morph' (first column in display order).
    await badges[0].vm.$emit('accept')
    await flushPromises()

    // Exactly one write, and its payload carries only the reviewed column.
    expect(jsonld.updateMappingFromForm).toHaveBeenCalledTimes(1)
    const payload = jsonld.updateMappingFromForm.mock.calls[0][0]
    expect(payload.test_db_morph?.description).toBeTruthy()
    expect(payload.test_db_sex).toBeUndefined()

    // The explicit accept also marks the field reviewed (WS1.4): it must
    // no longer count as unreviewed.
    const store = useSuggestionsStore()
    expect(store.isTouched('test_db_morph')).toBe(true)
    expect(store.unreviewedKeys()).toEqual(['test_db_sex'])
  })

  it('restores an applied suggestion after a reload without writing it', async () => {
    // Reload scenario: the applied marks survived in IndexedDB, the
    // in-memory form state did not. Simulate the persisted marks on the
    // real store before mounting.
    const store = useSuggestionsStore()
    store.setPhase('variables')
    store.markApplied('test_db_morph')
    store.markUserTouched('test_db_morph')

    mount(DescribeVariablesView)
    await flushPromises()

    // The reviewed value is restored into the form state for display, but
    // a value the user never reviewed in this session is not re-written.
    expect(jsonld.updateMappingFromForm).not.toHaveBeenCalled()
  })

  it('applying an alternative writes that column through the same accept path', async () => {
    // Give the morph record one real alternative (a different match);
    // null matches and duplicates of the winner must not be offered.
    const withAlt = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    withAlt.records.test_db_morph.alternatives = [
      { match: 'year_of_initial_diagnosis', confidence: 0.7, source: 'string', tier: 1 },
      { match: null, confidence: 0.0, source: 'string', tier: 1 },
    ]
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: withAlt }],
    ])

    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const badge = wrapper.findAllComponents({ name: 'SuggestionBadge' })[0]
    await badge.vm.$emit('apply-alternative', {
      match: 'year_of_initial_diagnosis',
      confidence: 0.7,
      source: 'string',
      tier: 1,
    })
    await flushPromises()

    // The alternative's match was applied to the morph column through the
    // accept path and marked reviewed — a write the user explicitly made.
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.test_db_morph?.description).toBe('Year of initial diagnosis')
    const store = useSuggestionsStore()
    expect(store.isTouched('test_db_morph')).toBe(true)
  })
})

describe('DescribeVariableDetailsView — zero writes without explicit review', () => {
  beforeEach(() => {
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: DETAILS_STATE }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: VALUES_SNAPSHOT }],
    ])
  })

  function mountDetails() {
    return mount(DescribeVariableDetailsView, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
  }

  it('pre-fills the category dropdown for display but never calls updateCategoryMapping', async () => {
    const wrapper = mountDetails()
    await flushPromises()

    // The real store landed the records and the pre-fill watch fired.
    const store = useSuggestionsStore()
    expect(store.isApplied('patients_sex_M')).toBe(true)
    expect(store.isApplied('patients_sex_F')).toBe(true)
    expect(wrapper.vm.categorySelections.patients_sex_M).toBe('Male')
    expect(wrapper.vm.categorySelections.patients_sex_F).toBe('Female')

    // Nothing was written: category selections pre-filled by suggestions
    // are display-only until reviewed.
    expect(jsonld.updateCategoryMapping).not.toHaveBeenCalled()
  })

  it('persists only the accepted value when the user accepts one suggestion', async () => {
    const wrapper = mountDetails()
    await flushPromises()

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges.length).toBe(2)

    await badges[0].vm.$emit('accept')
    await flushPromises()

    expect(jsonld.updateCategoryMapping).toHaveBeenCalledTimes(1)
    const args = jsonld.updateCategoryMapping.mock.calls[0]
    expect(args[3]).toBe('M') // categoryValue
    expect(args[4]).toBe('Male') // selected option

    // The accepted value is reviewed; the other is still display-only.
    const store = useSuggestionsStore()
    expect(store.isTouched('patients_sex_M')).toBe(true)
    expect(store.unreviewedKeys()).toEqual(['patients_sex_F'])
  })

  it('does not call updateCategoryMapping when dismissing a never-persisted pre-fill', async () => {
    const wrapper = mountDetails()
    await flushPromises()

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    await badges[0].vm.$emit('dismiss')
    await flushPromises()

    // Dismissing a pre-fill that was never written must not invoke the
    // category writer with a previousOption that was never persisted.
    expect(jsonld.updateCategoryMapping).not.toHaveBeenCalled()
    const store = useSuggestionsStore()
    expect(store.isDismissed('patients_sex_M')).toBe(true)
    // The dismissed pill is gone; the other value's pill remains.
    expect(wrapper.findAllComponents({ name: 'SuggestionBadge' })).toHaveLength(
      badges.length - 1,
    )
  })
})
