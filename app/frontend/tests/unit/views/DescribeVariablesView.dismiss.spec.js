import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { reactive } from 'vue'
import { setActivePinia, createPinia } from 'pinia'

// Regression cover for the dismissal semantics: dismissing a suggestion
// must only clear fields the suggestion itself pre-filled (applied and
// never reviewed). A value the user chose manually — before or instead of
// accepting the suggestion — must survive the dismissal.

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/lib/jsonld', () => ({
  loadFromIndexedDB: vi.fn(async () => {}),
  getGlobalVariableNames: vi.fn(() => ['Age', 'Biological Sex', 'Other']),
  computePreselectionsForDatabases: vi.fn(() => ({
    preselectedDescriptions: {},
    preselectedDatatypes: {},
    descriptionToDatatype: {},
  })),
  updateMappingFromForm: vi.fn(async () => {}),
  getMapping: vi.fn(() => ({})),
}))

// Reactive store stub so the view's watchers actually fire when init
// populates the records; a static mock cannot exercise the pre-fill path.
vi.mock('@/stores/suggestions', () => {
  let _store = null
  return {
    useSuggestionsStore: () => _store,
    __setStore: (s) => {
      _store = s
    },
    SOURCE_ICONS: {
      alias: 'fa-link',
      value_regex: 'fa-table-list',
      string: 'fa-text-width',
    },
  }
})

import api from '@/services/api'
import { __setStore } from '@/stores/suggestions'
import DescribeVariablesView from '@/views/DescribeVariablesView.vue'
import SuggestionBadge from '@/components/SuggestionBadge.vue'

function createStoreStub({ appliedKeys = [], records = {} } = {}) {
  const variables = reactive({
    status: 'done',
    reason: null,
    progress: { done: 0, total: 0 },
    byKey: {},
    gaveUp: false,
  })
  const applied = reactive({})
  const touched = reactive({})
  const dismissed = reactive({})
  for (const k of appliedKeys) applied[k] = true

  return {
    enabled: true,
    compute: 'host',
    tiers: {},
    rulesVersion: null,
    variables,
    values: reactive({
      status: 'idle',
      reason: null,
      progress: { done: 0, total: 0 },
      byKey: {},
      gaveUp: false,
    }),
    isApplied: (k) => !!applied[k],
    isTouched: (k) => !!touched[k],
    isDismissed: (k) => !!dismissed[k],
    markApplied: (k) => {
      applied[k] = true
    },
    markUserTouched: (k) => {
      if (applied[k]) touched[k] = true
    },
    dismiss: (k) => {
      dismissed[k] = true
      delete applied[k]
      delete touched[k]
    },
    clearAllApplied: () => [],
    unreviewedKeys: () =>
      Object.keys(applied).filter((k) => applied[k] && !touched[k]),
    init: vi.fn(async () => {
      // Simulate refresh() landing the suggestion records.
      for (const [k, v] of Object.entries(records)) variables.byKey[k] = v
    }),
    refresh: vi.fn(async () => {}),
    startPolling: vi.fn(),
    stopPolling: vi.fn(),
    isPolling: () => false,
    bumpPriority: vi.fn(async () => {}),
    setPhase: vi.fn(),
  }
}

function ageRecord() {
  return {
    status: 'done',
    item: 'age',
    match: 'age',
    display: 'Age',
    confidence: 0.95,
    reason: '',
    source: 'alias',
    tier: 1,
    alternatives: [],
  }
}

describe('DescribeVariablesView — dismissing a suggestion keeps manual input', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    api.get.mockResolvedValue({ data: { column_info: { patients: ['age'] } } })
    api.post.mockReset()
  })

  it('clears a pre-filled field when its suggestion is dismissed', async () => {
    __setStore(
      createStoreStub({
        appliedKeys: ['patients_age'],
        records: { patients_age: ageRecord() },
      }),
    )
    const w = mount(DescribeVariablesView)
    await flushPromises()

    // The pre-fill watch restored the field from the suggestion.
    const select = w.find('select[name="ncit_comment_patients_age"]')
    expect(select.element.value).toBe('Age')

    const badge = w.findComponent(SuggestionBadge)
    expect(badge.exists()).toBe(true)
    badge.vm.$emit('dismiss')
    await flushPromises()

    expect(select.element.value).toBe('')
  })

  it('keeps a manually chosen value when its suggestion is dismissed', async () => {
    // The suggestion arrives after the user already filled the field, so
    // the pre-fill watch skips it and no applied mark exists.
    const stub = createStoreStub({})
    __setStore(stub)
    const w = mount(DescribeVariablesView)
    await flushPromises()

    const select = w.find('select[name="ncit_comment_patients_age"]')
    await select.setValue('Age')
    expect(select.element.value).toBe('Age')

    // A poll lands the suggestion record for the already-filled field.
    stub.variables.byKey['patients_age'] = ageRecord()
    await flushPromises()

    const badge = w.findComponent(SuggestionBadge)
    expect(badge.exists()).toBe(true)
    badge.vm.$emit('dismiss')
    await flushPromises()

    // The manual choice survives the dismissal.
    expect(select.element.value).toBe('Age')
  })
})
