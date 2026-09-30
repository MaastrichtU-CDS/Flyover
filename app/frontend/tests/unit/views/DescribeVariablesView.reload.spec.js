import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { reactive } from 'vue'
import { setActivePinia, createPinia } from 'pinia'

// Regression cover for the hard-reload bug: the "applied" suggestion marks
// persist to IndexedDB but the in-memory form state (formStateCache) does not,
// so the watch that auto-fills from suggestions must re-fill applied keys when
// their in-memory field is empty. Previously the watch skipped any key that
// was already `applied`, so on reload `unreviewedFieldCount` collapsed to 0,
// the "needs review" hint and the dismiss/clear buttons vanished, and the
// submit gate stayed closed.

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/lib/jsonld', () => ({
  loadFromIndexedDB: vi.fn(async () => {}),
  getGlobalVariableNames: vi.fn(() => ['Age', 'Biological Sex']),
  computePreselectionsForDatabases: vi.fn(() => ({
    preselectedDescriptions: {},
    preselectedDatatypes: {},
    descriptionToDatatype: {},
  })),
  updateMappingFromForm: vi.fn(async () => {}),
  getMapping: vi.fn(() => ({})),
}))

// Reactive store stub so the view's `watch(() => suggestions.variables.byKey)`
// actually fires when `init` populates the records — a static mock can't
// exercise the reconciliation path.
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
import * as jsonld from '@/lib/jsonld'
import { __setStore } from '@/stores/suggestions'
import DescribeVariablesView from '@/views/DescribeVariablesView.vue'

function createStoreStub({ appliedKeys = [], records = {} } = {}) {
  const variables = reactive({
    status: 'done',
    reason: null,
    progress: { done: 0, total: 0 },
    byKey: {},
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
    }),
    isApplied: (k) => !!applied[k],
    isTouched: (k) => !!touched[k],
    isDismissed: (k) => !!dismissed[k],
    isConfident: (r) => (r?.confidence ?? 0) >= 0.8,
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

    coachmarkSeen: { loaded: true, variables: true, values: true },

    markCoachmarkSeen: vi.fn(async () => {}),
  }
}

describe('DescribeVariablesView — reload reconciliation of applied suggestions', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    api.post.mockReset()
    jsonld.updateMappingFromForm.mockClear()
  })

  it('re-fills the form field and shows "1 suggestion needs review" after a hard reload', async () => {
    // Reload scenario: the "applied" mark for patients_age survived in
    // IndexedDB, but formStateCache is empty. The watch must restore the
    // field from the suggestion so the unreviewed count and submit gate work.
    __setStore(
      createStoreStub({
        appliedKeys: ['patients_age'],
        records: {
          patients_age: {
            status: 'done',
            item: 'age',
            match: 'age',
            display: 'Age',
            confidence: 0.95,
            reason: '',
            source: 'alias',
            tier: 1,
            alternatives: [],
          },
        },
      }),
    )
    api.get.mockResolvedValue({ data: { column_info: { patients: ['age'] } } })

    const w = mount(DescribeVariablesView)
    await flushPromises()

    // The watch restored the field from the suggestion (formStateCache is not
    // exposed, so assert via the rendered hint, which only appears when
    // unreviewedFieldCount > 0 — i.e. the applied key has a populated field).
    expect(w.text()).toContain('1 suggestion needs review')

    // The restore is display-only (WS1.1): a pre-fill restored from the
    // applied marks must not be written to the JSON-LD.
    expect(jsonld.updateMappingFromForm).not.toHaveBeenCalled()
  })

  it('does not re-fill a suggestion the user already reviewed (touched)', async () => {
    // The "applied" mark survives but the user already reviewed the field
    // (touched). The watch must skip touched keys, so the form state is not
    // auto-filled from the suggestion and no unreviewed hint renders.
    const stub = createStoreStub({
      appliedKeys: ['patients_age'],
      records: {
        patients_age: {
          status: 'done',
          item: 'age',
          match: 'age',
          display: 'Age',
          confidence: 0.95,
          reason: '',
          source: 'alias',
          tier: 1,
          alternatives: [],
        },
      },
    })
    stub.markUserTouched('patients_age') // applied && touched
    __setStore(stub)
    api.get.mockResolvedValue({ data: { column_info: { patients: ['age'] } } })

    const w = mount(DescribeVariablesView)
    await flushPromises()

    // Touched keys are excluded from unreviewedKeys and skipped by the watch,
    // so no hint renders.
    expect(w.text()).not.toContain('suggestion needs review')
  })
})
