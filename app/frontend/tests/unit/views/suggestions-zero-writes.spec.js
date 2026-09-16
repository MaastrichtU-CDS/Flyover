import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// ---------------------------------------------------------------------------
// Zero JSON-LD writes without an explicit accept.
//
// The plan requires: "a Vitest test loads suggestions, asserts
// jsonld.getMapping() unchanged, accepts one, asserts only that column
// changed." This test mounts DescribeVariablesView with the suggestions
// store enabled and arriving suggestions, and asserts that the JSON-LD
// writer (updateMappingFromForm) is NOT called until the user explicitly
// accepts a suggestion via the badge.
// ---------------------------------------------------------------------------

// Use vi.hoisted so the mock factory closures can reference these without
// the "Cannot access X before initialization" hoisting error.
const { updateMappingFromForm, getMapping, store } = vi.hoisted(() => {
  const store = {
    enabled: true,
    variables: {
      status: 'done',
      reason: null,
      progress: { done: 2, total: 2 },
      byKey: {
        test_db_morph: {
          status: 'done',
          item: 'morph',
          match: 'tumour_morphology_icd_o',
          display: 'Tumour Morphology ICD-O',
          confidence: 0.92,
          reason: 'Alias hit from christie',
          source: 'alias',
          tier: 1,
          alternatives: [],
        },
        test_db_sex: {
          status: 'done',
          item: 'sex',
          match: 'biological_sex',
          display: 'Biological Sex',
          confidence: 0.9,
          reason: 'Alias hit from christie',
          source: 'alias',
          tier: 1,
          alternatives: [],
        },
      },
    },
    values: { status: 'idle', byKey: {}, progress: { done: 0, total: 0 } },
    tiers: { 1: { state: 'active' }, 2: { state: 'inactive', reason: 'not enabled' } },
    compute: 'host',
    _applied: {},
    _touched: {},
    _dismissed: {},
    isApplied(key) {
      return !!this._applied[key]
    },
    isTouched(key) {
      return !!this._touched[key]
    },
    isDismissed(key) {
      return !!this._dismissed[key]
    },
    markApplied(key) {
      this._applied[key] = true
    },
    markUserTouched(key) {
      if (this._applied[key]) this._touched[key] = true
    },
    dismiss(key) {
      this._dismissed[key] = true
      delete this._applied[key]
      delete this._touched[key]
    },
    clearAllApplied() {
      const c = Object.keys(this._applied)
      this._applied = {}
      this._touched = {}
      return c
    },
    unreviewedKeys() {
      return Object.keys(this._applied).filter((k) => this._applied[k] && !this._touched[k])
    },
    init: vi.fn(async () => {}),
    refresh: vi.fn(async () => {}),
    startPolling: vi.fn(),
    stopPolling: vi.fn(),
    isPolling: () => false,
    bumpPriority: vi.fn(async () => {}),
    setPhase: vi.fn(),
  }
  return {
    updateMappingFromForm: vi.fn(async () => {}),
    getMapping: vi.fn(() => ({
      test_db_morph: { description: 'Tumour Morphology ICD-O' },
    })),
    store,
  }
})

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/lib/jsonld', () => ({
  loadFromIndexedDB: vi.fn(async () => {}),
  getGlobalVariableNames: vi.fn(() => ['Tumour Morphology ICD-O', 'Biological Sex']),
  computePreselectionsForDatabases: vi.fn(() => ({
    preselectedDescriptions: {},
    preselectedDatatypes: {},
    descriptionToDatatype: {},
  })),
  updateMappingFromForm,
  getMapping,
}))

vi.mock('@/stores/suggestions', () => ({
  useSuggestionsStore: () => store,
  SOURCE_ICONS: { alias: 'fa-link', value_regex: 'fa-table-list', string: 'fa-text-width' },
}))

import api from '@/services/api'
import DescribeVariablesView from '@/views/DescribeVariablesView.vue'

describe('DescribeVariablesView — zero writes without explicit accept', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    api.post.mockReset()
    updateMappingFromForm.mockClear()
    store._applied = {}
    store._touched = {}
    store._dismissed = {}
  })

  it('does not write to JSON-LD when suggestions arrive but are not accepted', async () => {
    api.get.mockResolvedValue({
      data: { column_info: { test_db: ['morph', 'sex'] } },
    })

    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    // Suggestions have arrived (store is pre-populated), but the user has not
    // clicked any badge. The JSON-LD writer must not have been called.
    expect(updateMappingFromForm).not.toHaveBeenCalled()

    // The suggestion badge should be visible (suggestion is done and has display).
    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges.length).toBeGreaterThan(0)
  })

  it('writes to JSON-LD only for the accepted column when the user accepts one suggestion', async () => {
    api.get.mockResolvedValue({
      data: { column_info: { test_db: ['morph', 'sex'] } },
    })

    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    // Clear any calls from mount lifecycle.
    updateMappingFromForm.mockClear()

    // Accept the suggestion for 'morph' by triggering the badge accept event.
    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges.length).toBe(2)

    // The first badge is for 'morph' (first column in the page).
    await badges[0].vm.$emit('accept')
    await flushPromises()

    // Now updateMappingFromForm should have been called exactly once.
    expect(updateMappingFromForm).toHaveBeenCalledTimes(1)

    // The call should include the morph column's form state, not sex's.
    const callArg = updateMappingFromForm.mock.calls[0][0]
    expect(callArg['test_db_morph']).toBeDefined()
    expect(callArg['test_db_morph'].description).toBe('Tumour Morphology ICD-O')
    // sex should not have a description set by the accept.
    expect(callArg['test_db_sex']?.description).toBeFalsy()
  })
})
