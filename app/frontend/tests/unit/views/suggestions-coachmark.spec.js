import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

// ---------------------------------------------------------------------------
// First-visit cue on the describe pages (WS2).
//
// Runs against the real Pinia suggestions store (only api and db are
// mocked) so records arrive through refresh(), the pre-fill watchers
// fire, and the coachmark targeting logic behaves as in the app.
//
// Covered: shows once, anchored to the first unreviewed pill; the header
// variant with the "expand to review" copy while collapsed; not shown
// when suggestions are disabled, when there are no suggestions, or once
// the phase flag is seen; Got it / Escape / accepting a pill close it
// and persist the flag; the status-bar link reopens it.
// ---------------------------------------------------------------------------

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

// The browser's semantic map in IndexedDB: suggestions need one with
// variables, and a suggestion only reaches a dropdown that offers it.
const { SEMANTIC_MAP } = vi.hoisted(() => ({
  SEMANTIC_MAP: {
    '@context': { schema: 'mapping:schema/', mapping: 'http://example.org/mapping#' },
    '@id': 'mapping:root',
    '@type': 'mapping:SemanticMapping',
    schema: {
      '@id': 'schema:root',
      '@type': 'mapping:Schema',
      variables: {
        tumour_morphology_icd_o: { dataType: 'standardised' },
        biological_sex: { dataType: 'categorical' },
        year_of_initial_diagnosis: { dataType: 'continuous' },
      },
    },
    databases: {},
  },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/lib/jsonld', async (importOriginal) => {
  const actual = await importOriginal()
  return {
    ...actual,
    updateMappingFromForm: vi.fn(actual.updateMappingFromForm),
    updateCategoryMapping: vi.fn(actual.updateCategoryMapping),
  }
})

import api from '@/services/api'
import * as db from '@/lib/db'
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
      reason: 'Alias hit',
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
      reason: 'Alias hit',
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
      reason: 'Alias hit',
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
      reason: 'Alias hit',
      source: 'alias',
      tier: 1,
      database: 'patients',
      column: 'sex',
      value: 'F',
    },
  },
}

function mockApiRoutes(routes) {
  api.get.mockImplementation(async (url) => {
    for (const [prefix, response] of routes) {
      if (url === prefix) return response
    }
    return { data: {} }
  })
  api.post.mockResolvedValue({ data: { status: 'started' } })
}

function variablesRoutes(snapshot = VARIABLES_SNAPSHOT) {
  mockApiRoutes([
    ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
    ['/api/v1/suggestions/status', { data: STATUS }],
    ['/api/v1/suggestions/variables', { data: snapshot }],
  ])
}

function valuesRoutes(snapshot = VALUES_SNAPSHOT) {
  mockApiRoutes([
    ['/api/v1/describe-variable-details-state', { data: DETAILS_STATE }],
    ['/api/v1/suggestions/status', { data: STATUS }],
    ['/api/v1/suggestions/values', { data: snapshot }],
  ])
}

// IndexedDB reads: the semantic map plus any extra keys a test needs.
function idb(extra = {}) {
  return async (_store, key) => {
    if (key === 'semantic_map') return { data: structuredClone(SEMANTIC_MAP) }
    return extra[key] ?? null
  }
}

function singleCallout(wrapper) {
  const callouts = wrapper.findAllComponents({ name: 'SuggestionCoachmark' })
  expect(callouts.length).toBe(1)
  return callouts[0]
}

beforeEach(() => {
  setActivePinia(createPinia())
  api.get.mockReset()
  api.post.mockReset()
  db.saveData.mockClear()
  db.getData.mockReset()
  db.getData.mockImplementation(idb())
})

describe('First-visit cue — DescribeVariablesView', () => {
  it('anchors to the first database header with the expand copy while collapsed', async () => {
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()

    const callout = singleCallout(w)
    // Anchored to the database heading, not to a (hidden) pill.
    expect(callout.element.closest('.database-heading')).toBeTruthy()
    expect(callout.text()).toContain('Suggestions to review')
    expect(callout.text()).toContain('2 suggested columns — expand to review.')
  })

  it('moves to the first unreviewed pill once the database is expanded, with the field copy', async () => {
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()
    singleCallout(w) // header variant while collapsed

    await w.find('.toggle-button').trigger('click')
    await flushPromises()

    const callout = singleCallout(w)
    expect(callout.element.closest('.database-heading')).toBeFalsy()
    expect(callout.element.closest('.variable-row')).toBeTruthy()
    expect(callout.text()).toContain('Check this suggestion')
    expect(callout.text()).toContain('pre-filled this field')
    expect(callout.text()).toContain('nothing is saved until you do')
    // Anchored to the first pill in display order (morph): the callout
    // lives in the same row as the morph badge.
    const row = callout.element.closest('.variable-row')
    expect(row.querySelector('.suggestion-badge')).toBeTruthy()
  })

  it('anchors to a pre-filled pill, skipping a low-confidence hint', async () => {
    // morph is below the threshold: a hint, not pre-filled. The copy says
    // Flyover filled the field in, so the callout goes to sex instead.
    const weak = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    weak.records.test_db_morph.confidence = 0.55
    variablesRoutes(weak)
    const w = mount(DescribeVariablesView)
    await flushPromises()
    await w.find('.toggle-button').trigger('click')
    await flushPromises()

    const row = singleCallout(w).element.closest('.variable-row')
    expect(row.querySelector('.variable-label').textContent).toContain('sex')
  })

  it('does not show once the phase flag is seen', async () => {
    db.getData.mockImplementation(
      idb({ suggestion_coachmark_seen: { variables: true, values: true } }),
    )
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
  })

  it('does not show when suggestions are disabled', async () => {
    api.get.mockImplementation(async (url) => {
      if (url === '/api/v1/describe-variables-state')
        return { data: { column_info: { test_db: ['morph', 'sex'] } } }
      if (url === '/api/v1/suggestions/status')
        return { data: { ...STATUS, enabled: false } }
      return { data: {} }
    })
    api.post.mockResolvedValue({ data: { status: 'disabled' } })

    const w = mount(DescribeVariablesView)
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
  })

  it('does not show when there are no suggestions', async () => {
    variablesRoutes({ ...VARIABLES_SNAPSHOT, records: {} })
    const w = mount(DescribeVariablesView)
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
  })

  it('"Got it" closes the callout and persists the seen flag', async () => {
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()
    const callout = singleCallout(w)

    await callout.vm.$emit('close')
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
    expect(useSuggestionsStore().coachmarkSeen.variables).toBe(true)

    const saved = db.saveData.mock.calls.filter(
      ([storeName, rec]) =>
        storeName === 'metadata' && rec.key === 'suggestion_coachmark_seen'
    )
    expect(saved.length).toBe(1)
    expect(saved[0][1].variables).toBe(true)
  })

  it('Escape closes the callout and persists the seen flag', async () => {
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()
    singleCallout(w)

    // The callout only listens once visible (~300 ms after mount).
    await new Promise((resolve) => setTimeout(resolve, 450))
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
    expect(useSuggestionsStore().coachmarkSeen.variables).toBe(true)
  })

  it('accepting a suggestion closes the callout and persists the seen flag', async () => {
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()
    singleCallout(w)

    await w.findAllComponents({ name: 'SuggestionBadge' })[0].vm.$emit('accept')
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
    expect(useSuggestionsStore().coachmarkSeen.variables).toBe(true)
  })

  it('the status-bar link reopens the callout after it was seen', async () => {
    db.getData.mockImplementation(
      idb({ suggestion_coachmark_seen: { variables: true, values: true } }),
    )
    variablesRoutes()
    const w = mount(DescribeVariablesView)
    await flushPromises()
    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)

    await w.find('.suggestion-help-link').trigger('click')
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(1)
  })
})

describe('First-visit cue — DescribeVariableDetailsView', () => {
  function mountDetails() {
    return mount(DescribeVariableDetailsView, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
  }

  it('anchors to the database header with the values expand copy while collapsed', async () => {
    valuesRoutes()
    const w = mountDetails()
    await flushPromises()

    const callout = singleCallout(w)
    expect(callout.element.closest('.database-heading')).toBeTruthy()
    expect(callout.text()).toContain('2 suggested values — expand to review.')
  })

  it('moves to the first unreviewed pill once expanded, with the value copy', async () => {
    valuesRoutes()
    const w = mountDetails()
    await flushPromises()

    await w.find('.toggle-button').trigger('click')
    await w.find('.item-toggle-button').trigger('click')
    await flushPromises()

    const callout = singleCallout(w)
    expect(callout.element.closest('.category-item')).toBeTruthy()
    expect(callout.text()).toContain('pre-filled this value')
    expect(callout.text()).toContain('nothing is saved until you do')
  })

  it('accepting a value closes the callout and persists the seen flag', async () => {
    valuesRoutes()
    const w = mountDetails()
    await flushPromises()
    singleCallout(w)

    await w.find('.toggle-button').trigger('click')
    await w.find('.item-toggle-button').trigger('click')
    await flushPromises()

    await w.findAllComponents({ name: 'SuggestionBadge' })[0].vm.$emit('accept')
    await flushPromises()

    expect(w.findAllComponents({ name: 'SuggestionCoachmark' }).length).toBe(0)
    expect(useSuggestionsStore().coachmarkSeen.values).toBe(true)
  })
})
