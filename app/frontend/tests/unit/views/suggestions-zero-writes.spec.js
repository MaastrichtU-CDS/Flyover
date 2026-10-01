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
  getData: vi.fn(async (_store, key) =>
    key === 'semantic_map' ? { data: structuredClone(SEMANTIC_MAP) } : null,
  ),
}))

// A test can set `preselection.value` to stand in for mappings the loaded
// JSON-LD already holds for the described database.
const preselection = vi.hoisted(() => ({ value: null }))

// Keep the real jsonld implementation (the store and the views use many of
// its helpers) but spy on the two writers the invariant is about.
vi.mock('@/lib/jsonld', async (importOriginal) => {
  const actual = await importOriginal()
  return {
    ...actual,
    updateMappingFromForm: vi.fn(actual.updateMappingFromForm),
    updateCategoryMapping: vi.fn(actual.updateCategoryMapping),
    computePreselectionsForDatabases: (...args) =>
      preselection.value ?? actual.computePreselectionsForDatabases(...args),
  }
})

import api from '@/services/api'
import * as db from '@/lib/db'
import * as jsonld from '@/lib/jsonld'
import { useStatusStore } from '@/stores/status'
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

// Route api.get/api.post by URL so the store's status/poll calls, the
// views' state calls and the suggestions /start can coexist in one mock.
// The details state arrives by POST (it carries the browser's map); any
// other POST (suggestions /start) defaults to a started job.
function mockApiRoutes(routes) {
  const respond = async (url) => {
    for (const [prefix, response] of routes) {
      if (url === prefix) return response
    }
    return { data: {} }
  }
  api.get.mockImplementation(respond)
  api.post.mockImplementation(async (url) => {
    for (const [prefix, response] of routes) {
      if (url === prefix) return response
    }
    return { data: { status: 'started' } }
  })
}

beforeEach(() => {
  setActivePinia(createPinia())
  api.get.mockReset()
  api.post.mockReset()
  jsonld.updateMappingFromForm.mockClear()
  jsonld.updateCategoryMapping.mockClear()
  preselection.value = null
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

  it('shows a below-threshold suggestion as a hint instead of pre-filling it', async () => {
    const weak = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    weak.records.test_db_morph.confidence = 0.55
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: weak }],
    ])
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const store = useSuggestionsStore()
    // Not pre-filled and not gating submit; sex (0.9) still is.
    expect(store.isApplied('test_db_morph')).toBe(false)
    expect(store.isApplied('test_db_sex')).toBe(true)
    expect(wrapper.text()).toContain('1 suggestion needs review')
    // Still offered: highlighted dropdown and a pill the user can accept.
    const morphSelect = wrapper.find('select[name="ncit_comment_test_db_morph"]')
    expect(morphSelect.element.value).toBe('')
    expect(morphSelect.classes()).toContain('suggestion-highlight')
    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    await badges[0].vm.$emit('accept')
    await flushPromises()
    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.test_db_morph?.description).toBeTruthy()
    expect(store.isTouched('test_db_morph')).toBe(true)
  })

  it('ignores a suggestion whose variable the dropdown does not offer', async () => {
    // A match from another map (the backend fell back to its session's
    // map) must not pre-fill a dropdown that cannot show it: the field
    // would look empty yet count as needing review.
    const foreign = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    foreign.records.test_db_morph.match = 'clinical_stage_group'
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: foreign }],
    ])
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const store = useSuggestionsStore()
    expect(store.isApplied('test_db_morph')).toBe(false)
    expect(store.isApplied('test_db_sex')).toBe(true)
    expect(wrapper.text()).toContain('1 suggestion needs review')
    // Only sex gets a pill; morph shows neither pill nor highlight.
    expect(wrapper.findAllComponents({ name: 'SuggestionBadge' })).toHaveLength(1)
    const morphSelect = wrapper.find('select[name="ncit_comment_test_db_morph"]')
    expect(morphSelect.classes()).not.toContain('suggestion-highlight')
  })

  it('never pre-fills over a mapping the loaded JSON-LD already holds', async () => {
    // morph is already mapped in the uploaded JSON-LD; the suggestion
    // disagrees. The existing mapping must stay, unreviewed-free.
    preselection.value = {
      preselectedDescriptions: { test_db_morph: 'Year of initial diagnosis' },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    }
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const store = useSuggestionsStore()
    expect(store.isApplied('test_db_morph')).toBe(false)
    // sex has no mapping yet, so it is still pre-filled for review.
    expect(store.isApplied('test_db_sex')).toBe(true)
    expect(wrapper.text()).toContain('1 suggestion needs review')
    // The preselected column is not highlighted as needing review.
    const morphSelect = wrapper.find('select[name="ncit_comment_test_db_morph"]')
    expect(morphSelect.classes()).not.toContain('suggestion-highlight')
    // Its pill is informational ("already filled in"), not an
    // accept/dismiss one: a suggestion on a mapped column must not read
    // as if the column still needed a suggestion review.
    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    const morphBadge = badges.find((b) => b.props('suggestion').item === 'morph')
    const sexBadge = badges.find((b) => b.props('suggestion').item === 'sex')
    expect(morphBadge.props('alreadyFilled')).toBe(true)
    expect(morphBadge.find('.suggestion-badge').classes()).toContain('already-filled')
    expect(morphBadge.text()).toContain('already filled in')
    expect(morphBadge.find('button.suggestion-accept').exists()).toBe(false)
    expect(sexBadge.props('alreadyFilled')).toBe(false)
    expect(sexBadge.find('button.suggestion-accept').exists()).toBe(true)
    expect(jsonld.updateMappingFromForm).not.toHaveBeenCalled()
  })

  it('shows the already-filled pill on a mapped column even without a suggestion', async () => {
    // The pill must not depend on the suggestion job also producing a
    // record for the column: any column the loaded JSON-LD already maps
    // gets the informational pill. A column the user changes themselves is
    // not "already" filled in — its pill disappears.
    preselection.value = {
      preselectedDescriptions: { test_db_morph: 'Year of initial diagnosis' },
      preselectedDatatypes: {},
      descriptionToDatatype: {},
    }
    const noSuggestion = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    delete noSuggestion.records.test_db_morph
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: noSuggestion }],
    ])
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const labelFor = (item) =>
      wrapper.findAll('.variable-label').find((el) => el.text().startsWith(item))
    const morphBadge = labelFor('morph').find('.suggestion-badge')
    expect(morphBadge.classes()).toContain('already-filled')
    expect(labelFor('morph').text()).toContain('already filled in')
    expect(labelFor('morph').find('button.suggestion-accept').exists()).toBe(false)
    // sex has no mapping and a live suggestion: the normal review pill.
    expect(labelFor('sex').find('button.suggestion-accept').exists()).toBe(true)

    // The user picks the pre-filled column's value themselves: the field
    // is no longer "already" filled in, so its pill goes away.
    await wrapper
      .find('select[name="ncit_comment_test_db_morph"]')
      .setValue('Year of initial diagnosis')
    await flushPromises()
    expect(labelFor('morph').find('.suggestion-badge').exists()).toBe(false)
  })

  it('explains an accept blocked by the one-variable-per-database rule', async () => {
    // Both columns suggest the same variable: morph pre-fills it first,
    // so accepting it for sex must not write, and must say why.
    const clash = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    clash.records.test_db_sex.match = 'tumour_morphology_icd_o'
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: clash }],
    ])

    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    await badges[1].vm.$emit('accept')
    await flushPromises()

    expect(jsonld.updateMappingFromForm).not.toHaveBeenCalled()
    const warnings = useStatusStore().messages.filter((m) => m.level === 'warning')
    expect(warnings.at(-1)?.text).toContain("already used by column 'morph'")
  })

  it('offers a conflict loser its contested variable as an alternative (D2)', async () => {
    // sex lost 'biological_sex' to another column that the user has since
    // remapped, so the variable is free again: applying the kept
    // alternative writes it through the normal accept path.
    const loser = JSON.parse(JSON.stringify(VARIABLES_SNAPSHOT))
    Object.assign(loser.records.test_db_sex, {
      match: null,
      confidence: 0,
      reason: "conflict: column 'gender' is a stronger candidate for biological_sex",
      alternatives: [{ match: 'biological_sex', confidence: 0.9, source: 'alias', tier: 1 }],
    })
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/variables', { data: loser }],
    ])

    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges).toHaveLength(2)
    const sexBadge = badges[1]
    expect(sexBadge.find('.suggestion-badge').classes()).toContain('alternatives-only')
    // Nothing was pre-filled for the loser, and it does not gate submit.
    const store = useSuggestionsStore()
    expect(store.isApplied('test_db_sex')).toBe(false)

    await sexBadge.vm.$emit('apply-alternative', {
      match: 'biological_sex',
      confidence: 0.9,
      source: 'alias',
      tier: 1,
    })
    await flushPromises()

    const payload = jsonld.updateMappingFromForm.mock.calls.at(-1)[0]
    expect(payload.test_db_sex?.description).toBe('Biological sex')
    expect(store.isTouched('test_db_sex')).toBe(true)
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

  it('requests the details state on the browser semantic map', async () => {
    // The dropdowns collect their value-mapping options from the map in
    // this browser's IndexedDB, so the state request must carry it: the
    // backend renders the variables and preselected values on that map.
    mountDetails()
    await flushPromises()

    const call = api.post.mock.calls.find(
      (c) => c[0] === '/api/v1/describe-variable-details-state',
    )
    expect(call).toBeTruthy()
    expect(call[1].mapping).toEqual(SEMANTIC_MAP)
  })

  it('pre-fills a value suggestion below the column threshold', async () => {
    // Value scores sit on another scale than column names ('1' against
    // 'score_1_not_at_all' scores ~0.69 and is usually right), so the
    // values page does not apply the variables threshold.
    const weak = JSON.parse(JSON.stringify(VALUES_SNAPSHOT))
    weak.records.patients_sex_M.confidence = 0.55
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: DETAILS_STATE }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: weak }],
    ])
    const wrapper = mountDetails()
    await flushPromises()

    const store = useSuggestionsStore()
    expect(store.isApplied('patients_sex_M')).toBe(true)
    expect(wrapper.vm.categorySelections.patients_sex_M).toBe('Male')
    expect(jsonld.updateCategoryMapping).not.toHaveBeenCalled()
  })

  it('collects a variable\'s value mappings from the state when the browser has no map', async () => {
    // A fresh or incognito browser views the session\'s describe state but
    // has no semantic map in IndexedDB, so it cannot collect the value
    // mappings client-side. The state response carries them (computed from
    // the same map that produced the page), so the dropdown still offers
    // the selected variable\'s terms instead of the generic Yes/No list.
    const state = JSON.parse(JSON.stringify(DETAILS_STATE))
    state.category_options = { Sex: ['Male', 'Female', 'Missing or unspecified'] }
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: state }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: VALUES_SNAPSHOT }],
    ])
    const defaultGetData = db.getData.getMockImplementation()
    db.getData.mockResolvedValue(null)
    try {
      const wrapper = mountDetails()
      await flushPromises()

      const select = wrapper.find('select.category-select')
      expect(select.exists()).toBe(true)
      expect([...select.element.options].map((o) => o.value)).toEqual([
        '',
        'Male',
        'Female',
        'Missing or unspecified',
        'Other',
      ])
    } finally {
      db.getData.mockImplementation(defaultGetData)
    }
  })

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

  it('shows a value the loaded JSON-LD already mapped an informational pill', async () => {
    // M is already mapped to Male in the uploaded JSON-LD; the suggestion
    // agrees, but nothing needs reviewing. The pill must say "already
    // filled in" instead of offering an accept/dismiss review flow.
    const state = JSON.parse(JSON.stringify(DETAILS_STATE))
    state.preselected_values = { 'patients_sex_category_"M"': 'Male' }
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: state }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: VALUES_SNAPSHOT }],
    ])
    const wrapper = mountDetails()
    await flushPromises()

    const store = useSuggestionsStore()
    expect(store.isApplied('patients_sex_M')).toBe(false)
    expect(store.isApplied('patients_sex_F')).toBe(true)
    expect(wrapper.vm.categorySelections.patients_sex_M).toBe('Male')

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    const mBadge = badges.find((b) => b.props('suggestion').item === 'M')
    const fBadge = badges.find((b) => b.props('suggestion').item === 'F')
    expect(mBadge.props('alreadyFilled')).toBe(true)
    expect(mBadge.find('.suggestion-badge').classes()).toContain('already-filled')
    expect(mBadge.text()).toContain('already filled in')
    expect(mBadge.find('button.suggestion-accept').exists()).toBe(false)
    expect(fBadge.props('alreadyFilled')).toBe(false)
    expect(fBadge.find('button.suggestion-accept').exists()).toBe(true)
    expect(jsonld.updateCategoryMapping).not.toHaveBeenCalled()
  })

  it('shows the already-filled pill on a mapped value even without a suggestion', async () => {
    // The pill must not depend on the suggestion job also producing a
    // record for the value: any value the loaded JSON-LD already maps gets
    // the informational pill. A value the user picks themselves is not
    // "already" filled in — its pill disappears.
    const state = JSON.parse(JSON.stringify(DETAILS_STATE))
    state.preselected_values = { 'patients_sex_category_"M"': 'Male' }
    const noM = JSON.parse(JSON.stringify(VALUES_SNAPSHOT))
    delete noM.records.patients_sex_M
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: state }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: noM }],
    ])
    const wrapper = mountDetails()
    await flushPromises()

    const items = () => wrapper.findAll('.category-item')
    expect(wrapper.vm.categorySelections.patients_sex_M).toBe('Male')
    expect(items()[0].find('.suggestion-badge').classes()).toContain('already-filled')
    expect(items()[0].text()).toContain('already filled in')
    expect(items()[0].find('button.suggestion-accept').exists()).toBe(false)
    // F has no mapping and a live suggestion: the normal review pill.
    expect(items()[1].find('button.suggestion-accept').exists()).toBe(true)

    // The user picks the pre-filled value's option themselves: the field
    // is no longer "already" filled in, so its pill goes away.
    await wrapper.findAll('select.category-select')[0].setValue('Female')
    await flushPromises()
    expect(items()[0].find('.suggestion-badge').exists()).toBe(false)
  })

  it('accept-all reviews the suggestions only, not the pre-filled values', async () => {
    // M is pre-filled from the loaded JSON-LD, F by its suggestion. "Accept
    // all suggestions" must review F — and leave M's pre-filled selection
    // and its informational pill alone: overwriting it with the suggested
    // term and marking it reviewed is not the user's review.
    const state = JSON.parse(JSON.stringify(DETAILS_STATE))
    state.preselected_values = { 'patients_sex_category_"M"': 'Male' }
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: state }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: VALUES_SNAPSHOT }],
    ])
    const wrapper = mountDetails()
    await flushPromises()

    const acceptAll = wrapper
      .findAll('button')
      .find((b) => b.text().includes('Accept all suggestions'))
    await acceptAll.trigger('click')
    await flushPromises()

    const store = useSuggestionsStore()
    // The suggestion pre-fill is reviewed through the normal change path.
    expect(store.isTouched('patients_sex_F')).toBe(true)
    // The map pre-fill is untouched: same selection, no marks, no write.
    expect(wrapper.vm.categorySelections.patients_sex_M).toBe('Male')
    expect(store.isApplied('patients_sex_M')).toBe(false)
    expect(store.isTouched('patients_sex_M')).toBe(false)
    const writtenValues = jsonld.updateCategoryMapping.mock.calls.map((args) => args[3])
    expect(writtenValues).toEqual(['F'])
  })

  it('dismiss-all clears the suggestion pre-fills only, not the pre-filled values', async () => {
    const state = JSON.parse(JSON.stringify(DETAILS_STATE))
    state.preselected_values = { 'patients_sex_category_"M"': 'Male' }
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: state }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: VALUES_SNAPSHOT }],
    ])
    const wrapper = mountDetails()
    await flushPromises()

    const dismissAll = wrapper
      .findAll('button')
      .find((b) => b.text().includes('Dismiss all suggestions'))
    await dismissAll.trigger('click')
    await flushPromises()

    const store = useSuggestionsStore()
    // The suggestion pre-fill is dismissed and cleared (display-only).
    expect(store.isDismissed('patients_sex_F')).toBe(true)
    expect(wrapper.vm.categorySelections.patients_sex_F).toBe('')
    // The map pre-fill keeps its selection — and its suggestion record
    // stays live: the value was never the suggestion's to dismiss.
    expect(store.isDismissed('patients_sex_M')).toBe(false)
    expect(wrapper.vm.categorySelections.patients_sex_M).toBe('Male')
  })

  it('"Go to next" expands the section and scrolls to the first unreviewed value', async () => {
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: DETAILS_STATE }],
      ['/api/v1/suggestions/status', { data: STATUS }],
      ['/api/v1/suggestions/values', { data: VALUES_SNAPSHOT }],
    ])
    // The category backend name embeds the quoted value, which no CSS
    // attribute selector can carry: "Go to next" used to build one anyway,
    // so the element was never found and the scroll silently never
    // happened. The select now carries an id for the lookup. The view is
    // attached to the document because the jump looks the element up
    // through document.getElementById.
    const scrolled = []
    const proto = window.Element.prototype
    const original = proto.scrollIntoView
    proto.scrollIntoView = function scrollSpy() {
      scrolled.push(this)
    }
    let wrapper
    try {
      wrapper = mount(DescribeVariableDetailsView, {
        attachTo: document.body,
        global: { stubs: { RouterLink: RouterLinkStub } },
      })
      await flushPromises()

      const jump = wrapper.find('button.jump-to-unreviewed')
      expect(jump.exists()).toBe(true)
      await jump.trigger('click')
      await flushPromises()
      await flushPromises()

      expect(scrolled).toHaveLength(1)
      expect(scrolled[0].getAttribute('name')).toBe('patients_sex_category_"M"')
    } finally {
      proto.scrollIntoView = original
      wrapper?.unmount()
    }
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

// ---------------------------------------------------------------------------
// Paste-back round trip (issue 2): an imported LLM answer becomes ordinary
// pasted_llm records. It must render as pills (even on a stack with every
// tier off), never write to the JSON-LD, and never touch review marks.
// ---------------------------------------------------------------------------

const STATUS_TIERS_OFF = {
  enabled: false,
  compute: 'host',
  tiers: { 1: { state: 'inactive', reason: 'disabled by FLYOVER_SUGGESTION_TIERS' } },
  threshold: 0.8,
  rules_version: null,
  prompt_export: { state: 'active' },
}

const IDLE_SNAPSHOT = {
  enabled: false,
  status: 'idle',
  progress: { done: 0, total: 0 },
  error: null,
  records: {},
}

function pastedVariablesIngest() {
  return {
    data: {
      accepted: 1,
      nulled: 1,
      rejected: 0,
      skipped: 0,
      messages: [],
      job: {
        status: 'done',
        fingerprint: '+pasted-1',
        progress: { done: 2, total: 2 },
        records: {
          test_db_morph: {
            status: 'done',
            item: 'morph',
            match: 'tumour_morphology_icd_o',
            confidence: 0.7,
            reason: 'Looks like a morphology code.',
            source: 'pasted_llm',
            tier: 3,
            database: 'test_db',
            column: 'morph',
          },
          test_db_sex: {
            status: 'done',
            item: 'sex',
            match: null,
            confidence: 0,
            reason: "[invalid key from LLM] 'gender' is not a schema variable",
            source: 'pasted_llm',
            tier: 3,
            database: 'test_db',
            column: 'sex',
          },
        },
      },
    },
  }
}

function pastedValuesIngest() {
  return {
    data: {
      accepted: 1,
      nulled: 0,
      rejected: 0,
      skipped: 1,
      messages: ["'sex' = 'M' is already mapped; left unchanged."],
      job: {
        status: 'done',
        fingerprint: 'fp-values-1+pasted-1',
        progress: { done: 2, total: 2 },
        records: {
          ...VALUES_SNAPSHOT.records,
          patients_sex_F: {
            status: 'done',
            item: 'F',
            match: 'female',
            confidence: 0.95,
            reason: 'F = female',
            source: 'pasted_llm',
            tier: 3,
            database: 'patients',
            column: 'sex',
            value: 'F',
          },
        },
      },
    },
  }
}

async function importThroughPanel(wrapper, answer = '{"databases": {}}') {
  const panel = wrapper.findComponent({ name: 'LlmPromptPanel' })
  await panel.find('.llm-help-toggle').trigger('click')
  await panel.find('.llm-help-answer').setValue(answer)
  await panel.find('.llm-help-import').trigger('click')
  await flushPromises()
  return panel
}

describe('DescribeVariablesView — pasted LLM answer', () => {
  it('imports the answer as pasted_llm pills without writing the JSON-LD, with every tier off', async () => {
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: STATUS_TIERS_OFF }],
      ['/api/v1/suggestions/variables', { data: IDLE_SNAPSHOT }],
      ['/api/v1/suggestions/variables/ingest', pastedVariablesIngest()],
    ])
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()

    // Tiers off: no suggestion UI, but the panel is there.
    const store = useSuggestionsStore()
    expect(store.enabled).toBe(false)
    expect(wrapper.findComponent({ name: 'SuggestionStatusBar' }).exists()).toBe(false)
    expect(wrapper.findAllComponents({ name: 'SuggestionBadge' })).toHaveLength(0)
    expect(wrapper.findAllComponents({ name: 'LlmPromptPanel' })).toHaveLength(1)

    await importThroughPanel(wrapper)

    // The store posted the paste with the browser's map and flipped the UI on.
    const ingestCall = api.post.mock.calls.find(([url]) => url.endsWith('/variables/ingest'))
    expect(ingestCall[1]).toMatchObject({
      database: 'test_db',
      source: 'pasted_llm',
      answer: '{"databases": {}}',
    })
    expect(ingestCall[1].mapping.schema.variables).toHaveProperty('biological_sex')
    expect(store.enabled).toBe(true)
    expect(wrapper.findComponent({ name: 'SuggestionStatusBar' }).exists()).toBe(true)

    // One pill for the accepted record (a below-threshold hint, so not
    // pre-filled); the nulled one has no match and no pill.
    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges).toHaveLength(1)
    expect(badges[0].props('suggestion').source).toBe('pasted_llm')
    expect(badges[0].find('.fa-clipboard').exists()).toBe(true)
    expect(store.isApplied('test_db_morph')).toBe(false)
    expect(wrapper.find('select.description-select').element.value).toBe('')

    // Nothing reached the JSON-LD writer.
    expect(jsonld.updateMappingFromForm).not.toHaveBeenCalled()
    expect(jsonld.getMapping().databases).toEqual({})

    // Accepting the pasted suggestion goes through the normal review path.
    await badges[0].vm.$emit('accept')
    await flushPromises()
    expect(jsonld.updateMappingFromForm).toHaveBeenCalledTimes(1)
    expect(Object.keys(jsonld.updateMappingFromForm.mock.calls[0][0])).toEqual(['test_db_morph'])
    expect(store.isTouched('test_db_morph')).toBe(true)
  })

  it('keeps existing marks when a paste arrives and merges into a running job', async () => {
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: { ...STATUS, prompt_export: { state: 'active' } } }],
      ['/api/v1/suggestions/variables', { data: VARIABLES_SNAPSHOT }],
      ['/api/v1/suggestions/variables/ingest', pastedVariablesIngest()],
    ])
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()
    const store = useSuggestionsStore()
    expect(store.isApplied('test_db_sex')).toBe(true)
    // The user dismisses the tier-1 suggestion for 'sex'.
    const sexBadge = wrapper
      .findAllComponents({ name: 'SuggestionBadge' })
      .find((b) => b.props('suggestion').item === 'sex')
    await sexBadge.vm.$emit('dismiss')
    await flushPromises()
    expect(store.isDismissed('test_db_sex')).toBe(true)
    // Dismissing a pre-fill clears the field through the form path; count
    // those writes so the paste can be shown to add none.
    const writesBefore = jsonld.updateMappingFromForm.mock.calls.length

    await importThroughPanel(wrapper)

    // The paste replaced morph's record and nulled sex's: 'sex' shows no
    // pill (nothing to accept) and the import wrote nothing.
    expect(store.variables.byKey.test_db_morph.source).toBe('pasted_llm')
    expect(wrapper.findAllComponents({ name: 'SuggestionBadge' }).map((b) => b.props('suggestion').item)).toEqual(['morph'])
    expect(jsonld.updateMappingFromForm).toHaveBeenCalledTimes(writesBefore)
  })
})

describe('DescribeVariablesView — pasted answer re-opens a dismissed field', () => {
  it('shows the pasted suggestion on a field whose tier-1 suggestion was dismissed', async () => {
    // The paste for 'sex' replaces the dismissed alias record (the server
    // reports the key as reopened); 'morph', reviewed by the user, is
    // already in the JSON-LD and the server skipped it.
    const ingest = pastedVariablesIngest()
    ingest.data.accepted = 1
    ingest.data.nulled = 0
    ingest.data.skipped = 1
    ingest.data.reopened = ['test_db_sex']
    ingest.data.job.records = {
      test_db_morph: VARIABLES_SNAPSHOT.records.test_db_morph,
      test_db_sex: {
        status: 'done',
        item: 'sex',
        match: 'year_of_initial_diagnosis',
        confidence: 0.9,
        reason: 'The LLM read it as a year.',
        source: 'pasted_llm',
        tier: 3,
        database: 'test_db',
        column: 'sex',
        reopens: true,
        alternatives: [VARIABLES_SNAPSHOT.records.test_db_sex],
      },
    }
    mockApiRoutes([
      ['/api/v1/describe-variables-state', { data: { column_info: { test_db: ['morph', 'sex'] } } }],
      ['/api/v1/suggestions/status', { data: { ...STATUS, prompt_export: { state: 'active' } } }],
      ['/api/v1/suggestions/variables', { data: VARIABLES_SNAPSHOT }],
      ['/api/v1/suggestions/variables/ingest', ingest],
    ])
    const wrapper = mount(DescribeVariablesView)
    await flushPromises()
    const store = useSuggestionsStore()
    const badgeFor = (item) =>
      wrapper.findAllComponents({ name: 'SuggestionBadge' }).find((b) => b.props('suggestion').item === item)

    // The user accepts 'morph' (reviewed, written) and dismisses 'sex'.
    await badgeFor('morph').vm.$emit('accept')
    await badgeFor('sex').vm.$emit('dismiss')
    await flushPromises()
    expect(store.isTouched('test_db_morph')).toBe(true)
    expect(store.isDismissed('test_db_sex')).toBe(true)
    expect(badgeFor('sex')).toBeUndefined()
    const sexSelect = wrapper.find('[id="ncit_comment_test_db_sex"]')
    expect(sexSelect.element.value).toBe('')
    const writesBefore = jsonld.updateMappingFromForm.mock.calls.length

    await importThroughPanel(wrapper)

    // The dismissed keys went along, the dismissal is gone, and the pasted
    // suggestion is pre-filled for review (confident) with a pasted pill.
    const ingestCall = api.post.mock.calls.find(([url]) => url.endsWith('/variables/ingest'))
    expect(ingestCall[1].dismissed).toEqual(['test_db_sex'])
    expect(store.isDismissed('test_db_sex')).toBe(false)
    expect(store.variables.byKey.test_db_sex.source).toBe('pasted_llm')
    const sexBadge = badgeFor('sex')
    expect(sexBadge).toBeDefined()
    expect(sexBadge.props('suggestion').match).toBe('year_of_initial_diagnosis')
    expect(store.isApplied('test_db_sex')).toBe(true)
    expect(store.isTouched('test_db_sex')).toBe(false)
    expect(sexSelect.element.value).toBe('Year of initial diagnosis')
    expect(wrapper.text()).toContain('1 suggestion needs review')

    // The reviewed field is untouched: still reviewed, still its value,
    // and the paste itself wrote nothing.
    expect(store.isTouched('test_db_morph')).toBe(true)
    expect(wrapper.find('[id="ncit_comment_test_db_morph"]').element.value).toBe('Tumour morphology icd o')
    expect(jsonld.updateMappingFromForm).toHaveBeenCalledTimes(writesBefore)
  })
})

describe('DescribeVariableDetailsView — pasted LLM answer', () => {
  it('imports value suggestions as pills without calling updateCategoryMapping', async () => {
    mockApiRoutes([
      ['/api/v1/describe-variable-details-state', { data: DETAILS_STATE }],
      ['/api/v1/suggestions/status', { data: { ...STATUS, prompt_export: { state: 'active' } } }],
      ['/api/v1/suggestions/values', { data: { ...VALUES_SNAPSHOT, records: { patients_sex_M: VALUES_SNAPSHOT.records.patients_sex_M } } }],
      ['/api/v1/suggestions/values/ingest', pastedValuesIngest()],
    ])
    const wrapper = mount(DescribeVariableDetailsView, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    expect(wrapper.findAllComponents({ name: 'LlmPromptPanel' })).toHaveLength(1)
    expect(wrapper.findAllComponents({ name: 'SuggestionBadge' })).toHaveLength(1)

    const panel = await importThroughPanel(wrapper)
    expect(api.post.mock.calls.find(([url]) => url.endsWith('/values/ingest'))[1].database).toBe('patients')
    expect(panel.find('.llm-help-import-result').text()).toMatch(/1 imported, 1 already mapped/)
    expect(panel.text()).toContain("'sex' = 'M' is already mapped")

    const badges = wrapper.findAllComponents({ name: 'SuggestionBadge' })
    expect(badges).toHaveLength(2)
    const pasted = badges.find((b) => b.props('suggestion').source === 'pasted_llm')
    expect(pasted.props('suggestion').match).toBe('female')
    // The values page pre-fills for display (no confidence gate), but the
    // JSON-LD writer is untouched until the user reviews the field.
    const store = useSuggestionsStore()
    expect(store.isApplied('patients_sex_F')).toBe(true)
    expect(wrapper.vm.categorySelections.patients_sex_F).toBe('Female')
    expect(jsonld.updateCategoryMapping).not.toHaveBeenCalled()
  })
})
