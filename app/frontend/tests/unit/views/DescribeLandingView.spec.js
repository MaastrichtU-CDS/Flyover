import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { ref } from 'vue'
import { setActivePinia, createPinia } from 'pinia'

// Uploading a semantic map on the describe landing page starts the
// describe flow over: the fields are rebuilt from the uploaded map, so the
// suggestion review marks of the previous run must go with them.

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

vi.mock('@/composables/useNavigation', () => ({
  useNavigation: () => ({
    dataExists: ref(true),
    refreshDataExists: vi.fn(async () => {}),
    stepStates: ref([]),
    currentStep: ref(null),
  }),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import { useSuggestionsStore } from '@/stores/suggestions'
import DescribeLandingView from '@/views/DescribeLandingView.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

function mountLanding() {
  return mount(DescribeLandingView, {
    global: { stubs: { RouterLink: RouterLinkStub } },
  })
}

const EMPTY_MAP = {
  '@context': { schema: 'mapping:schema/' },
  '@type': 'mapping:SemanticMapping',
  schema: { variables: { age_at_diagnosis: {} } },
  databases: {},
}

async function uploadMap(wrapper, map) {
  const file = new File([JSON.stringify(map)], 'mapping_template.jsonld', {
    type: 'application/ld+json',
  })
  const input = wrapper.find('input[type="file"]').element
  Object.defineProperty(input, 'files', { value: [file], configurable: true })
  await wrapper.find('input[type="file"]').trigger('change')
  await wrapper.find('form').trigger('submit')
  // FileReader resolves on a later task than flushPromises alone covers.
  await new Promise((resolve) => setTimeout(resolve, 0))
  await flushPromises()
}

describe('DescribeLandingView — uploading a semantic map', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.get.mockReset()
    api.post.mockReset()
    db.saveData.mockClear()
    api.get.mockResolvedValue({ data: { exists: true } })
    api.post.mockResolvedValue({ data: { valid: true, validation_warnings: [] } })
  })

  it('forgets the previous run\'s suggestion reviews', async () => {
    // The user reviewed a suggestion, went back, and uploads the same map
    // again: the field is rebuilt empty, so "reviewed" must not survive.
    const store = useSuggestionsStore()
    store.setPhase('variables')
    store.markApplied('db_age')
    store.markUserTouched('db_age')
    expect(store.isTouched('db_age')).toBe(true)

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)

    // The map was saved, then the marks were reset and persisted empty.
    expect(db.saveData).toHaveBeenCalledWith(
      'metadata',
      expect.objectContaining({ key: 'semantic_map' }),
    )
    expect(store.isTouched('db_age')).toBe(false)
    expect(store.isApplied('db_age')).toBe(false)
    const savedMarks = db.saveData.mock.calls
      .map((c) => c[1])
      .filter((row) => row.key === 'suggestion_marks_variables')
      .at(-1)
    expect(savedMarks).toMatchObject({ applied: [], touched: [] })
  })

  it('keeps the reviews when the map fails validation', async () => {
    // Nothing was replaced, so the previous run's reviews still apply.
    api.post.mockResolvedValue({ data: { valid: false, validation_errors: [{ path: '$', message: 'bad' }] } })
    const store = useSuggestionsStore()
    store.setPhase('variables')
    store.markApplied('db_age')
    store.markUserTouched('db_age')

    const wrapper = mountLanding()
    await flushPromises()
    await uploadMap(wrapper, EMPTY_MAP)

    expect(store.isTouched('db_age')).toBe(true)
  })
})
