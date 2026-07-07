import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

const routerPush = vi.fn()
vi.mock('vue-router', () => ({
  useRouter: () => ({ push: routerPush }),
}))

import api from '@/services/api'
import * as db from '@/lib/db'
import AnnotationReviewView from '@/views/AnnotationReviewView.vue'

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="String(to)"><slot /></a>',
}

const SEMANTIC_MAP = {
  schema: {
    variables: {
      age: { predicate: 'roo:P100027', class: 'ncit:C25150', dataType: 'continuous' },
    },
  },
  databases: {
    db_a: {
      tables: {
        patients: {
          sourceFile: 'patients.csv',
          columns: { age_col: { mapsTo: 'schema:variable/age', localColumn: 'age' } },
        },
      },
    },
  },
}

function mountView() {
  return mount(AnnotationReviewView, {
    global: { stubs: { RouterLink: RouterLinkStub } },
  })
}

function jsonResponse(body, { ok = true, status = 200 } = {}) {
  return { ok, status, json: async () => body }
}

describe('AnnotationReviewView', () => {
  beforeEach(() => {
    api.get.mockReset()
    db.saveData.mockClear()
    db.getData.mockReset()
    db.getData.mockResolvedValue(null)
    routerPush.mockClear()
    vi.stubGlobal('fetch', vi.fn())
    vi.stubGlobal('alert', vi.fn())
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('hydrates from api + IndexedDB and renders variable cards', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })

    const w = mountView()
    await flushPromises()

    expect(api.get).toHaveBeenCalledWith('/api/rdf-store-databases')
    expect(w.findAll('.variable-card').length).toBeGreaterThan(0)
    expect(w.text()).toContain('age')
  })

  it('falls back to IndexedDB when /api/rdf-store-databases fails', async () => {
    api.get.mockRejectedValue(new Error('net'))
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'rdf_store_databases') return { data: ['patients.csv'] }
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })

    const w = mountView()
    await flushPromises()

    const keysRead = db.getData.mock.calls.map((c) => c[1])
    expect(keysRead).toContain('rdf_store_databases')
    expect(w.findAll('.variable-card').length).toBeGreaterThan(0)
  })

  it('shows no-data warning when no databases and no IndexedDB fallback', async () => {
    api.get.mockResolvedValue({ data: { success: false, databases: [] } })
    db.getData.mockResolvedValue(null)

    const w = mountView()
    await flushPromises()

    expect(w.find('.alert-warning').exists()).toBe(true)
    expect(w.find('.alert-warning').html()).toContain('No databases found')
  })

  it('submits semantic map then starts annotation then pushes to /annotate/verify', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })
    fetch.mockResolvedValueOnce(jsonResponse({ success: true }))
    fetch.mockResolvedValueOnce(jsonResponse({ success: true }))

    const w = mountView()
    await flushPromises()

    await w.find('button.btn-primary').trigger('click')
    await flushPromises()

    expect(fetch).toHaveBeenCalledTimes(2)
    expect(fetch.mock.calls[0][0]).toBe('/submit-indexeddb-semantic-map')
    expect(fetch.mock.calls[0][1].method).toBe('POST')
    // We send the FULL semantic map to preserve original for Share page
    const submittedMap = JSON.parse(fetch.mock.calls[0][1].body)
    expect(submittedMap).toEqual(SEMANTIC_MAP)
    expect(fetch.mock.calls[1][0]).toBe('/start-annotation')
    // We send the filtered semantic map (all tables selected by default) to annotation endpoint
    const annotationBody = JSON.parse(fetch.mock.calls[1][1].body)
    expect(annotationBody).toEqual(SEMANTIC_MAP)
    expect(routerPush).toHaveBeenCalledWith('/annotate/verify')
  })

  it('aborts and does not navigate when /submit-indexeddb-semantic-map fails', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })
    fetch.mockResolvedValueOnce(jsonResponse({ success: false, error: 'bad' }, { ok: false, status: 500 }))

    const w = mountView()
    await flushPromises()

    await w.find('button.btn-primary').trigger('click')
    await flushPromises()

    expect(fetch).toHaveBeenCalledTimes(1)
    expect(routerPush).not.toHaveBeenCalled()
  })

  it('renders table selectors for each database', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })

    const w = mountView()
    await flushPromises()

    expect(w.findAll('.table-selector').length).toBeGreaterThan(0)
    expect(w.find('.table-selector i.fa-check').exists()).toBe(true)
  })

  it('has all tables selected by default', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })

    const w = mountView()
    await flushPromises()

    const selectors = w.findAll('.table-selector')
    expect(selectors.length).toBeGreaterThan(0)
    
    // All selectors should have the 'selected' class by default (tables are selected by default)
    selectors.forEach(selector => {
      expect(selector.classes()).toContain('selected')
    })
  })

  it('toggles table selection when clicking selector', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })

    const w = mountView()
    await flushPromises()

    const firstSelector = w.find('.table-selector')
    expect(firstSelector.classes()).toContain('selected')
    expect(firstSelector.find('i.fa-check').exists()).toBe(true)

    await firstSelector.trigger('click')
    await flushPromises()

    expect(firstSelector.classes()).not.toContain('selected')
    expect(firstSelector.find('i.fa-times').exists()).toBe(true)

    // Click again to re-select
    await firstSelector.trigger('click')
    await flushPromises()

    expect(firstSelector.classes()).toContain('selected')
    expect(firstSelector.find('i.fa-check').exists()).toBe(true)
  })

  it('shows alert when trying to annotate with no tables selected', async () => {
    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: SEMANTIC_MAP }
      return null
    })
    
    const alertMock = vi.fn()
    vi.stubGlobal('alert', alertMock)

    const w = mountView()
    await flushPromises()

    // Deselect all tables
    const selectors = w.findAll('.table-selector')
    for (const selector of selectors) {
      await selector.trigger('click')
    }
    await flushPromises()

    await w.find('button.btn-primary').trigger('click')
    await flushPromises()

    expect(alertMock).toHaveBeenCalledWith('Please select at least one table to annotate.')
    expect(fetch).not.toHaveBeenCalled()
  })

  it('passes filtered semantic map to annotation endpoint', async () => {
    // Create a semantic map with two databases
    const TWO_DB_MAP = {
      schema: {
        variables: {
          age: { predicate: 'roo:P100027', class: 'ncit:C25150', dataType: 'continuous' },
        },
      },
      databases: {
        db_a: {
          tables: {
            patients: {
              sourceFile: 'patients.csv',
              columns: { age_col: { mapsTo: 'schema:variable/age', localColumn: 'age' } },
            },
          },
        },
        db_b: {
          tables: {
            other: {
              sourceFile: 'other.csv',
              columns: { age_col: { mapsTo: 'schema:variable/age', localColumn: 'age' } },
            },
          },
        },
      },
    }

    api.get.mockResolvedValue({ data: { success: true, databases: ['patients.csv', 'other.csv'] } })
    db.getData.mockImplementation(async (_store, key) => {
      if (key === 'semantic_map') return { data: TWO_DB_MAP }
      return null
    })
    fetch.mockResolvedValueOnce(jsonResponse({ success: true }))
    fetch.mockResolvedValueOnce(jsonResponse({ success: true }))

    const w = mountView()
    await flushPromises()

    // Deselect the second database by clicking its selector
    const selectors = w.findAll('.table-selector')
    if (selectors.length > 1) {
      await selectors[1].trigger('click')
      await flushPromises()
    }

    await w.find('button.btn-primary').trigger('click')
    await flushPromises()

    expect(fetch).toHaveBeenCalledTimes(2)
    // The first call should be to submit the FULL semantic map (to preserve original for Share page)
    const submittedMap = JSON.parse(fetch.mock.calls[0][1].body)
    expect(submittedMap.databases).toBeDefined()
    // Should contain both databases since we send the full map
    expect(submittedMap.databases.db_a).toBeDefined()
    expect(submittedMap.databases.db_b).toBeDefined()
    // The second call to start-annotation with filtered semantic map (only selected tables)
    const annotationBody = JSON.parse(fetch.mock.calls[1][1].body)
    expect(annotationBody.databases).toBeDefined()
    // Should only contain the selected database (patients.csv)
    expect(annotationBody.databases.db_a).toBeDefined()
    expect(annotationBody.databases.db_b).toBeUndefined()
  })
})
