import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async () => null),
  saveData: vi.fn(async () => {}),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({
  refreshDataExists: vi.fn(async () => {}),
}))

import ProjectStatusBar from '@/components/ProjectStatusBar.vue'
import { useProjectStore } from '@/stores/project'

// The strip above the step bar says which RDF store Flyover works on,
// whether it answers, and which project is open.

const CONFIGURED = {
  kind: 'rdf-store',
  repository: 'userRepo',
  url: 'http://rdf-store:7200/repositories/userRepo',
  source: 'configured',
  note: '',
}

function mountBar(state = {}) {
  const project = useProjectStore()
  project.store = CONFIGURED
  project.storeStatus = { state: 'reachable', reason: '' }
  Object.assign(project, state)
  return { wrapper: mount(ProjectStatusBar), project }
}

describe('ProjectStatusBar', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('names the RDF store and its repository, and says it answers', () => {
    const { wrapper } = mountBar()
    const storePart = wrapper.find('#statusbar-store')

    expect(storePart.text()).toContain('RDF store')
    expect(storePart.find('strong').text()).toBe('userRepo')
    expect(storePart.find('.statusbar-dot').classes()).toContain('is-reachable')
    expect(storePart.find('.sr-only').text()).toBe('The store answers.')
    expect(storePart.attributes('title')).toBe(
      "RDF store: http://rdf-store:7200/repositories/userRepo\nFlyover's configured store."
    )
  })

  it('shows a store that cannot be reached, with the reason on hover', () => {
    const { wrapper } = mountBar({
      storeStatus: { state: 'unreachable', reason: 'The store has no repository named \'aya\'.' },
    })
    const storePart = wrapper.find('#statusbar-store')

    expect(storePart.find('.statusbar-dot').classes()).toContain('is-unreachable')
    expect(storePart.find('.sr-only').text()).toBe('The store cannot be reached.')
    expect(storePart.attributes('title')).toContain("no repository named 'aya'")
  })

  it('says whose store it is and why, for a project\'s store', () => {
    const { wrapper } = mountBar({
      store: {
        ...CONFIGURED,
        repository: 'aya',
        source: 'project',
        note: 'localhost is read as the configured store.',
      },
    })

    const title = wrapper.find('#statusbar-store').attributes('title')
    expect(title).toContain("Named by the open project's map.")
    expect(title).toContain('localhost is read as the configured store.')
  })

  it('says when it has not heard from the server yet', () => {
    const { wrapper } = mountBar({ store: null, storeStatus: { state: 'unknown', reason: '' } })
    const storePart = wrapper.find('#statusbar-store')

    expect(storePart.text()).not.toContain('·')
    expect(storePart.find('.statusbar-dot').classes()).toContain('is-unknown')
    expect(storePart.attributes('title')).toBe('Flyover has not heard from the server yet.')
  })

  it('shows "No project" without one', () => {
    const { wrapper } = mountBar()
    const projectPart = wrapper.find('#statusbar-project')

    expect(projectPart.text()).toBe('No project')
    expect(projectPart.attributes('title')).toContain('the standard RDF flow')
  })

  it('names the open project with its version, and tells more on hover', () => {
    const { wrapper } = mountBar({
      project: {
        name: 'AYA mapping',
        version: '1.2.0',
        description: 'Centre A',
        schemaName: 'AYA schema',
        fileName: 'aya.jsonld',
        sourceUrl: '',
      },
    })
    const projectPart = wrapper.find('#statusbar-project')

    expect(projectPart.text()).toMatch(/^Project: AYA mapping\s+· v1\.2\.0$/)
    expect(projectPart.attributes('title')).toBe(
      'AYA mapping · v1.2.0\nCentre A\nSchema: AYA schema\nFile: aya.jsonld'
    )
  })

  it('names a project without a version or a name of its own after its schema', () => {
    const { wrapper } = mountBar({
      project: {
        name: 'REPLACE_WITH_YOUR_MAPPING_NAME',
        version: '',
        schemaName: 'AYA schema',
        sourceUrl: 'https://example.org/aya.jsonld',
      },
    })
    const projectPart = wrapper.find('#statusbar-project')

    expect(projectPart.text()).toBe('Project: AYA schema')
    expect(projectPart.attributes('title')).toContain('From: https://example.org/aya.jsonld')
  })
})
