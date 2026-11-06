import { describe, it, expect, beforeEach, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { reactive } from 'vue'

const route = reactive({ path: '/describe' })

vi.mock('vue-router', () => ({ useRoute: () => route }))

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

// The text as it reads: the separators sit between flex items, so the
// template's spaces around them are not kept.
function reads(wrapper) {
  return wrapper.text().replace(/\s*·\s*/g, ' · ').replace(/\s+/g, ' ').trim()
}

function mountBar(state = {}) {
  const project = useProjectStore()
  project.store = CONFIGURED
  project.storeStatus = { state: 'reachable', reason: '', checking: false }
  Object.assign(project, state)
  vi.spyOn(project, 'refreshStoreStatus').mockResolvedValue()
  return { wrapper: mount(ProjectStatusBar), project }
}

describe('ProjectStatusBar', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    route.path = '/describe'
  })

  it('names the RDF store and its repository, and says it is connected', () => {
    const { wrapper } = mountBar()
    const storePart = wrapper.find('#statusbar-store')

    expect(reads(storePart)).toBe('RDF store · userRepo · Connection to the store: Connected')
    expect(storePart.find('strong').text()).toBe('userRepo')
    expect(storePart.find('.statusbar-dot').classes()).toContain('is-reachable')
    expect(storePart.find('.sr-only').text()).toBe('Connection to the store:')
    expect(storePart.attributes('title')).toBe(
      "RDF store: http://rdf-store:7200/repositories/userRepo\nFlyover's configured store."
    )
  })

  it('shows a store that cannot be reached, with the reason on hover', () => {
    const { wrapper } = mountBar({
      storeStatus: {
        state: 'unreachable',
        reason: 'The store has no repository named \'aya\'.',
        checking: false,
      },
    })
    const storePart = wrapper.find('#statusbar-store')

    expect(storePart.find('.statusbar-dot').classes()).toContain('is-unreachable')
    expect(wrapper.find('#statusbar-connection').text()).toMatch(/Not connected$/)
    expect(storePart.attributes('title')).toContain("no repository named 'aya'")
  })

  it('checks the connection again on a click', async () => {
    const { wrapper, project } = mountBar()

    const button = wrapper.find('#statusbar-connection')
    expect(button.element.tagName).toBe('BUTTON')
    expect(button.attributes('title')).toBe('Check the connection again')
    await button.trigger('click')

    expect(project.refreshStoreStatus).toHaveBeenCalledTimes(1)
  })

  it('checks the connection again on every step, but not before the first', async () => {
    const { project } = mountBar()
    await flushPromises()
    expect(project.refreshStoreStatus).not.toHaveBeenCalled()

    route.path = '/annotate'
    await flushPromises()
    route.path = '/share'
    await flushPromises()

    expect(project.refreshStoreStatus).toHaveBeenCalledTimes(2)
  })

  it('pulses the dot while a check is under way, keeping the last answer', () => {
    const { wrapper } = mountBar({
      storeStatus: { state: 'reachable', reason: '', checking: true },
    })

    expect(wrapper.find('.statusbar-dot').classes()).toEqual(
      expect.arrayContaining(['is-reachable', 'is-checking'])
    )
    expect(wrapper.find('#statusbar-connection').text()).toMatch(/Connected$/)
  })

  it('lines up with the page content, not the step bar', () => {
    const { wrapper } = mountBar()

    expect(wrapper.find('.statusbar-inner').classes()).toEqual(
      expect.arrayContaining(['container', 'container-flyover'])
    )
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
    const { wrapper } = mountBar({
      store: null,
      storeStatus: { state: 'unknown', reason: '', checking: false },
    })
    const storePart = wrapper.find('#statusbar-store')

    expect(reads(storePart)).toBe('RDF store · Connection to the store: Checking…')
    expect(storePart.find('.statusbar-dot').classes()).toContain('is-unknown')
    expect(storePart.attributes('title')).toBe('Flyover has not heard from the server yet.')
  })

  it('shows "No project" without one, and the way to choose one', async () => {
    const { wrapper, project } = mountBar()
    const projectPart = wrapper.find('#statusbar-project')

    expect(reads(projectPart)).toBe('No project · Choose a project')
    expect(projectPart.attributes('title')).toContain('the standard RDF flow')

    await wrapper.find('#statusbar-switch').trigger('click')
    expect(project.picker).toEqual({ open: true, mode: 'switch' })
  })

  it('offers to switch an open project', async () => {
    const { wrapper, project } = mountBar({ project: { name: 'AYA mapping' } })

    expect(wrapper.find('#statusbar-switch').text()).toBe('Switch project')
    await wrapper.find('#statusbar-switch').trigger('click')
    expect(project.picker.open).toBe(true)
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

    expect(reads(projectPart)).toBe('Project: AYA mapping · v1.2.0 · Switch project')
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

    expect(reads(projectPart)).toBe('Project: AYA schema · Switch project')
    expect(projectPart.attributes('title')).toContain('From: https://example.org/aya.jsonld')
  })
})
