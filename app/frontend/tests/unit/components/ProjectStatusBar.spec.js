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

// The tooltip a part of the bar shows on hover, in the step tiles' style.
async function tipOf(part) {
  await part.find('.hover-tip').trigger('mouseenter')
  const tip = part.find('[role="tooltip"]')
  expect(tip.classes()).toEqual(expect.arrayContaining(['bootstrap-tooltip', 'is-below']))
  return tip.text()
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

  it('names the RDF store and its repository, and says it is connected', async () => {
    const { wrapper } = mountBar()
    const storePart = wrapper.find('#statusbar-store')

    expect(reads(storePart)).toBe('RDF store · userRepo · Connection to the store: Connected')
    expect(storePart.find('strong').text()).toBe('userRepo')
    expect(storePart.find('.statusbar-dot').classes()).toContain('is-reachable')
    expect(storePart.find('.sr-only').text()).toBe('Connection to the store:')
    // The hover is short: just where the store is.
    expect(await tipOf(storePart)).toBe('http://rdf-store:7200/repositories/userRepo')
    expect(storePart.attributes('title')).toBeUndefined()
  })

  it('hides the hover again when the pointer leaves', async () => {
    const { wrapper } = mountBar()
    const storePart = wrapper.find('#statusbar-store')

    await tipOf(storePart)
    await storePart.find('.hover-tip').trigger('mouseleave')

    expect(storePart.find('[role="tooltip"]').exists()).toBe(false)
  })

  it('shows a store that cannot be reached, with the reason on hover', async () => {
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
    expect(await tipOf(storePart)).toBe(
      "http://rdf-store:7200/repositories/userRepo\nThe store has no repository named 'aya'."
    )
  })

  it('checks the connection again on a click', async () => {
    const { wrapper, project } = mountBar()

    const button = wrapper.find('#statusbar-connection')
    expect(button.element.tagName).toBe('BUTTON')
    await button.trigger('focusin')
    const tip = wrapper.find('[role="tooltip"]')
    expect(tip.text()).toBe('Check the connection again')
    expect(button.attributes('aria-describedby')).toBe(tip.attributes('id'))
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

  it('says a project\'s map named the store, and nothing more', async () => {
    const { wrapper } = mountBar({
      store: {
        ...CONFIGURED,
        url: 'http://localhost:7200/repositories/aya',
        repository: 'aya',
        source: 'project',
        note: 'The map\'s endpoint names this machine (localhost), …',
      },
    })

    expect(await tipOf(wrapper.find('#statusbar-store'))).toBe(
      "http://localhost:7200/repositories/aya\nNamed by the open project's map."
    )
  })

  it('says when it has not heard from the server yet', async () => {
    const { wrapper } = mountBar({
      store: null,
      storeStatus: { state: 'unknown', reason: '', checking: false },
    })
    const storePart = wrapper.find('#statusbar-store')

    expect(reads(storePart)).toBe('RDF store · Connection to the store: Connecting.')
    expect(storePart.find('.statusbar-dot').classes()).toContain('is-unknown')
    expect(await tipOf(storePart)).toBe('Flyover has not heard from the server yet.')
  })

  it('waits for a project choice, naming no store, until someone chooses', async () => {
    const { wrapper } = mountBar({ storeStatus: { state: 'waiting', reason: '', checking: false } })
    const storePart = wrapper.find('#statusbar-store')

    expect(reads(storePart)).toBe('Waiting for a project choice')
    expect(storePart.find('.statusbar-dot').exists()).toBe(false)
    expect(storePart.find('#statusbar-store-button').exists()).toBe(false)
    expect(await tipOf(storePart)).toBe(
      'Flyover starts a store once you choose a project, or continue without one.'
    )
  })

  it("waits while this tab's start-up pop-up is open, whatever the server chose", () => {
    const { wrapper } = mountBar({ picker: { open: true, mode: 'startup' } })

    expect(reads(wrapper.find('#statusbar-store'))).toBe('Waiting for a project choice')
  })

  it('names the store again when the pop-up was opened to switch', () => {
    const { wrapper } = mountBar({ picker: { open: true, mode: 'switch' } })

    expect(reads(wrapper.find('#statusbar-store'))).toBe(
      'RDF store · userRepo · Connection to the store: Connected'
    )
  })

  it('says the store is starting, with the dots counting up', async () => {
    vi.useFakeTimers()
    try {
      const { wrapper } = mountBar({
        storeStatus: { state: 'starting', reason: '', checking: false },
      })
      const storePart = wrapper.find('#statusbar-store')

      expect(reads(storePart)).toBe('RDF store · userRepo · Connection to the store: Starting.')
      expect(storePart.find('.statusbar-dot').classes()).toContain('is-starting')
      await vi.advanceTimersByTimeAsync(400)
      expect(reads(storePart)).toMatch(/Starting\.\.$/)
      await vi.advanceTimersByTimeAsync(800)
      expect(reads(storePart)).toMatch(/Starting\.$/)
      // Screen readers hear the word, not the dots.
      expect(storePart.find('.statusbar-dots').attributes('aria-hidden')).toBe('true')
      vi.useRealTimers()
      expect(await tipOf(storePart)).toBe(
        'http://rdf-store:7200/repositories/userRepo\nFlyover is starting the store; this can take a minute.'
      )
    } finally {
      vi.useRealTimers()
    }
  })

  it('says when the work uses no database', () => {
    const { wrapper } = mountBar({ storeStatus: { state: 'none', reason: '', checking: false } })

    expect(reads(wrapper.find('#statusbar-store'))).toBe('No database in use')
  })

  it('shows "No project" without one, and the way to choose one', async () => {
    const { wrapper, project } = mountBar()
    const projectPart = wrapper.find('#statusbar-project')

    expect(reads(projectPart)).toBe('No project · Choose a project')
    expect(await tipOf(projectPart)).toContain('the standard RDF flow')

    await wrapper.find('#statusbar-switch').trigger('click')
    expect(project.picker).toEqual({ open: true, mode: 'switch' })
  })

  it('offers to switch an open project', async () => {
    const { wrapper, project } = mountBar({ project: { name: 'AYA mapping' } })

    expect(wrapper.find('#statusbar-switch').text()).toBe('Switch project')
    await wrapper.find('#statusbar-switch').trigger('click')
    expect(project.picker.open).toBe(true)
  })

  it('names the open project with its version, and tells more on hover', async () => {
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
    expect(await tipOf(projectPart)).toBe(
      'AYA mapping · v1.2.0\nCentre A\nSchema: AYA schema\nFile: aya.jsonld'
    )
  })

  it('names a project without a version or a name of its own after its schema', async () => {
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
    expect(await tipOf(projectPart)).toContain('From: https://example.org/aya.jsonld')
  })
})
