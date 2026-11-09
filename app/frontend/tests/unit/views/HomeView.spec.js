import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'

// Flyover's home page offers the workflows. RDF asks for the project here
// first, then opens its own landing page; OMOP and CSV say they are under
// development.

const router = { push: vi.fn() }
vi.mock('vue-router', () => ({ useRouter: () => router }))
vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async () => null),
  saveData: vi.fn(async () => {}),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({ refreshDataExists: vi.fn(async () => {}) }))

import HomeView from '@/views/HomeView.vue'
import { useProjectStore } from '@/stores/project'
import { useWorkflowsStore } from '@/stores/workflows'

const RouterLinkStub = { props: ['to'], template: '<a :href="String(to)"><slot /></a>' }

function mountHome() {
  return mount(HomeView, { global: { stubs: { RouterLink: RouterLinkStub } } })
}

describe('HomeView', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    router.push.mockClear()
  })

  it('looks like the landing page, with the workflows as its tiles', () => {
    const wrapper = mountHome()

    expect(wrapper.find('h1').text()).toBe('Welcome to Flyover')
    expect(wrapper.findAll('.workflow-card .step-title').map((t) => t.text())).toEqual([
      'RDF',
      'OMOP',
      'CSV',
    ])
    // The steps belong to a workflow, not to the home page.
    expect(wrapper.text()).not.toMatch(/Ingest|Annotate/)
  })

  it('marks the workflows that are not built yet', () => {
    const wrapper = mountHome()

    expect(wrapper.find('#rdf-card .workflow-badge').exists()).toBe(false)
    expect(wrapper.find('#omop-card .workflow-badge').text()).toBe('Under development')
    expect(wrapper.find('#csv-card .workflow-badge').text()).toBe('Under development')
  })

  it('asks for the project here first, from the RDF tile and the start button', async () => {
    const wrapper = mountHome()
    const project = useProjectStore()

    await wrapper.find('#rdf-card').trigger('click')

    // The pop-up goes on to the RDF landing once the tab has chosen.
    expect(project.picker).toEqual({ open: true, mode: 'enter', target: '/rdf' })
    expect(router.push).not.toHaveBeenCalled()

    project.closePicker()
    await wrapper.find('#home-start').trigger('click')
    expect(project.picker).toEqual({ open: true, mode: 'enter', target: '/rdf' })
    expect(router.push).not.toHaveBeenCalled()
  })

  it('goes straight to the RDF landing for a tab in the workflow already', async () => {
    const wrapper = mountHome()
    const project = useProjectStore()
    project.enterWorkflow('rdf')

    await wrapper.find('#rdf-card').trigger('keyup.enter')

    expect(router.push).toHaveBeenCalledWith('/rdf')
    expect(project.picker.open).toBe(false)
  })

  it('says OMOP and CSV are under development, by click or by keyboard', async () => {
    const wrapper = mountHome()
    const workflows = useWorkflowsStore()

    await wrapper.find('#omop-card').trigger('click')
    expect(workflows.soon.label).toBe('OMOP')

    await wrapper.find('#csv-card').trigger('keyup.enter')
    expect(workflows.soon.label).toBe('CSV')
    expect(router.push).not.toHaveBeenCalled()
  })
})
