import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'

// Flyover's home page offers the workflows. RDF opens its own landing page;
// OMOP and CSV say they are under development.

const router = { push: vi.fn() }
vi.mock('vue-router', () => ({ useRouter: () => router }))

import HomeView from '@/views/HomeView.vue'
import { useWorkflowsStore } from '@/stores/workflows'

const RouterLinkStub = { props: ['to'], template: '<a :href="String(to)"><slot /></a>' }

function mountHome() {
  return mount(HomeView, { global: { stubs: { RouterLink: RouterLinkStub } } })
}

describe('HomeView', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
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

  it('enters the RDF workflow from its tile and from the start button', async () => {
    const wrapper = mountHome()

    await wrapper.find('#rdf-card').trigger('click')

    expect(router.push).toHaveBeenCalledWith('/rdf')
    expect(wrapper.find('#home-start').attributes('href')).toBe('/rdf')
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
