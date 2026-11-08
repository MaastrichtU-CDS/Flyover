import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { reactive, ref } from 'vue'

// The bar under the status strip: the workflows on the home page, and
// "RDF home" with the steps inside the RDF workflow.

const route = reactive({ path: '/', meta: {} })
vi.mock('vue-router', () => ({
  useRoute: () => route,
  RouterLink: { props: ['to'], template: '<a :href="String(to)"><slot /></a>' },
}))
vi.mock('@/composables/useNavigation', () => ({
  useNavigation: () => ({
    stepStates: ref(
      ['ingest', 'describe', 'annotate', 'share'].map((name) => ({
        name,
        label: name[0].toUpperCase() + name.slice(1),
        to: `/${name}`,
        icon: 'fa-edit',
        completedIcon: 'fa-check',
        active: false,
        completed: false,
        disabled: false,
      }))
    ),
  }),
}))

import AppNav from '@/components/AppNav.vue'
import { useWorkflowsStore } from '@/stores/workflows'

function labels(wrapper) {
  return wrapper.findAll('.nav-step .step-text').map((item) => item.text())
}

describe('AppNav', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('offers the workflows on the home page, and no steps', () => {
    route.path = '/'
    route.meta = {}
    const wrapper = mount(AppNav)

    expect(labels(wrapper)).toEqual(['Home', 'RDF', 'OMOP', 'CSV'])
    expect(wrapper.find('#home-step').classes()).toContain('active')
    expect(wrapper.find('#rdf-workflow-step').attributes('href')).toBe('/rdf')
  })

  it('says a workflow under development is so', async () => {
    route.path = '/'
    route.meta = {}
    const wrapper = mount(AppNav)

    await wrapper.find('#omop-workflow-step').trigger('click')

    expect(useWorkflowsStore().soon.label).toBe('OMOP')
  })

  it('shows RDF home and the steps inside the RDF workflow', () => {
    route.path = '/rdf'
    route.meta = { workflow: 'rdf' }
    const wrapper = mount(AppNav)

    expect(labels(wrapper)).toEqual(['RDF home', 'Ingest', 'Describe', 'Annotate', 'Share'])
    expect(wrapper.find('#rdf-home-step').attributes('href')).toBe('/rdf')
    expect(wrapper.find('#rdf-home-step').classes()).toContain('active')
    expect(wrapper.find('#home-step').exists()).toBe(false)
  })
})
