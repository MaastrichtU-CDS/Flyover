import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { reactive, ref } from 'vue'

// The bar under the status strip: the workflows on the home page, and
// "RDF home" with the steps inside the RDF workflow.

const route = reactive({ path: '/', meta: {} })
const router = { push: vi.fn() }
vi.mock('vue-router', () => ({
  useRoute: () => route,
  useRouter: () => router,
  RouterLink: { props: ['to'], template: '<a :href="String(to)"><slot /></a>' },
}))
vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async () => null),
  saveData: vi.fn(async () => {}),
  deleteData: vi.fn(async () => {}),
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
import { useProjectStore } from '@/stores/project'
import { useWorkflowsStore } from '@/stores/workflows'

function labels(wrapper) {
  return wrapper.findAll('.nav-step .step-text').map((item) => item.text())
}

describe('AppNav', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('offers the workflows on the home page, and no steps or Home', () => {
    route.path = '/'
    route.meta = {}
    const wrapper = mount(AppNav)

    // Home would point to the page itself; every item leads away from it.
    expect(labels(wrapper)).toEqual(['RDF', 'OMOP', 'CSV'])
    expect(wrapper.find('#home-step').exists()).toBe(false)
    expect(wrapper.findAll('.step-separator')).toHaveLength(2)
  })

  it("shows the tiles' logos in white in the workflows' discs", () => {
    route.path = '/'
    route.meta = {}
    const wrapper = mount(AppNav)
    const disc = (id) => wrapper.find(`#${id}-workflow-step .workflow-disc`)

    // W3C lets the cube take other colours; OHDSI publishes its own white logo.
    const cube = disc('rdf').find('img')
    expect(cube.attributes('src')).toBe('/static/logos/w3c-semantic-web-cube.svg')
    expect(cube.classes()).toContain('is-whitened')
    const ohdsi = disc('omop').find('img')
    expect(ohdsi.attributes('src')).toBe('/static/logos/ohdsi-logo-only-white.png')
    expect(ohdsi.classes()).not.toContain('is-whitened')
    // The name beside each disc says it; the logos add nothing to read out.
    expect(cube.attributes('alt')).toBe('')
    expect(disc('csv').find('i').classes()).toContain('fa-file-csv')
  })

  it('says a workflow under development is so', async () => {
    route.path = '/'
    route.meta = {}
    const wrapper = mount(AppNav)

    await wrapper.find('#omop-workflow-step').trigger('click')

    expect(useWorkflowsStore().soon.label).toBe('OMOP')
  })

  it('asks for the project on the home page before it opens the RDF workflow', async () => {
    route.path = '/'
    route.meta = {}
    router.push.mockClear()
    const wrapper = mount(AppNav)

    await wrapper.find('#rdf-workflow-step').trigger('click')

    expect(useProjectStore().picker).toEqual({ open: true, mode: 'enter', target: '/rdf' })
    expect(router.push).not.toHaveBeenCalled()
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
