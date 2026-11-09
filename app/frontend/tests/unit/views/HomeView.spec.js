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

  it('says only who it is for in its banner', () => {
    const wrapper = mountHome()
    const hero = wrapper.find('.hero-section')

    expect(hero.findAll('h1, p').map((line) => line.text())).toEqual([
      'Welcome to Flyover',
      'A privacy-aware tool to transform your data into F.A.I.R. semantically interoperable resources',
    ])
  })

  it('says what each workflow is for, and what sets it apart', () => {
    const wrapper = mountHome()
    const tile = (id) => wrapper.find(`#${id}-card`)
    const features = (id) => tile(id).findAll('.step-features li').map((item) => item.text())

    expect(tile('rdf').find('.step-description').text()).toBe(
      'Transform your data to F.A.I.R. resources using the flexibility of RDF, coupled with an RDF store.'
    )
    // RDF's flexibility against OMOP's standards, point for point.
    expect(features('rdf')).toEqual([
      'Flexible vocabularies',
      'Flexible data schemas',
      'Enables complex reasoning',
    ])
    expect(tile('omop').find('.step-description').text()).toBe(
      'Transform your data into OMOP CDM-conformant schemas, coupled with an OMOP database.'
    )
    expect(features('omop')).toEqual([
      'Standardised vocabularies',
      'Standardised data schema',
      'OHDSI tools and network studies',
    ])
    expect(tile('csv').find('.step-description').text()).toBe(
      'Describe and transform your data to a standardised terminology with the simplicity of CSV files.'
    )
    expect(features('csv')).toEqual([
      'No database needed',
      'Works with any analysis tool',
      'Portable and easily shareable files',
    ])
  })

  it("shows W3C's and OHDSI's logos in white in the circles, and says Flyover is not OHDSI's", () => {
    const wrapper = mountHome()

    // W3C asks for this alt text on its Semantic Web cube.
    const rdf = wrapper.find('#rdf-card .step-icon img')
    expect(rdf.attributes('src')).toBe('/static/logos/w3c-semantic-web-cube.svg')
    expect(rdf.attributes('alt')).toBe('W3C Semantic Web Logo')
    // W3C lets the cube take other colours; OHDSI publishes its own white logo.
    expect(rdf.classes()).toContain('is-whitened')
    const ohdsi = wrapper.find('#omop-card .step-icon img')
    expect(ohdsi.attributes('src')).toBe('/static/logos/ohdsi-logo-only-white.png')
    expect(ohdsi.classes()).not.toContain('is-whitened')
    expect(ohdsi.attributes('alt')).toBe('OHDSI logo')
    const omopLines = wrapper.findAll('#omop-card .workflow-note-line').map((l) => l.text())
    expect(omopLines).toEqual([
      'The OMOP common data model is maintained by OHDSI.',
      'Flyover is not affiliated with OHDSI.',
    ])
    // CSV keeps an icon.
    expect(wrapper.find('#csv-card .step-icon i.fa-file-csv').exists()).toBe(true)
  })

  it('says briefly, in small print, where RDF and OMOP come from, and what CSV files are', () => {
    const wrapper = mountHome()
    const note = (id) => wrapper.find(`#${id}-card .workflow-note`)

    // The non-affiliation on a line of its own, as on the OMOP tile.
    expect(note('rdf').findAll('.workflow-note-line').map((l) => l.text())).toEqual([
      'RDF is a Semantic Web technology of the World Wide Web Consortium (W3C).',
      'Flyover is not affiliated with W3C.',
    ])
    expect(note('rdf').find('a').attributes('href')).toBe('https://www.w3.org/')
    expect(note('csv').text()).toBe('CSV files are simple text files, usable as spreadsheets.')
    expect(note('csv').find('a').exists()).toBe(false)
  })

  it("links OHDSI in the small print, beside Flyover, without opening the tile", async () => {
    const wrapper = mountHome()
    const workflows = useWorkflowsStore()

    const link = wrapper.find('#omop-card .workflow-note a')
    expect(link.text()).toBe('OHDSI')
    expect(link.attributes('href')).toBe('https://www.ohdsi.org/')
    expect(link.attributes('target')).toBe('_blank')
    expect(link.attributes('rel')).toBe('noopener noreferrer')

    await link.trigger('click')
    await link.trigger('keyup.enter')
    expect(workflows.soon).toBe(null)
  })

  it('invites a start with a shared JSON-LD, or without one', () => {
    const wrapper = mountHome()

    expect(wrapper.find('.cta-section p').text()).toBe(
      'Use a shared JSON-LD to align your data with partners, or familiarise yourself with the workflow without one.'
    )
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
