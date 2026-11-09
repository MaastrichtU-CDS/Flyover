import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { nextTick } from 'vue'
import { createPinia, setActivePinia } from 'pinia'

// The RDF workflow's landing page, "Flyover × RDF": its four steps as
// tiles. Describe and Annotate wait for data in the store; Ingest and Share
// always open.

const router = { push: vi.fn() }
vi.mock('vue-router', () => ({ useRouter: () => router }))
vi.mock('@/composables/useNavigation', async () => {
  const { ref } = await import('vue')
  const dataExists = ref(false)
  return { useNavigation: () => ({ dataExists }), dataExists }
})

import { dataExists } from '@/composables/useNavigation'
import RdfLandingView from '@/views/RdfLandingView.vue'
import { useStatusStore } from '@/stores/status'

const RouterLinkStub = { props: ['to'], template: '<a :href="String(to)"><slot /></a>' }

function mountLanding() {
  return mount(RdfLandingView, { global: { stubs: { RouterLink: RouterLinkStub } } })
}

const NEEDS_DATA = 'Please complete the Ingest step first by submitting your data.'
const TOOLTIP = 'First complete the data ingest step to proceed with this step.'

describe('RdfLandingView', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    router.push.mockClear()
    dataExists.value = false
  })

  it('is Flyover × RDF, with the four-step process in its banner and nothing more', () => {
    const wrapper = mountLanding()
    const hero = wrapper.find('.hero-section')

    expect(hero.findAll('h1, p').map((line) => line.text().replace(/\s+/g, ' '))).toEqual([
      'Flyover × RDF',
      'Transform your data into Resource Description Framework (RDF) triples through our guided four-step process',
    ])
    // The × is set apart from the names either side of it, so it can sit
    // at the centre of the page, at about half their size.
    expect(hero.findAll('h1 > span').map((part) => part.text())).toEqual(['Flyover', '×', 'RDF'])
    expect(hero.find('h1 .hero-times').text()).toBe('×')
  })

  it('shows the four steps in order', () => {
    const wrapper = mountLanding()

    expect(wrapper.findAll('.workflow-card .step-title').map((t) => t.text())).toEqual([
      '1. Ingest',
      '2. Describe',
      '3. Annotate',
      '4. Share',
    ])
  })

  it('opens a step on a click or on Enter once the store holds data', async () => {
    dataExists.value = true
    const wrapper = mountLanding()

    await wrapper.find('#describe-card').trigger('click')
    await wrapper.find('#annotate-card').trigger('keyup.enter')

    expect(router.push.mock.calls).toEqual([['/describe'], ['/annotate']])
    expect(wrapper.findAll('.workflow-card.disabled')).toHaveLength(0)
  })

  it('keeps Describe and Annotate closed until the store holds data, and says why', async () => {
    const wrapper = mountLanding()
    const describeCard = wrapper.find('#describe-card')

    expect(wrapper.findAll('.workflow-card.disabled').map((card) => card.attributes('id'))).toEqual([
      'describe-card',
      'annotate-card',
    ])
    expect(describeCard.attributes('aria-disabled')).toBe('true')

    await describeCard.trigger('click')
    await wrapper.find('#annotate-card').trigger('keyup.enter')
    expect(router.push).not.toHaveBeenCalled()
    expect(useStatusStore().messages.map(({ text, level }) => [level, text])).toEqual([
      ['warning', NEEDS_DATA],
      ['warning', NEEDS_DATA],
    ])

    await describeCard.trigger('mouseenter')
    expect(wrapper.find('[role="tooltip"]').text()).toBe(TOOLTIP)
    await describeCard.trigger('mouseleave')
    expect(wrapper.find('[role="tooltip"]').exists()).toBe(false)
  })

  it('always opens Ingest and Share, and shows them no tooltip', async () => {
    const wrapper = mountLanding()

    await wrapper.find('#ingest-card').trigger('focus')
    expect(wrapper.find('[role="tooltip"]').exists()).toBe(false)
    await wrapper.find('#ingest-card').trigger('click')
    await wrapper.find('#share-card').trigger('click')

    expect(router.push.mock.calls).toEqual([['/ingest'], ['/share']])
    expect(useStatusStore().messages).toEqual([])
  })

  it('opens Describe as soon as the data arrives', async () => {
    const wrapper = mountLanding()
    expect(wrapper.find('#describe-card').classes()).toContain('disabled')

    dataExists.value = true
    await nextTick()

    expect(wrapper.find('#describe-card').classes()).not.toContain('disabled')
    expect(wrapper.find('#describe-card').attributes('aria-disabled')).toBe('false')
  })

  it('starts the process at Ingest', () => {
    const wrapper = mountLanding()
    const start = wrapper.find('.cta-section a')

    expect(start.attributes('href')).toBe('/ingest')
    expect(start.text()).toBe('Start Data F.A.I.R.-ification Process')
  })
})
