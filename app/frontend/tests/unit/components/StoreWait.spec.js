import { describe, it, expect, beforeEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick, reactive } from 'vue'

// A page that needs the store waits for it in its place: while nobody has
// chosen what to work on, while the store starts, and until the first
// answer. Once open, a page stays open; the next one waits again.

const route = reactive({ path: '/describe', meta: { needsStore: true } })

vi.mock('vue-router', () => ({ useRoute: () => route }))
vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async () => null),
  saveData: vi.fn(async () => {}),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({ refreshDataExists: vi.fn(async () => {}) }))

import StoreWait from '@/components/StoreWait.vue'
import { useStoreGate } from '@/composables/useStoreGate'
import { useProjectStore } from '@/stores/project'

function title(wrapper) {
  return wrapper.find('.store-wait-title').text()
}

function detail(wrapper) {
  const found = wrapper.find('.store-wait-detail')
  return found.exists() ? found.text() : ''
}

describe('StoreWait', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('says Flyover waits for a project choice, without dots', () => {
    const wrapper = mount(StoreWait, { props: { reason: 'waiting' } })

    expect(title(wrapper)).toBe('Waiting for a project choice')
    expect(detail(wrapper)).toBe('Choose a project, or continue without one, and this page opens.')
    expect(wrapper.find('.store-wait-dots').exists()).toBe(false)
    expect(wrapper.attributes('role')).toBe('status')
  })

  it('says the store starts, with the dots counting up out of screen readers', async () => {
    vi.useFakeTimers()
    try {
      const wrapper = mount(StoreWait, { props: { reason: 'starting' } })

      expect(title(wrapper)).toBe('Starting the RDF store.')
      expect(detail(wrapper)).toBe('This can take a minute. The page opens once the store answers.')
      await vi.advanceTimersByTimeAsync(800)
      expect(wrapper.find('.store-wait-dots').text()).toBe('...')
      expect(wrapper.find('.store-wait-dots').attributes('aria-hidden')).toBe('true')
    } finally {
      vi.useRealTimers()
    }
  })

  it('says Flyover connects to the store', () => {
    const wrapper = mount(StoreWait, { props: { reason: 'connecting' } })

    expect(title(wrapper)).toBe('Connecting to the RDF store.')
    expect(detail(wrapper)).toBe('')
    expect(wrapper.find('button').exists()).toBe(false)
  })

  it('offers to start a store someone stopped', async () => {
    const project = useProjectStore()
    const start = vi.spyOn(project, 'startStore').mockResolvedValue()
    const wrapper = mount(StoreWait, { props: { reason: 'stopped' } })

    expect(title(wrapper)).toBe('The RDF store is not running')
    expect(detail(wrapper)).toBe(
      'It was stopped, and Flyover leaves a stopped store alone. Start it to open this page.'
    )
    await wrapper.get('button').trigger('click')
    expect(start).toHaveBeenCalledOnce()
  })
})

describe('useStoreGate', () => {
  let project

  const Harness = {
    setup() {
      return useStoreGate()
    },
    template: '<p>{{ waitingFor }}</p>',
  }

  function state(value) {
    project.storeStatus = { state: value, reason: '', checking: false }
  }

  beforeEach(() => {
    setActivePinia(createPinia())
    project = useProjectStore()
    project.ready = true
    route.path = '/describe'
    route.meta = { needsStore: true }
  })

  it('waits until Flyover knows whether it will ask for a project', async () => {
    project.ready = false
    state('reachable')
    const wrapper = mount(Harness)
    expect(wrapper.text()).toBe('connecting')

    // Loaded with nothing chosen in this tab: the start-up pop-up opens.
    project.ready = true
    project.picker = { open: true, mode: 'startup' }
    await nextTick()
    expect(wrapper.text()).toBe('waiting')
  })

  it('keeps a store page waiting until someone chooses and the store has started', async () => {
    project.picker = { open: true, mode: 'startup' }
    state('reachable')
    const wrapper = mount(Harness)
    expect(wrapper.text()).toBe('waiting')

    project.picker = { open: false, mode: 'startup' }
    state('starting')
    await nextTick()
    expect(wrapper.text()).toBe('starting')

    state('reachable')
    await nextTick()
    expect(wrapper.text()).toBe('')
  })

  it('waits for the first answer, and for a server that says nobody chose', async () => {
    state('unknown')
    const wrapper = mount(Harness)
    expect(wrapper.text()).toBe('connecting')

    state('waiting')
    await nextTick()
    expect(wrapper.text()).toBe('waiting')
  })

  it('lets a page open on a store that does not answer, and says why above it', () => {
    project.storeStatus = { state: 'unreachable', reason: 'No answer.', checking: false }
    const TroubleHarness = {
      setup() {
        return useStoreGate()
      },
      template: '<p>{{ waitingFor }}|{{ trouble }}</p>',
    }

    expect(mount(TroubleHarness).text()).toBe('|No answer.')
  })

  it('holds a page while someone has stopped the store', async () => {
    state('stopped')
    const wrapper = mount(Harness)
    expect(wrapper.text()).toBe('stopped')

    state('starting')
    await nextTick()
    expect(wrapper.text()).toBe('starting')
  })

  it('keeps an open page open when the store starts again, and the next page waits', async () => {
    state('reachable')
    const wrapper = mount(Harness)

    state('starting')
    await nextTick()
    expect(wrapper.text()).toBe('')

    route.path = '/annotate'
    await nextTick()
    expect(wrapper.text()).toBe('starting')
  })

  it('never holds up a page that works without the store', () => {
    route.path = '/'
    route.meta = {}
    project.picker = { open: true, mode: 'startup' }
    state('starting')

    expect(mount(Harness).text()).toBe('')
  })

  it('does not hold a page up for the pop-up opened to switch projects', () => {
    project.picker = { open: true, mode: 'switch' }
    state('reachable')

    expect(mount(Harness).text()).toBe('')
  })
})
