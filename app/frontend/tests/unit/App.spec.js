import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { createMemoryHistory, createRouter } from 'vue-router'

// The app asks a tab for its project as it enters the RDF workflow (from the
// home page, or by a link to one of the workflow's pages), and a tab that
// comes back to the home page has left the workflow. The page under the
// pop-up opens afresh once the tab has chosen.

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async () => null),
  saveData: vi.fn(async () => {}),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({ refreshDataExists: vi.fn(async () => {}) }))

import api from '@/services/api'
import App from '@/App.vue'
import { useProjectStore } from '@/stores/project'

const CONFIGURED = {
  kind: 'rdf-store',
  repository: 'userRepo',
  url: 'http://rdf-store:7200/repositories/userRepo',
  source: 'configured',
  note: '',
}

let mounts = 0
// What the server says of its store: 'reachable' when another tab chose.
let storeState = 'waiting'
const Page = (name) => ({
  name,
  mounted() {
    mounts += 1
  },
  template: `<p class="page">${name}</p>`,
})

function makeRouter() {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/', name: 'home', component: Page('home') },
      { path: '/rdf', name: 'rdf', component: Page('rdf'), meta: { workflow: 'rdf' } },
      {
        path: '/share/mock',
        name: 'share-mock',
        component: Page('mock'),
        meta: { workflow: 'rdf' },
      },
      {
        path: '/describe',
        name: 'describe',
        component: Page('describe'),
        meta: { workflow: 'rdf', needsStore: true },
      },
    ],
  })
}

function mountShell(router) {
  return mount(App, {
    global: {
      plugins: [router],
      stubs: {
        AppNav: true,
        AppFooter: true,
        MapUploadDialog: true,
        ProjectPicker: true,
        ProjectStatusBar: true,
        StatusBanner: true,
        StoreWait: true,
        WorkflowSoonDialog: true,
      },
    },
  })
}

async function mountApp(path) {
  const router = makeRouter()
  const wrapper = mountShell(router)
  await router.push(path)
  await router.isReady()
  await flushPromises()
  return { wrapper, router, project: useProjectStore() }
}

describe('App', () => {
  let wrapper

  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    vi.stubGlobal('BroadcastChannel', undefined)
    mounts = 0
    storeState = 'waiting'
    api.get.mockImplementation(async (url) => {
      if (url === '/api/v1/project') {
        return {
          data: { project: null, route: null, store: CONFIGURED, configuredStore: CONFIGURED },
        }
      }
      return {
        data: {
          state: storeState,
          reachable: storeState === 'reachable',
          reason: '',
          store: CONFIGURED,
        },
      }
    })
  })

  afterEach(() => {
    wrapper?.unmount()
    wrapper = undefined
    useProjectStore().stopStatusChecks()
    vi.unstubAllGlobals()
  })

  it('asks nothing on the home page', async () => {
    let project
    ;({ wrapper, project } = await mountApp('/'))

    expect(wrapper.find('.page').text()).toBe('home')
    expect(project.picker.open).toBe(false)
  })

  it('asks for the project as the tab enters the RDF workflow', async () => {
    let project, router
    ;({ wrapper, project, router } = await mountApp('/'))

    await router.push('/rdf')
    await flushPromises()

    expect(project.picker).toEqual({ open: true, mode: 'enter' })
  })

  it('asks a tab that comes by a link to a page of the workflow', async () => {
    let project
    ;({ wrapper, project } = await mountApp('/share/mock'))

    expect(project.picker).toEqual({ open: true, mode: 'enter' })
  })

  it('does not ask a reloaded tab that is in the workflow already', async () => {
    sessionStorage.setItem('flyover.workflow', 'rdf')
    let project
    ;({ wrapper, project } = await mountApp('/share/mock'))

    expect(project.picker.open).toBe(false)
    // The page the router starts from is no home page to leave the workflow on.
    expect(project.workflow).toBe('rdf')
  })

  it('does not ask again when the pop-up closes on the same page', async () => {
    let project
    ;({ wrapper, project } = await mountApp('/rdf'))

    project.closePicker()
    await flushPromises()

    expect(project.picker.open).toBe(false)
  })

  it('leaves the workflow when the tab comes back to the home page', async () => {
    sessionStorage.setItem('flyover.workflow', 'rdf')
    let project, router
    ;({ wrapper, project, router } = await mountApp('/rdf'))

    await router.push('/')
    await flushPromises()

    expect(project.workflow).toBe(null)
    expect(sessionStorage.getItem('flyover.workflow')).toBe(null)

    await router.push('/rdf')
    await flushPromises()
    expect(project.picker).toEqual({ open: true, mode: 'enter' })
  })

  it('does not ask again as the tab exits the workflow from one of its pages', async () => {
    sessionStorage.setItem('flyover.workflow', 'rdf')
    let project, router
    ;({ wrapper, project, router } = await mountApp('/rdf'))

    // As Exit RDF does: out of the workflow a moment before the home page.
    project.exitWorkflow()
    await flushPromises()
    expect(project.picker.open).toBe(false)

    await router.push('/')
    await flushPromises()
    expect(project.picker.open).toBe(false)
    expect(wrapper.find('.page').text()).toBe('home')
  })

  it('holds a step page for the choice when Flyover knew its store before the router settled', async () => {
    // Another tab chose, so the store answers; this tab has not entered.
    storeState = 'reachable'
    const router = makeRouter()
    wrapper = mountShell(router)
    const project = useProjectStore()
    await flushPromises()
    expect(project.ready).toBe(true)

    await router.push('/describe')
    await flushPromises()

    expect(project.picker).toEqual({ open: true, mode: 'enter' })
    expect(wrapper.find('.page').exists()).toBe(false)
    expect(wrapper.find('store-wait-stub').attributes('reason')).toBe('waiting')

    project.enterWorkflow('rdf')
    project.closePicker()
    await flushPromises()
    expect(wrapper.find('.page').text()).toBe('describe')
  })

  it('opens the page under the pop-up afresh once the tab has chosen', async () => {
    let project
    ;({ wrapper, project } = await mountApp('/share/mock'))
    expect(mounts).toBe(1)

    project.enterWorkflow('rdf')
    project.closePicker()
    await flushPromises()

    expect(wrapper.find('.page').text()).toBe('mock')
    expect(mounts).toBe(2)
  })

  it('keeps the page as it is when nothing about the workflow changes', async () => {
    sessionStorage.setItem('flyover.workflow', 'rdf')
    let project
    ;({ wrapper, project } = await mountApp('/share/mock'))

    project.enterWorkflow('rdf')
    await flushPromises()

    expect(mounts).toBe(1)
  })
})
