import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { createMemoryHistory, createRouter } from 'vue-router'

// The app asks a tab that comes by a link to a page of the RDF workflow for
// its project, and holds the page until it has chosen; the home page asks
// before it goes there (see useWorkflowEntry). A tab that comes back to the
// home page has left the workflow.

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
import { useWorkflowsStore } from '@/stores/workflows'

const CONFIGURED = {
  kind: 'rdf-store',
  repository: 'userRepo',
  url: 'http://rdf-store:7200/repositories/userRepo',
  source: 'configured',
  note: '',
}

let mounts = 0
// What the server says of its store: 'reachable' when another tab chose;
// `storeManaged`: whether Flyover runs it (the stack has a launcher).
let storeState = 'waiting'
let storeManaged = false
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
      // Another workflow's page: only RDF is built, but nothing here is RDF's.
      { path: '/csv', name: 'csv', component: Page('csv'), meta: { workflow: 'csv' } },
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
        ExitWorkflowDialog: true,
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
    storeManaged = false
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
          managed: storeManaged,
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

    expect(project.picker).toEqual({ open: true, mode: 'enter', workflow: 'rdf', target: null })
  })

  it("asks for the project on any workflow's page, for that workflow", async () => {
    let project, router
    ;({ wrapper, project, router } = await mountApp('/'))

    await router.push('/csv')
    await flushPromises()

    expect(project.picker).toEqual({ open: true, mode: 'enter', workflow: 'csv', target: null })
    expect(wrapper.find('.page').exists()).toBe(false)
  })

  it('asks a tab that comes by a link to a page of the workflow', async () => {
    let project
    ;({ wrapper, project } = await mountApp('/share/mock'))

    expect(project.picker).toEqual({ open: true, mode: 'enter', workflow: 'rdf', target: null })
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
    expect(project.picker).toEqual({ open: true, mode: 'enter', workflow: 'rdf', target: null })
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

    expect(project.picker).toEqual({ open: true, mode: 'enter', workflow: 'rdf', target: null })
    expect(wrapper.find('.page').exists()).toBe(false)
    expect(wrapper.find('store-wait-stub').attributes('reason')).toBe('waiting')

    project.enterWorkflow('rdf')
    project.closePicker()
    await flushPromises()
    expect(wrapper.find('.page').text()).toBe('describe')
  })

  it('holds a page that needs no store until the tab has chosen, and opens it once', async () => {
    let project
    ;({ wrapper, project } = await mountApp('/share/mock'))
    expect(wrapper.find('.page').exists()).toBe(false)
    expect(wrapper.find('store-wait-stub').attributes('reason')).toBe('waiting')
    expect(mounts).toBe(0)

    project.enterWorkflow('rdf')
    project.closePicker()
    await flushPromises()

    expect(wrapper.find('.page').text()).toBe('mock')
    expect(mounts).toBe(1)
  })

  it('stays in the workflow a tab entered on the home page, as it goes there', async () => {
    let project, router
    ;({ wrapper, project, router } = await mountApp('/'))
    project.openPicker('enter', { target: '/rdf' })

    // The choice: the tab enters while the home page still shows...
    project.enterWorkflow('rdf')
    project.closePicker()
    await flushPromises()
    expect(project.workflow).toBe('rdf')

    // ...and the pop-up goes on to the workflow's page.
    await router.push('/rdf')
    await flushPromises()
    expect(project.workflow).toBe('rdf')
    expect(project.picker.open).toBe(false)
    expect(wrapper.find('.page').text()).toBe('rdf')
  })

  describe('leaving the workflow with its store running', () => {
    beforeEach(() => {
      sessionStorage.setItem('flyover.workflow', 'rdf')
      storeState = 'reachable'
      storeManaged = true
    })

    it('asks whether to keep the store running, and goes home once answered', async () => {
      let project, router
      ;({ wrapper, project, router } = await mountApp('/rdf'))
      const workflows = useWorkflowsStore()

      const going = router.push('/')
      await flushPromises()
      expect(workflows.exitQuestion.workflow.id).toBe('rdf')
      expect(wrapper.find('.page').text()).toBe('rdf')

      workflows.answerExit('keep')
      await going
      await flushPromises()
      expect(wrapper.find('.page').text()).toBe('home')
      expect(project.workflow).toBe(null)
    })

    it('stays in the workflow when the user chooses to stay', async () => {
      let project, router
      ;({ wrapper, project, router } = await mountApp('/rdf'))
      const workflows = useWorkflowsStore()

      const going = router.push('/')
      await flushPromises()
      workflows.answerExit('stay')
      await going
      await flushPromises()

      expect(router.currentRoute.value.path).toBe('/rdf')
      expect(project.workflow).toBe('rdf')
    })

    it('asks nothing for a store Flyover does not run, or one that is not running', async () => {
      for (const [state, managed] of [
        ['reachable', false],
        ['stopped', true],
      ]) {
        setActivePinia(createPinia())
        storeState = state
        storeManaged = managed
        let router
        ;({ wrapper, router } = await mountApp('/rdf'))

        await router.push('/')
        await flushPromises()

        expect(useWorkflowsStore().exitQuestion).toBe(null)
        expect(wrapper.find('.page').text()).toBe('home')
        wrapper.unmount()
      }
      wrapper = undefined
    })

    it('asks nothing of a tab that only came by a link and went back without choosing', async () => {
      sessionStorage.clear()
      let project, router
      ;({ wrapper, project, router } = await mountApp('/rdf'))
      expect(project.picker.open).toBe(true)

      // As the pop-up's Cancel does.
      project.exitWorkflow()
      await router.push('/')
      await flushPromises()

      expect(useWorkflowsStore().exitQuestion).toBe(null)
      expect(wrapper.find('.page').text()).toBe('home')
    })
  })
})
