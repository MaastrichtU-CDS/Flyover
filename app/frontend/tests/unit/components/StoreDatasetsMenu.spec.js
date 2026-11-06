import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { reactive } from 'vue'

// The status bar's store opens a small panel: the datasets in the store
// Flyover works on, and the deletes, each asked first with the TriG file
// of what goes on offer.

const route = reactive({ path: '/describe' })

vi.mock('vue-router', () => ({ useRoute: () => route }))
vi.mock('@/lib/browser', () => ({ reloadPage: vi.fn() }))
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
import * as db from '@/lib/db'
import { reloadPage } from '@/lib/browser'
import { refreshDataExists } from '@/composables/useNavigation'
import StoreDatasetsMenu from '@/components/StoreDatasetsMenu.vue'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'

const DATASETS = [
  { name: 'patients', rows: 1234, described: true, annotated: true },
  { name: 'visits', rows: 1, described: false, annotated: false },
]

const RouterLinkStub = {
  props: ['to'],
  emits: ['click'],
  template: '<a :href="String(to)" @click.prevent="$emit(\'click\')"><slot /></a>',
}

function mountMenu() {
  const project = useProjectStore()
  project.store = { repository: 'userRepo', url: 'http://localhost:7200/repositories/userRepo' }
  return mount(StoreDatasetsMenu, {
    attachTo: document.body,
    slots: { default: 'RDF store · userRepo' },
    global: { stubs: { RouterLink: RouterLinkStub } },
  })
}

async function openPanel(wrapper) {
  await wrapper.find('#statusbar-store-button').trigger('click')
  await flushPromises()
  return wrapper.find('#store-datasets-panel')
}

function button(wrapper, label) {
  return wrapper.findAll('button').find((b) => b.text().includes(label))
}

function text(element) {
  return element.text().replace(/\s+/g, ' ').trim()
}

describe('StoreDatasetsMenu', () => {
  let wrapper

  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    route.path = '/describe'
    api.get.mockImplementation(async (url) => {
      if (url === '/api/v1/store/datasets') return { data: { datasets: DATASETS } }
      return {
        data: {
          reachable: true,
          reason: '',
          store: { repository: 'userRepo', url: 'http://localhost:7200/repositories/userRepo' },
        },
      }
    })
    api.delete.mockResolvedValue({ data: { deleted: [], datasets: [DATASETS[1]] } })
  })

  afterEach(() => {
    wrapper?.unmount()
    wrapper = undefined
  })

  it('opens under the store with every dataset, its rows and progress', async () => {
    wrapper = mountMenu()
    const toggle = wrapper.find('#statusbar-store-button')
    expect(toggle.attributes('aria-expanded')).toBe('false')

    const panel = await openPanel(wrapper)

    expect(toggle.attributes('aria-expanded')).toBe('true')
    expect(panel.attributes('role')).toBe('dialog')
    expect(panel.attributes('aria-label')).toBe('Datasets in userRepo')
    const items = panel.findAll('.store-panel-item').map(text)
    expect(items).toEqual([
      'patients 1,234 rows · Described · Annotated',
      'visits 1 row · Not described yet · Not annotated yet',
    ])
    expect(api.get).toHaveBeenCalledWith('/api/v1/store/datasets')
  })

  it('says when the store holds no datasets, and links to the ingest', async () => {
    api.get.mockResolvedValue({ data: { datasets: [] } })
    wrapper = mountMenu()

    const panel = await openPanel(wrapper)

    expect(panel.text()).toContain('No datasets in this store yet.')
    expect(panel.find('a[href="/ingest"]').text()).toContain('Add a dataset')
    expect(button(wrapper, 'Delete every dataset').attributes('disabled')).toBeDefined()
    expect(button(wrapper, 'Delete all annotations').attributes('disabled')).toBeDefined()
  })

  it('says why the store cannot be listed, and lets the user try again', async () => {
    api.get.mockRejectedValueOnce({
      response: { data: { error: 'Currently, the store cannot be reached by Flyover.' } },
    })
    wrapper = mountMenu()

    const panel = await openPanel(wrapper)
    expect(panel.find('[role="alert"]').text()).toContain('cannot be reached by Flyover')

    await button(wrapper, 'Try again').trigger('click')
    await flushPromises()
    expect(wrapper.findAll('.store-panel-item')).toHaveLength(2)
  })

  it('asks before deleting a dataset, offering its TriG file first', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    await wrapper.find('button[aria-label="Delete patients"]').trigger('click')

    const confirm = wrapper.find('.store-panel-confirm')
    expect(text(confirm.find('.store-panel-question'))).toBe('Delete patients?')
    expect(confirm.text()).toContain('Its data (1,234 rows), its describe answers')
    const download = confirm.find('a[download]')
    expect(download.attributes('href')).toBe('/api/v1/store/datasets/patients/export')
    expect(download.text()).toContain('Download it first (.trig)')
    expect(api.delete).not.toHaveBeenCalled()

    await button(wrapper, 'Delete patients').trigger('click')
    await flushPromises()

    expect(api.delete).toHaveBeenCalledWith('/api/v1/store/datasets/patients')
    expect(wrapper.findAll('.store-panel-item').map(text)).toEqual([
      'visits 1 row · Not described yet · Not annotated yet',
    ])
    // The browser forgets what it cached of the store; the steps ask again.
    expect(db.deleteData).toHaveBeenCalledWith('metadata', 'column_info')
    expect(refreshDataExists).toHaveBeenCalled()
  })

  it('names a dataset in its addresses safely', async () => {
    api.get.mockResolvedValue({
      data: { datasets: [{ name: 'a#b', rows: 0, described: false, annotated: false }] },
    })
    wrapper = mountMenu()
    await openPanel(wrapper)

    await wrapper.find('button[aria-label="Delete a#b"]').trigger('click')
    await button(wrapper, 'Delete a#b').trigger('click')
    await flushPromises()

    expect(api.delete).toHaveBeenCalledWith('/api/v1/store/datasets/a%23b')
  })

  it('says the open project\'s map stays when a project is open', async () => {
    wrapper = mountMenu()
    useProjectStore().project = { name: 'AYA mapping' }
    await openPanel(wrapper)

    await wrapper.find('button[aria-label="Delete visits"]').trigger('click')

    expect(wrapper.find('.store-panel-confirm').text()).toContain(
      "The open project's map stays as it is."
    )
  })

  it('asks before deleting every dataset, and only Flyover\'s', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    await button(wrapper, 'Delete every dataset').trigger('click')

    const confirm = wrapper.find('.store-panel-confirm')
    expect(confirm.text()).toContain('All 2 datasets in userRepo')
    expect(confirm.text()).toContain('Anything else the repository holds stays.')
    expect(confirm.find('a[download]').attributes('href')).toBe('/api/v1/store/export')

    await wrapper.findAll('.store-panel-confirm button').find((b) => b.text().includes('Delete every dataset')).trigger('click')
    await flushPromises()
    expect(api.delete).toHaveBeenCalledWith('/api/v1/store/datasets')
  })

  it('asks before deleting every annotation, keeping data and descriptions', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    await button(wrapper, 'Delete all annotations').trigger('click')

    const confirm = wrapper.find('.store-panel-confirm')
    expect(confirm.text()).toContain('The data and the describe answers stay.')
    expect(confirm.find('a[download]').attributes('href')).toBe(
      '/api/v1/store/annotations/export'
    )
    await button(wrapper, 'Delete every annotation').trigger('click')
    await flushPromises()
    expect(api.delete).toHaveBeenCalledWith('/api/v1/store/annotations')
  })

  it('goes back to the list on Cancel, or on Escape while asking', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    await wrapper.find('button[aria-label="Delete patients"]').trigger('click')
    await button(wrapper, 'Cancel').trigger('click')
    expect(wrapper.find('.store-panel-confirm').exists()).toBe(false)

    await wrapper.find('button[aria-label="Delete patients"]').trigger('click')
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    await flushPromises()
    expect(wrapper.find('.store-panel-confirm').exists()).toBe(false)
    expect(wrapper.find('#store-datasets-panel').exists()).toBe(true)
    expect(api.delete).not.toHaveBeenCalled()
  })

  it('shows a refused delete and keeps the question open', async () => {
    api.delete.mockRejectedValueOnce({
      response: { data: { error: 'The store refused to delete (HTTP 500).' } },
    })
    wrapper = mountMenu()
    await openPanel(wrapper)

    await wrapper.find('button[aria-label="Delete patients"]').trigger('click')
    await button(wrapper, 'Delete patients').trigger('click')
    await flushPromises()

    expect(wrapper.find('[role="alert"]').text()).toContain('refused to delete (HTTP 500)')
    expect(wrapper.find('.store-panel-confirm').exists()).toBe(true)
  })

  it('closes on Escape or a click elsewhere, giving the focus back', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    await flushPromises()
    expect(wrapper.find('#store-datasets-panel').exists()).toBe(false)
    expect(document.activeElement).toBe(wrapper.find('#statusbar-store-button').element)

    await openPanel(wrapper)
    document.body.dispatchEvent(new Event('pointerdown', { bubbles: true }))
    await flushPromises()
    expect(wrapper.find('#store-datasets-panel').exists()).toBe(false)
  })

  it('stays open while a delete runs, whatever the user presses', async () => {
    let answer
    api.delete.mockImplementationOnce(() => new Promise((resolve) => (answer = resolve)))
    wrapper = mountMenu()
    await openPanel(wrapper)
    await wrapper.find('button[aria-label="Delete patients"]').trigger('click')
    await button(wrapper, 'Delete patients').trigger('click')

    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    document.body.dispatchEvent(new Event('pointerdown', { bubbles: true }))
    await flushPromises()
    expect(wrapper.find('#store-datasets-panel').exists()).toBe(true)
    expect(wrapper.find('.store-panel-close').attributes('disabled')).toBeDefined()

    answer({ data: { deleted: ['patients'], datasets: [DATASETS[1]] } })
    await flushPromises()
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    await flushPromises()

    expect(wrapper.find('#store-datasets-panel').exists()).toBe(false)
    expect(reloadPage).toHaveBeenCalledTimes(1)
    sessionStorage.clear()
  })

  it('reloads a step page after a delete, once the panel closes', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)
    await wrapper.find('button[aria-label="Delete patients"]').trigger('click')
    await button(wrapper, 'Delete patients').trigger('click')
    await flushPromises()
    expect(reloadPage).not.toHaveBeenCalled()

    await wrapper.find('.store-panel-close').trigger('click')

    expect(reloadPage).toHaveBeenCalledTimes(1)
    expect(sessionStorage.getItem('flyover.project.notice')).toBe(
      'Deleted patients from userRepo.'
    )
    sessionStorage.clear()
  })

  it('says what went on the home page instead of reloading', async () => {
    route.path = '/'
    wrapper = mountMenu()
    await openPanel(wrapper)
    await button(wrapper, 'Delete all annotations').trigger('click')
    await button(wrapper, 'Delete every annotation').trigger('click')
    await flushPromises()

    await wrapper.find('#statusbar-store-button').trigger('click')

    expect(reloadPage).not.toHaveBeenCalled()
    expect(useStatusStore().messages.at(-1).text).toBe(
      'Deleted every annotation from userRepo.'
    )
  })

  it('closes without a word when nothing went', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    await wrapper.find('.store-panel-close').trigger('click')

    expect(reloadPage).not.toHaveBeenCalled()
    expect(useStatusStore().messages).toEqual([])
  })

  it('closes when the user goes to add a dataset', async () => {
    wrapper = mountMenu()
    await openPanel(wrapper)

    await wrapper.find('a[href="/ingest"]').trigger('click')

    expect(wrapper.find('#store-datasets-panel').exists()).toBe(false)
  })
})
