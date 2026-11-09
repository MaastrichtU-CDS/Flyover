import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'

// Leaving a workflow asks whether to keep its store running or stop it;
// the dialog stops it itself, and says so, or why it could not.

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
import ExitWorkflowDialog from '@/components/ExitWorkflowDialog.vue'
import { workflowById } from '@/lib/workflows'
import { useProjectStore } from '@/stores/project'
import { useWorkflowsStore } from '@/stores/workflows'

function button(wrapper, label) {
  return wrapper.findAll('button').find((b) => b.text().includes(label))
}

describe('ExitWorkflowDialog', () => {
  let wrapper
  let workflows
  let asked

  beforeEach(async () => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
    wrapper = mount(ExitWorkflowDialog, { attachTo: document.body })
    workflows = useWorkflowsStore()
    asked = workflows.askExit(workflowById('rdf'))
    await flushPromises()
  })

  afterEach(() => {
    wrapper.unmount()
  })

  it('asks whether to keep the RDF store running, and says a stopped one cannot be queried', () => {
    const dialog = wrapper.find('[role="dialog"]')
    expect(dialog.attributes('aria-modal')).toBe('true')
    expect(wrapper.find('#exit-workflow-title').text()).toBe('Leave the RDF workflow?')
    const [question, note] = wrapper.findAll('.exit-workflow-body p').map((p) =>
      p.text().replace(/\s+/g, ' ')
    )
    expect(question).toBe(
      'Do you want to keep the RDF store running, for your next visit and for other tools that use it, or stop it to free its memory?'
    )
    expect(note).toBe(
      'Once stopped, the RDF store cannot be queried, by Flyover or any other tool, until it starts again. Its data stays in it.'
    )
    expect(button(wrapper, 'Keep it running')).toBeTruthy()
    expect(button(wrapper, 'Stop the RDF store')).toBeTruthy()
  })

  it('leaves the store running', async () => {
    await button(wrapper, 'Keep it running').trigger('click')

    expect(await asked).toBe('keep')
    expect(api.post).not.toHaveBeenCalled()
    await flushPromises()
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
  })

  it('stops the store, saying so while it does', async () => {
    let answer
    api.post.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          answer = resolve
        })
    )

    await button(wrapper, 'Stop the RDF store').trigger('click')
    expect(api.post).toHaveBeenCalledWith('/api/v1/project/store-stop')
    expect(wrapper.find('#exit-workflow-stop').text()).toMatch(/^Stopping/)
    expect(button(wrapper, 'Keep it running').element.disabled).toBe(true)
    // Nothing closes it while the store stops.
    await wrapper.find('.exit-workflow-backdrop').trigger('click')
    expect(workflows.exitQuestion).not.toBe(null)

    answer({ data: { state: 'stopped', reachable: false, reason: '', managed: true } })
    expect(await asked).toBe('stopped')
    expect(useProjectStore().storeStatus.state).toBe('stopped')
  })

  it('says why the store could not be stopped, and lets the user choose again', async () => {
    api.post.mockRejectedValueOnce({
      response: { status: 502, data: { error: 'The store launcher refused: busy' } },
    })

    await button(wrapper, 'Stop the RDF store').trigger('click')
    await flushPromises()

    expect(wrapper.find('.alert-danger').text()).toBe(
      'Flyover could not stop the RDF store: The store launcher refused: busy'
    )
    expect(workflows.exitQuestion).not.toBe(null)
    await button(wrapper, 'Keep it running').trigger('click')
    expect(await asked).toBe('keep')
  })

  it.each([
    ['its Stay button', (w) => button(w, 'Stay').trigger('click')],
    ['its close button', (w) => w.find('.exit-workflow-close').trigger('click')],
    ['a click beside it', (w) => w.find('.exit-workflow-backdrop').trigger('click')],
    [
      'Escape',
      () => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' })),
    ],
  ])('keeps the user where they were on %s', async (_how, act) => {
    await act(wrapper)

    expect(await asked).toBe('stay')
    expect(api.post).not.toHaveBeenCalled()
  })
})
