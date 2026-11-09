import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useWorkflowsStore } from '@/stores/workflows'

// The notes the workflows show: the one for a workflow under development,
// and the notice beside Exit, which shows until the user clicks "Got it".

describe('the workflows store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    localStorage.clear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('says which workflow is under development', () => {
    const workflows = useWorkflowsStore()

    workflows.showSoon('omop')
    expect(workflows.soon.label).toBe('OMOP')

    workflows.closeSoon()
    expect(workflows.soon).toBe(null)
  })

  it('shows the notice beside Exit each time until the user clicks "Got it"', () => {
    const workflows = useWorkflowsStore()

    workflows.showExitNotice()
    expect(workflows.exitNotice).toBe(true)
    // Escape, or moving on: it shows again next time.
    workflows.closeExitNotice()
    expect(workflows.exitNotice).toBe(false)
    workflows.showExitNotice()
    expect(workflows.exitNotice).toBe(true)

    workflows.closeExitNotice({ acknowledged: true })
    workflows.showExitNotice()
    expect(workflows.exitNotice).toBe(false)
  })

  it('remembers "Got it" in this browser', () => {
    useWorkflowsStore().showExitNotice()
    useWorkflowsStore().closeExitNotice({ acknowledged: true })

    setActivePinia(createPinia())
    const later = useWorkflowsStore()
    later.showExitNotice()

    expect(localStorage.getItem('flyover.exitNoticeSeen')).toBe('1')
    expect(later.exitNotice).toBe(false)
  })

  it('asks whether to keep the store running as a user leaves, and hands on the answer', async () => {
    const workflows = useWorkflowsStore()

    const asked = workflows.askExit({ id: 'rdf', label: 'RDF' })
    expect(workflows.exitQuestion.workflow.label).toBe('RDF')

    workflows.answerExit('keep')
    expect(await asked).toBe('keep')
    expect(workflows.exitQuestion).toBe(null)
  })

  it('closes the notice beside Exit as it asks, without counting it as read', () => {
    const workflows = useWorkflowsStore()
    workflows.showExitNotice()

    // The user did what the notice says: it must not sit over the question.
    workflows.askExit({ id: 'rdf', label: 'RDF' })
    expect(workflows.exitNotice).toBe(false)

    workflows.answerExit('stay')
    workflows.showExitNotice()
    expect(workflows.exitNotice).toBe(true)
    expect(localStorage.getItem('flyover.exitNoticeSeen')).toBe(null)
  })

  it('lets a second question replace one still open, which then stays', async () => {
    const workflows = useWorkflowsStore()

    const first = workflows.askExit({ id: 'rdf', label: 'RDF' })
    const second = workflows.askExit({ id: 'rdf', label: 'RDF' })
    workflows.answerExit('stopped')

    expect(await first).toBe('stay')
    expect(await second).toBe('stopped')
  })

  it('still shows and closes the notice where the browser keeps nothing', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('SecurityError')
    })
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('SecurityError')
    })
    const workflows = useWorkflowsStore()

    workflows.showExitNotice()
    expect(workflows.exitNotice).toBe(true)
    expect(() => workflows.closeExitNotice({ acknowledged: true })).not.toThrow()
    expect(workflows.exitNotice).toBe(false)
  })
})
