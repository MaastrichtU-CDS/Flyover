import { describe, it, expect, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { nextTick } from 'vue'

// A workflow that is not built yet says so, in the style of the project
// pop-up, and closes again.

import WorkflowSoonDialog from '@/components/WorkflowSoonDialog.vue'
import { useWorkflowsStore } from '@/stores/workflows'

describe('WorkflowSoonDialog', () => {
  let workflows
  let wrapper

  beforeEach(() => {
    setActivePinia(createPinia())
    workflows = useWorkflowsStore()
    wrapper = mount(WorkflowSoonDialog, { attachTo: document.body })
  })

  it('shows nothing until a workflow asks', () => {
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('says the workflow is under development, and closes', async () => {
    workflows.showSoon('omop')
    await nextTick()

    const dialog = wrapper.find('[role="dialog"]')
    expect(dialog.find('#workflow-soon-title').text()).toBe('The OMOP workflow')
    expect(dialog.text()).toContain('This workflow is under development.')
    expect(dialog.text()).toContain('the RDF workflow is the one to use')

    await dialog.find('.workflow-soon-footer button').trigger('click')
    expect(workflows.soon).toBe(null)
    wrapper.unmount()
  })

  it('closes on a click beside it', async () => {
    workflows.showSoon('csv')
    await nextTick()

    await wrapper.find('.workflow-soon-backdrop').trigger('click')

    expect(workflows.soon).toBe(null)
    wrapper.unmount()
  })
})
