import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: vi.fn(), delete: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn(async () => null),
  saveData: vi.fn(async () => {}),
  deleteData: vi.fn(async () => {}),
}))
vi.mock('@/composables/useNavigation', () => ({ refreshDataExists: vi.fn(async () => {}) }))
const router = { push: vi.fn() }
vi.mock('vue-router', () => ({ useRouter: () => router, useRoute: () => ({ meta: {} }) }))

import MapUploadDialog from '@/components/MapUploadDialog.vue'
import { useProjectStore } from '@/stores/project'
import { useWorkflowsStore } from '@/stores/workflows'

// The question a map uploaded in a step raises: open it as the project, or
// keep what Flyover works on.

const CONFIGURED = {
  kind: 'rdf-store',
  repository: 'userRepo',
  url: 'http://rdf-store:7200/repositories/userRepo',
  source: 'configured',
  note: '',
}
const AYA_STORE = {
  kind: 'rdf-store',
  repository: 'aya',
  url: 'https://store.example.org/repositories/aya',
  source: 'project',
  note: '',
}

function question(kind, extra = {}) {
  return {
    kind,
    fileName: 'aya.jsonld',
    edited: kind === 'same-project',
    check: {
      valid: true,
      project: { name: 'AYA mapping', version: '1.0.0', schemaName: 'AYA schema' },
      store: AYA_STORE,
      ...extra,
    },
  }
}

function setUp(asked, { open = true } = {}) {
  const project = useProjectStore()
  project.configuredStore = CONFIGURED
  if (open) project.project = { name: 'Open mapping', schemaName: 'Open schema' }
  const answers = []
  vi.spyOn(project, 'answerUpload').mockImplementation((choice) => {
    answers.push(choice)
    project.uploadQuestion = null
  })
  project.uploadQuestion = asked
  const wrapper = mount(MapUploadDialog, { attachTo: document.body })
  return { wrapper, project, answers }
}

function button(wrapper, label) {
  return wrapper.findAll('button').find((b) => b.text().includes(label))
}

describe('MapUploadDialog', () => {
  let wrapper

  beforeEach(() => {
    setActivePinia(createPinia())
  })

  afterEach(() => {
    wrapper?.unmount()
    wrapper = undefined
  })

  it('says a map for another workflow is not for this one, and offers to switch', async () => {
    let answers
    ;({ wrapper, answers } = setUp({ ...question('other-standard'), standard: 'omop' }))

    expect(wrapper.find('#upload-question-title').text()).toBe(
      'This JSON-LD is for the OMOP workflow'
    )
    expect(wrapper.find('.upload-question-body').text().replace(/\s+/g, ' ')).toBe(
      'aya.jsonld targets the OMOP common data model, so the RDF workflow cannot use it. ' +
        'Switch to the OMOP workflow, or choose a different JSON-LD.'
    )
    // Nothing of this workflow's to download, and no store to name.
    expect(button(wrapper, 'Download the current map')).toBeUndefined()
    expect(wrapper.find('.upload-question-store').exists()).toBe(false)

    await button(wrapper, 'Switch to OMOP').trigger('click')

    // The upload is set aside; OMOP says it is under development, for now.
    expect(answers).toEqual(['keep'])
    expect(useWorkflowsStore().soon.label).toBe('OMOP')
    expect(router.push).not.toHaveBeenCalled()
  })

  it('lets the user choose a different JSON-LD instead', async () => {
    let answers
    ;({ wrapper, answers } = setUp({ ...question('other-standard'), standard: 'omop' }))

    await button(wrapper, 'Choose a different JSON-LD').trigger('click')

    expect(answers).toEqual(['keep'])
    expect(useWorkflowsStore().soon).toBe(null)
  })

  it('shows nothing until an upload asks', () => {
    ;({ wrapper } = setUp(null))

    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
  })

  it('without a project, offers to open the map as one or go on with the standard RDF flow', async () => {
    let answers
    ;({ wrapper, answers } = setUp(question('no-project'), { open: false }))

    expect(wrapper.find('#upload-question-title').text()).toBe(
      'Open AYA mapping · v1.0.0 as a project?'
    )
    const text = wrapper.text().replace(/\s+/g, ' ')
    expect(text).toContain('aya.jsonld belongs to the project AYA mapping · v1.0.0')
    expect(text).toContain(
      "the standard RDF flow on Flyover's configured RDF store (repository userRepo)"
    )
    expect(text).toContain('repository aya · https://store.example.org/repositories/aya')
    expect(button(wrapper, 'Download the current map')).toBeUndefined()

    await button(wrapper, 'Open as the project').trigger('click')
    expect(answers).toEqual(['open'])
  })

  it('keeps going without a project when told', async () => {
    let answers
    ;({ wrapper, answers } = setUp(question('no-project'), { open: false }))

    await button(wrapper, 'Keep going without a project').trigger('click')

    expect(answers).toEqual(['keep'])
  })

  it('offers to switch to another project, or keep the open one', async () => {
    let answers
    let project
    ;({ wrapper, answers, project } = setUp(question('other-project')))
    const download = vi.spyOn(project, 'downloadWorkingCopy').mockResolvedValue(true)

    expect(wrapper.find('#upload-question-title').text()).toBe('Switch to AYA mapping · v1.0.0?')
    expect(wrapper.text().replace(/\s+/g, ' ')).toContain(
      'belongs to another project than the open one, Open mapping'
    )
    await button(wrapper, 'Download the current map').trigger('click')
    expect(download).toHaveBeenCalled()

    await button(wrapper, 'Keep going with Open mapping').trigger('click')
    expect(answers).toEqual(['keep'])
  })

  it('offers to move the open project to the store the map names', async () => {
    let answers
    ;({ wrapper, answers } = setUp(
      question('other-store', {
        store: { ...AYA_STORE, note: 'localhost is read as the configured store.' },
      })
    ))

    expect(wrapper.find('#upload-question-title').text()).toBe('Switch to another RDF store?')
    // The store reads as its users know it; how Flyover reaches it is not theirs to read.
    expect(wrapper.text()).not.toContain('localhost is read as the configured store.')
    await button(wrapper, 'Switch store').trigger('click')
    expect(answers).toEqual(['open'])
  })

  it("names the tab's workflow and its store, as the workflow list does", () => {
    // Only RDF is built; nothing in the question's words is RDF's own.
    useProjectStore().enterWorkflow('omop')
    ;({ wrapper } = setUp(question('other-store')))

    expect(wrapper.find('#upload-question-title').text()).toBe('Switch to another OMOP database?')
    expect(wrapper.text()).toContain('Its OMOP database: repository aya')
  })

  it('asks before replacing a working copy with describe answers in it', () => {
    ;({ wrapper } = setUp(question('same-project')))

    expect(wrapper.find('#upload-question-title').text()).toBe(
      "Replace the project's working copy?"
    )
    expect(button(wrapper, 'Replace the working copy')).toBeTruthy()
    expect(button(wrapper, 'Keep the current copy')).toBeTruthy()
  })

  it('keeps what is open on Escape or a click beside it', async () => {
    let answers
    let project
    ;({ wrapper, answers, project } = setUp(question('other-project')))

    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    await flushPromises()
    project.uploadQuestion = question('other-project')
    await flushPromises()
    await wrapper.find('.upload-question-backdrop').trigger('click')

    expect(answers).toEqual(['keep', 'keep'])
  })

  it('moves the focus into the question', async () => {
    ;({ wrapper } = setUp(question('other-project')))
    await flushPromises()

    expect(wrapper.find('[role="dialog"]').element.contains(document.activeElement)).toBe(true)
  })
})
