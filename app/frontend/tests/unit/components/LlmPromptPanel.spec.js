import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))
vi.mock('@/lib/db', () => ({
  getData: vi.fn().mockResolvedValue(null),
  saveData: vi.fn().mockResolvedValue(true),
}))
const MAPPING = vi.hoisted(() => ({ schema: { variables: { biological_sex: {} } }, databases: {} }))
vi.mock('@/lib/jsonld', () => ({
  getMapping: () => MAPPING,
  formatToTitleCase: (key) => key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
}))

import api from '@/services/api'
import LlmPromptPanel, { copyText } from '@/components/LlmPromptPanel.vue'
import { useStatusStore } from '@/stores/status'
import { useSuggestionsStore } from '@/stores/suggestions'

function promptResponse(chunks = 1, itemCount = 3) {
  return {
    data: {
      phase: 'variables',
      database: 'nki',
      prompt: 'PROMPT 1',
      chunks: Array.from({ length: chunks }, (_, i) => ({
        index: i + 1,
        items: ['a'],
        item_count: chunks > 1 ? 1 : itemCount,
        prompt: `PROMPT ${i + 1}`,
      })),
      item_count: itemCount,
      chunk_hint: 40,
      contains: ['variable keys', 'column names'],
      privacy: 'SERVER PRIVACY NOTE',
      already_mapped: 2,
      answer_schema: {},
    },
  }
}

const ingestResponse = {
  data: {
    accepted: 1,
    nulled: 1,
    rejected: 0,
    skipped: 0,
    messages: [],
    job: {
      status: 'done',
      fingerprint: 'fp',
      progress: { done: 2, total: 2 },
      records: {
        nki_taal: {
          status: 'done',
          item: 'taal',
          match: 'biological_sex',
          confidence: 0.7,
          reason: 'r',
          source: 'pasted_llm',
          tier: 3,
          database: 'nki',
          column: 'taal',
        },
      },
    },
  },
}

async function mountOpen(props = {}) {
  const wrapper = mount(LlmPromptPanel, {
    props: { phase: 'variables', database: 'nki', ...props },
  })
  await wrapper.find('.llm-help-toggle').trigger('click')
  return wrapper
}

describe('Frontend unit: LlmPromptPanel', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    api.post.mockReset()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('is collapsed by default and makes no request until the user acts', async () => {
    const wrapper = mount(LlmPromptPanel, { props: { phase: 'variables', database: 'nki' } })
    expect(wrapper.find('.llm-help-body').exists()).toBe(false)
    expect(wrapper.find('.llm-help-toggle').text()).toMatch(/Use an LLM/)
    await wrapper.find('.llm-help-toggle').trigger('click')
    expect(wrapper.find('.llm-help-body').exists()).toBe(true)
    expect(wrapper.find('.llm-help-intro').text()).toMatch(/nothing is saved until you accept/)
    expect(wrapper.find('.llm-prompt-modal').exists()).toBe(false)
    expect(api.post).not.toHaveBeenCalled()
  })

  it('generates the prompt with the chosen options and opens it in the modal', async () => {
    const response = promptResponse()
    response.data.chunks[0].prompt = 'HEAD\n## 1. Schema\n- a\n## 3. Local columns\n- clin_t\n## 4. Answer\nTAIL'
    api.post.mockResolvedValue(response)
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-chunk-size').setValue('80')
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()

    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/prompt', {
      phase: 'variables',
      database: 'nki',
      mapping: MAPPING,
      chunk: 80,
    })
    expect(wrapper.find('.llm-help-summary').text()).toBe('3 columns still to map')
    const modal = wrapper.find('.llm-prompt-modal')
    expect(modal.exists()).toBe(true)
    // Section 3, the user's own names, is what the modal shows first.
    expect(modal.find('.llm-prompt-modal-heading').text()).toMatch(/Column names that leave the browser/)
    expect(modal.find('.llm-prompt-modal-data').text()).toBe('## 3. Local columns\n- clin_t')
    expect(modal.find('.llm-prompt-modal-privacy').text()).toBe('SERVER PRIVACY NOTE')
    expect(modal.find('.llm-help-preview').element.value).toBe(response.data.chunks[0].prompt)
    expect(modal.find('.llm-prompt-parts').exists()).toBe(false)
    // The paste-back field lives in the modal, not in the panel body, so a
    // multi-part round trip never has to leave it.
    expect(modal.find('.llm-help-answer').exists()).toBe(true)
    expect(modal.find('.llm-help-import').exists()).toBe(true)
    expect(wrapper.find('.llm-help-body .llm-help-answer').exists()).toBe(false)

    // Closing hides it; "Show prompt" brings it back.
    await modal.find('.llm-prompt-modal-close').trigger('click')
    expect(wrapper.find('.llm-prompt-modal').exists()).toBe(false)
    await wrapper.find('.llm-help-show').trigger('click')
    expect(wrapper.find('.llm-prompt-modal').exists()).toBe(true)
  })

  it('the values phase posts the same options as the variables phase', async () => {
    api.post.mockResolvedValue(promptResponse())
    const wrapper = await mountOpen({ phase: 'values' })
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(api.post.mock.calls[0][1]).toEqual({
      phase: 'values',
      database: 'nki',
      mapping: MAPPING,
      chunk: 40,
    })
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/3 values still to map/)
    expect(wrapper.find('.llm-prompt-modal-heading').text()).toMatch(/Values that leave the browser/)
  })

  it('starts from the site chunk default and offers it when non-standard', async () => {
    useSuggestionsStore().promptExportChunk = 90
    api.post.mockResolvedValue(promptResponse())
    const wrapper = await mountOpen()
    expect(wrapper.find('.llm-help-chunk-size').element.value).toBe('90')
    expect(wrapper.findAll('.llm-help-chunk-size option').map((o) => o.element.value)).toEqual([
      '20', '40', '80', '90', '160', '400',
    ])
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(api.post.mock.calls[0][1].chunk).toBe(90)
  })

  it('gates copy and download on the acknowledgement, then copies the selected part', async () => {
    api.post.mockResolvedValue(promptResponse(3))
    const writeText = vi.fn().mockResolvedValue()
    vi.spyOn(navigator, 'clipboard', 'get').mockReturnValue({ writeText })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()

    const modal = wrapper.find('.llm-prompt-modal')
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/3 parts of at most 40/)
    expect(modal.findAll('.llm-prompt-part')).toHaveLength(3)
    expect(modal.find('.llm-help-copy').attributes('disabled')).toBeDefined()
    expect(modal.find('.llm-help-download').attributes('disabled')).toBeDefined()
    await modal.find('.llm-help-copy').trigger('click')
    expect(writeText).not.toHaveBeenCalled()

    await modal.find('.llm-prompt-ack-input').setValue(true)
    expect(modal.find('.llm-help-copy').attributes('disabled')).toBeUndefined()
    await modal.findAll('.llm-prompt-part')[1].trigger('click')
    expect(modal.find('.llm-prompt-modal-heading').text()).toMatch(/in part 2/)
    await modal.find('.llm-help-copy').trigger('click')
    await flushPromises()
    expect(writeText).toHaveBeenCalledWith('PROMPT 2')
    expect(useStatusStore().messages.at(-1).text).toMatch(/Part 2 of 3 copied/)

    // Reopening asks again.
    await modal.find('.llm-prompt-modal-close').trigger('click')
    await wrapper.find('.llm-help-show').trigger('click')
    expect(wrapper.find('.llm-help-copy').attributes('disabled')).toBeDefined()
  })

  it('falls back to the legacy copy command and, failing that, unfolds the full prompt', async () => {
    vi.spyOn(navigator, 'clipboard', 'get').mockReturnValue(undefined)
    document.execCommand = vi.fn().mockReturnValue(true)
    expect(await copyText('x')).toBe(true)
    expect(document.execCommand).toHaveBeenCalledWith('copy')

    document.execCommand = vi.fn().mockReturnValue(false)
    api.post.mockResolvedValue(promptResponse())
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    await wrapper.find('.llm-prompt-ack-input').setValue(true)
    await wrapper.find('.llm-help-copy').trigger('click')
    await flushPromises()
    expect(useStatusStore().messages.at(-1).level).toBe('warning')
    expect(wrapper.find('.llm-prompt-modal-full').attributes('open')).toBeDefined()
  })

  it('names the held-back columns and the rare values left out', async () => {
    const response = promptResponse()
    response.data.phase = 'values'
    response.data.held_back = [
      { column: 'opmerking', variable: 'survival_status', distinct: 65, reason: '65 distinct values (more than 50)' },
    ]
    response.data.suppressed = 7
    response.data.min_value_count = 10
    api.post.mockResolvedValue(response)
    const wrapper = await mountOpen({ phase: 'values' })
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()

    expect(wrapper.find('.llm-help-summary').text()).toBe(
      '3 values still to map · 1 column held back · 7 rare values left out',
    )
    expect(wrapper.find('.llm-prompt-modal-left-out').text()).toBe(
      'Left out: opmerking held back (65 distinct values (more than 50)) · 7 rare values left out (seen fewer than 10 times)',
    )
  })

  it('says so when there is nothing left to map and opens no modal', async () => {
    api.post.mockResolvedValue({ data: { ...promptResponse().data, item_count: 0, chunks: [] } })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/nothing to ask an LLM/)
    expect(wrapper.find('.llm-prompt-modal').exists()).toBe(false)
    expect(wrapper.find('.llm-help-show').exists()).toBe(false)
  })

  it('shows the server message when the prompt cannot be generated', async () => {
    api.post.mockRejectedValue({ response: { data: { error: "unknown database 'nki'" } } })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(wrapper.find('.llm-help-error').text()).toBe("unknown database 'nki'")
  })

  it('imports the pasted answer through the store and emits ingested', async () => {
    api.post.mockImplementation(async (url) =>
      url === '/api/v1/suggestions/prompt' ? promptResponse() : ingestResponse,
    )
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()

    const modal = wrapper.find('.llm-prompt-modal')
    const importButton = modal.find('.llm-help-import')
    expect(importButton.attributes('disabled')).toBeDefined()
    await modal.find('.llm-help-answer').setValue('```json\n{"databases": {}}\n```')
    expect(importButton.attributes('disabled')).toBeUndefined()
    await importButton.trigger('click')
    await flushPromises()

    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/ingest', {
      database: 'nki',
      source: 'pasted_llm',
      mapping: MAPPING,
      dismissed: [],
      answer: '```json\n{"databases": {}}\n```',
    })
    const store = useSuggestionsStore()
    expect(store.variables.byKey.nki_taal.source).toBe('pasted_llm')
    expect(wrapper.emitted('ingested')[0][0].accepted).toBe(1)
    expect(modal.find('.llm-help-import-result').text()).toMatch(/1 imported, 1 left for you to decide/)
    expect(modal.find('.llm-help-answer').element.value).toBe('')
  })

  it('keeps the paste and shows the message when the import fails', async () => {
    api.post.mockImplementation(async (url) => {
      if (url === '/api/v1/suggestions/prompt') return promptResponse()
      return Promise.reject({
        response: { status: 400, data: { error: 'Could not find valid JSON in the pasted text.' } },
      })
    })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    const modal = wrapper.find('.llm-prompt-modal')
    await modal.find('.llm-help-answer').setValue('not json')
    await modal.find('.llm-help-import').trigger('click')
    await flushPromises()
    expect(modal.find('.llm-help-error').text()).toMatch(/Could not find valid JSON/)
    expect(modal.find('.llm-help-answer').element.value).toBe('not json')
    expect(wrapper.emitted('ingested')).toBeUndefined()
  })
})
