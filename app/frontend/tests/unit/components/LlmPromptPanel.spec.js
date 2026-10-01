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
    expect(wrapper.find('.llm-help-privacy').text()).toMatch(/contains no data rows/i)
    expect(api.post).not.toHaveBeenCalled()
  })

  it('generates the prompt with the chosen options and lists what leaves the browser', async () => {
    api.post.mockResolvedValue(promptResponse())
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
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/3 columns still to map/)
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/2 columns already mapped/)
    expect(wrapper.findAll('.llm-help-contains li').map((li) => li.text())).toEqual([
      'variable keys',
      'column names',
    ])
    // Once generated, the server's privacy note (worded for the options
    // actually used) replaces the pre-generation one; the short review
    // reminder is appended once.
    expect(wrapper.find('.llm-help-privacy').text()).toBe(
      'SERVER PRIVACY NOTE Review it before sending.',
    )
    expect(wrapper.findAll('.llm-help-chunk')).toHaveLength(1)
    expect(wrapper.find('.llm-help-preview').exists()).toBe(false)
    await wrapper.find('.llm-help-preview-toggle').trigger('click')
    expect(wrapper.find('.llm-help-preview').element.value).toBe('PROMPT 1')
  })

  it('the values phase sends exclude_free_text instead of include_values', async () => {
    api.post.mockResolvedValue(promptResponse())
    const wrapper = await mountOpen({ phase: 'values' })
    await wrapper.find('.llm-help-include-free-text').setValue(true)
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(api.post.mock.calls[0][1]).toEqual({
      phase: 'values',
      database: 'nki',
      mapping: MAPPING,
      exclude_free_text: false,
      chunk: 40,
    })
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/3 values still to map/)
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

  it('offers one copy and one download button per chunk and copies through the clipboard', async () => {
    api.post.mockResolvedValue(promptResponse(3))
    const writeText = vi.fn().mockResolvedValue()
    vi.spyOn(navigator, 'clipboard', 'get').mockReturnValue({ writeText })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()

    const chunks = wrapper.findAll('.llm-help-chunk')
    expect(chunks).toHaveLength(3)
    expect(chunks[1].find('.llm-help-chunk-label').text()).toMatch(/Part 2 of 3/)
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/split into 3 parts/)
    await chunks[1].find('.llm-help-copy').trigger('click')
    await flushPromises()
    expect(writeText).toHaveBeenCalledWith('PROMPT 2')
    expect(useStatusStore().messages.at(-1).text).toMatch(/Part 2 of 3 copied/)
    expect(chunks[1].find('.llm-help-download').exists()).toBe(true)
  })

  it('falls back to the legacy copy command and, failing that, opens the preview', async () => {
    vi.spyOn(navigator, 'clipboard', 'get').mockReturnValue(undefined)
    document.execCommand = vi.fn().mockReturnValue(true)
    expect(await copyText('x')).toBe(true)
    expect(document.execCommand).toHaveBeenCalledWith('copy')

    document.execCommand = vi.fn().mockReturnValue(false)
    api.post.mockResolvedValue(promptResponse())
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    await wrapper.find('.llm-help-copy').trigger('click')
    await flushPromises()
    expect(useStatusStore().messages.at(-1).level).toBe('warning')
    expect(wrapper.find('.llm-help-preview').exists()).toBe(true)
  })

  it('says so when there is nothing left to map', async () => {
    api.post.mockResolvedValue({ data: { ...promptResponse().data, item_count: 0, chunks: [] } })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(wrapper.find('.llm-help-summary').text()).toMatch(/nothing to ask an LLM/)
    expect(wrapper.findAll('.llm-help-chunk')).toHaveLength(0)
  })

  it('shows the server message when the prompt cannot be generated', async () => {
    api.post.mockRejectedValue({ response: { data: { error: "unknown database 'nki'" } } })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-generate').trigger('click')
    await flushPromises()
    expect(wrapper.find('.llm-help-error').text()).toBe("unknown database 'nki'")
  })

  it('imports the pasted answer through the store and emits ingested', async () => {
    api.post.mockResolvedValue(ingestResponse)
    const wrapper = await mountOpen()
    const importButton = wrapper.find('.llm-help-import')
    expect(importButton.attributes('disabled')).toBeDefined()
    await wrapper.find('.llm-help-answer').setValue('```json\n{"databases": {}}\n```')
    expect(importButton.attributes('disabled')).toBeUndefined()
    await importButton.trigger('click')
    await flushPromises()

    expect(api.post).toHaveBeenCalledWith('/api/v1/suggestions/variables/ingest', {
      database: 'nki',
      source: 'pasted_llm',
      mapping: MAPPING,
      answer: '```json\n{"databases": {}}\n```',
    })
    const store = useSuggestionsStore()
    expect(store.variables.byKey.nki_taal.source).toBe('pasted_llm')
    expect(wrapper.emitted('ingested')[0][0].accepted).toBe(1)
    expect(wrapper.find('.llm-help-import-result').text()).toMatch(/1 imported, 1 left for you to decide/)
    expect(wrapper.find('.llm-help-answer').element.value).toBe('')
  })

  it('keeps the paste and shows the message when the import fails', async () => {
    api.post.mockRejectedValue({
      response: { status: 400, data: { error: 'Could not find valid JSON in the pasted text.' } },
    })
    const wrapper = await mountOpen()
    await wrapper.find('.llm-help-answer').setValue('not json')
    await wrapper.find('.llm-help-import').trigger('click')
    await flushPromises()
    expect(wrapper.find('.llm-help-error').text()).toMatch(/Could not find valid JSON/)
    expect(wrapper.find('.llm-help-answer').element.value).toBe('not json')
    expect(wrapper.emitted('ingested')).toBeUndefined()
  })
})
