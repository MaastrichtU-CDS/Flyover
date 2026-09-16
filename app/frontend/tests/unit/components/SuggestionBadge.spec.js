import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import SuggestionBadge from '@/components/SuggestionBadge.vue'

function makeRecord(source = 'alias', tier = 1) {
  return {
    status: 'done',
    item: 'morf',
    match: 'tumour_morphology_icd_o',
    confidence: 0.92,
    reason: 'Alias: column morph in database christie',
    source,
    tier,
    alternatives: [],
  }
}

describe('Frontend unit: SuggestionBadge', () => {
  it('renders the confidence percentage', () => {
    const wrapper = mount(SuggestionBadge, {
      props: { suggestion: makeRecord(), applied: false, touched: false },
    })
    expect(wrapper.text()).toContain('92%')
  })

  it('renders identically for alias, value_regex, and string sources', () => {
    const sources = ['alias', 'value_regex', 'string']
    for (const source of sources) {
      const wrapper = mount(SuggestionBadge, {
        props: {
          suggestion: makeRecord(source),
          applied: false,
          touched: false,
        },
      })
      // All tier-1 sources render a percentage badge with the same structure.
      expect(wrapper.find('.suggestion-badge').exists()).toBe(true)
      expect(wrapper.text()).toContain('92%')
      // The "reviewed" state is not shown for non-touched badges.
      expect(wrapper.text()).not.toContain('reviewed')
    }
  })

  it('renders identically for a fabricated llm source', () => {
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord('llm', 3),
        applied: false,
        touched: false,
      },
    })
    expect(wrapper.find('.suggestion-badge').exists()).toBe(true)
    expect(wrapper.text()).toContain('92%')
    // The robot icon class should be present for llm source.
    expect(wrapper.find('.fa-robot').exists()).toBe(true)
  })

  it('shows the reviewed state when touched', () => {
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: true,
        touched: true,
      },
    })
    expect(wrapper.find('.suggestion-badge.confirmed').exists()).toBe(true)
    expect(wrapper.text()).toContain('reviewed')
    expect(wrapper.text()).not.toContain('92%')
  })

  it('shows the applied (green) state when applied but not touched', () => {
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: true,
        touched: false,
      },
    })
    expect(wrapper.find('.suggestion-badge.applied').exists()).toBe(true)
  })

  it('emits dismiss when the × button is clicked', async () => {
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: false,
        touched: false,
      },
    })
    await wrapper.find('.suggestion-dismiss').trigger('click')
    expect(wrapper.emitted('dismiss')).toBeTruthy()
  })

  it('emits accept when the badge body is clicked', async () => {
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: false,
        touched: false,
      },
    })
    await wrapper.find('.suggestion-badge').trigger('click')
    expect(wrapper.emitted('accept')).toBeTruthy()
  })

  it('shows the alternatives indicator when alternatives exist', () => {
    const record = makeRecord()
    record.alternatives = [
      { match: 'biological_sex', confidence: 0.7, source: 'string', tier: 1 },
    ]
    const wrapper = mount(SuggestionBadge, {
      props: { suggestion: record, applied: false, touched: false },
    })
    expect(wrapper.find('.suggestion-alternatives').exists()).toBe(true)
  })

  it('does not show the dismiss button when showDismiss is false', () => {
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: false,
        touched: false,
        showDismiss: false,
      },
    })
    expect(wrapper.find('.suggestion-dismiss').exists()).toBe(false)
  })

  it('uses the reason as tooltip', () => {
    const wrapper = mount(SuggestionBadge, {
      props: { suggestion: makeRecord(), applied: false, touched: false },
    })
    expect(wrapper.attributes('title')).toBe('Alias: column morph in database christie')
  })
})
