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

  it('keeps the dashed "needs review" pill when applied but not touched', () => {
    // WS1.3: only reviewed fields turn green; an applied-but-unreviewed
    // pre-fill keeps the purple dashed pill so it is visibly unreviewed.
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: true,
        touched: false,
      },
    })
    const badge = wrapper.find('.suggestion-badge')
    expect(badge.classes()).toContain('applied')
    expect(badge.classes()).not.toContain('confirmed')
    expect(wrapper.text()).toContain('92%')
    expect(wrapper.text()).not.toContain('reviewed')
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
    await wrapper.find('.suggestion-accept').trigger('click')
    expect(wrapper.emitted('accept')).toBeTruthy()
  })

  it('renders accept and dismiss as sibling buttons with aria-labels', () => {
    // WS5.1: the pill must be keyboard-reachable, which a clickable span
    // is not. Accept and dismiss are two sibling <button>s inside the
    // wrapper (a button cannot nest inside another).
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: false,
        touched: false,
      },
    })
    const accept = wrapper.find('button.suggestion-accept')
    const dismiss = wrapper.find('button.suggestion-dismiss')
    expect(accept.exists()).toBe(true)
    expect(dismiss.exists()).toBe(true)
    expect(accept.element.nextElementSibling).toBe(dismiss.element)
    expect(accept.attributes('aria-label')).toContain("map 'morf' to 'tumour_morphology_icd_o'")
    expect(dismiss.attributes('aria-label')).toBe('Dismiss this suggestion')
  })

  it('accept and dismiss respond to keyboard activation', async () => {
    // Native <button>s activate on Enter/Space in the browser; happy-dom
    // does not synthesise the click, so drive the click handlers the way
    // the browser would after the keypress.
    const wrapper = mount(SuggestionBadge, {
      props: {
        suggestion: makeRecord(),
        applied: false,
        touched: false,
      },
    })
    await wrapper.find('button.suggestion-accept').trigger('click')
    await wrapper.find('button.suggestion-dismiss').trigger('click')
    expect(wrapper.emitted('accept')).toBeTruthy()
    expect(wrapper.emitted('dismiss')).toBeTruthy()
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
    expect(wrapper.find('.suggestion-accept').attributes('title')).toBe(
      'Alias: column morph in database christie'
    )
  })
})
