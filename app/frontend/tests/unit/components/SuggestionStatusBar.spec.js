import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import SuggestionStatusBar from '@/components/SuggestionStatusBar.vue'

const DONE_WITH_MATCH = {
  status: 'done',
  reason: null,
  progress: { done: 2, total: 2 },
  byKey: {
    nki_taal: { match: 'administered_prom_language', confidence: 0.9 },
  },
}

const TIERS = {
  1: { state: 'active' },
  2: { state: 'inactive', reason: 'disabled by FLYOVER_SUGGESTION_TIERS' },
  3: { state: 'inactive', reason: 'disabled by FLYOVER_SUGGESTION_TIERS' },
}

function mountBar(props = {}) {
  return mount(SuggestionStatusBar, {
    props: {
      phaseState: DONE_WITH_MATCH,
      tiers: TIERS,
      unreviewedCount: 1,
      ...props,
    },
  })
}

describe('Frontend unit: SuggestionStatusBar', () => {
  it('hides the tier note until the help link is clicked', async () => {
    const wrapper = mountBar()
    expect(wrapper.text()).toContain('Suggestions ready')
    expect(wrapper.find('.suggestion-tier-note').exists()).toBe(false)
    // The tooltip explains what the link does; once the note is open the
    // button reads "Hide" and carries no tooltip.
    const help = wrapper.find('.suggestion-help-link')
    expect(help.attributes('title')).toBe(
      'Show the explanation of the suggestion review flow again',
    )

    await help.trigger('click')
    expect(wrapper.find('.suggestion-tier-note').text()).toContain(
      'regex and string matching (tier 1) enabled',
    )
    expect(wrapper.emitted('show-coachmark')).toHaveLength(1)
    expect(wrapper.find('.suggestion-help-link').attributes('title')).toBeUndefined()
  })

  it('clicking the link again hides the note and does not re-fire the callout', async () => {
    const wrapper = mountBar()
    await wrapper.find('.suggestion-help-link').trigger('click')
    await wrapper.find('.suggestion-help-link').trigger('click')
    expect(wrapper.find('.suggestion-tier-note').exists()).toBe(false)
    expect(wrapper.emitted('show-coachmark')).toHaveLength(1)
  })

  it('shows no note at all when there is nothing to open it with', () => {
    const wrapper = mountBar({ phaseState: { ...DONE_WITH_MATCH, byKey: {} } })
    expect(wrapper.find('.suggestion-help-link').exists()).toBe(false)
    expect(wrapper.find('.suggestion-tier-note').exists()).toBe(false)
  })
})
