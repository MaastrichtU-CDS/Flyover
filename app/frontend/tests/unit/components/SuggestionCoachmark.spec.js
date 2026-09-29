import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mount } from '@vue/test-utils'
import SuggestionCoachmark from '@/components/SuggestionCoachmark.vue'

// ---------------------------------------------------------------------------
// SuggestionCoachmark — the first-visit review cue (WS2).
// ---------------------------------------------------------------------------

const COPY = {
  title: 'Check this suggestion',
  body: 'Flyover pre-filled this field.',
}

function mountCoachmark(props = {}) {
  return mount(SuggestionCoachmark, {
    props: { title: COPY.title, body: COPY.body, ...props },
  })
}

describe('Frontend unit: SuggestionCoachmark', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('renders as a labelled, non-modal dialog', () => {
    const w = mountCoachmark()
    const dialog = w.find('[role="dialog"]')
    expect(dialog.exists()).toBe(true)
    expect(dialog.attributes('aria-modal')).toBe('false')
    // The dialog is labelled by its own heading.
    const labelledBy = dialog.attributes('aria-labelledby')
    const heading = w.find(`#${labelledBy}`)
    expect(heading.text()).toBe(COPY.title)
  })

  it('stays hidden for the first ~300 ms so it does not flash during layout', async () => {
    const w = mountCoachmark()
    const dialog = w.find('[role="dialog"]')
    // v-show keeps the element in the DOM but hidden.
    expect(dialog.attributes('style') || '').toContain('display: none')

    vi.advanceTimersByTime(299)
    await vi.advanceTimersByTimeAsync(0)
    expect(w.find('[role="dialog"]').attributes('style') || '').toContain(
      'display: none'
    )

    vi.advanceTimersByTime(300)
    await vi.advanceTimersByTimeAsync(0)
    const style = w.find('[role="dialog"]').attributes('style') || ''
    expect(style).not.toContain('display: none')
  })

  it('announces itself once through a polite live region after appearing', async () => {
    const w = mountCoachmark()
    const live = w.find('[aria-live="polite"]')
    expect(live.attributes('aria-live')).toBe('polite')
    expect(live.text()).toBe('')

    await vi.advanceTimersByTimeAsync(400)
    expect(live.text()).toContain(COPY.title)
    expect(live.text()).toContain(COPY.body)
  })

  it('emits close when the user clicks "Got it"', async () => {
    const w = mountCoachmark()
    await vi.advanceTimersByTimeAsync(400)
    await w.find('button.coachmark-confirm').trigger('click')
    expect(w.emitted('close')).toBeTruthy()
  })

  it('emits close on Escape once visible', async () => {
    const w = mountCoachmark()
    // Before the delay, Escape does nothing.
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    expect(w.emitted('close')).toBeFalsy()

    await vi.advanceTimersByTimeAsync(400)
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    expect(w.emitted('close')).toBeTruthy()
  })

  it('stops listening for Escape when unmounted', async () => {
    const w = mountCoachmark()
    await vi.advanceTimersByTimeAsync(400)
    w.unmount()
    expect(() =>
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))
    ).not.toThrow()
  })
})
