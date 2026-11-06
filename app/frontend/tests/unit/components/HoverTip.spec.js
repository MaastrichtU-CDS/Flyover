import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import HoverTip from '@/components/HoverTip.vue'

// The app's tooltip, in the style of the home page's step tiles.

function mountTip(props = {}, slot = '<span class="anchor">Anchor</span>') {
  return mount(HoverTip, { props: { text: 'Explains the anchor', ...props }, slots: { default: slot } })
}

describe('HoverTip', () => {
  it('shows on hover, above the anchor, centred, in the tiles\' style', async () => {
    const wrapper = mountTip()
    expect(wrapper.find('[role="tooltip"]').exists()).toBe(false)

    await wrapper.trigger('mouseenter')

    const tip = wrapper.find('[role="tooltip"]')
    expect(tip.text()).toBe('Explains the anchor')
    expect(tip.classes()).toEqual(['bootstrap-tooltip'])
    await wrapper.trigger('mouseleave')
    expect(wrapper.find('[role="tooltip"]').exists()).toBe(false)
  })

  it('can sit below its anchor, lined up with either edge, and wrap', async () => {
    const wrapper = mountTip({ placement: 'below', align: 'end', wrap: true })

    await wrapper.trigger('mouseenter')

    expect(wrapper.find('[role="tooltip"]').classes()).toEqual([
      'bootstrap-tooltip',
      'is-below',
      'is-end',
      'is-wrapping',
    ])
  })

  it('shows while the keyboard focus is inside, and Escape hides it', async () => {
    const wrapper = mount(HoverTip, {
      props: { text: 'Check again' },
      slots: {
        default: `<template #default="{ describedby }">
          <button :aria-describedby="describedby">Go</button>
        </template>`,
      },
    })
    const button = wrapper.find('button')

    await button.trigger('focusin')
    const tip = wrapper.find('[role="tooltip"]')
    expect(button.attributes('aria-describedby')).toBe(tip.attributes('id'))

    await button.trigger('keydown', { key: 'Escape' })
    expect(wrapper.find('[role="tooltip"]').exists()).toBe(false)
    expect(button.attributes('aria-describedby')).toBeUndefined()
  })

  it('shows nothing without text', async () => {
    const wrapper = mountTip({ text: '' })

    await wrapper.trigger('mouseenter')

    expect(wrapper.find('[role="tooltip"]').exists()).toBe(false)
  })
})
