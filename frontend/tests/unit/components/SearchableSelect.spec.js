import { mount } from '@vue/test-utils'
import { describe, it, expect, afterEach } from 'vitest'
import SearchableSelect from '@/components/SearchableSelect.vue'

let wrapper

function mountSelect(props = {}) {
  wrapper = mount(SearchableSelect, {
    attachTo: document.body,
    props: {
      options: ['Age', 'Biological Sex', 'Tumour Type', 'Other'],
      name: 'test_field',
      ...props,
    },
  })
  return wrapper
}

function panel() {
  return document.body.querySelector('.searchable-select-panel')
}

function panelOptions() {
  return [...document.body.querySelectorAll('.searchable-select-option')]
}

afterEach(() => {
  wrapper?.unmount()
  document.body.innerHTML = ''
})

describe('Frontend unit: SearchableSelect', () => {
  it('carries the value in a hidden input for native form POSTs', async () => {
    const w = mountSelect({ modelValue: 'Age' })
    const hidden = w.find('input[type="hidden"][name="test_field"]')
    expect(hidden.exists()).toBe(true)
    expect(hidden.element.value).toBe('Age')
  })

  it('opens on focus and lists all options', async () => {
    const w = mountSelect()
    await w.find('input[type="text"]').trigger('focus')
    expect(panel()).not.toBeNull()
    expect(panelOptions().map((o) => o.textContent.trim())).toEqual([
      'Age',
      'Biological Sex',
      'Tumour Type',
      'Other',
    ])
  })

  it('filters case-insensitively while typing', async () => {
    const w = mountSelect()
    const input = w.find('input[type="text"]')
    await input.trigger('focus')
    await input.setValue('sex')
    expect(panelOptions().map((o) => o.textContent.trim())).toEqual([
      'Biological Sex',
    ])
  })

  it('caps rendering and reports how many options are hidden', async () => {
    const many = Array.from({ length: 250 }, (_, i) => `option_${i}`)
    const w = mountSelect({ options: many })
    await w.find('input[type="text"]').trigger('focus')
    expect(panelOptions()).toHaveLength(100)
    expect(
      document.body.querySelector('.searchable-select-more').textContent
    ).toContain('150 more')
  })

  it('selecting an option emits the value and closes the panel', async () => {
    const w = mountSelect()
    await w.find('input[type="text"]').trigger('focus')
    await panelOptions()[1].dispatchEvent(new MouseEvent('mousedown'))
    expect(w.emitted('update:modelValue')).toEqual([['Biological Sex']])
    expect(w.emitted('change')).toEqual([['Biological Sex']])
    expect(panel()).toBeNull()
  })

  it('disabled options cannot be selected', async () => {
    const w = mountSelect({ disabledOption: (v) => v === 'Age' })
    await w.find('input[type="text"]').trigger('focus')
    await panelOptions()[0].dispatchEvent(new MouseEvent('mousedown'))
    expect(w.emitted('update:modelValue')).toBeUndefined()
    expect(panelOptions()[0].classList.contains('disabled')).toBe(true)
  })

  it('supports keyboard navigation and Enter selection', async () => {
    const w = mountSelect()
    const input = w.find('input[type="text"]')
    await input.trigger('focus')
    await input.trigger('keydown', { key: 'ArrowDown' })
    await input.trigger('keydown', { key: 'ArrowDown' })
    await input.trigger('keydown', { key: 'Enter' })
    expect(w.emitted('update:modelValue')).toEqual([['Biological Sex']])
  })

  it('the clear button empties the selection', async () => {
    const w = mountSelect({ modelValue: 'Age' })
    await w.find('.searchable-select-clear').trigger('click')
    expect(w.emitted('update:modelValue')).toEqual([['']])
    expect(w.emitted('change')).toEqual([['']])
  })

  it('accepts {value, label} option objects', async () => {
    const w = mountSelect({
      options: [{ value: 'Male', label: 'Male sex' }],
      modelValue: 'Male',
    })
    expect(w.find('input[type="text"]').element.value).toBe('Male sex')
    await w.find('input[type="text"]').trigger('focus')
    expect(panelOptions()[0].textContent.trim()).toBe('Male sex')
  })

  it('shows the selected value again after typing without selecting', async () => {
    const w = mountSelect({ modelValue: 'Age' })
    const input = w.find('input[type="text"]')
    await input.trigger('focus')
    await input.setValue('tum')
    await input.trigger('keydown', { key: 'Escape' })
    expect(input.element.value).toBe('Age')
    expect(w.emitted('update:modelValue')).toBeUndefined()
  })
})
