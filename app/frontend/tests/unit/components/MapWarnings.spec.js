import { mount } from '@vue/test-utils'
import { describe, it, expect } from 'vitest'
import MapWarnings from '@/components/MapWarnings.vue'

// The warnings of a semantic map, as the routes that open a map send them.

function warning(n, extra = {}) {
  return {
    path: `databases.db.tables.t.columns.c${n}`,
    message: `Table 't', column 'c${n}': warning ${n}`,
    suggestion: `What warning ${n} means`,
    ...extra,
  }
}

function items(wrapper) {
  return wrapper.findAll('li.map-warning')
}

describe('MapWarnings', () => {
  it('shows nothing when the map has no warnings', () => {
    const wrapper = mount(MapWarnings, { props: { warnings: [] } })

    expect(wrapper.find('.map-warnings').exists()).toBe(false)
  })

  it('lists each warning with what it means for the annotation', () => {
    const wrapper = mount(MapWarnings, { props: { warnings: [warning(1), warning(2)] } })

    expect(wrapper.find('strong').text()).toBe('The semantic map has 2 warnings.')
    expect(wrapper.text()).toContain('You can go on; each one says what it means')
    expect(items(wrapper).map((li) => li.find('.map-warning-suggestion').text())).toEqual([
      'What warning 1 means',
      'What warning 2 means',
    ])
    expect(items(wrapper)[0].text()).toContain("Table 't', column 'c1': warning 1")
  })

  it('says one warning in the singular', () => {
    const wrapper = mount(MapWarnings, { props: { warnings: [warning(1)] } })

    expect(wrapper.find('strong').text()).toBe('The semantic map has 1 warning.')
  })

  it('shows a message as text, never as markup', () => {
    // Table and column names come from an uploaded file.
    const wrapper = mount(MapWarnings, {
      props: { warnings: [warning(1, { message: "Table '<img src=x onerror=alert(1)>'" })] },
    })

    expect(wrapper.find('img').exists()).toBe(false)
    expect(items(wrapper)[0].text()).toContain('<img src=x onerror=alert(1)>')
  })

  it('leaves out an empty suggestion', () => {
    const wrapper = mount(MapWarnings, { props: { warnings: [warning(1, { suggestion: '' })] } })

    expect(wrapper.find('.map-warning-suggestion').exists()).toBe(false)
  })

  it('shows five warnings at first and the rest on request', async () => {
    const warnings = [1, 2, 3, 4, 5, 6, 7].map((n) => warning(n))
    const wrapper = mount(MapWarnings, { props: { warnings } })

    expect(items(wrapper)).toHaveLength(5)
    const more = wrapper.find('button.map-warnings-more')
    expect(more.text()).toBe('Show all 7 warnings')

    await more.trigger('click')

    expect(items(wrapper)).toHaveLength(7)
    expect(wrapper.find('button.map-warnings-more').exists()).toBe(false)
  })

  it('needs no button for five warnings or fewer', () => {
    const warnings = [1, 2, 3, 4, 5].map((n) => warning(n))
    const wrapper = mount(MapWarnings, { props: { warnings } })

    expect(items(wrapper)).toHaveLength(5)
    expect(wrapper.find('button.map-warnings-more').exists()).toBe(false)
  })
})
