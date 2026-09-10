import { mount, flushPromises } from '@vue/test-utils'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { ref } from 'vue'

vi.mock('@/services/api', () => ({
  default: { get: vi.fn(), post: vi.fn() },
}))

const dataExists = ref(false)
const refreshDataExists = vi.fn(async () => {})

vi.mock('@/composables/useNavigation', () => ({
  useNavigation: () => ({
    dataExists,
    refreshDataExists,
    stepStates: ref([]),
    currentStep: ref(null),
  }),
}))

import api from '@/services/api'
import IngestView from '@/views/IngestView.vue'

function mountIngest() {
  return mount(IngestView)
}

async function pickFiles(wrapper, files) {
  const input = wrapper.find('#csvFile').element
  Object.defineProperty(input, 'files', { value: files, configurable: true })
  await input.dispatchEvent(new Event('change'))
  await flushPromises()
}

function csvFile(name, header = 'col1,col2,col3') {
  return new File([`${header}\n1,2,3\n`], name, { type: 'text/csv' })
}

describe('IngestView', () => {
  beforeEach(() => {
    dataExists.value = false
    refreshDataExists.mockClear()
    api.get.mockReset()
    api.get.mockResolvedValue({ data: { tables: [], tableColumns: {} } })
  })

  it('renders the form with POST /upload', () => {
    const w = mountIngest()
    const form = w.find('form')
    expect(form.attributes('action')).toBe('/upload')
    expect(form.attributes('method')).toBe('POST')
  })

  it('disables submit until a CSV is picked', async () => {
    const w = mountIngest()
    await flushPromises()
    const submit = w.find('button[type="submit"]')
    expect(submit.attributes('disabled')).toBeDefined()

    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('one.csv')])
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('reflects the picked filename in the path input', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('alpha.csv')])
    expect(w.find('#csvPath').element.value).toBe('alpha.csv')
  })

  it('shows PK/FK section only when more than one file is selected', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()

    const pkfkSection = () => {
      const alert = w.findAll('.alert-info').find((a) =>
        a.text().includes('Multiple CSV Files Detected')
      )
      return alert?.element.closest('div.mt-4')
    }

    await pickFiles(w, [csvFile('only.csv')])
    expect(pkfkSection().style.display).toBe('none')

    await pickFiles(w, [csvFile('a.csv'), csvFile('b.csv')])
    expect(pkfkSection().style.display).not.toBe('none')
  })

  it('enables submit for Excel when file is selected', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Excel').setValue()
    const submit = w.find('button[type="submit"]')
    expect(submit.attributes('disabled')).toBeDefined()

    await pickFiles(w, [csvFile('data.xlsx')])
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('enables submit on Postgres when all four fields are filled', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    const submit = w.find('button[type="submit"]')
    expect(submit.attributes('disabled')).toBeDefined()

    await w.find('#username').setValue('u')
    await w.find('#password').setValue('p')
    await w.find('#POSTGRES_URL').setValue('http://db')
    await w.find('#POSTGRES_DB').setValue('flyover')
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('loads existing graph structure when data already exists', async () => {
    dataExists.value = true
    api.get.mockResolvedValue({
      data: { tables: ['patients'], tableColumns: { patients: ['id', 'age'] } },
    })
    const w = mountIngest()
    await flushPromises()
    expect(refreshDataExists).toHaveBeenCalled()
    expect(api.get).toHaveBeenCalledWith('/get-existing-graph-structure')
    expect(w.text()).toContain('RDF store already contains a data graph')
  })

  it('does not crash when /get-existing-graph-structure rejects', async () => {
    dataExists.value = true
    api.get.mockRejectedValue(new Error('boom'))
    const w = mountIngest()
    await flushPromises()
    expect(w.find('form').exists()).toBe(true)
  })

  it('renders the Specify Source Information card header', () => {
    const w = mountIngest()
    expect(w.text()).toContain('Specify Source Information')
  })

  it('shows file upload section only for CSV and Excel', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    expect(w.find('#csvPath').exists()).toBe(true)

    await w.find('#Postgres').setValue()
    expect(w.find('#csvPath').element.style.display).toBe('none')

    await w.find('#CSV').setValue()
    expect(w.find('#csvPath').element.style.display).not.toBe('none')
  })

  it('shows separator and decimal dropdowns only when CSV is selected', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    expect(w.find('#csv_separator_sign').element.style.display).not.toBe('none')
    expect(w.find('#csv_decimal_sign').element.style.display).not.toBe('none')

    await w.find('#Excel').setValue()
    expect(w.find('#csv_separator_sign').element.style.display).toBe('none')
    expect(w.find('#csv_decimal_sign').element.style.display).toBe('none')
  })

  it('shows Postgres fields only when PostgreSQL is selected', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    expect(w.find('#username').element.style.display).toBe('none')

    await w.find('#Postgres').setValue()
    expect(w.find('#username').element.style.display).not.toBe('none')
    expect(w.find('#password').element.style.display).not.toBe('none')
    expect(w.find('#POSTGRES_URL').element.style.display).not.toBe('none')
    expect(w.find('#POSTGRES_DB').element.style.display).not.toBe('none')
  })

  it('hides Specify Source Information card when Other is selected', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    expect(w.text()).toContain('Specify Source Information')

    await w.find('#Other').setValue()
    expect(w.text()).not.toContain('Specify Source Information')
  })

  it('shows tooltip with repo link when hovering the Other tile', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#Other').setValue()
    const otherTile = w.find('#Other').element.closest('.source-tile')

    otherTile.dispatchEvent(new Event('mouseenter'))
    await flushPromises()

    expect(w.text()).toContain('Missing a source type')
    const link = w.find('a[href="https://github.com/MaastrichtU-CDS/Flyover/issues"]')
    expect(link.exists()).toBe(true)

    otherTile.dispatchEvent(new Event('mouseleave'))
    await flushPromises()

    expect(w.text()).not.toContain('Missing a source type')
  })

  it('highlights the selected source tile', async () => {
    const w = mountIngest()
    await flushPromises()

    const csvTile = w.find('#CSV').element.closest('.source-tile')
    expect(csvTile.classList.contains('selected-source')).toBe(true)

    await w.find('#Excel').setValue()
    expect(csvTile.classList.contains('selected-source')).toBe(false)

    const excelTile = w.find('#Excel').element.closest('.source-tile')
    expect(excelTile.classList.contains('selected-source')).toBe(true)
  })
})
