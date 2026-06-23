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
})
