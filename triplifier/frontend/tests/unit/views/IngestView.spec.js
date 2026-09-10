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

function findTile(wrapper, id) {
  return wrapper.find(`#${id}`).element.closest('.source-tile')
}

function makeDropEvent(files) {
  const event = new Event('drop', { bubbles: true, cancelable: true })
  Object.defineProperty(event, 'dataTransfer', {
    value: { files },
    configurable: true,
  })
  return event
}

function makeDragEvent(type) {
  return new Event(type, { bubbles: true, cancelable: true })
}

async function dropOnTile(wrapper, id, files) {
  findTile(wrapper, id).dispatchEvent(makeDropEvent(files))
  await flushPromises()
}

async function dropOnPage(wrapper, files) {
  wrapper.find('h1').element.dispatchEvent(makeDropEvent(files))
  await flushPromises()
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

  it('shows "Enter Credentials" label when Postgres is selected', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    expect(w.find('button[type="submit"]').text()).toContain('Enter Credentials')
  })

  it('shows "Submit Files" label when CSV is selected', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#CSV').setValue()
    expect(w.find('button[type="submit"]').text()).toContain('Submit Files')
  })

  it('shows a validation error for an invalid Postgres URL after blur', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#username').setValue('u')
    await w.find('#password').setValue('p')
    await w.find('#POSTGRES_URL').setValue('not a url')
    await w.find('#POSTGRES_DB').setValue('flyover')

    // Blur the URL field to trigger validation
    await w.find('#POSTGRES_URL').trigger('blur')
    await flushPromises()

    expect(w.find('#POSTGRES_URL').classes()).toContain('is-invalid')
    expect(w.text()).toContain('Enter a valid host:port')
    expect(w.find('button[type="submit"]').attributes('disabled')).toBeDefined()
  })

  it('does not show a URL validation error before the field is blurred', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#POSTGRES_URL').setValue('bad')
    await flushPromises()

    expect(w.find('#POSTGRES_URL').classes()).not.toContain('is-invalid')
  })

  it('accepts host:port format for the Postgres URL', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    const submit = w.find('button[type="submit"]')

    await w.find('#username').setValue('u')
    await w.find('#password').setValue('p')
    await w.find('#POSTGRES_URL').setValue('localhost:5432')
    await w.find('#POSTGRES_DB').setValue('flyover')
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('clears the URL error when a valid URL is entered', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#POSTGRES_URL').setValue('bad')
    await w.find('#POSTGRES_URL').trigger('blur')
    await flushPromises()
    expect(w.find('#POSTGRES_URL').classes()).toContain('is-invalid')

    await w.find('#POSTGRES_URL').setValue('localhost:5432')
    await w.find('#POSTGRES_URL').trigger('blur')
    await flushPromises()
    expect(w.find('#POSTGRES_URL').classes()).not.toContain('is-invalid')
  })

  it('shows an error when the URL contains an @ after blur', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#POSTGRES_URL').setValue('user@host:5432')
    await w.find('#POSTGRES_URL').trigger('blur')
    await flushPromises()
    expect(w.find('#POSTGRES_URL').classes()).toContain('is-invalid')
    expect(w.text()).toContain('not allowed')
    expect(w.text()).toContain('"@"')
  })

  it('disables submit when any Postgres field contains an @', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    const submit = w.find('button[type="submit"]')

    await w.find('#username').setValue('user@host')
    await w.find('#password').setValue('p')
    await w.find('#POSTGRES_URL').setValue('localhost:5432')
    await w.find('#POSTGRES_DB').setValue('flyover')
    expect(submit.attributes('disabled')).toBeDefined()

    // Fix the username — submit should enable
    await w.find('#username').setValue('user')
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('shows an error when the username contains an @ after blur', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#username').setValue('user@host')
    await w.find('#username').trigger('blur')
    await flushPromises()
    expect(w.find('#username').classes()).toContain('is-invalid')
    expect(w.text()).toContain('not allowed')
    expect(w.text()).toContain('"@"')
  })

  it('shows an error when the database name contains an @ after blur', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#POSTGRES_DB').setValue('db@name')
    await w.find('#POSTGRES_DB').trigger('blur')
    await flushPromises()
    expect(w.find('#POSTGRES_DB').classes()).toContain('is-invalid')
    expect(w.text()).toContain('not allowed')
    expect(w.text()).toContain('"@"')
  })

  it('blocks = in username and database fields', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    const submit = w.find('button[type="submit"]')

    await w.find('#username').setValue('user=name')
    await w.find('#password').setValue('p')
    await w.find('#POSTGRES_URL').setValue('localhost:5432')
    await w.find('#POSTGRES_DB').setValue('flyover')
    expect(submit.attributes('disabled')).toBeDefined()

    await w.find('#username').setValue('user')
    await w.find('#POSTGRES_DB').setValue('db=name')
    expect(submit.attributes('disabled')).toBeDefined()
  })

  it('blocks / in the database name', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()

    await w.find('#POSTGRES_DB').setValue('db/name')
    await w.find('#POSTGRES_DB').trigger('blur')
    await flushPromises()
    expect(w.find('#POSTGRES_DB').classes()).toContain('is-invalid')
    expect(w.text()).toContain('"/"')
  })

  it('allows strong passwords with special characters', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    const submit = w.find('button[type="submit"]')

    await w.find('#username').setValue('user')
    await w.find('#password').setValue('P@ss/w0rd=St!ong&%')
    await w.find('#POSTGRES_URL').setValue('localhost:5432')
    await w.find('#POSTGRES_DB').setValue('flyover')
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('blocks newlines in the password field', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Postgres').setValue()
    const submit = w.find('button[type="submit"]')

    await w.find('#username').setValue('user')
    // Set value directly to preserve the newline character
    const pwInput = w.find('#password').element
    Object.defineProperty(pwInput, 'value', { value: 'pass\nword', configurable: true })
    await pwInput.dispatchEvent(new Event('input'))
    await w.find('#POSTGRES_URL').setValue('localhost:5432')
    await w.find('#POSTGRES_DB').setValue('flyover')
    await flushPromises()
    expect(submit.attributes('disabled')).toBeDefined()
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

  it('defaults separator and decimal based on the browser locale', () => {
    const w = mountIngest()
    const sep = w.find('#csv_separator_sign').element
    const dec = w.find('#csv_decimal_sign').element
    const detectedDecimal = (1.1).toLocaleString(navigator.language).match(/[.,]/)?.[0] || '.'
    expect(dec.value).toBe(detectedDecimal)
    expect(sep.value).toBe(detectedDecimal === ',' ? ';' : ',')
  })

  it('allows changing the separator and decimal sign', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await w.find('#csv_separator_sign').setValue(';')
    await w.find('#csv_decimal_sign').setValue(',')
    expect(w.find('#csv_separator_sign').element.value).toBe(';')
    expect(w.find('#csv_decimal_sign').element.value).toBe(',')
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

  it('does not select any source tile by default', async () => {
    const w = mountIngest()
    await flushPromises()

    const csvTile = w.find('#CSV').element.closest('.source-tile')
    expect(csvTile.classList.contains('selected-source')).toBe(false)

    const excelTile = w.find('#Excel').element.closest('.source-tile')
    expect(excelTile.classList.contains('selected-source')).toBe(false)
  })

  it('highlights the selected source tile', async () => {
    const w = mountIngest()
    await flushPromises()

    const csvTile = w.find('#CSV').element.closest('.source-tile')
    await w.find('#CSV').setValue()
    expect(csvTile.classList.contains('selected-source')).toBe(true)

    await w.find('#Excel').setValue()
    expect(csvTile.classList.contains('selected-source')).toBe(false)

    const excelTile = w.find('#Excel').element.closest('.source-tile')
    expect(excelTile.classList.contains('selected-source')).toBe(true)
  })

  it('accepts CSV files dropped on the CSV tile', async () => {
    const w = mountIngest()
    await flushPromises()
    const submit = w.find('button[type="submit"]')

    await dropOnTile(w, 'CSV', [csvFile('dropped.csv')])
    expect(w.find('#csvPath').element.value).toBe('dropped.csv')
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('selects the CSV source type when files are dropped on the CSV tile', async () => {
    const w = mountIngest()
    await flushPromises()
    await w.find('#Excel').setValue()

    await dropOnTile(w, 'CSV', [csvFile('dropped.csv')])
    const csvTile = findTile(w, 'CSV')
    expect(csvTile.classList.contains('selected-source')).toBe(true)
  })

  it('accepts Excel files dropped on the Excel tile', async () => {
    const w = mountIngest()
    await flushPromises()
    const submit = w.find('button[type="submit"]')

    await dropOnTile(w, 'Excel', [csvFile('report.xlsx')])
    expect(w.find('#csvPath').element.value).toBe('report.xlsx')
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('shows an error when non-matching files are dropped on the CSV tile', async () => {
    const w = mountIngest()
    await flushPromises()

    await dropOnTile(w, 'CSV', [csvFile('notes.txt')])
    expect(w.find('#csvPath').element.value).toBe('')
    expect(w.text()).toContain('Please drop only .csv files')
  })

  it('shows an error when CSV files are dropped on the Excel tile', async () => {
    const w = mountIngest()
    await flushPromises()

    await dropOnTile(w, 'Excel', [csvFile('data.csv')])
    expect(w.find('#csvPath').element.value).toBe('')
    expect(w.text()).toContain('Please drop only .xlsx or .xls files')
  })

  it('shows the drag-over highlight while dragging over the CSV tile', async () => {
    const w = mountIngest()
    await flushPromises()
    const csvTile = findTile(w, 'CSV')

    csvTile.dispatchEvent(makeDragEvent('dragenter'))
    await flushPromises()
    expect(csvTile.classList.contains('drag-over')).toBe(true)

    csvTile.dispatchEvent(makeDragEvent('dragleave'))
    await flushPromises()
    expect(csvTile.classList.contains('drag-over')).toBe(false)
  })

  it('shows the drag-over highlight while dragging over the Excel tile', async () => {
    const w = mountIngest()
    await flushPromises()
    const excelTile = findTile(w, 'Excel')

    excelTile.dispatchEvent(makeDragEvent('dragenter'))
    await flushPromises()
    expect(excelTile.classList.contains('drag-over')).toBe(true)

    excelTile.dispatchEvent(makeDragEvent('dragleave'))
    await flushPromises()
    expect(excelTile.classList.contains('drag-over')).toBe(false)
  })

  it('auto-detects CSV when files are dropped anywhere on the page', async () => {
    const w = mountIngest()
    await flushPromises()
    const submit = w.find('button[type="submit"]')

    await dropOnPage(w, [csvFile('anywhere.csv')])
    expect(w.find('#csvPath').element.value).toBe('anywhere.csv')
    const csvTile = findTile(w, 'CSV')
    expect(csvTile.classList.contains('selected-source')).toBe(true)
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('auto-detects Excel when files are dropped anywhere on the page', async () => {
    const w = mountIngest()
    await flushPromises()
    const submit = w.find('button[type="submit"]')

    await dropOnPage(w, [csvFile('report.xlsx')])
    expect(w.find('#csvPath').element.value).toBe('report.xlsx')
    const excelTile = findTile(w, 'Excel')
    expect(excelTile.classList.contains('selected-source')).toBe(true)
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('shows an error when unsupported files are dropped on the page', async () => {
    const w = mountIngest()
    await flushPromises()

    await dropOnPage(w, [csvFile('notes.txt')])
    expect(w.find('#csvPath').element.value).toBe('')
    expect(w.text()).toContain('Unsupported file type')
    expect(w.text()).toContain('notes.txt')
  })

  it('shows an error when mixed CSV and Excel files are dropped on the page', async () => {
    const w = mountIngest()
    await flushPromises()

    await dropOnPage(w, [csvFile('a.csv'), csvFile('b.xlsx')])
    expect(w.find('#csvPath').element.value).toBe('')
    expect(w.text()).toContain('Unsupported file type')
  })

  it('clears the drop error on a successful subsequent drop', async () => {
    const w = mountIngest()
    await flushPromises()

    await dropOnPage(w, [csvFile('bad.txt')])
    expect(w.text()).toContain('Unsupported file type')

    await dropOnPage(w, [csvFile('good.csv')])
    expect(w.text()).not.toContain('Unsupported file type')
    expect(w.find('#csvPath').element.value).toBe('good.csv')
  })

  it('shows the full-page overlay while dragging files anywhere', async () => {
    const w = mountIngest()
    await flushPromises()

    w.find('h1').element.dispatchEvent(makeDragEvent('dragenter'))
    await flushPromises()
    expect(w.find('.drop-overlay').exists()).toBe(true)

    w.find('h1').element.dispatchEvent(makeDragEvent('dragleave'))
    await new Promise((r) => setTimeout(r, 110))
    await flushPromises()
    expect(w.find('.drop-overlay').exists()).toBe(false)
  })

  it('does not leave the overlay stuck after dragging over a tile then away', async () => {
    const w = mountIngest()
    await flushPromises()

    // Drag enters the page (via a tile), then leaves. The overlay should
    // not get stuck because of the counter going out of sync.
    const csvTile = findTile(w, 'CSV')
    csvTile.dispatchEvent(makeDragEvent('dragenter'))
    await flushPromises()
    expect(w.find('.drop-overlay').exists()).toBe(true)

    csvTile.dispatchEvent(makeDragEvent('dragleave'))
    await new Promise((r) => setTimeout(r, 110))
    await flushPromises()
    expect(w.find('.drop-overlay').exists()).toBe(false)
  })

  it('keeps the overlay visible when dragover fires between dragleave and dragenter', async () => {
    const w = mountIngest()
    await flushPromises()

    w.find('h1').element.dispatchEvent(makeDragEvent('dragenter'))
    await flushPromises()
    expect(w.find('.drop-overlay').exists()).toBe(true)

    // Simulate moving between child elements: dragleave then dragover then dragenter
    w.find('h1').element.dispatchEvent(makeDragEvent('dragleave'))
    w.find('h1').element.dispatchEvent(makeDragEvent('dragover'))
    w.find('h1').element.dispatchEvent(makeDragEvent('dragenter'))
    await new Promise((r) => setTimeout(r, 110))
    await flushPromises()
    // The dragover cancelled the hide timer, so the overlay stays visible
    expect(w.find('.drop-overlay').exists()).toBe(true)

    // Now truly leave — no dragover to save it
    w.find('h1').element.dispatchEvent(makeDragEvent('dragleave'))
    await new Promise((r) => setTimeout(r, 110))
    await flushPromises()
    expect(w.find('.drop-overlay').exists()).toBe(false)
  })
})
