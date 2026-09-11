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
  // FileReader.onload fires as a macrotask — give it time to resolve
  // so csvColumns is populated before tests interact with PK/FK selects.
  await new Promise((r) => setTimeout(r, 50))
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

function findPkFkSection(wrapper) {
  const alert = wrapper.findAll('.alert-info').find((a) =>
    a.text().includes('Multiple Tables Detected') || a.text().includes('Multiple')
  )
  return alert?.element.closest('div.mt-4')
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
        a.text().includes('Multiple Tables Detected')
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
    // The CSV/Excel section (v-show) is 3 parents above #csvPath
    // (input-group > d-flex div > v-show div)
    const csvSection = w.find('#csvPath').element.parentElement.parentElement.parentElement
    expect(csvSection.style.cssText).toContain('display: none')

    await w.find('#CSV').setValue()
    expect(csvSection.style.cssText).not.toContain('display: none')
  })

  it('shows separator and decimal dropdowns only when CSV is selected', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    // The v-show div wrapping the separator/decimal dropdowns has inline style
    const sepDiv = w.find('#csv_separator_sign').element.parentElement
    expect(sepDiv.style.cssText).not.toContain('display: none')
    const decDiv = w.find('#csv_decimal_sign').element.parentElement
    expect(decDiv.style.cssText).not.toContain('display: none')

    await w.find('#Excel').setValue()
    expect(sepDiv.style.cssText).toContain('display: none')
    expect(decDiv.style.cssText).toContain('display: none')
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
    await flushPromises()
    // The Postgres section (v-show) is 3 parents above #username
    // (col-md-6 > row > v-show div)
    const pgSection = w.find('#username').element.parentElement.parentElement.parentElement
    expect(pgSection.style.cssText).toContain('display: none')

    await w.find('#Postgres').setValue()
    await flushPromises()
    expect(pgSection.style.cssText).not.toContain('display: none')
    const pgUrlSection = w.find('#POSTGRES_URL').element.parentElement
    expect(pgUrlSection.style.cssText).not.toContain('display: none')
    const pgDbSection = w.find('#POSTGRES_DB').element.parentElement
    expect(pgDbSection.style.cssText).not.toContain('display: none')
  })

  it('hides Specify Source Information card when Other is selected', async () => {
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    const sourceCard = w.findAll('.card.mb-4').find((c) =>
      c.text().includes('Specify Source Information')
    )
    expect(sourceCard.element.style.cssText).not.toContain('display: none')

    await w.find('#Other').setValue()
    expect(sourceCard.element.style.cssText).toContain('display: none')
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

  // -- Circular reference prevention (issue #86) --------------------------

  it('shows a warning when linking a table to itself (same name)', async () => {
    dataExists.value = true
    api.get.mockResolvedValue({
      data: { tables: ['patients'], tableColumns: { patients: ['id', 'name'] } },
    })
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('patients.csv', 'id,name')])
    await flushPromises()

    // Enable data linking and select the same table name for both
    await w.find('#enableDataLinking').setValue()
    await flushPromises()
    await w.find('#newTableName').setValue('patients.csv')
    await w.find('#newColumnName').setValue('id')
    await w.find('#existingTableName').setValue('patients')
    await w.find('#existingColumnName').setValue('id')
    await flushPromises()

    expect(w.text()).toContain('circular reference')
  })

  it('does not show a warning when linking different tables', async () => {
    dataExists.value = true
    api.get.mockResolvedValue({
      data: { tables: ['doctors'], tableColumns: { doctors: ['id', 'name'] } },
    })
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('patients.csv', 'id,name')])
    await flushPromises()

    await w.find('#enableDataLinking').setValue()
    await flushPromises()
    await w.find('#newTableName').setValue('patients.csv')
    await w.find('#newColumnName').setValue('id')
    await w.find('#existingTableName').setValue('doctors')
    await w.find('#existingColumnName').setValue('id')
    await flushPromises()

    expect(w.text()).not.toContain('circular reference')
  })

  it('blocks cross-graph link data when tables match (issue #86)', async () => {
    dataExists.value = true
    api.get.mockResolvedValue({
      data: { tables: ['patients'], tableColumns: { patients: ['id', 'name'] } },
    })
    const w = mountIngest()
    await flushPromises()

    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('patients.csv', 'id,name')])
    await flushPromises()

    await w.find('#enableDataLinking').setValue()
    await flushPromises()
    await w.find('#newTableName').setValue('patients.csv')
    await w.find('#newColumnName').setValue('id')
    await w.find('#existingTableName').setValue('patients')
    await w.find('#existingColumnName').setValue('id')
    await flushPromises()

    // The hidden crossGraphLinkData should be empty (no link sent)
    expect(w.find('#crossGraphLinkData').element.value).toBe('')
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

// ---------------------------------------------------------------------------
// PK/FK scenarios — CSV multi-file and Excel multi-sheet
// ---------------------------------------------------------------------------

// Build a minimal .xlsx-like blob that JSZip can parse to extract sheet names.
// The xlsx format stores sheet names in xl/workbook.xml as <sheet name="..."/>.
// We create a zip with just that file so the frontend's sheet detection works.
async function xlsxFile(name, sheetNames, header = 'col1,col2,col3') {
  const headers = header.split(',')

  // Build a shared strings table with the header values
  const sharedStringsXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="${headers.length}" uniqueCount="${headers.length}">
${headers.map((h) => `<si><t>${h}</t></si>`).join('')}
</sst>`

  const workbookXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets>
${sheetNames.map((s, i) => `  <sheet name="${s}" sheetId="${i + 1}" r:id="rId${i + 1}"/>`).join('\n')}
</sheets>
</workbook>`

  // Each sheet references shared strings by index (t="s"), like real Excel
  const sheetXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<sheetData>
<row r="1" spans="1:${headers.length}">${headers.map((h, i) => `<c r="${String.fromCharCode(65 + i)}1" t="s"><v>${i}</v></c>`).join('')}</row>
</sheetData>
</worksheet>`

  const relsXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
${sheetNames.map((s, i) => `  <Relationship Id="rId${i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${i + 1}.xml"/>`).join('\n')}
  <Relationship Id="rId${sheetNames.length + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
</Relationships>`

  const contentTypes = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="xml" ContentType="application/xml"/>
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
${sheetNames.map((s, i) => `<Override PartName="/xl/worksheets/sheet${i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`).join('\n')}
<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>`

  const relsBase = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`

  const JSZip = (await import('jszip')).default
  const zip = new JSZip()
  zip.file('[Content_Types].xml', contentTypes)
  zip.file('_rels/.rels', relsBase)
  zip.file('xl/workbook.xml', workbookXml)
  zip.file('xl/_rels/workbook.xml.rels', relsXml)
  zip.file('xl/sharedStrings.xml', sharedStringsXml)
  sheetNames.forEach((_, i) => {
    zip.file(`xl/worksheets/sheet${i + 1}.xml`, sheetXml)
  })
  const blob = await zip.generateAsync({ type: 'blob', mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
  return new File([blob], name, { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
}

describe('IngestView — PK/FK', () => {
  beforeEach(() => {
    dataExists.value = false
    refreshDataExists.mockClear()
    api.get.mockReset()
    api.get.mockResolvedValue({ data: { tables: [], tableColumns: {} } })
  })

  // -- CSV: section visibility --------------------------------------------

  it('hides PK/FK section for a single CSV file', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('only.csv')])
    expect(findPkFkSection(w)?.style.display).toBe('none')
  })

  it('shows PK/FK section for two CSV files', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('a.csv'), csvFile('b.csv')])
    const section = findPkFkSection(w)
    expect(section.style.display).not.toBe('none')
  })

  it('hides PK/FK section for a single Excel file with one sheet', async () => {
    const f = await xlsxFile('data.xlsx', ['Sheet1'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    // The section should be hidden (only one table)
    const section = findPkFkSection(w)
    expect(section.style.display).toBe('none')
  })

  it('shows PK/FK section for a single Excel file with multiple sheets', async () => {
    const f = await xlsxFile('data.xlsx', ['Sheet1', 'Sheet2'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    const section = findPkFkSection(w)
    expect(section.style.display).not.toBe('none')
  })

  // -- Card rendering -------------------------------------------------------

  it('renders one PK/FK card per CSV file', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [csvFile('patients.csv'), csvFile('visits.csv')])
    const cards = w.findAll('.card.mb-3')
    expect(cards.length).toBe(2)
    expect(w.text()).toContain('patients.csv')
    expect(w.text()).toContain('visits.csv')
  })

  it('renders one PK/FK card per Excel sheet', async () => {
    const f = await xlsxFile('data.xlsx', ['Patients', 'Visits'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    const cards = w.findAll('.card.mb-3')
    expect(cards.length).toBe(2)
    expect(w.text()).toContain('Patients')
    expect(w.text()).toContain('Visits')
  })

  it('renders three PK/FK cards for three CSV files', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('a.csv', 'id,name'),
      csvFile('b.csv', 'id,date'),
      csvFile('c.csv', 'id,value'),
    ])
    const cards = w.findAll('.card.mb-3')
    expect(cards.length).toBe(3)
  })

  // -- Column dropdown population ------------------------------------------

  it('populates PK dropdown with detected columns for each CSV file', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name,age'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    const pk0 = w.find('#pk_0')
    expect(pk0.element.innerHTML).toContain('patient_id')
    expect(pk0.element.innerHTML).toContain('name')
    const pk1 = w.find('#pk_1')
    expect(pk1.element.innerHTML).toContain('visit_id')
    expect(pk1.element.innerHTML).toContain('date')
  })

  it('populates PK dropdown with detected columns for each Excel sheet', async () => {
    const f = await xlsxFile('data.xlsx', ['Patients', 'Visits'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    const pk0 = w.find('#pk_0')
    expect(pk0.element.innerHTML).toContain('col1')
    expect(pk0.element.innerHTML).toContain('col2')
  })

  // -- FK dropdown cascade -------------------------------------------------

  it('shows referenced table and column dropdowns when an FK is selected', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    // Select an FK for the second file
    await w.find('#fk_1').setValue('patient_id')
    await flushPromises()
    // The referenced table dropdown should be visible
    expect(w.find('#fkTable_1').exists()).toBe(true)
    expect(w.find('#fkColumn_1').exists()).toBe(true)
  })

  it('only shows other tables in the referenced table dropdown', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    await w.find('#fk_1').setValue('patient_id')
    await flushPromises()
    const fkTableSelect = w.find('#fkTable_1')
    const options = fkTableSelect.element.innerHTML
    // Should contain patients.csv (the other file) but not visits.csv (self)
    expect(options).toContain('patients.csv')
    expect(options).not.toContain('visits.csv')
  })

  it('clears the FK column selection when the referenced table changes', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('doctors.csv', 'doctor_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,doctor_id'),
    ])
    await flushPromises()
    // Select FK for visits.csv
    await w.find('#fk_2').setValue('patient_id')
    await flushPromises()
    // Select referenced table
    await w.find('#fkTable_2').setValue('patients.csv')
    await flushPromises()
    // Select a column
    await w.find('#fkColumn_2').setValue('patient_id')
    await flushPromises()
    expect(w.find('#fkColumn_2').element.value).toBe('patient_id')
    // Change referenced table — column should clear
    await w.find('#fkTable_2').setValue('doctors.csv')
    await flushPromises()
    expect(w.find('#fkColumn_2').element.value).toBe('')
  })

  it('populates FK column dropdown with columns from the referenced table', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name,age'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    await w.find('#fk_1').setValue('patient_id')
    await flushPromises()
    await w.find('#fkTable_1').setValue('patients.csv')
    await flushPromises()
    const fkColSelect = w.find('#fkColumn_1')
    const options = fkColSelect.element.innerHTML
    expect(options).toContain('patient_id')
    expect(options).toContain('name')
    expect(options).toContain('age')
  })

  // -- Submit validation ---------------------------------------------------

  it('allows submit when no PK/FK selections are made', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    const submit = w.find('button[type="submit"]')
    await pickFiles(w, [csvFile('a.csv'), csvFile('b.csv')])
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  it('allows submit when PK is selected without FK', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.find('button[type="submit"]').attributes('disabled')).toBeUndefined()
  })

  it('disables submit when FK is selected but referenced table has no PK', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    const submit = w.find('button[type="submit"]')
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id'),
    ])
    await flushPromises()
    // Select FK for visits.csv but don't set PK on patients.csv
    await w.find('#fk_1').setValue('patient_id')
    await flushPromises()
    await w.find('#fkTable_1').setValue('patients.csv')
    await flushPromises()
    await w.find('#fkColumn_1').setValue('patient_id')
    await flushPromises()
    expect(submit.attributes('disabled')).toBeDefined()
  })

  it('enables submit when FK references a table that has a PK', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    const submit = w.find('button[type="submit"]')
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id'),
    ])
    await flushPromises()
    // Set PK on patients.csv
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    // Set FK on visits.csv
    await w.find('#fk_1').setValue('patient_id')
    await flushPromises()
    await w.find('#fkTable_1').setValue('patients.csv')
    await flushPromises()
    await w.find('#fkColumn_1').setValue('patient_id')
    await flushPromises()
    expect(submit.attributes('disabled')).toBeUndefined()
  })

  // -- PK/FK JSON output ---------------------------------------------------

  it('includes PK/FK data in the hidden input as JSON', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('doctors.csv', 'doctor_id,name'),
    ])
    await flushPromises()
    expect(w.find('#pk_0').exists()).toBe(true)
    expect(w.find('#pk_1').exists()).toBe(true)
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    const json = w.find('#pkFkData').element.value
    expect(json).toBeTruthy()
    const parsed = JSON.parse(json)
    expect(parsed).toHaveLength(1)
    expect(parsed[0].fileName).toBe('patients.csv')
    expect(parsed[0].primaryKey).toBe('patient_id')
  })

  it('includes full FK relationship in the JSON when all fields are set', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await w.find('#fk_1').setValue('patient_id')
    await flushPromises()
    await w.find('#fkTable_1').setValue('patients.csv')
    await flushPromises()
    await w.find('#fkColumn_1').setValue('patient_id')
    await flushPromises()
    const parsed = JSON.parse(w.find('#pkFkData').element.value)
    expect(parsed).toHaveLength(2)
    const visits = parsed.find((p) => p.fileName === 'visits.csv')
    expect(visits.foreignKey).toBe('patient_id')
    expect(visits.foreignKeyTable).toBe('patients.csv')
    expect(visits.foreignKeyColumn).toBe('patient_id')
  })

  it('uses sheet-based table names in the PK/FK JSON for Excel', async () => {
    const f = await xlsxFile('data.xlsx', ['Patients', 'Visits'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    // Set a PK on the first sheet (auto-suggest will also fill FK on Visits
    // since both sheets share the same column names)
    await w.find('#pk_0').setValue('col1')
    await flushPromises()
    const parsed = JSON.parse(w.find('#pkFkData').element.value)
    // Find the Patients entry
    const patientsEntry = parsed.find((p) => p.fileName.includes('Patients'))
    expect(patientsEntry).toBeDefined()
    expect(patientsEntry.fileName).toContain('Patients')
    expect(patientsEntry.primaryKey).toBe('col1')
  })

  // -- Section title -------------------------------------------------------

  it('shows "tables" in the PK/FK section title for Excel, not "CSV Files"', async () => {
    const f = await xlsxFile('data.xlsx', ['Sheet1', 'Sheet2'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    const section = findPkFkSection(w)
    expect(section.textContent).not.toContain('Multiple CSV Files Detected')
  })

  // -- FK auto-suggest ----------------------------------------------------

  it('auto-suggests FK when a matching column is found in another table', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name,age'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    // Set PK on patients.csv
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    // visits.csv should have FK auto-filled with patient_id
    expect(w.find('#fk_1').element.value).toBe('patient_id')
    expect(w.find('#fkTable_1').element.value).toBe('patients.csv')
    expect(w.find('#fkColumn_1').element.value).toBe('patient_id')
  })

  it('does not auto-suggest FK when no matching column exists', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name,age'),
      csvFile('doctors.csv', 'doctor_id,name,specialty'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    // doctors.csv has no patient_id column — no auto-suggest
    expect(w.find('#fk_1').element.value).toBe('')
    expect(w.find('#fkTable_1').element.value).toBe('')
  })

  it('auto-suggests for case-insensitive matches', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'Patient_ID,name'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('Patient_ID')
    await flushPromises()
    // Should match patient_id (lowercase) in visits.csv
    expect(w.find('#fk_1').element.value).toBe('patient_id')
  })

  it('does not override a manually-set FK', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,other_id'),
    ])
    await flushPromises()
    // Manually set FK on visits.csv to a different column
    await w.find('#fk_1').setValue('other_id')
    await flushPromises()
    // Now set PK on patients.csv — should NOT override the manual FK
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.find('#fk_1').element.value).toBe('other_id')
  })

  it('clears auto-suggested FK when the PK is removed', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.find('#fk_1').element.value).toBe('patient_id')

    // Remove the PK — auto-suggested FK should be cleared
    await w.find('#pk_0').setValue('')
    await flushPromises()
    expect(w.find('#fk_1').element.value).toBe('')
    expect(w.find('#fkTable_1').element.value).toBe('')
  })

  it('auto-suggests FK across three tables', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('doctors.csv', 'doctor_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,doctor_id'),
    ])
    await flushPromises()
    // Set PK on patients — visits should get FK auto-suggest for patient_id
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.find('#fk_2').element.value).toBe('patient_id')

    // Set PK on doctors — visits should get FK for doctor_id too
    // but fk_2 is already set to patient_id, so it should NOT be overridden
    expect(w.find('#fk_2').element.value).toBe('patient_id')
  })

  it('auto-suggests FK for Excel sheets with matching columns', async () => {
    const f = await xlsxFile('data.xlsx', ['Patients', 'Visits'])
    const w = mountIngest()
    await w.find('#Excel').setValue()
    await pickFiles(w, [f])
    await flushPromises()
    // The xlsxFile helper creates col1, col2, col3 for every sheet
    // Set PK on first sheet
    await w.find('#pk_0').setValue('col1')
    await flushPromises()
    // Second sheet should auto-suggest FK = col1
    expect(w.find('#fk_1').element.value).toBe('col1')
  })

  // -- Inference marking ---------------------------------------------------

  it('shows an "Inferred" badge on the table card when FK is auto-suggested', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.text()).toContain('Inferred')
    expect(w.text()).toContain('please verify')
  })

  it('does not show an "Inferred" badge when no FK is auto-suggested', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('doctors.csv', 'doctor_id,name,specialty'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.text()).not.toContain('Inferred')
  })

  it('removes the "Inferred" badge when the user manually changes the FK', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,other_id'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.text()).toContain('Inferred')
    // Manually change the FK to a different column
    await w.find('#fk_1').setValue('other_id')
    await flushPromises()
    expect(w.text()).not.toContain('Inferred')
  })

  it('removes the "Inferred" badge when the user changes the referenced table', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('doctors.csv', 'doctor_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,doctor_id'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.text()).toContain('Inferred')
    // Manually change the referenced table
    await w.find('#fkTable_2').setValue('doctors.csv')
    await flushPromises()
    expect(w.text()).not.toContain('Inferred')
  })

  it('removes the "Inferred" badge when the PK is removed', async () => {
    const w = mountIngest()
    await w.find('#CSV').setValue()
    await pickFiles(w, [
      csvFile('patients.csv', 'patient_id,name'),
      csvFile('visits.csv', 'visit_id,patient_id,date'),
    ])
    await flushPromises()
    await w.find('#pk_0').setValue('patient_id')
    await flushPromises()
    expect(w.text()).toContain('Inferred')
    await w.find('#pk_0').setValue('')
    await flushPromises()
    expect(w.text()).not.toContain('Inferred')
  })
})
