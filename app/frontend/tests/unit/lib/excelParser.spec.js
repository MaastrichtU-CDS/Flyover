import { describe, it, expect } from 'vitest'
import JSZip from 'jszip'

import {
  readExcelSheetInfo,
  parseOdsSheetInfo,
  parseOdsHeaderColumns,
} from '@/lib/excelParser'

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const ODS_NS =
  'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" ' +
  'xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" ' +
  'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"'

function odsCell(text, attrs = '') {
  return `<table:table-cell office:value-type="string"${attrs ? ` ${attrs}` : ''}><text:p>${text}</text:p></table:table-cell>`
}

function odsRow(cells) {
  return `<table:table-row table:style-name="ro1">${cells.join('')}</table:table-row>`
}

/** Wrap sheet bodies in a content.xml document the way LibreOffice writes it. */
function odsContent(tables) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<office:document-content ${ODS_NS} office:version="1.3">
<office:body><office:spreadsheet>
${tables.join('\n')}
</office:spreadsheet></office:body></office:document-content>`
}

function odsTable(name, rows) {
  return `<table:table table:name="${name}" table:style-name="ta1"><table:table-column table:style-name="co1" table:number-columns-repeated="3"/>${rows.join('')}</table:table>`
}

async function odsFile(name, contentXml) {
  const zip = new JSZip()
  zip.file('mimetype', 'application/vnd.oasis.opendocument.spreadsheet', { compression: 'STORE' })
  zip.file('content.xml', contentXml)
  const blob = await zip.generateAsync({ type: 'blob' })
  return new File([blob], name, { type: 'application/vnd.oasis.opendocument.spreadsheet' })
}

async function minimalXlsxFile(name, sheetName, headers) {
  const zip = new JSZip()
  zip.file(
    'xl/workbook.xml',
    `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="${sheetName}" sheetId="1" r:id="rId1"/></sheets></workbook>`
  )
  zip.file(
    'xl/_rels/workbook.xml.rels',
    `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>`
  )
  zip.file(
    'xl/worksheets/sheet1.xml',
    `<worksheet><sheetData><row r="1">${headers.map((h, i) => `<c r="${String.fromCharCode(65 + i)}1" t="inlineStr"><is><t>${h}</t></is></c>`).join('')}</row></sheetData></worksheet>`
  )
  const blob = await zip.generateAsync({ type: 'blob' })
  return new File([blob], name)
}

// ---------------------------------------------------------------------------
// ODS content.xml parsing
// ---------------------------------------------------------------------------

describe('parseOdsSheetInfo', () => {
  it('returns one entry per <table:table> with its header row', () => {
    const xml = odsContent([
      odsTable('Patients', [odsRow([odsCell('id'), odsCell('name')]), odsRow([odsCell('1'), odsCell('Ann')])]),
      odsTable('Visits', [odsRow([odsCell('visit_id'), odsCell('patient_id')])]),
    ])
    expect(parseOdsSheetInfo(xml)).toEqual([
      { name: 'Patients', columns: ['id', 'name'] },
      { name: 'Visits', columns: ['visit_id', 'patient_id'] },
    ])
  })

  it('returns no columns for a sheet without rows', () => {
    const xml = odsContent([odsTable('Empty', [])])
    expect(parseOdsSheetInfo(xml)).toEqual([{ name: 'Empty', columns: [] }])
  })

  it('decodes XML entities in sheet names and headers', () => {
    const xml = odsContent([odsTable('R&amp;D', [odsRow([odsCell('a &lt; b'), odsCell('&quot;q&quot;')])])])
    expect(parseOdsSheetInfo(xml)).toEqual([{ name: 'R&D', columns: ['a < b', '"q"'] }])
  })

  it('does not confuse <table:table-row> and <table:table-column> with sheets', () => {
    const xml = odsContent([odsTable('Only', [odsRow([odsCell('x')])])])
    expect(parseOdsSheetInfo(xml).map((s) => s.name)).toEqual(['Only'])
  })
})

describe('parseOdsHeaderColumns', () => {
  it('skips empty and self-closing cells, including trailing repeated blanks', () => {
    const table = odsTable('S', [
      odsRow([
        odsCell('a'),
        '<table:table-cell/>',
        odsCell('b'),
        '<table:table-cell table:number-columns-repeated="1021"/>',
      ]),
    ])
    expect(parseOdsHeaderColumns(table)).toEqual(['a', 'b'])
  })

  it('expands table:number-columns-repeated on non-empty header cells', () => {
    const table = odsTable('S', [odsRow([odsCell('dup', 'table:number-columns-repeated="2"'), odsCell('z')])])
    expect(parseOdsHeaderColumns(table)).toEqual(['dup', 'dup', 'z'])
  })

  it('flattens spans, spaces and multiple paragraphs into one header string', () => {
    const cell =
      '<table:table-cell office:value-type="string">' +
      '<text:p><text:span text:style-name="T1">first</text:span><text:s/>name</text:p>' +
      '<text:p>line two</text:p>' +
      '</table:table-cell>'
    const table = odsTable('S', [odsRow([cell])])
    expect(parseOdsHeaderColumns(table)).toEqual(['first name line two'])
  })

  it('ignores covered cells left by merged ranges', () => {
    const table = odsTable('S', [
      odsRow([odsCell('merged', 'table:number-columns-spanned="2"'), '<table:covered-table-cell/>', odsCell('c')]),
    ])
    expect(parseOdsHeaderColumns(table)).toEqual(['merged', 'c'])
  })
})

// ---------------------------------------------------------------------------
// readExcelSheetInfo dispatch
// ---------------------------------------------------------------------------

describe('readExcelSheetInfo', () => {
  it('reads sheets and headers from an .ods file', async () => {
    const file = await odsFile(
      'clinic.ods',
      odsContent([
        odsTable('Patients', [odsRow([odsCell('id'), odsCell('name')])]),
        odsTable('Visits', [odsRow([odsCell('visit_id'), odsCell('patient_id')])]),
      ])
    )
    expect(await readExcelSheetInfo(file)).toEqual([
      { name: 'Patients', columns: ['id', 'name'] },
      { name: 'Visits', columns: ['visit_id', 'patient_id'] },
    ])
  })

  it('still reads an .xlsx file', async () => {
    const file = await minimalXlsxFile('data.xlsx', 'Sheet1', ['col1', 'col2'])
    expect(await readExcelSheetInfo(file)).toEqual([{ name: 'Sheet1', columns: ['col1', 'col2'] }])
  })

  it('returns an empty list for a zip that is neither format', async () => {
    const zip = new JSZip()
    zip.file('readme.txt', 'not a workbook')
    const file = new File([await zip.generateAsync({ type: 'blob' })], 'other.zip')
    expect(await readExcelSheetInfo(file)).toEqual([])
  })

  it('returns an empty list for a file that is not a zip', async () => {
    const file = new File(['col1,col2\n1,2\n'], 'data.csv', { type: 'text/csv' })
    expect(await readExcelSheetInfo(file)).toEqual([])
  })
})
