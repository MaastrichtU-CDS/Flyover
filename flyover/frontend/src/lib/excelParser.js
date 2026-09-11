// Excel .xlsx parsing utilities.
//
// Reads sheet names and column headers from an .xlsx file using JSZip.
// Handles both shared strings (t="s" + <v>index</v>) and inline strings
// (t="inlineStr" + <is><t>text</t></is>).

import JSZip from 'jszip'

// --- Low-level XML extraction helpers ---

/** Collect all matches of a regex with capture group 1 into an array. */
function collectMatches(xml, regex) {
  const results = []
  let m
  while ((m = regex.exec(xml)) !== null) {
    results.push(m[1])
  }
  return results
}

/** Collect key-value pairs from a regex with two capture groups. */
function collectPairs(xml, regex) {
  const results = []
  let m
  while ((m = regex.exec(xml)) !== null) {
    results.push({ id: m[1], target: m[2] })
  }
  return results
}

// --- Workbook structure helpers ---

/** Extract sheet display names from xl/workbook.xml. */
function parseSheetNames(workbookXml) {
  return collectMatches(workbookXml, /<sheet\s+[^>]*name="([^"]+)"/g)
}

/** Parse the shared strings table (xl/sharedStrings.xml) into a string array. */
function parseSharedStrings(sharedStringsXml) {
  if (!sharedStringsXml) return []
  const strings = []
  const siRegex = /<si>([\s\S]*?)<\/si>/g
  let siMatch
  while ((siMatch = siRegex.exec(sharedStringsXml)) !== null) {
    // Extract text from <t>...</t> within the <si> (may have rich text runs)
    const tMatches = siMatch[1].match(/<t[^>]*>([^<]*)<\/t>/g)
    if (tMatches) {
      strings.push(tMatches.map((t) => t.replace(/<[^>]*>/g, '')).join(''))
    } else {
      strings.push('')
    }
  }
  return strings
}

/**
 * Map sheet names to their XML file paths via the workbook relationships.
 * Returns an array of { name, path } in the same order as parseSheetNames.
 */
function resolveSheetPaths(workbookXml, relsXml) {
  if (!relsXml) return []
  const rels = collectPairs(
    relsXml,
    /<Relationship\s+[^>]*Id="([^"]+)"[^>]*Target="([^"]+)"/g
  )
  const paths = []
  const sheetRelRegex = /<sheet\s+[^>]*name="([^"]+)"[^>]*r:id="([^"]+)"/g
  let relMatch
  while ((relMatch = sheetRelRegex.exec(workbookXml)) !== null) {
    const rel = rels.find((r) => r.id === relMatch[2])
    if (rel) {
      paths.push({
        name: relMatch[1],
        path: rel.target.startsWith('/') ? rel.target.slice(1) : `xl/${rel.target}`,
      })
    }
  }
  return paths
}

// --- Cell value extraction ---

/**
 * Extract a single cell value from its XML content and type.
 *
 * Cell types:
 *   t="s"         → shared string: <v>index</v> into sharedStrings
 *   t="inlineStr" → inline string: <is><t>text</t></is>
 *   t="str"       → formula string: <v>text</v>
 *   no t          → numeric: <v>42</v> (use the value as-is for header)
 *   t="b"         → boolean: skip (returns null)
 */
function extractCellValue(cellContent, cellTag, sharedStrings) {
  const typeMatch = cellTag.match(/\bt="([^"]+)"/)
  const type = typeMatch ? typeMatch[1] : ''

  if (type === 's') {
    const vMatch = cellContent.match(/<v>([^<]*)<\/v>/)
    if (vMatch) {
      const idx = parseInt(vMatch[1], 10)
      return sharedStrings[idx] ?? null
    }
    return null
  }
  if (type === 'inlineStr') {
    const tMatch = cellContent.match(/<t[^>]*>([^<]*)<\/t>/)
    return tMatch ? tMatch[1] : null
  }
  if (type === 'str') {
    const vMatch = cellContent.match(/<v>([^<]*)<\/v>/)
    return vMatch ? vMatch[1] : null
  }
  if (!type) {
    const vMatch = cellContent.match(/<v>([^<]*)<\/v>/)
    return vMatch ? vMatch[1] : null
  }
  return null
}

/**
 * Extract column headers from the first row of a sheet's XML.
 * Returns an array of trimmed, non-empty header strings.
 */
export function parseSheetHeaderColumns(sheetXml, sharedStrings) {
  const rowMatch = sheetXml.match(/<row\s+r="1"[^>]*>([\s\S]*?)<\/row>/)
  if (!rowMatch) return []

  const cellRegex = /<c\b[^>]*>([\s\S]*?)<\/c>|<c\b[^>]*\/>/g
  const cols = []
  let cellMatch
  while ((cellMatch = cellRegex.exec(rowMatch[1])) !== null) {
    const cellContent = cellMatch[1] || ''
    const cellTag = cellMatch[0]
    const value = extractCellValue(cellContent, cellTag, sharedStrings)
    if (value !== null && value !== '') {
      cols.push(String(value).trim())
    }
  }
  return cols
}

// --- High-level API ---

/**
 * Read sheet names and column headers from an .xlsx file.
 * Returns a list of { name, columns } entries, one per sheet.
 */
export async function readExcelSheetInfo(file) {
  try {
    const zip = await JSZip.loadAsync(file)
    const workbookXml = await zip.file('xl/workbook.xml')?.async('string')
    if (!workbookXml) return []

    const sheetNames = parseSheetNames(workbookXml)
    if (!sheetNames.length) return []

    const sharedStringsXml = await zip.file('xl/sharedStrings.xml')?.async('string')
    const sharedStrings = parseSharedStrings(sharedStringsXml)

    const relsXml = await zip.file('xl/_rels/workbook.xml.rels')?.async('string')
    const sheetPaths = resolveSheetPaths(workbookXml, relsXml)

    const result = []
    for (let i = 0; i < sheetNames.length; i++) {
      const sheetInfo = sheetPaths[i]
      let columns = []
      if (sheetInfo) {
        try {
          const sheetXml = await zip.file(sheetInfo.path)?.async('string')
          if (sheetXml) {
            columns = parseSheetHeaderColumns(sheetXml, sharedStrings)
          }
        } catch { /* empty columns */ }
      }
      result.push({ name: sheetNames[i], columns })
    }
    return result
  } catch {
    return []
  }
}
