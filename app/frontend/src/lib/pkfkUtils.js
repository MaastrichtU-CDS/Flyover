// PK/FK auto-suggestion and validation helpers.
//
// Pure functions that operate on plain data — the ingest view wires them
// to reactive state.

/**
 * Find a column in `columns` that matches `pkColumn` (case-insensitive).
 * Tries exact match first, then a loose contains/substring match.
 * Returns the matching column name or null.
 */
function findFkMatch(pkColumn, columns) {
  if (!columns || !columns.length) return null
  const pkLower = pkColumn.toLowerCase()

  let match = columns.find((c) => c.toLowerCase() === pkLower)
  if (!match) {
    match = columns.find(
      (c) => c.toLowerCase().includes(pkLower) || pkLower.includes(c.toLowerCase())
    )
  }
  return match || null
}

/**
 * Given the current PK/FK state, compute auto-suggestions for all tables
 * when a PK is set on table `pkIndex`.
 *
 * Returns a map of { index: { fk, fkTable, fkColumn } } for tables that
 * should receive a suggestion. Tables with an existing manual FK are skipped.
 */
export function computeAutoSuggestions(tables, columns, pkSelections, pkIndex) {
  const pkColumn = pkSelections[pkIndex]
  const pkTableName = tables[pkIndex]
  if (!pkColumn || !pkTableName) return {}

  const suggestions = {}
  tables.forEach((tableName, index) => {
    if (index === pkIndex) return
    // Don't override a manually-set FK (caller checks)
    const cols = columns[tableName]
    const match = findFkMatch(pkColumn, cols)
    if (match) {
      suggestions[index] = {
        fk: match,
        fkTable: pkTableName,
        fkColumn: pkColumn,
      }
    }
  })
  return suggestions
}

/**
 * Identify which table indices should have their auto-suggested FK cleared
 * when the PK on table `pkIndex` is removed.
 *
 * Returns an array of indices whose FK references `pkTableName`.
 */
export function computeClearIndices(fkTableSelections, pkTableName) {
  return Object.keys(fkTableSelections)
    .filter((k) => fkTableSelections[k] === pkTableName)
    .map(Number)
}

/**
 * Validate that every FK reference points to a table that has a PK set.
 * Returns true if valid, false otherwise.
 */
export function validatePkFkRelationships(
  showPkFkSection,
  tables,
  pkSelections,
  fkSelections,
  fkTableSelections
) {
  if (!showPkFkSection) return true
  let valid = true
  tables.forEach((_, index) => {
    const fk = fkSelections[index] || ''
    const fkTable = fkTableSelections[index] || ''
    if (fk && fkTable) {
      const refIdx = tables.findIndex((t) => t === fkTable)
      if (refIdx !== -1) {
        const refPk = pkSelections[refIdx] || ''
        if (!refPk) valid = false
      }
    }
  })
  return valid
}

/**
 * Build the serialised PK/FK JSON payload for form submission.
 * Returns a JSON string or '' if the section is not shown.
 */
export function buildPkFkDataJson(
  showPkFkSection,
  tables,
  pkSelections,
  fkSelections,
  fkTableSelections,
  fkColumnSelections
) {
  if (!showPkFkSection) return ''
  const data = []
  tables.forEach((tableName, index) => {
    const pk = pkSelections[index] || ''
    const fk = fkSelections[index] || ''
    const fkTable = fkTableSelections[index] || ''
    const fkColumn = fkColumnSelections[index] || ''
    if (pk || fk) {
      data.push({
        fileName: tableName,
        primaryKey: pk || null,
        foreignKey: fk || null,
        foreignKeyTable: fkTable || null,
        foreignKeyColumn: fkColumn || null,
      })
    }
  })
  return JSON.stringify(data)
}
