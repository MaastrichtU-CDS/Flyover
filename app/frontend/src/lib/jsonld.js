// JSON-LD semantic-mapping helpers. Ported from static/js/jsonld-mapper.js,
// dropping the jQuery DOM helpers — those move into the SFCs that use them.

import * as db from './db'

let mapping = null

export function getMapping() {
  return mapping
}

export function setMapping(m) {
  mapping = m
}

export function formatToTitleCase(str) {
  if (str == null) return ''
  return String(str).charAt(0).toUpperCase() + String(str).slice(1).replace(/_/g, ' ')
}

export function formatToSnakeCase(str) {
  if (str == null) return ''
  return String(str).toLowerCase().replace(/\s+/g, '_')
}

export function formatDataTypeDisplay(t) {
  return t == null ? '' : String(t).toLowerCase()
}

export function getVariableKeyFromColumn(colData) {
  return colData?.mapsTo?.split('/').pop()
}

export function normalizeLocalMappings(localMappings) {
  for (const [key, value] of Object.entries(localMappings)) {
    if (!Array.isArray(value)) {
      localMappings[key] = value != null ? [value] : []
    }
  }
  return localMappings
}

export async function saveMapping(data) {
  try {
    await db.saveData('metadata', {
      key: 'semantic_map',
      data: data || mapping,
      timestamp: new Date().toISOString(),
    })
    return true
  } catch {
    return false
  }
}

export function getUniqueVariableName(baseName, usedVariables) {
  if (!(baseName in usedVariables)) {
    usedVariables[baseName] = 0
    return baseName
  }
  const suffix = usedVariables[baseName] + 1
  usedVariables[baseName] = suffix
  return `${baseName}_${suffix}`
}

export function forEachColumn(databases, callback) {
  for (const [dbKey, dbData] of Object.entries(databases || {})) {
    if (!dbData?.tables) continue
    for (const [tableKey, tableData] of Object.entries(dbData.tables)) {
      if (!tableData?.columns) continue
      for (const [colKey, colData] of Object.entries(tableData.columns)) {
        const cont = callback(colData, colKey, tableData, tableKey, dbData, dbKey)
        if (cont === false) return
      }
    }
  }
}

function withoutCsv(name) {
  return name.endsWith('.csv') ? name.slice(0, -4) : name
}

// Whether a name in the map and an ingested table's name are the same table,
// with or without a .csv extension. An empty name names no table.
export function namesMatch(mapName, storeTable) {
  if (!mapName || !storeTable) return false
  return withoutCsv(String(mapName)) === withoutCsv(String(storeTable))
}

// The ingested table a map table describes: its sourceFile, else its key.
export function mapTableName(tableKey, tableData) {
  return tableData?.sourceFile || tableKey || ''
}

// Whether a map table describes the ingested table `storeTable`. Only the
// table's own name decides, as on the annotate pages: a database's `name` is
// a label (it may read 'Centre B Electronic Health Records'), so a database
// named after one table never takes in another table's columns.
export function mapTableMatches(tableKey, tableData, storeTable) {
  return namesMatch(mapTableName(tableKey, tableData), storeTable)
}

// The ingested table, among `storeTables`, that a map table name refers to.
export function matchStoreTable(mapName, storeTables) {
  return (storeTables || []).find((table) => namesMatch(mapName, table)) || null
}

// How a map's data sources line up with the datasets in the store:
// `matched` are the store's datasets the map describes, `missing` the map's
// tables the store lacks (by the map's name), and `others` the store's
// datasets the map does not name. Each name is listed once.
export function compareWithStore(map, storeTables) {
  const tables = storeTables || []
  const matched = []
  const missing = []
  for (const dbData of Object.values(map?.databases || {})) {
    for (const [tableKey, tableData] of Object.entries(dbData?.tables || {})) {
      const name = mapTableName(tableKey, tableData)
      const match = matchStoreTable(name, tables)
      if (match) {
        if (!matched.includes(match)) matched.push(match)
      } else if (name && !missing.includes(name)) {
        missing.push(name)
      }
    }
  }
  const others = tables.filter((table) => !matched.includes(table))
  return { matched, missing, others }
}

export function findColumnForVariable(
  databases,
  globalVarName,
  localVariable = null,
  matchingDatabase = null
) {
  let found = null
  forEachColumn(databases, (colData, colKey, tableData, tableKey, dbData, dbKey) => {
    if (matchingDatabase && !mapTableMatches(tableKey, tableData, matchingDatabase)) {
      return
    }
    const varKey = getVariableKeyFromColumn(colData)
    if (varKey === globalVarName) {
      if (localVariable) {
        if (colData.localColumn === localVariable) {
          found = { colData, colKey, tableData, tableKey, dbData, dbKey }
          return false
        }
      } else {
        found = { colData, colKey, tableData, tableKey, dbData, dbKey }
        return false
      }
    }
  })
  return found
}

export async function loadFromIndexedDB() {
  const result = await db.getData('metadata', 'semantic_map')
  if (result?.data) {
    mapping = result.data
    return true
  }
  return false
}

export function initializeEmptyMapping() {
  mapping = {
    '@context': {
      schema: 'mapping:schema/',
      mapping: 'http://example.org/mapping#',
    },
    '@id': 'mapping:root',
    '@type': 'mapping:SemanticMapping',
    schema: {
      '@id': 'schema:root',
      '@type': 'mapping:Schema',
      variables: {},
    },
    databases: {},
  }
}

export function getAllVariableKeys() {
  return mapping?.schema?.variables ? Object.keys(mapping.schema.variables) : []
}

export function getVariable(varKey) {
  return mapping?.schema?.variables?.[varKey] || null
}

export function getGlobalVariableNames() {
  if (!mapping?.schema?.variables) return ['Other']
  return getAllVariableKeys().map(formatToTitleCase).concat(['Other'])
}

// The describe pages show a variable or value-mapping term by its label,
// formatToTitleCase(key): 'eortc_qlq_c30_overall_qol_QL2' reads 'Eortc qlq
// c30 overall qol QL2'. Lower-casing a label does not give the key back, so
// a label is resolved against the keys it can stand for: the key itself, the
// key it is the label of, then the key it matches ignoring case and spacing
// (the backend titles labels word by word). Only a label no key answers to is
// snake-cased.
function keyForLabel(label, keys) {
  if (label == null) return ''
  const text = String(label)
  if (keys.includes(text)) return text
  const labelled = keys.find((key) => formatToTitleCase(key) === text)
  if (labelled !== undefined) return labelled
  const loose = formatToSnakeCase(text)
  const matched = keys.find((key) => formatToSnakeCase(key) === loose)
  return matched !== undefined ? matched : loose
}

/** The schema variable a description label stands for. */
export function variableKeyForLabel(label) {
  return keyForLabel(label, getAllVariableKeys())
}

/** The value-mapping term of `varKey` that an option label stands for. */
export function termKeyForLabel(varKey, label) {
  const terms = getVariable(varKey)?.valueMapping?.terms
  return keyForLabel(label, terms ? Object.keys(terms) : [])
}

export function getCategoryOptionsForVariable(_database, globalVarName, localVariable = null) {
  if (!mapping?.schema?.variables) return []
  let varInfo = mapping.schema.variables[globalVarName]
  if (!varInfo && localVariable) {
    varInfo = mapping.schema.variables[localVariable]
  }
  if (!varInfo) {
    const normalized = globalVarName.toLowerCase().replace(/\s+/g, '_')
    for (const [k, v] of Object.entries(mapping.schema.variables)) {
      if (k.toLowerCase().replace(/\s+/g, '_') === normalized) {
        varInfo = v
        break
      }
    }
    // Also try localVariable normalized
    if (!varInfo && localVariable) {
      const normalizedLocal = localVariable.toLowerCase().replace(/\s+/g, '_')
      for (const [k, v] of Object.entries(mapping.schema.variables)) {
        if (k.toLowerCase().replace(/\s+/g, '_') === normalizedLocal) {
          varInfo = v
          break
        }
      }
    }
  }
  // Last resort: find the schema variable by looking up which column in the
  // databases section has localColumn === localVariable (or _database + localVariable).
  // This handles the "Missing Description" case where globalVarName is wrong but
  // localVariable is the actual DB column name that links to a schema variable.
  if (!varInfo && localVariable && mapping.databases) {
    forEachColumn(mapping.databases, (colData) => {
      if (colData.localColumn === localVariable) {
        const varKey = getVariableKeyFromColumn(colData)
        if (varKey && mapping.schema?.variables?.[varKey]) {
          varInfo = mapping.schema.variables[varKey]
          return false
        }
      }
    })
  }
  if (!varInfo?.valueMapping?.terms) return []
  return Object.keys(varInfo.valueMapping.terms).map(formatToTitleCase)
}

export function getLocalMappingsForVariable(database, localVariable, globalVarName) {
  if (!mapping?.databases) return {}
  let result = findColumnForVariable(
    mapping.databases,
    globalVarName,
    localVariable,
    database
  )
  if (!result) {
    result = findColumnForVariable(mapping.databases, globalVarName, null, database)
  }
  if (!result) {
    result = findColumnForVariable(mapping.databases, globalVarName, null, null)
  }
  if (result) {
    const localMappings = result.colData.localMappings || {}
    return normalizeLocalMappings(localMappings)
  }
  return {}
}

export async function updateCategoryMapping(
  database,
  localVariable,
  globalVarName,
  localValue,
  selectedOption,
  previousOption
) {
  if (!mapping?.databases) return false
  localValue = String(localValue)
  globalVarName = variableKeyForLabel(globalVarName)
  const termKey = selectedOption ? termKeyForLabel(globalVarName, selectedOption) : null
  const previousTermKey = previousOption
    ? termKeyForLabel(globalVarName, previousOption)
    : null

  const result = findColumnForVariable(
    mapping.databases,
    globalVarName,
    localVariable,
    database
  )
  if (!result) return false
  const colData = result.colData
  if (!colData.localMappings) colData.localMappings = {}

  if (
    previousTermKey &&
    colData.localMappings[previousTermKey] != null
  ) {
    if (!Array.isArray(colData.localMappings[previousTermKey])) {
      colData.localMappings[previousTermKey] = [colData.localMappings[previousTermKey]]
    }
    const i = colData.localMappings[previousTermKey].indexOf(localValue)
    if (i > -1) {
      colData.localMappings[previousTermKey].splice(i, 1)
      if (!colData.localMappings[previousTermKey].length) {
        delete colData.localMappings[previousTermKey]
      }
    }
  }

  // "Other" is the catch-all option for values the schema doesn't define a
  // term for; the user's free-text override is captured separately as a
  // comment in descriptive_info. Skip the write so we don't create a bogus
  // "other" localMappings key that the schema validator would reject.
  if (termKey && selectedOption !== 'Other') {
    if (!colData.localMappings[termKey]) colData.localMappings[termKey] = []
    else if (!Array.isArray(colData.localMappings[termKey])) {
      colData.localMappings[termKey] = [colData.localMappings[termKey]]
    }
    if (!colData.localMappings[termKey].includes(localValue)) {
      colData.localMappings[termKey].push(localValue)
    }
  }

  return await saveMapping()
}

// The map table that holds an ingested table's columns: the first map table
// describing it, or else a new database entry made for it, under a key no
// other entry has (an unrelated entry may share the snake-cased name).
function ensureMapTable(storeTable) {
  if (!mapping.databases) mapping.databases = {}
  for (const dbData of Object.values(mapping.databases)) {
    for (const [tableKey, tableData] of Object.entries(dbData?.tables || {})) {
      if (tableData && mapTableMatches(tableKey, tableData, storeTable)) {
        if (!tableData.columns) tableData.columns = {}
        return tableData
      }
    }
  }
  const baseKey = formatToSnakeCase(storeTable)
  let dbKey = baseKey
  for (let n = 2; mapping.databases[dbKey]; n++) dbKey = `${baseKey}_${n}`
  const tableKey = 'data'
  mapping.databases[dbKey] = {
    '@id': `mapping:database/${dbKey}`,
    '@type': 'mapping:Database',
    name: storeTable,
    description: '',
    tables: {
      [tableKey]: {
        '@id': `mapping:table/${dbKey}/${tableKey}`,
        '@type': 'mapping:Table',
        sourceFile: storeTable,
        description: '',
        columns: {},
      },
    },
  }
  return mapping.databases[dbKey].tables[tableKey]
}

// Deterministic projection from the describe-variables form state onto the
// JSON-LD mapping. The form is the source of truth: every entry in `formData`
// is either upserted (description selected) or removed (description empty or
// "Other"). Without the remove path, deselecting a value left a stale column
// behind in IndexedDB — that ghost data then reappeared on re-mount.
//
// We only touch columns whose localColumn appears in `formData`, so entries
// the form doesn't know about (e.g. another database that wasn't on screen)
// are left alone. After applying, orphaned schema.variables are GCed so the
// mapping stays a clean reflection of what columns currently reference.
export async function updateMappingFromForm(formData) {
  if (!mapping) initializeEmptyMapping()

  // Group form entries by database so we can decide per (db, localColumn)
  // whether the entry is "active" (description selected) or a tombstone.
  const targetsByDb = {}
  // Snapshot of variables referenced by columns in the databases covered by
  // this form update, captured BEFORE Pass 1 removes anything. Pass 3 only
  // GCs variables that appear in this snapshot but are no longer referenced
  // after the update — this prevents the GC from wiping schema variables that
  // were defined in an uploaded (non-prefilled) JSON-LD but haven't been
  // assigned to any column yet.
  const prevReferencedByFormDbs = new Set()
  for (const [key, data] of Object.entries(formData)) {
    const databaseName = data?.database
    if (!databaseName) continue
    const prefix = databaseName + '_'
    const localColumnName = key.startsWith(prefix)
      ? key.substring(prefix.length)
      : key
    const descriptionDisplay = data.description
    const isActive = descriptionDisplay && descriptionDisplay !== 'Other'
    if (!targetsByDb[databaseName]) targetsByDb[databaseName] = new Map()
    targetsByDb[databaseName].set(
      localColumnName,
      isActive
        ? {
            varName: variableKeyForLabel(descriptionDisplay),
            descriptionDisplay,
            dataType: data.datatype || null,
          }
        : null
    )
  }

  // Populate prevReferencedByFormDbs: collect all variables that are
  // currently referenced by columns in the databases covered by this form
  // update, BEFORE Pass 1 removes any columns.
  forEachColumn(mapping.databases || {}, (colData, _colKey, tableData, tableKey) => {
    for (const databaseName of Object.keys(targetsByDb)) {
      if (mapTableMatches(tableKey, tableData, databaseName)) {
        const k = getVariableKeyFromColumn(colData)
        if (k) prevReferencedByFormDbs.add(k)
        break
      }
    }
  })

  // Pass 1 — sweep stale columns. For each (db, localColumn) covered by the
  // form, delete any column whose variable no longer matches the form's
  // current choice (or any column at all when the form's choice is empty).
  for (const [databaseName, localColumns] of Object.entries(targetsByDb)) {
    forEachColumn(
      mapping.databases || {},
      (colData, colKey, tableData, tableKey) => {
        if (!mapTableMatches(tableKey, tableData, databaseName)) return
        const localCol = colData.localColumn
        if (localCol == null || !localColumns.has(localCol)) return
        const target = localColumns.get(localCol)
        const colVarKey = getVariableKeyFromColumn(colData)
        if (target == null) {
          delete tableData.columns[colKey]
          return
        }
        if (
          colVarKey !== target.varName &&
          colData.mapsTo !== `schema:variable/${target.varName}`
        ) {
          delete tableData.columns[colKey]
        }
      }
    )
  }

  // Pass 2 — upsert the active entries.
  for (const [databaseName, localColumns] of Object.entries(targetsByDb)) {
    for (const [localColumnName, target] of localColumns) {
      if (!target) continue
      const { varName, dataType } = target

      // Only touch schema.variables for variables the uploaded/global schema
      // already defines. A fully-specified SchemaVariable requires @type,
      // dataType, predicate and class; we cannot synthesise those from the
      // describe form, so writing a {name, description, dataType} stub would
      // produce a schema-invalid mapping (the "@type/predicate/class is a
      // required property" submit error). When the user maps a column to a
      // variable that does not exist in the schema yet, we therefore only
      // create the entry in the databases → columns section (mirroring how
      // localColumn / localMappings live there), and leave the schema alone.
      if (mapping.schema?.variables?.[varName] && dataType) {
        mapping.schema.variables[varName].dataType = dataType
      }

      let columnFound = false
      forEachColumn(
        mapping.databases || {},
        (colData, _colKey, tableData, tableKey) => {
          if (columnFound) return false
          if (!mapTableMatches(tableKey, tableData, databaseName)) return
          const colVarKey = getVariableKeyFromColumn(colData)
          if (
            colVarKey === varName ||
            colData.mapsTo === `schema:variable/${varName}`
          ) {
            colData.localColumn = localColumnName
            columnFound = true
            return false
          }
        }
      )

      if (!columnFound) {
        // A new column, in the table's own map table. Its key is free-form, so
        // a column for another variable may already hold this one's name.
        const columns = ensureMapTable(databaseName).columns
        let colKey = varName
        for (let n = 2; columns[colKey]; n++) colKey = `${varName}_${n}`
        columns[colKey] = {
          mapsTo: `schema:variable/${varName}`,
          variable: varName,
          localColumn: localColumnName,
        }
      }
    }
  }

  // Pass 3 — GC schema.variables that have become orphaned by this update.
  // Only variables that were previously referenced by a column in one of the
  // databases covered by this form update (prevReferencedByFormDbs) and are
  // no longer referenced anywhere are deleted. Variables that were never
  // assigned to any column (e.g. from a non-prefilled uploaded JSON-LD) are
  // preserved so their valueMapping.terms remain available in the details view.
  if (mapping.schema?.variables) {
    const referenced = new Set()
    forEachColumn(mapping.databases || {}, (colData) => {
      const k = getVariableKeyFromColumn(colData)
      if (k) referenced.add(k)
    })
    for (const varName of Object.keys(mapping.schema.variables)) {
      if (!referenced.has(varName) && prevReferencedByFormDbs.has(varName)) {
        delete mapping.schema.variables[varName]
      }
    }
  }

  return await saveMapping()
}

export function computePreselectionsForDatabases(databases) {
  // Pure version: caller provides the displayed database names instead of
  // scraping them from the DOM.
  const preselectedDescriptions = {}
  const preselectedDatatypes = {}
  const descriptionToDatatype = {}
  if (!mapping) {
    return { preselectedDescriptions, preselectedDatatypes, descriptionToDatatype }
  }

  for (const varName of getAllVariableKeys()) {
    const varInfo = getVariable(varName)
    if (!varInfo) continue
    const descriptionDisplay = formatToTitleCase(varName)
    const datatypeDisplay = varInfo.dataType
      ? formatDataTypeDisplay(varInfo.dataType)
      : null
    if (datatypeDisplay) descriptionToDatatype[descriptionDisplay] = datatypeDisplay

    forEachColumn(mapping.databases || {}, (colData, _colKey, tableData, tableKey) => {
      const colVarKey = getVariableKeyFromColumn(colData)
      if (colVarKey !== varName) return
      const localCol = colData.localColumn
      if (!localCol) return
      const matchingDb = matchStoreTable(mapTableName(tableKey, tableData), databases)
      if (matchingDb) {
        const key = `${matchingDb}_${localCol}`
        preselectedDescriptions[key] = descriptionDisplay
        if (datatypeDisplay) preselectedDatatypes[key] = datatypeDisplay
      }
    })
  }
  return { preselectedDescriptions, preselectedDatatypes, descriptionToDatatype }
}
