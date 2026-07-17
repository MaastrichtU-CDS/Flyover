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

export function graphDatabaseFindNameMatch(mapDbName, targetDb) {
  if (!mapDbName || mapDbName === '') return true
  if (mapDbName === targetDb) return true
  const a = mapDbName.endsWith('.csv') ? mapDbName.slice(0, -4) : mapDbName
  const b = targetDb?.endsWith('.csv') ? targetDb.slice(0, -4) : targetDb
  return a === b
}

export function findColumnForVariable(
  databases,
  globalVarName,
  localVariable = null,
  matchingDatabase = null
) {
  let found = null
  forEachColumn(databases, (colData, colKey, tableData, tableKey, dbData, dbKey) => {
    if (
      matchingDatabase &&
      !graphDatabaseFindNameMatch(dbData.name, matchingDatabase) &&
      !graphDatabaseFindNameMatch(tableData.sourceFile, matchingDatabase)
    ) {
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
  const termKey = selectedOption ? formatToSnakeCase(selectedOption) : null
  const previousTermKey = previousOption ? formatToSnakeCase(previousOption) : null

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

function ensureDatabaseExists(databaseName) {
  if (!mapping.databases) mapping.databases = {}
  const dbKey = formatToSnakeCase(databaseName)
  let exists = false
  for (const [, dbData] of Object.entries(mapping.databases)) {
    if (graphDatabaseFindNameMatch(dbData.name, databaseName)) {
      exists = true
      break
    }
  }
  if (!exists) {
    const tableKey = 'data'
    mapping.databases[dbKey] = {
      '@id': `mapping:database/${dbKey}`,
      '@type': 'mapping:Database',
      name: databaseName,
      description: '',
      tables: {
        [tableKey]: {
          '@id': `mapping:table/${dbKey}/${tableKey}`,
          '@type': 'mapping:Table',
          sourceFile: databaseName,
          description: '',
          columns: {},
        },
      },
    }
  }
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
            varName: formatToSnakeCase(descriptionDisplay),
            descriptionDisplay,
            dataType: data.datatype || null,
          }
        : null
    )
  }

  // Pass 1 — sweep stale columns. For each (db, localColumn) covered by the
  // form, delete any column whose variable no longer matches the form's
  // current choice (or any column at all when the form's choice is empty).
  for (const [databaseName, localColumns] of Object.entries(targetsByDb)) {
    forEachColumn(
      mapping.databases || {},
      (colData, colKey, tableData, _tableKey, dbData) => {
        if (
          !graphDatabaseFindNameMatch(dbData.name, databaseName) &&
          !graphDatabaseFindNameMatch(tableData.sourceFile, databaseName)
        )
          return
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
        (colData, _colKey, tableData, _tableKey, dbData) => {
          if (columnFound) return false
          if (
            !graphDatabaseFindNameMatch(dbData.name, databaseName) &&
            !graphDatabaseFindNameMatch(tableData.sourceFile, databaseName)
          )
            return
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
        let dbFound = false
        forEachColumn(
          mapping.databases || {},
          (_colData, _colKey, tableData, _tableKey, dbData) => {
            if (dbFound) return false
            if (
              !graphDatabaseFindNameMatch(dbData.name, databaseName) &&
              !graphDatabaseFindNameMatch(tableData.sourceFile, databaseName)
            )
              return
            dbFound = true
            tableData.columns[varName] = {
              mapsTo: `schema:variable/${varName}`,
              variable: varName,
              localColumn: localColumnName,
            }
            return false
          }
        )
        if (!dbFound) {
          ensureDatabaseExists(databaseName)
          const dbKey = formatToSnakeCase(databaseName)
          const tk = Object.keys(mapping.databases[dbKey].tables)[0]
          mapping.databases[dbKey].tables[tk].columns[varName] = {
            mapsTo: `schema:variable/${varName}`,
            variable: varName,
            localColumn: localColumnName,
          }
        }
      }
    }
  }

  // Pass 3 — GC orphaned schema.variables. A variable with no column
  // referencing it can never produce data, so it should not linger in IDB
  // and reappear on the next mount.
  if (mapping.schema?.variables) {
    const referenced = new Set()
    forEachColumn(mapping.databases || {}, (colData) => {
      const k = getVariableKeyFromColumn(colData)
      if (k) referenced.add(k)
    })
    for (const varName of Object.keys(mapping.schema.variables)) {
      if (!referenced.has(varName)) delete mapping.schema.variables[varName]
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

    forEachColumn(mapping.databases || {}, (colData, _colKey, tableData, _tableKey, dbData) => {
      const colVarKey = getVariableKeyFromColumn(colData)
      if (colVarKey !== varName) return
      const localCol = colData.localColumn
      if (!localCol) return
      const sourceFile = tableData.sourceFile
      const dbName = dbData.name
      const matchingDb = databases.find(
        (d) =>
          graphDatabaseFindNameMatch(sourceFile, d) ||
          graphDatabaseFindNameMatch(dbName, d)
      )
      if (matchingDb) {
        const key = `${matchingDb}_${localCol}`
        preselectedDescriptions[key] = descriptionDisplay
        if (datatypeDisplay) preselectedDatatypes[key] = datatypeDisplay
      }
    })
  }
  return { preselectedDescriptions, preselectedDatatypes, descriptionToDatatype }
}
