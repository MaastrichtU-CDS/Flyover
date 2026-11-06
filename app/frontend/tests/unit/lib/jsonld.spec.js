import { describe, it, expect, beforeEach, vi } from 'vitest'

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

import {
  formatToTitleCase,
  formatToSnakeCase,
  namesMatch,
  mapTableMatches,
  matchStoreTable,
  computePreselectionsForDatabases,
  getVariableKeyFromColumn,
  normalizeLocalMappings,
  forEachColumn,
  findColumnForVariable,
  setMapping,
  getMapping,
  getCategoryOptionsForVariable,
  updateCategoryMapping,
  updateMappingFromForm,
  initializeEmptyMapping,
  variableKeyForLabel,
  termKeyForLabel,
} from '@/lib/jsonld.js'

describe('formatToTitleCase', () => {
  it('uppercases the first letter and replaces underscores with spaces', () => {
    expect(formatToTitleCase('tumor_locatie')).toBe('Tumor locatie')
    expect(formatToTitleCase('age')).toBe('Age')
  })

  it('handles empty/null inputs without throwing', () => {
    expect(formatToTitleCase('')).toBe('')
    expect(formatToTitleCase(null)).toBe('')
    expect(formatToTitleCase(undefined)).toBe('')
  })
})

describe('formatToSnakeCase', () => {
  it('lowercases and replaces whitespace with underscores', () => {
    expect(formatToSnakeCase('Tumor Location')).toBe('tumor_location')
    expect(formatToSnakeCase('Age')).toBe('age')
    expect(formatToSnakeCase('  multiple   spaces  ')).toBe('_multiple_spaces_')
  })

  it('handles non-strings safely', () => {
    expect(formatToSnakeCase(null)).toBe('')
    expect(formatToSnakeCase(42)).toBe('42')
  })
})

describe('namesMatch', () => {
  it('matches identical names', () => {
    expect(namesMatch('foo', 'foo')).toBe(true)
  })

  it('matches with .csv stripped on either side', () => {
    expect(namesMatch('foo.csv', 'foo')).toBe(true)
    expect(namesMatch('foo', 'foo.csv')).toBe(true)
  })

  it('never matches an empty name, on either side', () => {
    expect(namesMatch('', 'whatever')).toBe(false)
    expect(namesMatch(null, 'whatever')).toBe(false)
    expect(namesMatch(undefined, 'whatever')).toBe(false)
    expect(namesMatch('foo', '')).toBe(false)
  })

  it('rejects unrelated names', () => {
    expect(namesMatch('foo', 'bar')).toBe(false)
  })
})

describe('mapTableMatches and matchStoreTable', () => {
  it('matches a map table on its sourceFile, else on its key', () => {
    expect(mapTableMatches('t', { sourceFile: 'patients.csv' }, 'patients')).toBe(true)
    expect(mapTableMatches('patients', {}, 'patients')).toBe(true)
    expect(mapTableMatches('t', { sourceFile: 'patients.csv' }, 't')).toBe(false)
  })

  it('finds the ingested table a map table name refers to', () => {
    expect(matchStoreTable('visits.csv', ['patients', 'visits'])).toBe('visits')
    expect(matchStoreTable('', ['patients', 'visits'])).toBeNull()
    expect(matchStoreTable('labs', ['patients', 'visits'])).toBeNull()
  })
})

describe('getVariableKeyFromColumn', () => {
  it('extracts the trailing segment of mapsTo', () => {
    expect(getVariableKeyFromColumn({ mapsTo: 'schema:variable/age' })).toBe('age')
  })

  it('returns undefined for missing mapsTo', () => {
    expect(getVariableKeyFromColumn({})).toBeUndefined()
    expect(getVariableKeyFromColumn({ mapsTo: null })).toBeUndefined()
  })
})

describe('normalizeLocalMappings', () => {
  it('wraps scalars in arrays', () => {
    const m = { yes: 'Y', no: 'N' }
    normalizeLocalMappings(m)
    expect(m.yes).toEqual(['Y'])
    expect(m.no).toEqual(['N'])
  })

  it('replaces null/undefined with empty arrays', () => {
    const m = { missing: null, undef: undefined }
    normalizeLocalMappings(m)
    expect(m.missing).toEqual([])
    expect(m.undef).toEqual([])
  })

  it('leaves existing arrays alone', () => {
    const m = { multi: ['a', 'b'] }
    normalizeLocalMappings(m)
    expect(m.multi).toEqual(['a', 'b'])
  })
})

describe('forEachColumn', () => {
  const fixture = {
    db1: {
      tables: {
        t1: {
          columns: {
            c1: { mapsTo: 'schema:variable/age', localColumn: 'age_yrs' },
            c2: { mapsTo: 'schema:variable/sex', localColumn: 'sex' },
          },
        },
      },
    },
  }

  it('iterates over every column', () => {
    const seen = []
    forEachColumn(fixture, (col) => {
      seen.push(col.localColumn)
    })
    expect(seen).toEqual(['age_yrs', 'sex'])
  })

  it('aborts early when callback returns false', () => {
    const seen = []
    forEachColumn(fixture, (col) => {
      seen.push(col.localColumn)
      return false
    })
    expect(seen).toEqual(['age_yrs'])
  })

  it('handles missing tables/columns without throwing', () => {
    expect(() => forEachColumn({ x: {} }, () => {})).not.toThrow()
    expect(() => forEachColumn({ x: { tables: { y: {} } } }, () => {})).not.toThrow()
  })
})

describe('findColumnForVariable', () => {
  const databases = {
    db1: {
      name: 'data.csv',
      tables: {
        t1: {
          sourceFile: 'data.csv',
          columns: {
            c1: { mapsTo: 'schema:variable/age', localColumn: 'age_yrs' },
            c2: { mapsTo: 'schema:variable/sex', localColumn: 'sex' },
          },
        },
      },
    },
  }

  it('finds a column by global variable name', () => {
    const found = findColumnForVariable(databases, 'age')
    expect(found?.colData?.localColumn).toBe('age_yrs')
  })

  it('filters by localColumn when provided', () => {
    const found = findColumnForVariable(databases, 'age', 'age_yrs')
    expect(found?.colData?.localColumn).toBe('age_yrs')
    expect(findColumnForVariable(databases, 'age', 'something_else')).toBeNull()
  })

  it('filters by matchingDatabase using csv-equivalence', () => {
    expect(findColumnForVariable(databases, 'age', null, 'data')?.colData).toBeTruthy()
    expect(findColumnForVariable(databases, 'age', null, 'unrelated')).toBeNull()
  })

  it("matches a table by its own name, never by its database's name alone", () => {
    const named = {
      db1: {
        name: 'visits',
        tables: {
          t1: {
            sourceFile: 'patients.csv',
            columns: { c1: { mapsTo: 'schema:variable/age', localColumn: 'age_yrs' } },
          },
        },
      },
    }
    expect(findColumnForVariable(named, 'age', null, 'patients')?.colData).toBeTruthy()
    expect(findColumnForVariable(named, 'age', null, 'visits')).toBeNull()
  })
})

describe('updateCategoryMapping', () => {
  function freshMapping() {
    return {
      databases: {
        db1: {
          name: 'data.csv',
          tables: {
            t1: {
              sourceFile: 'data.csv',
              columns: {
                sex: {
                  mapsTo: 'schema:variable/sex',
                  localColumn: 'sex',
                  localMappings: {},
                },
              },
            },
          },
        },
      },
    }
  }

  beforeEach(() => {
    setMapping(freshMapping())
  })

  it('writes the new term key for a valid selection', async () => {
    await updateCategoryMapping('data', 'sex', 'sex', 'M', 'Male', null)
    const cols = getMapping().databases.db1.tables.t1.columns.sex
    expect(cols.localMappings.male).toEqual(['M'])
  })

  it('does not write "other" when selectedOption is "Other"', async () => {
    await updateCategoryMapping('data', 'sex', 'sex', 'unknown', 'Other', null)
    const cols = getMapping().databases.db1.tables.t1.columns.sex
    expect(cols.localMappings.other).toBeUndefined()
    expect(Object.keys(cols.localMappings)).toEqual([])
  })

  it('still strips the previous term when switching from a real term to "Other"', async () => {
    await updateCategoryMapping('data', 'sex', 'sex', 'M', 'Male', null)
    await updateCategoryMapping('data', 'sex', 'sex', 'M', 'Other', 'Male')
    const cols = getMapping().databases.db1.tables.t1.columns.sex
    expect(cols.localMappings.male).toBeUndefined()
    expect(cols.localMappings.other).toBeUndefined()
  })

  it('writes the new term when switching from "Other" to a real term', async () => {
    await updateCategoryMapping('data', 'sex', 'sex', 'M', 'Other', null)
    await updateCategoryMapping('data', 'sex', 'sex', 'M', 'Male', 'Other')
    const cols = getMapping().databases.db1.tables.t1.columns.sex
    expect(cols.localMappings.male).toEqual(['M'])
    expect(cols.localMappings.other).toBeUndefined()
  })

  it('removes the previous term when the user deselects (selectedOption empty)', async () => {
    await updateCategoryMapping('data', 'sex', 'sex', 'M', 'Male', null)
    await updateCategoryMapping('data', 'sex', 'sex', 'M', '', 'Male')
    const cols = getMapping().databases.db1.tables.t1.columns.sex
    expect(cols.localMappings.male).toBeUndefined()
  })
})

describe('updateMappingFromForm — deselection and tab-switching robustness', () => {
  beforeEach(() => {
    initializeEmptyMapping()
  })

  function formEntry(database, localColumn, description, datatype = '') {
    return {
      [`${database}_${localColumn}`]: {
        database,
        description,
        datatype,
        comment: '',
      },
    }
  }

  it('adds only a column (no schema stub) for a variable not in the schema', async () => {
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    const m = getMapping()
    // A variable that the uploaded schema does not define must NOT be written
    // into schema.variables: a SchemaVariable requires @type/predicate/class,
    // which the describe form cannot supply, so a stub would fail validation.
    expect(m.schema.variables.age).toBeUndefined()
    const cols = m.databases.patients.tables.data.columns
    expect(cols.age).toMatchObject({
      mapsTo: 'schema:variable/age',
      localColumn: 'age_yrs',
    })
  })

  it('updates dataType on an existing schema variable without re-creating it', async () => {
    setMapping({
      '@context': {},
      '@id': 'mapping:root',
      '@type': 'mapping:SemanticMapping',
      schema: {
        variables: {
          age: {
            '@id': 'schema:variable/age',
            '@type': 'schema:ContinuousVariable',
            dataType: 'continuous',
            predicate: 'sio:SIO_000008',
            class: 'ncit:C156420',
          },
        },
      },
      databases: {},
    })
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    const m = getMapping()
    // The pre-existing, fully-specified schema variable is preserved.
    expect(m.schema.variables.age).toMatchObject({
      '@type': 'schema:ContinuousVariable',
      predicate: 'sio:SIO_000008',
      class: 'ncit:C156420',
      dataType: 'continuous',
    })
    expect(m.databases.patients.tables.data.columns.age).toMatchObject({
      mapsTo: 'schema:variable/age',
      localColumn: 'age_yrs',
    })
  })

  it('removes the column from IDB when description is cleared (deselect)', async () => {
    // Initial select.
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    // User deselects — the form sends description='' for the same key.
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    const cols = m.databases.patients?.tables?.data?.columns || {}
    expect(cols.age).toBeUndefined()
    // And the now-orphaned schema variable is GCed.
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('removes the column when description is changed to "Other"', async () => {
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Other', '')
    )
    const m = getMapping()
    const cols = m.databases.patients?.tables?.data?.columns || {}
    expect(cols.age).toBeUndefined()
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('swaps a description (Age → Weight) without leaving the old column behind', async () => {
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Weight', 'continuous')
    )
    const m = getMapping()
    const cols = m.databases.patients.tables.data.columns
    // The new column exists with the same localColumn.
    expect(cols.weight).toMatchObject({
      mapsTo: 'schema:variable/weight',
      localColumn: 'age_yrs',
    })
    // The old column was removed.
    expect(cols.age).toBeUndefined()
    // Neither variable is defined in the schema — both live only as columns.
    expect(m.schema.variables.age).toBeUndefined()
    expect(m.schema.variables.weight).toBeUndefined()
  })

  it('does not touch columns from databases not present in the form payload', async () => {
    // Pre-seed with two databases worth of data.
    await updateMappingFromForm({
      ...formEntry('patients', 'age_yrs', 'Age', 'continuous'),
      ...formEntry('lab', 'h_g', 'Hemoglobin', 'continuous'),
    })
    // Only deselect in `patients`. `lab` is not present in this sync payload
    // (simulating partial form state after remount).
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    // `lab` survived untouched.
    expect(m.databases.lab.tables.data.columns.hemoglobin).toMatchObject({
      localColumn: 'h_g',
    })
    // `patients` got cleaned up.
    expect(m.databases.patients?.tables?.data?.columns?.age).toBeUndefined()
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('preserves an unrelated column in the same database', async () => {
    await updateMappingFromForm({
      ...formEntry('patients', 'age_yrs', 'Age', 'continuous'),
      ...formEntry('patients', 'sex', 'Biological Sex', 'categorical'),
    })
    // Now deselect just one field.
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    const cols = m.databases.patients.tables.data.columns
    expect(cols.age).toBeUndefined()
    expect(cols.biological_sex).toMatchObject({ localColumn: 'sex' })
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('GCs an existing schema variable once the form orphans it across syncs', async () => {
    // Seed a fully-specified schema variable (as an uploaded map would have).
    setMapping({
      '@context': {},
      '@id': 'mapping:root',
      '@type': 'mapping:SemanticMapping',
      schema: {
        variables: {
          age: {
            '@id': 'schema:variable/age',
            '@type': 'schema:ContinuousVariable',
            dataType: 'continuous',
            predicate: 'sio:SIO_000008',
            class: 'ncit:C156420',
          },
        },
      },
      databases: {},
    })
    // Two columns initially map to the same variable (Age) in different DBs.
    await updateMappingFromForm({
      ...formEntry('cohort_a', 'age_yrs', 'Age', 'continuous'),
      ...formEntry('cohort_b', 'leeftijd', 'Age', 'continuous'),
    })
    expect(getMapping().schema.variables.age).toBeDefined()
    // Deselect just cohort_a — variable still referenced by cohort_b.
    await updateMappingFromForm(formEntry('cohort_a', 'age_yrs', '', ''))
    expect(getMapping().schema.variables.age).toBeDefined()
    // Deselect cohort_b too — now nothing references Age → GC.
    await updateMappingFromForm(formEntry('cohort_b', 'leeftijd', '', ''))
    expect(getMapping().schema.variables.age).toBeUndefined()
  })

  it('reapplying the same description is idempotent and keeps state stable', async () => {
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    const beforeCols = JSON.stringify(getMapping().databases.patients.tables.data.columns)
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    const afterCols = JSON.stringify(getMapping().databases.patients.tables.data.columns)
    expect(afterCols).toBe(beforeCols)
  })

  it('discards a column whose mapping survived from a prior, now-stale IDB state', async () => {
    // Simulate the bug scenario: IDB already contains an Age mapping
    // (loaded from a previous session). The user navigates back to the
    // describe page and explicitly deselects Age. The form sync only
    // carries the one deselected entry.
    setMapping({
      '@context': {},
      '@id': 'mapping:root',
      '@type': 'mapping:SemanticMapping',
      schema: {
        variables: {
          age: { name: 'age', description: 'Age', dataType: 'continuous' },
        },
      },
      databases: {
        patients: {
          name: 'patients',
          tables: {
            data: {
              sourceFile: 'patients',
              columns: {
                age: {
                  mapsTo: 'schema:variable/age',
                  variable: 'age',
                  localColumn: 'age_yrs',
                },
              },
            },
          },
        },
      },
    })
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    expect(m.databases.patients.tables.data.columns.age).toBeUndefined()
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('preserves schema variables from a non-prefilled JSON-LD when only one column is mapped', async () => {
    // Simulate a non-prefilled JSON-LD: schema.variables populated but
    // databases is empty. When the user first maps ONE column on the Variables
    // page, the GC must NOT delete the other schema variables (issue-132).
    setMapping({
      '@context': {},
      '@id': 'mapping:root',
      '@type': 'mapping:SemanticMapping',
      schema: {
        variables: {
          biological_sex: {
            '@type': 'schema:CategoricalVariable',
            dataType: 'categorical',
            predicate: 'roo:P100018',
            class: 'ncit:C28421',
            valueMapping: { terms: { male: {}, female: {} } },
          },
          tumor_locatie: {
            '@type': 'schema:CategoricalVariable',
            dataType: 'categorical',
            predicate: 'roo:P100018',
            class: 'ncit:C3263',
            valueMapping: { terms: { left: {}, right: {} } },
          },
        },
      },
      databases: {},
    })
    // User maps only biological_sex on the first sync.
    await updateMappingFromForm(formEntry('patients', 'biologic_sex', 'Biological sex', 'categorical'))
    const m = getMapping()
    // biological_sex now has a column entry.
    expect(m.databases.patients.tables.data.columns.biological_sex).toMatchObject({
      localColumn: 'biologic_sex',
    })
    // tumor_locatie was never referenced — it must be PRESERVED (not GC'd).
    expect(m.schema.variables.tumor_locatie).toBeDefined()
    expect(m.schema.variables.biological_sex).toBeDefined()
  })
})

describe('getCategoryOptionsForVariable', () => {
  beforeEach(() => {
    setMapping({
      '@context': {},
      '@id': 'mapping:root',
      '@type': 'mapping:SemanticMapping',
      schema: {
        variables: {
          biological_sex: {
            '@type': 'schema:CategoricalVariable',
            dataType: 'categorical',
            valueMapping: { terms: { male: {}, female: {} } },
          },
          tumor_locatie: {
            '@type': 'schema:CategoricalVariable',
            dataType: 'categorical',
            valueMapping: { terms: { left: {}, right: {} } },
          },
        },
      },
      databases: {
        patients: {
          name: 'patients',
          tables: {
            data: {
              sourceFile: 'patients',
              columns: {
                biological_sex: {
                  mapsTo: 'schema:variable/biological_sex',
                  localColumn: 'biologic_sex',
                },
                tumor_locatie: {
                  mapsTo: 'schema:variable/tumor_locatie',
                  localColumn: 'tumor_loc',
                },
              },
            },
          },
        },
      },
    })
  })

  it('returns valueMapping terms for a variable matched by exact globalVarName', () => {
    const opts = getCategoryOptionsForVariable('patients', 'biological_sex')
    expect(opts).toEqual(['Male', 'Female'])
  })

  it('returns options for a second categorical variable (regression test for issue-132)', () => {
    const opts = getCategoryOptionsForVariable('patients', 'tumor_locatie')
    expect(opts).toEqual(['Left', 'Right'])
  })

  it('falls back to localVariable when globalVarName does not match any schema key', () => {
    // Simulates the "Missing Description" scenario where globalVarName is wrong
    // but localVariable is the actual DB column that maps to a schema variable.
    const opts = getCategoryOptionsForVariable('patients', 'missing_description', 'biologic_sex')
    expect(opts).toEqual(['Male', 'Female'])
  })

  it('falls back to database lookup via localColumn when both key lookups fail', () => {
    // globalVarName = 'missing_description' (wrong), localVariable = 'tumor_loc'
    // (matches localColumn in databases → tumor_locatie in schema).
    const opts = getCategoryOptionsForVariable('patients', 'missing_description', 'tumor_loc')
    expect(opts).toEqual(['Left', 'Right'])
  })

  it('returns empty array when no mapping is loaded', () => {
    setMapping(null)
    expect(getCategoryOptionsForVariable('patients', 'biological_sex')).toEqual([])
  })

  it('returns empty array when variable has no valueMapping', () => {
    setMapping({
      '@context': {},
      '@id': 'mapping:root',
      '@type': 'mapping:SemanticMapping',
      schema: { variables: { age: { dataType: 'continuous' } } },
      databases: {},
    })
    expect(getCategoryOptionsForVariable('patients', 'age')).toEqual([])
  })
})

// A map whose schema keys carry capitals, as the AYA map's EORTC scale scores
// do ('eortc_qlq_c30_overall_qol_QL2') and its ISCED education terms.
function capitalisedMapping() {
  return {
    schema: {
      variables: {
        eortc_qlq_c30_q16: { dataType: 'categorical' },
        eortc_qlq_c30_overall_qol_QL2: { dataType: 'continuous' },
        educational_level: {
          dataType: 'categorical',
          valueMapping: { terms: { 'ISCED-0': {}, 'ISCED-1': {} } },
        },
      },
    },
    databases: {},
  }
}

describe('variableKeyForLabel and termKeyForLabel', () => {
  beforeEach(() => {
    setMapping(capitalisedMapping())
  })

  it('gives back the key a dropdown label was made from, capitals and all', () => {
    expect(variableKeyForLabel('Eortc qlq c30 overall qol QL2')).toBe(
      'eortc_qlq_c30_overall_qol_QL2'
    )
    expect(variableKeyForLabel('Eortc qlq c30 q16')).toBe('eortc_qlq_c30_q16')
  })

  it('accepts the key itself and a label titled word by word', () => {
    expect(variableKeyForLabel('eortc_qlq_c30_overall_qol_QL2')).toBe(
      'eortc_qlq_c30_overall_qol_QL2'
    )
    // The backend builds its labels with str.title().
    expect(variableKeyForLabel('Eortc Qlq C30 Overall Qol Ql2')).toBe(
      'eortc_qlq_c30_overall_qol_QL2'
    )
  })

  it('snake-cases a label no schema variable answers to', () => {
    expect(variableKeyForLabel('Body Weight')).toBe('body_weight')
    expect(variableKeyForLabel(null)).toBe('')
  })

  it('resolves a value-mapping term within its variable', () => {
    expect(termKeyForLabel('educational_level', 'ISCED-1')).toBe('ISCED-1')
    expect(termKeyForLabel('educational_level', 'Isced-1')).toBe('ISCED-1')
    expect(termKeyForLabel('educational_level', 'Other level')).toBe('other_level')
    expect(termKeyForLabel('not_a_variable', 'Male')).toBe('male')
  })
})

describe('describe answers keep capitalised schema keys', () => {
  beforeEach(() => {
    setMapping(capitalisedMapping())
  })

  it('maps a column to the capitalised variable and updates that variable', async () => {
    await updateMappingFromForm({
      survaya_ql: {
        database: 'survaya',
        description: 'Eortc qlq c30 overall qol QL2',
        datatype: 'continuous',
        comment: '',
      },
    })
    const m = getMapping()
    const columns = Object.values(m.databases.survaya.tables.data.columns)
    expect(columns).toEqual([
      expect.objectContaining({
        mapsTo: 'schema:variable/eortc_qlq_c30_overall_qol_QL2',
        localColumn: 'ql',
      }),
    ])
    // No lower-cased twin appears next to the schema's own variable.
    expect(Object.keys(m.schema.variables)).not.toContain('eortc_qlq_c30_overall_qol_ql2')
  })

  it('records a value under the capitalised term of the capitalised column', async () => {
    await updateMappingFromForm({
      cohort_opl: {
        database: 'cohort',
        description: 'Educational level',
        datatype: 'categorical',
        comment: '',
      },
    })
    await updateCategoryMapping('cohort', 'opl', 'educational_level', '3', 'ISCED-1', null)
    const column = getMapping().databases.cohort.tables.data.columns.educational_level
    expect(column.localMappings).toEqual({ 'ISCED-1': ['3'] })

    await updateCategoryMapping('cohort', 'opl', 'educational_level', '3', 'ISCED-0', 'ISCED-1')
    expect(column.localMappings).toEqual({ 'ISCED-0': ['3'] })
  })
})

describe('describing several tables', () => {
  function answer(database, localColumn, description, datatype = '') {
    return {
      [`${database}_${localColumn}`]: { database, description, datatype, comment: '' },
    }
  }

  // A map for another site: one database whose table has neither a sourceFile
  // nor a key naming an ingested table, and a database with no name at all.
  function otherSiteMapping() {
    return {
      schema: {
        variables: {
          biological_sex: { dataType: 'categorical' },
          age_at_diagnosis: { dataType: 'continuous' },
        },
      },
      databases: {
        msci: {
          name: 'MSCI_prospective_data',
          tables: {
            MSCI_prospective: {
              sourceFile: 'MSCI_prospective_batch_1',
              columns: {
                sex: { mapsTo: 'schema:variable/biological_sex', localColumn: 'geslacht' },
              },
            },
          },
        },
        template: {
          tables: {
            template_table: {
              columns: {
                age: { mapsTo: 'schema:variable/age_at_diagnosis', localColumn: 'leeftijd' },
              },
            },
          },
        },
      },
    }
  }

  function localColumns(table) {
    return Object.fromEntries(
      Object.values(table.columns).map((c) => [c.localColumn, c.mapsTo])
    )
  }

  beforeEach(() => {
    setMapping(otherSiteMapping())
  })

  it("gives each described table its own entry and leaves the map's other tables alone", async () => {
    await updateMappingFromForm({
      ...answer('compraya_t1', 'sex_t1', 'Biological sex', 'categorical'),
      ...answer('survaya', 'leeft', 'Age at diagnosis', 'continuous'),
      ...answer('survaya', 'geslacht', 'Biological sex', 'categorical'),
    })
    const dbs = getMapping().databases

    expect(localColumns(dbs.compraya_t1.tables.data)).toEqual({
      sex_t1: 'schema:variable/biological_sex',
    })
    expect(localColumns(dbs.survaya.tables.data)).toEqual({
      leeft: 'schema:variable/age_at_diagnosis',
      geslacht: 'schema:variable/biological_sex',
    })
    expect(dbs.survaya.name).toBe('survaya')
    expect(dbs.survaya.tables.data.sourceFile).toBe('survaya')
    // The other site's table, and the nameless one, keep their own columns.
    expect(localColumns(dbs.msci.tables.MSCI_prospective)).toEqual({
      geslacht: 'schema:variable/biological_sex',
    })
    expect(localColumns(dbs.template.tables.template_table)).toEqual({
      leeftijd: 'schema:variable/age_at_diagnosis',
    })
  })

  it('writes into the map table that names the ingested table by its key', async () => {
    await updateMappingFromForm(answer('template_table', 'age', 'Age at diagnosis'))
    const table = getMapping().databases.template.tables.template_table
    expect(localColumns(table)).toEqual({ age: 'schema:variable/age_at_diagnosis' })
  })

  it('does not take in a table through a database named after it', async () => {
    // The MSCI database is named like an ingested table, but its table is
    // another file: describing that table makes the table its own entry.
    await updateMappingFromForm(
      answer('MSCI_prospective_data', 'gender', 'Biological sex', 'categorical')
    )
    const dbs = getMapping().databases
    expect(localColumns(dbs.msci.tables.MSCI_prospective)).toEqual({
      geslacht: 'schema:variable/biological_sex',
    })
    expect(localColumns(dbs.msci_prospective_data.tables.data)).toEqual({
      gender: 'schema:variable/biological_sex',
    })
  })

  it('never overwrites an unrelated entry or column that shares a name', async () => {
    setMapping({
      schema: { variables: { biological_sex: {}, age_at_diagnosis: {} } },
      databases: {
        patients: {
          name: 'Clinic export',
          tables: {
            t: {
              sourceFile: 'clinic_2024.csv',
              columns: {
                biological_sex: { mapsTo: 'schema:variable/biological_sex', localColumn: 'sex' },
              },
            },
          },
        },
        visits: {
          name: 'visits',
          tables: {
            data: {
              sourceFile: 'visits',
              columns: {
                age_at_diagnosis: {
                  mapsTo: 'schema:variable/biological_sex',
                  localColumn: 'gender',
                },
              },
            },
          },
        },
      },
    })
    await updateMappingFromForm({
      ...answer('patients', 'geslacht', 'Biological sex', 'categorical'),
      ...answer('visits', 'age', 'Age at diagnosis', 'continuous'),
    })
    const dbs = getMapping().databases
    expect(localColumns(dbs.patients.tables.t)).toEqual({
      sex: 'schema:variable/biological_sex',
    })
    expect(localColumns(dbs.patients_2.tables.data)).toEqual({
      geslacht: 'schema:variable/biological_sex',
    })
    expect(localColumns(dbs.visits.tables.data)).toEqual({
      gender: 'schema:variable/biological_sex',
      age: 'schema:variable/age_at_diagnosis',
    })
  })

  it('preselects only the tables a map table names', () => {
    const { preselectedDescriptions } = computePreselectionsForDatabases([
      'MSCI_prospective_batch_1',
      'MSCI_prospective_data',
      'survaya',
      'template_table',
    ])
    expect(preselectedDescriptions).toEqual({
      MSCI_prospective_batch_1_geslacht: 'Biological sex',
      template_table_leeftijd: 'Age at diagnosis',
    })
  })
})
