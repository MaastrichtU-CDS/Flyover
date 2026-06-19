import { describe, it, expect, beforeEach, vi } from 'vitest'

vi.mock('@/lib/db', () => ({
  saveData: vi.fn(async () => {}),
  getData: vi.fn(async () => null),
}))

import {
  formatToTitleCase,
  formatToSnakeCase,
  graphDatabaseFindNameMatch,
  getVariableKeyFromColumn,
  normalizeLocalMappings,
  forEachColumn,
  findColumnForVariable,
  setMapping,
  getMapping,
  updateCategoryMapping,
  updateMappingFromForm,
  initializeEmptyMapping,
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

describe('graphDatabaseFindNameMatch', () => {
  it('matches identical names', () => {
    expect(graphDatabaseFindNameMatch('foo', 'foo')).toBe(true)
  })

  it('matches with .csv stripped on either side', () => {
    expect(graphDatabaseFindNameMatch('foo.csv', 'foo')).toBe(true)
    expect(graphDatabaseFindNameMatch('foo', 'foo.csv')).toBe(true)
  })

  it('returns true when mapDbName is empty (legacy "match anything" behavior)', () => {
    expect(graphDatabaseFindNameMatch('', 'whatever')).toBe(true)
    expect(graphDatabaseFindNameMatch(null, 'whatever')).toBe(true)
  })

  it('rejects unrelated names', () => {
    expect(graphDatabaseFindNameMatch('foo', 'bar')).toBe(false)
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

  it('adds a column and schema variable for a selected description', async () => {
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    const m = getMapping()
    expect(m.schema.variables.age).toMatchObject({
      name: 'age',
      dataType: 'continuous',
    })
    const cols = m.databases.patients.tables.data.columns
    expect(cols.age).toMatchObject({
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
    // The old schema variable was GCed.
    expect(m.schema.variables.age).toBeUndefined()
    expect(m.schema.variables.weight).toBeDefined()
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
    expect(m.schema.variables.hemoglobin).toBeDefined()
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
    expect(m.schema.variables.biological_sex).toBeDefined()
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('GCs schema variables that the form orphans across syncs', async () => {
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
})
