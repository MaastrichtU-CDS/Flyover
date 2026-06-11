import { describe, it, expect, vi, beforeEach } from 'vitest'

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

describe('Frontend unit: jsonld / formatToTitleCase', () => {
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

describe('Frontend unit: jsonld / formatToSnakeCase', () => {
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

describe('Frontend unit: jsonld / graphDatabaseFindNameMatch', () => {
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

describe('Frontend unit: jsonld / getVariableKeyFromColumn', () => {
  it('extracts the trailing segment of mapsTo', () => {
    expect(getVariableKeyFromColumn({ mapsTo: 'schema:variable/age' })).toBe('age')
  })

  it('returns undefined for missing mapsTo', () => {
    expect(getVariableKeyFromColumn({})).toBeUndefined()
    expect(getVariableKeyFromColumn({ mapsTo: null })).toBeUndefined()
  })
})

describe('Frontend unit: jsonld / normalizeLocalMappings', () => {
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

describe('Frontend unit: jsonld / forEachColumn', () => {
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

describe('Frontend unit: jsonld / findColumnForVariable', () => {
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

describe('Frontend unit: jsonld / updateCategoryMapping', () => {
  // "Other" is the UI placeholder on Describe Variable Details meaning "no
  // semantic mapping for this category". It must never make it into
  // localMappings — the backend MappingValidator rejects the whole map
  // because no schema term is keyed "other". This block locks that
  // contract down at the library boundary.
  function freshMapping() {
    return {
      schema: { variables: { sex: {} } },
      databases: {
        db_a: {
          name: 'patients',
          tables: {
            t1: {
              sourceFile: 'patients',
              columns: {
                sex_col: {
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

  it('writes the snake_cased term when a real category is selected', async () => {
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', 'Male', null)
    const col = getMapping().databases.db_a.tables.t1.columns.sex_col
    expect(col.localMappings).toEqual({ male: ['M'] })
  })

  it('does not write localMappings.other when "Other" is selected', async () => {
    await updateCategoryMapping('patients', 'sex', 'sex', 'X', 'Other', null)
    const col = getMapping().databases.db_a.tables.t1.columns.sex_col
    expect(col.localMappings).toEqual({})
    expect(col.localMappings).not.toHaveProperty('other')
  })

  it('switching from a real term to "Other" removes the previous entry without adding "other"', async () => {
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', 'Male', null)
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', 'Other', 'Male')
    const col = getMapping().databases.db_a.tables.t1.columns.sex_col
    expect(col.localMappings).toEqual({})
  })

  it('writes the new term when switching from "Other" back to a real category', async () => {
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', 'Other', null)
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', 'Male', 'Other')
    const col = getMapping().databases.db_a.tables.t1.columns.sex_col
    expect(col.localMappings).toEqual({ male: ['M'] })
  })

  it('removes the previous term when the user deselects (selectedOption empty)', async () => {
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', 'Male', null)
    await updateCategoryMapping('patients', 'sex', 'sex', 'M', '', 'Male')
    const col = getMapping().databases.db_a.tables.t1.columns.sex_col
    expect(col.localMappings).toEqual({})
  })
})

describe('Frontend unit: jsonld / updateMappingFromForm — deselection and tab-switching robustness', () => {
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
    await updateMappingFromForm(
      formEntry('patients', 'age_yrs', 'Age', 'continuous')
    )
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    const cols = m.databases.patients?.tables?.data?.columns || {}
    expect(cols.age).toBeUndefined()
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
    expect(cols.weight).toMatchObject({
      mapsTo: 'schema:variable/weight',
      localColumn: 'age_yrs',
    })
    expect(cols.age).toBeUndefined()
    expect(m.schema.variables.age).toBeUndefined()
    expect(m.schema.variables.weight).toBeDefined()
  })

  it('does not touch columns from databases not present in the form payload', async () => {
    await updateMappingFromForm({
      ...formEntry('patients', 'age_yrs', 'Age', 'continuous'),
      ...formEntry('lab', 'h_g', 'Hemoglobin', 'continuous'),
    })
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    expect(m.databases.lab.tables.data.columns.hemoglobin).toMatchObject({
      localColumn: 'h_g',
    })
    expect(m.schema.variables.hemoglobin).toBeDefined()
    expect(m.databases.patients?.tables?.data?.columns?.age).toBeUndefined()
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('preserves an unrelated column in the same database', async () => {
    await updateMappingFromForm({
      ...formEntry('patients', 'age_yrs', 'Age', 'continuous'),
      ...formEntry('patients', 'sex', 'Biological Sex', 'categorical'),
    })
    await updateMappingFromForm(formEntry('patients', 'age_yrs', '', ''))
    const m = getMapping()
    const cols = m.databases.patients.tables.data.columns
    expect(cols.age).toBeUndefined()
    expect(cols.biological_sex).toMatchObject({ localColumn: 'sex' })
    expect(m.schema.variables.biological_sex).toBeDefined()
    expect(m.schema.variables.age).toBeUndefined()
  })

  it('GCs schema variables that the form orphans across syncs', async () => {
    await updateMappingFromForm({
      ...formEntry('cohort_a', 'age_yrs', 'Age', 'continuous'),
      ...formEntry('cohort_b', 'leeftijd', 'Age', 'continuous'),
    })
    expect(getMapping().schema.variables.age).toBeDefined()
    await updateMappingFromForm(formEntry('cohort_a', 'age_yrs', '', ''))
    expect(getMapping().schema.variables.age).toBeDefined()
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

