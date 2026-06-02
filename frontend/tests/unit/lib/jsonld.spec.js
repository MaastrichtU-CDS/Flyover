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
})
