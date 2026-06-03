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
})
