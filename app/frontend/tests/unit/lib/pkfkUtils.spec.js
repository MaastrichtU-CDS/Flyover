import { describe, it, expect } from 'vitest'
import { computeAutoSuggestions } from '@/lib/pkfkUtils'

const tables = ['patients.csv', 'visits.csv', 'doctors.csv']

describe('computeAutoSuggestions', () => {
  it('flags an exact (case-insensitive) name match', () => {
    const columns = {
      'patients.csv': ['patient_id', 'name'],
      'visits.csv': ['visit_id', 'Patient_ID', 'date'],
      'doctors.csv': ['doctor_id', 'name'],
    }
    const result = computeAutoSuggestions(tables, columns, ['patient_id'], 0)
    expect(result).toEqual({
      1: { fk: 'Patient_ID', fkTable: 'patients.csv', fkColumn: 'patient_id', exact: true },
    })
  })

  it('flags a partial match where one name contains the other', () => {
    const columns = {
      'patients.csv': ['patient_id', 'name'],
      'visits.csv': ['visit_id', 'fk_patient_id'],
      'doctors.csv': ['id'],
    }
    const result = computeAutoSuggestions(tables, columns, ['patient_id'], 0)
    expect(result[1]).toEqual({
      fk: 'fk_patient_id', fkTable: 'patients.csv', fkColumn: 'patient_id', exact: false,
    })
    // 'id' is contained in 'patient_id', so it is also a (weak) partial match
    expect(result[2]).toEqual({
      fk: 'id', fkTable: 'patients.csv', fkColumn: 'patient_id', exact: false,
    })
  })

  it('prefers the exact match over an earlier partial one', () => {
    const columns = {
      'patients.csv': ['patient_id'],
      'visits.csv': ['fk_patient_id', 'patient_id'],
      'doctors.csv': [],
    }
    const result = computeAutoSuggestions(tables, columns, ['patient_id'], 0)
    expect(result[1]).toEqual({
      fk: 'patient_id', fkTable: 'patients.csv', fkColumn: 'patient_id', exact: true,
    })
    expect(result[2]).toBeUndefined()
  })

  it('returns nothing without a PK on the given table', () => {
    expect(computeAutoSuggestions(tables, {}, [''], 0)).toEqual({})
  })
})
