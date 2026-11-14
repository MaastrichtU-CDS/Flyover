import { describe, it, expect } from 'vitest'
import {
  canonicalJson,
  downloadName,
  fingerprint,
  mapDetails,
  newToken,
  projectName,
  projectVersion,
  sameProject,
  sameStore,
  targetStandard,
} from '@/lib/project'

// A map has a general part every site of a project shares (its schema) and
// a site-specific part (its own name, its databases). Which project a map
// belongs to is read from the general part only.

const TEMPLATE = {
  name: 'REPLACE_WITH_YOUR_MAPPING_NAME',
  version: '1.0.0',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema', version: '1' },
  databases: {},
}
const FILLED_IN = {
  name: 'Centre A - AYA mapping',
  version: '2.1.0',
  description: 'Centre A',
  schema: { '@id': 'schema:aya/v1', name: 'AYA schema (renamed)', version: '1' },
  databases: { centre_a: { tables: {} } },
}

describe('mapDetails', () => {
  it('reads the map\'s own name and version and its schema\'s', () => {
    expect(mapDetails(FILLED_IN)).toEqual({
      name: 'Centre A - AYA mapping',
      version: '2.1.0',
      description: 'Centre A',
      schemaId: 'schema:aya/v1',
      schemaName: 'AYA schema (renamed)',
      schemaVersion: '1',
      // Naming no standard, it is an RDF map, as every map was before.
      targetStandard: 'rdf',
      targetStandardDeclared: false,
    })
  })

  it('reads the standard a map names, and only a known one', () => {
    expect(mapDetails({ ...FILLED_IN, targetStandard: 'omop' })).toMatchObject({
      targetStandard: 'omop',
      targetStandardDeclared: true,
    })
    expect(mapDetails({ ...FILLED_IN, targetStandard: 'sql' })).toMatchObject({
      targetStandard: 'rdf',
      targetStandardDeclared: false,
    })
  })

  it('reads nothing from what is not text', () => {
    expect(mapDetails({ name: 3, schema: 'x' })).toEqual({
      name: '',
      version: '',
      description: '',
      schemaId: '',
      schemaName: '',
      schemaVersion: '',
      targetStandard: 'rdf',
      targetStandardDeclared: false,
    })
    expect(mapDetails(null).schemaId).toBe('')
  })
})

describe('targetStandard', () => {
  it("is the standard a map's details name, else RDF", () => {
    expect(targetStandard({ targetStandard: 'omop' })).toBe('omop')
    expect(targetStandard({ targetStandard: 'csv' })).toBe('csv')
    expect(targetStandard({ targetStandard: 'rdf' })).toBe('rdf')
    expect(targetStandard({ targetStandard: 'nonsense' })).toBe('rdf')
    expect(targetStandard({})).toBe('rdf')
    expect(targetStandard(null)).toBe('rdf')
  })
})

describe('sameProject', () => {
  it('matches a filled-in map to its template by the schema @id', () => {
    expect(sameProject(mapDetails(TEMPLATE), mapDetails(FILLED_IN))).toBe(true)
  })

  it('tells projects apart by the schema @id even when the names agree', () => {
    const v2 = { schemaId: 'schema:aya/v2', schemaName: 'AYA schema' }
    expect(sameProject({ schemaId: 'schema:aya/v1', schemaName: 'AYA schema' }, v2)).toBe(false)
  })

  it('falls back to the schema name when either map has no @id', () => {
    expect(sameProject({ schemaId: '', schemaName: 'AYA' }, { schemaId: 'x', schemaName: 'AYA' })).toBe(true)
    expect(sameProject({ schemaName: 'AYA' }, { schemaName: 'Other' })).toBe(false)
  })

  it('never calls two maps the same project when nothing says so', () => {
    expect(sameProject({}, {})).toBe(false)
    expect(sameProject({ schemaId: 'x' }, { schemaName: 'x' })).toBe(false)
    expect(sameProject(null, { schemaId: 'x' })).toBe(false)
  })
})

describe('sameStore', () => {
  const store = { url: 'http://rdf-store:7200/repositories/userRepo', repository: 'userRepo' }

  it('compares the URL and the repository', () => {
    expect(sameStore(store, { ...store, source: 'project' })).toBe(true)
    expect(sameStore(store, { ...store, repository: 'aya' })).toBe(false)
    expect(sameStore(store, null)).toBe(false)
  })
})

describe('projectName and projectVersion', () => {
  it('show the map\'s own name with its version', () => {
    expect(projectName(FILLED_IN)).toBe('Centre A - AYA mapping')
    expect(projectVersion(FILLED_IN)).toBe('v2.1.0')
  })

  it('take the template\'s placeholder name as no name', () => {
    expect(projectName({ ...mapDetails(TEMPLATE), fileName: 't.jsonld' })).toBe('AYA schema')
  })

  it('fall back to the file name, then to a plain label', () => {
    expect(projectName({ name: '', schemaName: '', fileName: 'aya.jsonld' })).toBe('aya.jsonld')
    expect(projectName({})).toBe('Unnamed project')
    expect(projectName({}, 'Unnamed semantic map')).toBe('Unnamed semantic map')
  })

  it('show a version that starts with a v once, and none when there is none', () => {
    expect(projectVersion({ version: 'v3' })).toBe('v3')
    expect(projectVersion({ version: '  ' })).toBe('')
    expect(projectVersion(null)).toBe('')
  })
})

describe('fingerprint', () => {
  it('is the same for the same map whatever the order of its keys', () => {
    const a = { schema: { b: 1, a: [1, { y: 2, x: 1 }] }, name: 'n' }
    const b = { name: 'n', schema: { a: [1, { x: 1, y: 2 }], b: 1 } }

    expect(canonicalJson(a)).toBe(canonicalJson(b))
    expect(fingerprint(a)).toBe(fingerprint(b))
  })

  it('changes when a describe answer changes the map', () => {
    const answered = structuredClone(FILLED_IN)
    answered.databases.centre_a.tables.t = { columns: { age: { localColumn: 'age' } } }

    expect(fingerprint(answered)).not.toBe(fingerprint(FILLED_IN))
  })

  it('tells an array from an object with the same entries', () => {
    expect(fingerprint([1, 2])).not.toBe(fingerprint({ 0: 1, 1: 2 }))
  })
})

describe('newToken', () => {
  it('is 32 hexadecimal characters and new every time', () => {
    const tokens = new Set(Array.from({ length: 50 }, newToken))

    expect(tokens.size).toBe(50)
    for (const token of tokens) expect(token).toMatch(/^[0-9a-f]{32}$/)
  })
})

describe('downloadName', () => {
  it('keeps the file\'s own name and adds the extension a map needs', () => {
    expect(downloadName({ fileName: 'aya.jsonld' })).toBe('aya.jsonld')
    expect(downloadName({ fileName: 'aya.json' })).toBe('aya.json')
    expect(downloadName({ fileName: 'aya' })).toBe('aya.jsonld')
    expect(downloadName({})).toBe('semantic_map.jsonld')
  })
})
