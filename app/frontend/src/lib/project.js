// Projects. A project is a JSON-LD semantic map the user opened on purpose:
// in the pop-up Flyover shows as a tab enters a workflow, through "Switch
// project" in the status bar, or by accepting the offer a map upload in a
// step makes.
//
// A map names the standard its data is transformed to, `targetStandard`
// (rdf, omop or csv): the workflow that uses it. A map without one is an RDF
// map, as every map was before.
//
// A map has a general part every site of a project shares (its `schema`)
// and a site-specific part (its own name, its `databases`). Two maps belong
// to the same project when their schemas are the same one: a site's filled-in
// map belongs to the project of the empty template it started from.
//
// These helpers are plain functions over maps and over the project details
// the server reports (`/api/v1/project`, `/api/v1/project/check`).

// The example template's name, which a site is meant to replace.
const PLACEHOLDER_NAME = /^REPLACE_WITH_/i

const TARGET_STANDARDS = ['rdf', 'omop', 'csv']
export const DEFAULT_TARGET_STANDARD = 'rdf'

// Said after opening a map that names no target standard.
export const ASSUMED_RDF_NOTE =
  'Its JSON-LD names no target standard, so Flyover takes it as an RDF project: ' +
  'add "targetStandard": "rdf" to the JSON-LD to say so.'

function text(value) {
  return typeof value === 'string' ? value.trim() : ''
}

// What a map says about itself, in the shape the server reports it.
export function mapDetails(map) {
  const schema = map && typeof map.schema === 'object' && map.schema ? map.schema : {}
  const declared = TARGET_STANDARDS.includes(map?.targetStandard)
  return {
    name: text(map?.name),
    version: text(map?.version),
    description: text(map?.description),
    schemaId: text(schema['@id']),
    schemaName: text(schema.name),
    schemaVersion: text(schema.version),
    targetStandard: declared ? map.targetStandard : DEFAULT_TARGET_STANDARD,
    targetStandardDeclared: declared,
  }
}

// The standard a map targets, from its details: its own, else RDF.
export function targetStandard(details) {
  const standard = details?.targetStandard
  return TARGET_STANDARDS.includes(standard) ? standard : DEFAULT_TARGET_STANDARD
}

// Whether two maps (or their details) belong to the same project: the same
// schema `@id` when both have one, otherwise the same schema name. Without
// either, nothing says they are the same, so they are not.
export function sameProject(a, b) {
  if (!a || !b) return false
  if (a.schemaId && b.schemaId) return a.schemaId === b.schemaId
  if (a.schemaName && b.schemaName) return a.schemaName === b.schemaName
  return false
}

// Whether two store descriptions name the same store.
export function sameStore(a, b) {
  return Boolean(a && b && a.url === b.url && a.repository === b.repository)
}

// The name the status bar shows: the map's own name, else its schema's,
// else the file's, else `fallback`. The template's placeholder name counts
// as no name.
export function projectName(details, fallback = 'Unnamed project') {
  const own = text(details?.name)
  if (own && !PLACEHOLDER_NAME.test(own)) return own
  return text(details?.schemaName) || text(details?.fileName) || fallback
}

// "v1.0.0" for a map that names its version, else ''.
export function projectVersion(details) {
  const version = text(details?.version)
  if (!version) return ''
  return /^v/i.test(version) ? version : `v${version}`
}

// JSON with every object's keys sorted, so two equal maps give equal text
// whatever order their keys were written in.
export function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort()
    return `{${keys
      .filter((key) => value[key] !== undefined)
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`
  }
  return JSON.stringify(value ?? null)
}

// A short fingerprint of a map, to tell whether the working copy changed
// since it was loaded (cyrb53; not for security). Pages served over plain
// http have no crypto.subtle, so this cannot rely on it.
export function fingerprint(value) {
  const textValue = canonicalJson(value)
  let h1 = 0xdeadbeef
  let h2 = 0x41c6ce57
  for (let i = 0; i < textValue.length; i++) {
    const ch = textValue.charCodeAt(i)
    h1 = Math.imul(h1 ^ ch, 2654435761)
    h2 = Math.imul(h2 ^ ch, 1597334677)
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909)
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909)
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(16).padStart(14, '0')
}

// A new name for a version of the project, which the server hands back so
// a later visit can tell whether it still works on the same one.
export function newToken() {
  const bytes = new Uint8Array(16)
  if (globalThis.crypto?.getRandomValues) {
    globalThis.crypto.getRandomValues(bytes)
  } else {
    for (let i = 0; i < bytes.length; i++) bytes[i] = Math.floor(Math.random() * 256)
  }
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

// The file name a download of the working copy gets.
export function downloadName(details) {
  const name = text(details?.fileName)
  if (name) return /\.(jsonld|json)$/i.test(name) ? name : `${name}.jsonld`
  return 'semantic_map.jsonld'
}
