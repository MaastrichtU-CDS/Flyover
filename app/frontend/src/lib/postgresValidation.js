// PostgreSQL connection field validation.
//
// Provides URL validation, blocked-character definitions, and event-handler
// factories used by the ingest form's Postgres section.

/** Validate a Postgres URL or host:port string. */
export function isValidPgUrl(url) {
  if (!url) return false
  // Accept host:port (e.g. localhost:5432) or a full URL with a scheme.
  if (/^https?:\/\//.test(url)) {
    try {
      new URL(url)
      return true
    } catch {
      return false
    }
  }
  // host:port — validate host (hostname or IP) and numeric port
  return /^[a-zA-Z0-9._-]+:\d+$/.test(url)
}

// Characters that must be blocked per Postgres field.
// @ — hijacks connection-string parsing (postgresql://user:pass@host)
// \n \r — inject new lines into the backend .properties file via raw f-string
// = — injects key-value pairs in .properties file format
// / — changes the path in jdbc:postgresql://{url}/{db}
// Password only blocks newlines — psycopg2.connect() handles everything else.
export const PG_BLOCKED_CHARS = {
  username: ['@', '\n', '\r', '='],
  password: ['\n', '\r'],
  url: ['@', '\n', '\r'],
  db: ['@', '\n', '\r', '=', '/'],
}

/** Return the list of blocked characters for a given field. */
export function pgBlockedCharsFor(field) {
  return PG_BLOCKED_CHARS[field] || []
}

/** Human-readable label listing the blocked characters for a field. */
export function pgBlockedCharsLabel(field) {
  const chars = pgBlockedCharsFor(field)
  const printable = chars
    .filter((c) => c !== '\n' && c !== '\r')
    .map((c) => `"${c}"`)
  const parts = []
  if (printable.length) parts.push(printable.join(', '))
  if (chars.includes('\n')) parts.push('line breaks')
  return parts.join(' and ')
}

/** Keydown handler factory: prevent typing blocked characters. */
export function preventBlockedKey(field) {
  return (e) => {
    if (pgBlockedCharsFor(field).includes(e.key)) e.preventDefault()
  }
}

/** Paste handler factory: strip blocked characters from pasted text. */
export function stripBlockedOnPaste(field) {
  return (e) => {
    const text = (e.clipboardData || window.clipboardData).getData('text')
    const blocked = pgBlockedCharsFor(field)
    if (blocked.some((c) => text.includes(c))) {
      e.preventDefault()
      const stripped = [...blocked].reduce(
        (s, c) => s.replaceAll(c, ''),
        text
      )
      document.execCommand('insertText', false, stripped)
    }
  }
}

/** Check whether a value contains any blocked characters for the field. */
export function pgHasBlockedChar(field, value) {
  if (!value) return false
  return pgBlockedCharsFor(field).some((c) => value.includes(c))
}

/** Return an error message if the field value contains blocked characters. */
export function pgFieldError(field, value, touched) {
  if (!touched) return ''
  if (!value) return ''
  if (pgHasBlockedChar(field, value)) {
    return `The following are not allowed: ${pgBlockedCharsLabel(field)}.`
  }
  return ''
}

/** Compute the URL-specific error message (blocked chars + format validation). */
export function pgUrlErrorValue(url, touched) {
  if (!touched) return ''
  if (url.includes('@') || /[\n\r]/.test(url)) {
    return `The following are not allowed: ${pgBlockedCharsLabel('url')}.`
  }
  if (!url) return 'URL is required.'
  if (!isValidPgUrl(url)) return 'Enter a valid host:port (e.g. localhost:5432) or a full URL.'
  return ''
}
