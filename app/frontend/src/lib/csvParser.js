// CSV header detection utilities.
//
// Lightweight client-side parsing that reads only the first line of a CSV
// to determine column names. Used by the ingest view to populate PK/FK
// dropdowns without sending data to the server.

/** Detect the most likely field separator from the first line of a CSV. */
export function detectSeparator(line) {
  const seps = [',', ';', '\t', '|']
  let best = ','
  let max = 0
  for (const s of seps) {
    const c = line.split(s).length - 1
    if (c > max) {
      max = c
      best = s
    }
  }
  return best
}

/** Parse a single CSV line into trimmed, non-empty field values. */
export function parseCSVLine(line, sep) {
  const out = []
  let cur = ''
  let q = false
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (ch === '"') q = !q
    else if (ch === sep && !q) {
      out.push(cur.trim())
      cur = ''
    } else cur += ch
  }
  out.push(cur.trim())
  return out.map((h) => h.replace(/"/g, '').trim()).filter(Boolean)
}

/** Read column headers from the first line of a CSV file (max 1 KB). */
export function readCSVColumns(file) {
  return new Promise((resolve) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const lines = e.target.result.split('\n')
        if (!lines.length) return resolve([])
        const sep = detectSeparator(lines[0])
        resolve(parseCSVLine(lines[0], sep))
      } catch {
        resolve([])
      }
    }
    reader.onerror = () => resolve([])
    reader.readAsText(file.slice(0, 1024))
  })
}
