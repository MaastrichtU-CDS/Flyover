// File drag-and-drop and type-detection helpers.
//
// Generic utilities for filtering dropped files by extension, detecting
// the common file type across a set of files, and programmatically
// assigning files to a hidden <input type="file"> element. The ingest view
// defines its own type-to-extension mapping and passes it in.

/**
 * Check whether a filename ends with any of the given extensions.
 * Comparison is case-insensitive.
 */
export function matchesExtension(name, extensions) {
  const lower = name.toLowerCase()
  return extensions.some((ext) => lower.endsWith(ext))
}

/**
 * Filter a FileList (or array) to only files whose names match one of the
 * given extensions.
 */
export function filterByExtension(fileList, extensions) {
  return Array.from(fileList).filter((f) => matchesExtension(f.name, extensions))
}

/**
 * Detect the common file type across all files using a type-to-extensions
 * mapping. Returns the type key if every file matches that type's
 * extensions, or null if the files are mixed or unmatched.
 *
 * Example: detectFileType(files, { CSV: ['.csv'], Excel: ['.xlsx', '.xls'] })
 */
export function detectFileType(files, typeExtensions) {
  const all = Array.from(files)
  for (const [type, exts] of Object.entries(typeExtensions)) {
    if (all.every((f) => matchesExtension(f.name, exts))) return type
  }
  return null
}

/**
 * Programmatically assign files to a hidden file input element using
 * DataTransfer. No-op if DataTransfer is unavailable or the element is null.
 */
export function setFileInputFiles(inputEl, files) {
  if (typeof DataTransfer === 'undefined' || !inputEl) return
  const dt = new DataTransfer()
  for (const file of files) dt.items.add(file)
  inputEl.files = dt.files
}
