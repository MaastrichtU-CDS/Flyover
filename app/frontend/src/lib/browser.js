// The browser's own actions, kept in one place so tests can stand in for
// them.

// Load the current page again, so every step reads the project and the
// store afresh.
export function reloadPage() {
  window.location.reload()
}

// Go to `path` with a full page load, so what the tab kept in memory (such
// as the map lib/jsonld last read) starts afresh.
export function loadPage(path) {
  window.location.assign(path)
}
