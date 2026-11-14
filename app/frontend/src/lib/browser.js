// The browser's own actions, kept in one place so tests can stand in for
// them.

// Load the current page again, so every step reads the project and the
// store afresh.
export function reloadPage() {
  window.location.reload()
}
