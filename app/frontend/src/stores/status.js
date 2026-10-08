import { defineStore } from 'pinia'
import { ref } from 'vue'

let nextId = 1

// How long a message stays in the top banner before it dismisses itself.
// Errors linger longer so a problem is not missed mid-browse; every
// message keeps its manual close button.
export const TOAST_TIMEOUT_MS = 5000
export const ERROR_TIMEOUT_MS = 10000

export const useStatusStore = defineStore('status', () => {
  const messages = ref([])
  // Pending auto-dismiss timers per message id, cleared on dismiss/clear.
  const timers = new Map()

  function add(text, level = 'info') {
    const id = nextId++
    messages.value.push({ id, text, level })
    const ms = level === 'error' ? ERROR_TIMEOUT_MS : TOAST_TIMEOUT_MS
    timers.set(id, setTimeout(() => dismiss(id), ms))
    return id
  }

  function dismiss(id) {
    const timer = timers.get(id)
    if (timer !== undefined) {
      clearTimeout(timer)
      timers.delete(id)
    }
    messages.value = messages.value.filter((m) => m.id !== id)
  }

  function clear() {
    for (const timer of timers.values()) clearTimeout(timer)
    timers.clear()
    messages.value = []
  }

  return {
    messages,
    add,
    dismiss,
    clear,
    info: (t) => add(t, 'info'),
    success: (t) => add(t, 'success'),
    warning: (t) => add(t, 'warning'),
    error: (t) => add(t, 'error'),
  }
})
