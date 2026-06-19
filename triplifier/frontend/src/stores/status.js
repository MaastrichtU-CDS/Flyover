import { defineStore } from 'pinia'
import { ref } from 'vue'

let nextId = 1

export const useStatusStore = defineStore('status', () => {
  const messages = ref([])

  function add(text, level = 'info') {
    const id = nextId++
    messages.value.push({ id, text, level })
    return id
  }

  function dismiss(id) {
    messages.value = messages.value.filter((m) => m.id !== id)
  }

  function clear() {
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
