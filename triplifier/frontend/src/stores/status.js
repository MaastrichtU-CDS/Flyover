import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useStatusStore = defineStore('status', () => {
  const message = ref('')
  const level = ref('info')

  function set(text, lvl = 'info') {
    message.value = text
    level.value = lvl
  }

  function clear() {
    message.value = ''
    level.value = 'info'
  }

  return { message, level, set, clear }
})
