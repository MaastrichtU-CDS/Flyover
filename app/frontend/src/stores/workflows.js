import { defineStore } from 'pinia'
import { shallowRef } from 'vue'
import { workflowById } from '@/lib/workflows'

// The note a workflow that is not built yet (OMOP, CSV) shows, from its
// tile on the home page or its item in the navigation bar.
export const useWorkflowsStore = defineStore('workflows', () => {
  const soon = shallowRef(null)

  function showSoon(id) {
    soon.value = workflowById(id)
  }

  function closeSoon() {
    soon.value = null
  }

  return { soon, showSoon, closeSoon }
})
