import { defineStore } from 'pinia'
import { ref, shallowRef } from 'vue'
import { workflowById } from '@/lib/workflows'

// The browser remembers that a user acknowledged the notice beside Exit.
const EXIT_NOTICE_SEEN = 'flyover.exitNoticeSeen'

function exitNoticeSeen() {
  try {
    return window.localStorage.getItem(EXIT_NOTICE_SEEN) === '1'
  } catch {
    return false
  }
}

// The notes the workflows show:
// - `soon`: the note a workflow that is not built yet (OMOP, CSV) shows,
//   from its tile on the home page or its item in the navigation bar;
// - `exitNotice`: the notice beside Exit in the status bar, which says how
//   to leave the workflow. "Home" in a workflow's navigation bar leads to
//   the workflow's own landing page, so a click on it shows the notice,
//   every time, until the user clicks "Got it".
export const useWorkflowsStore = defineStore('workflows', () => {
  const soon = shallowRef(null)
  const exitNotice = ref(false)

  function showSoon(id) {
    soon.value = workflowById(id)
  }

  function closeSoon() {
    soon.value = null
  }

  function showExitNotice() {
    if (!exitNoticeSeen()) exitNotice.value = true
  }

  // `acknowledged`: the user clicked "Got it", so the notice stays away.
  function closeExitNotice({ acknowledged = false } = {}) {
    exitNotice.value = false
    if (!acknowledged) return
    try {
      window.localStorage.setItem(EXIT_NOTICE_SEEN, '1')
    } catch {
      // Without storage the notice shows again next time; nothing breaks.
    }
  }

  return { soon, showSoon, closeSoon, exitNotice, showExitNotice, closeExitNotice }
})
