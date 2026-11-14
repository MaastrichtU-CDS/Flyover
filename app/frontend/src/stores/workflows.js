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

// The notes and questions of the workflows:
// - `soon`: the note a workflow that is not built yet (OMOP, CSV) shows,
//   from its tile on the home page or its item in the navigation bar;
// - `exitNotice`: the notice beside Exit in the status bar, which says how
//   to leave the workflow. "Home" in a workflow's navigation bar leads to
//   the workflow's own landing page, so a click on it shows the notice,
//   every time, until the user clicks "Got it";
// - `exitQuestion`: leaving a workflow for the home page asks whether to
//   keep its store running or stop it (see App.vue and ExitWorkflowDialog):
//   `{ workflow, resolve }` while it waits for the answer.
export const useWorkflowsStore = defineStore('workflows', () => {
  const soon = shallowRef(null)
  const exitNotice = ref(false)
  const exitQuestion = shallowRef(null)

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

  // Resolves with the answer: 'keep' (the store keeps running), 'stopped'
  // (the dialog stopped it) or 'stay' (the user stays in the workflow). The
  // notice beside Exit closes: the user did what it says, and it would sit
  // over the question. It does not count as "Got it".
  function askExit(workflow) {
    closeExitNotice()
    exitQuestion.value?.resolve('stay')
    return new Promise((resolve) => {
      exitQuestion.value = { workflow, resolve }
    })
  }

  function answerExit(answer) {
    const question = exitQuestion.value
    exitQuestion.value = null
    question?.resolve(answer)
  }

  return {
    soon,
    showSoon,
    closeSoon,
    exitNotice,
    showExitNotice,
    closeExitNotice,
    exitQuestion,
    askExit,
    answerExit,
  }
})
