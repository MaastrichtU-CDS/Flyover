import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useProjectStore } from '@/stores/project'

// Whether the page on screen has to wait for the store, and why. A route
// that needs the store (meta.needsStore) waits until Flyover knows whether
// to ask for a project, while nobody has chosen what to work on (the server
// says so, or this tab has not entered the page's workflow yet, so the
// pop-up asks it), while the store starts, and until the first answer about
// it. A page, once open, stays open whatever the store does next: it may
// hold the user's work. The next page waits again while the store is not
// there.
//
// A store someone stopped holds the page too, with a button to start it. A
// store that cannot be reached for another reason lets the page open, and
// `trouble` says why, above it.
//
// `waitingFor` is 'waiting', 'starting', 'connecting' or 'stopped', or ''
// when the page may open.
export function useStoreGate() {
  const route = useRoute()
  const { ready, picker, storeStatus, workflow } = storeToRefs(useProjectStore())

  const notYet = computed(() => {
    // Until the project store has loaded, Flyover does not know yet whether
    // it will ask for a project: a store that answers early must not open
    // the page under the pop-up.
    if (!ready.value) return 'connecting'
    // The tab has not entered this page's workflow: App.vue asks it, which
    // may follow a moment later (once the router has settled), so the page
    // waits on the same condition rather than on the pop-up.
    if (route.meta?.workflow && workflow.value !== route.meta.workflow) return 'waiting'
    if (picker.value.open && picker.value.mode === 'enter') return 'waiting'
    return (
      {
        waiting: 'waiting',
        starting: 'starting',
        unknown: 'connecting',
        stopped: 'stopped',
      }[storeStatus.value.state] || ''
    )
  })

  const opened = ref(!notYet.value)
  watch(notYet, (reason) => {
    if (!reason) opened.value = true
  })
  watch(
    () => route.path,
    () => {
      opened.value = !notYet.value
    }
  )

  const waitingFor = computed(() =>
    route.meta?.needsStore && !opened.value ? notYet.value : ''
  )
  const trouble = computed(() =>
    route.meta?.needsStore && storeStatus.value.state === 'unreachable'
      ? storeStatus.value.reason
      : ''
  )
  return { waitingFor, trouble }
}
