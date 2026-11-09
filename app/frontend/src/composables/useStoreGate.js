import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useProjectStore } from '@/stores/project'

// Whether the page on screen has to wait, and why. A page of a workflow
// (meta.workflow) waits until the tab has entered the workflow, by choosing
// a project or to go without one (App.vue asks). A page that needs the
// store (meta.needsStore) then also waits until Flyover knows the store,
// while nobody has chosen what to work on (the server says so), while the
// store starts, and until the first answer about it. A page, once open,
// stays open whatever happens next: it may hold the user's work. The next
// page waits again while it has to.
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
    // The tab has not entered this page's workflow: App.vue asks it, which
    // may follow a moment later (once the router has settled), so the page
    // waits on the same condition rather than on the pop-up.
    if (route.meta?.workflow && workflow.value !== route.meta.workflow) return 'waiting'
    if (picker.value.open && picker.value.mode === 'enter') return 'waiting'
    if (!route.meta?.needsStore) return ''
    // Until the project store has loaded, Flyover does not know the store:
    // one that answers early must not open the page before Flyover knows
    // whether it is the store the tab works on.
    if (!ready.value) return 'connecting'
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

  const held = computed(() => Boolean(route.meta?.needsStore || route.meta?.workflow))
  const waitingFor = computed(() => (held.value && !opened.value ? notYet.value : ''))
  const trouble = computed(() =>
    route.meta?.needsStore && storeStatus.value.state === 'unreachable'
      ? storeStatus.value.reason
      : ''
  )
  return { waitingFor, trouble }
}
