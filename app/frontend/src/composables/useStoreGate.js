import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useProjectStore } from '@/stores/project'

// Whether the page on screen has to wait for the store, and why. A route
// that needs the store (meta.needsStore) waits until Flyover knows whether
// to ask for a project, while nobody has chosen what to work on (the server
// says so, or this tab's start-up pop-up is open), while the store starts,
// and until the first answer about it. A page, once
// open, stays open whatever the store does next: it may hold the user's
// work. The next page waits again while the store is not there.
//
// `waitingFor` is 'waiting', 'starting' or 'connecting', or '' when the page
// may open.
export function useStoreGate() {
  const route = useRoute()
  const { ready, picker, storeStatus } = storeToRefs(useProjectStore())

  const notYet = computed(() => {
    // Until the project store has loaded, Flyover does not know yet whether
    // it will ask for a project: a store that answers early must not open
    // the page under the start-up pop-up.
    if (!ready.value) return 'connecting'
    if (picker.value.open && picker.value.mode === 'startup') return 'waiting'
    return (
      { waiting: 'waiting', starting: 'starting', unknown: 'connecting' }[
        storeStatus.value.state
      ] || ''
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
  return { waitingFor }
}
