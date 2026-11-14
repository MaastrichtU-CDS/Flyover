import { onBeforeUnmount, ref, watch } from 'vue'

// One to three dots, counting up every 400 ms while `active` is true: how
// the app shows that something is under way (a delete, a store starting).
// Back to one dot when it stops. `active` is a ref or a getter.
export function useCyclingDots(active, intervalMs = 400) {
  const dots = ref(1)
  let timer = null

  function stop() {
    clearInterval(timer)
    timer = null
    dots.value = 1
  }

  watch(
    active,
    (on) => {
      stop()
      if (on) timer = setInterval(() => (dots.value = (dots.value % 3) + 1), intervalMs)
    },
    { immediate: true }
  )
  onBeforeUnmount(stop)

  return dots
}
