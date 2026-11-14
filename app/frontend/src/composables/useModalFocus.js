import { nextTick, onBeforeUnmount, watch } from 'vue'

const FOCUSABLE =
  'button:not([disabled]), input:not([disabled]):not([type="file"]), a[href], [tabindex]:not([tabindex="-1"])'

// The keyboard handling a modal pop-up needs while `isOpen` is true: the
// focus moves into it when it opens, Tab and Shift+Tab stay inside it, and
// Escape calls `onEscape` (which decides whether Escape may close it).
// `onOpen` runs each time it opens. Returns `focusFirst`, for when the
// pop-up's content changes.
export function useModalFocus(dialog, isOpen, { onEscape = () => {}, onOpen = () => {} } = {}) {
  function focusables() {
    return Array.from(dialog.value?.querySelectorAll(FOCUSABLE) || [])
  }

  async function focusFirst() {
    await nextTick()
    focusables()[0]?.focus()
  }

  function onKeydown(event) {
    if (event.key === 'Escape') {
      event.preventDefault()
      onEscape()
      return
    }
    if (event.key !== 'Tab') return
    const items = focusables()
    if (!items.length) return
    const first = items[0]
    const last = items[items.length - 1]
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault()
      last.focus()
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault()
      first.focus()
    }
  }

  watch(
    isOpen,
    (open) => {
      if (typeof document === 'undefined') return
      if (open) {
        onOpen()
        document.addEventListener('keydown', onKeydown)
        focusFirst()
      } else {
        document.removeEventListener('keydown', onKeydown)
      }
    },
    { immediate: true }
  )

  onBeforeUnmount(() => document.removeEventListener('keydown', onKeydown))

  return { focusFirst }
}
