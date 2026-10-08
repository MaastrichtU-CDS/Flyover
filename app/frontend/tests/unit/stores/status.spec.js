import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useStatusStore, TOAST_TIMEOUT_MS, ERROR_TIMEOUT_MS } from '@/stores/status.js'

describe('useStatusStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('starts with no messages', () => {
    const s = useStatusStore()
    expect(s.messages).toEqual([])
  })

  it('add() returns an id and pushes a message', () => {
    const s = useStatusStore()
    const id = s.add('hello')
    expect(typeof id).toBe('number')
    expect(s.messages).toHaveLength(1)
    expect(s.messages[0]).toMatchObject({ id, text: 'hello', level: 'info' })
  })

  it('helper methods set the right level', () => {
    const s = useStatusStore()
    s.success('ok')
    s.warning('hmm')
    s.error('bad')
    expect(s.messages.map((m) => m.level)).toEqual(['success', 'warning', 'error'])
  })

  it('dismiss() removes a single message by id', () => {
    const s = useStatusStore()
    const a = s.add('a')
    const b = s.add('b')
    s.dismiss(a)
    expect(s.messages.map((m) => m.id)).toEqual([b])
  })

  it('clear() empties everything', () => {
    const s = useStatusStore()
    s.add('a')
    s.add('b')
    s.clear()
    expect(s.messages).toEqual([])
  })

  describe('auto-dismiss', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it('a message disappears after the toast timeout', () => {
      const s = useStatusStore()
      s.success('copied')
      expect(s.messages).toHaveLength(1)
      vi.advanceTimersByTime(TOAST_TIMEOUT_MS - 1)
      expect(s.messages).toHaveLength(1)
      vi.advanceTimersByTime(1)
      expect(s.messages).toEqual([])
    })

    it('errors stay longer than the default timeout', () => {
      const s = useStatusStore()
      s.error('bad')
      vi.advanceTimersByTime(TOAST_TIMEOUT_MS)
      expect(s.messages).toHaveLength(1)
      vi.advanceTimersByTime(ERROR_TIMEOUT_MS - TOAST_TIMEOUT_MS)
      expect(s.messages).toEqual([])
    })

    it('a manually dismissed message does not reappear', () => {
      const s = useStatusStore()
      const id = s.add('bye')
      s.dismiss(id)
      vi.advanceTimersByTime(ERROR_TIMEOUT_MS + 1)
      expect(s.messages).toEqual([])
    })
  })
})

