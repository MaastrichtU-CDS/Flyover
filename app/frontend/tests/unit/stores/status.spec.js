import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useStatusStore } from '@/stores/status.js'

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
})
