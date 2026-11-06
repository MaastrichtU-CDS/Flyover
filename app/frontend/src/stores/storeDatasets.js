import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import api from '@/services/api'
import { errorMessage, useProjectStore } from '@/stores/project'

// The datasets in the RDF store Flyover works on, for the panel the status
// bar's store opens (see /api/v1/store). A dataset is one ingested table.
// Deleting asks first (`ask`, then `confirm`), and the panel offers the
// matching TriG export (`exportUrl`) before anything goes.

export const useStoreDatasetsStore = defineStore('storeDatasets', () => {
  const open = ref(false)
  const loading = ref(false)
  const busy = ref(false)
  const error = ref('')
  const datasets = ref([])
  // What waits for the user's word: { kind: 'dataset' | 'all' | 'annotations', name }.
  const confirming = ref(null)
  // What went since the panel opened, for the message after it closes.
  const deleted = ref([])

  const anyAnnotated = computed(() => datasets.value.some((d) => d.annotated))

  async function load() {
    loading.value = true
    error.value = ''
    try {
      const { data } = await api.get('/api/v1/store/datasets')
      datasets.value = data?.datasets || []
    } catch (e) {
      error.value = errorMessage(e)
    } finally {
      loading.value = false
    }
  }

  async function show() {
    open.value = true
    confirming.value = null
    deleted.value = []
    await load()
  }

  // Close the panel; says what went while it was open.
  function close() {
    open.value = false
    confirming.value = null
    error.value = ''
    const gone = deleted.value
    deleted.value = []
    return gone
  }

  function ask(kind, name = '') {
    error.value = ''
    confirming.value = { kind, name }
  }

  function cancel() {
    confirming.value = null
  }

  function deleteUrl({ kind, name }) {
    if (kind === 'dataset') return `/api/v1/store/datasets/${encodeURIComponent(name)}`
    if (kind === 'all') return '/api/v1/store/datasets'
    return '/api/v1/store/annotations'
  }

  // The TriG file of what the delete in question removes.
  function exportUrl({ kind, name }) {
    if (kind === 'dataset') return `/api/v1/store/datasets/${encodeURIComponent(name)}/export`
    if (kind === 'all') return '/api/v1/store/export'
    return '/api/v1/store/annotations/export'
  }

  async function confirm() {
    const asked = confirming.value
    if (!asked) return
    busy.value = true
    error.value = ''
    try {
      const { data } = await api.delete(deleteUrl(asked))
      datasets.value = data?.datasets || []
      deleted.value.push(asked)
      confirming.value = null
      await useProjectStore().storeDataChanged()
    } catch (e) {
      error.value = errorMessage(e)
    } finally {
      busy.value = false
    }
  }

  return {
    open,
    loading,
    busy,
    error,
    datasets,
    confirming,
    anyAnnotated,
    load,
    show,
    close,
    ask,
    cancel,
    confirm,
    exportUrl,
  }
})
