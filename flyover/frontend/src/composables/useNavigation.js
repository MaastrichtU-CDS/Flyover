import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'
import api from '@/services/api'

const dataExists = ref(false)
let primed = false

const STEPS = [
  {
    name: 'ingest',
    label: 'Ingest',
    icon: 'fa-cookie-bite',
    completedIcon: 'fa-check',
    to: '/ingest',
    matches: ['/ingest'],
    alwaysAccessible: true,
  },
  {
    name: 'describe',
    label: 'Describe',
    icon: 'fa-edit',
    completedIcon: 'fa-check',
    to: '/describe',
    matches: ['/describe'],
    requiresData: true,
  },
  {
    name: 'annotate',
    label: 'Annotate',
    icon: 'fa-tags',
    completedIcon: 'fa-check',
    to: '/annotate',
    matches: ['/annotate'],
    requiresData: true,
  },
  {
    name: 'share',
    label: 'Share',
    icon: 'fa-mail-forward',
    to: '/share',
    matches: ['/share'],
    alwaysAccessible: true,
  },
]

async function refreshDataExists() {
  try {
    const { data } = await api.get('/api/check-graph-exists')
    dataExists.value = !!data?.exists
  } catch {
    dataExists.value = false
  }
}

export function useNavigation() {
  const route = useRoute()

  if (!primed) {
    primed = true
    refreshDataExists()
  }

  const currentStep = computed(() => {
    const path = route.path
    for (const step of STEPS) {
      if (step.matches.some((m) => path === m || path.startsWith(m + '/'))) {
        return step.name
      }
    }
    return null
  })

  const stepStates = computed(() => {
    const cs = currentStep.value
    return STEPS.map((step) => {
      let active = step.name === cs
      let completed = false
      let disabled = false

      if (dataExists.value) {
        if (step.name === 'ingest') {
          completed = true
          if (cs !== 'ingest') active = false
        }
        if (step.name === 'describe' && cs === 'annotate') completed = true
      }

      if (!dataExists.value && step.requiresData && step.name !== cs) {
        disabled = true
      }

      return { ...step, active, completed, disabled }
    })
  })

  return { stepStates, dataExists, refreshDataExists, currentStep }
}
