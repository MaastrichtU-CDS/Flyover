<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useCyclingDots } from '@/composables/useCyclingDots'
import { workflowById } from '@/lib/workflows'
import { useProjectStore } from '@/stores/project'

// What a page that needs the store shows in its place until the store is
// there: Flyover waits for a project choice, the store starts, Flyover
// connects to it, or someone stopped it (then with a button to start it).
// The page opens once the store has settled (see App.vue).

const props = defineProps({
  // waiting | starting | connecting | stopped
  reason: { type: String, required: true },
})

const projectStore = useProjectStore()
const { storeStatus } = storeToRefs(projectStore)
const route = useRoute()

// The store of the workflow whose page waits, as the workflow list names it.
const storeLabel = computed(() => workflowById(route.meta?.workflow)?.storeLabel || 'store')

const message = computed(() => {
  const messages = {
    waiting: {
      icon: 'fa-hourglass-half',
      title: 'Waiting for a project choice',
      detail: 'Choose a project, or continue without one, and this page opens.',
      underWay: false,
    },
    starting: {
      icon: 'fa-power-off',
      title: `Starting the ${storeLabel.value}`,
      detail: 'This can take a minute. The page opens once the store answers.',
      underWay: true,
    },
    connecting: {
      icon: 'fa-plug',
      title: `Connecting to the ${storeLabel.value}`,
      detail: '',
      underWay: true,
    },
    stopped: {
      icon: 'fa-circle-stop',
      title: `The ${storeLabel.value} is not running`,
      detail:
        'It was stopped, and Flyover leaves a stopped store alone. Start it to open this page.',
      underWay: false,
    },
  }
  return messages[props.reason] || messages.connecting
})
const dots = useCyclingDots(() => message.value.underWay)
</script>

<template>
  <div
    class="store-wait"
    role="status"
  >
    <i
      class="fas store-wait-icon"
      :class="message.icon"
      aria-hidden="true"
    />
    <div>
      <p class="store-wait-title">
        {{ message.title }}<span
          v-if="message.underWay"
          class="store-wait-dots"
          aria-hidden="true"
        >{{ '.'.repeat(dots) }}</span>
      </p>
      <p
        v-if="message.detail"
        class="store-wait-detail"
      >
        {{ message.detail }}
      </p>
      <button
        v-if="reason === 'stopped'"
        type="button"
        class="btn btn-primary btn-sm mt-2"
        :disabled="storeStatus.checking"
        @click="projectStore.startStore()"
      >
        <i class="fas fa-play" /> Start the {{ storeLabel }}
      </button>
    </div>
  </div>
</template>

<style scoped>
/* The app's purple edge, as on its information boxes. */
.store-wait {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin: 2rem 0;
  padding: 1.25rem 1.5rem;
  border-left: 4px solid rgba(118, 75, 162, 0.75);
  border-radius: 6px;
  background: #fff;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
}

.store-wait-icon {
  font-size: 1.75rem;
  color: #764ba2;
}

.store-wait-title {
  margin: 0;
  font-size: 1.15rem;
}

/* Room for three dots, so the title does not shift as they count. */
.store-wait-dots {
  display: inline-block;
  width: 1.2em;
  text-align: left;
}

.store-wait-detail {
  margin: 0.25rem 0 0;
  color: #6c757d;
  font-size: 0.9rem;
}
</style>
