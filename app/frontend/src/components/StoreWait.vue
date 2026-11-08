<script setup>
import { computed } from 'vue'
import { useCyclingDots } from '@/composables/useCyclingDots'

// What a page that needs the store shows in its place until the store is
// there: Flyover waits for a project choice, the store starts, or Flyover
// connects to it. The page opens once the store has settled (see App.vue).

const props = defineProps({
  // waiting | starting | connecting
  reason: { type: String, required: true },
})

const MESSAGES = {
  waiting: {
    icon: 'fa-hourglass-half',
    title: 'Waiting for a project choice',
    detail: 'Choose a project, or continue without one, and this page opens.',
    underWay: false,
  },
  starting: {
    icon: 'fa-power-off',
    title: 'Starting the RDF store',
    detail: 'This can take a minute. The page opens once the store answers.',
    underWay: true,
  },
  connecting: {
    icon: 'fa-plug',
    title: 'Connecting to the RDF store',
    detail: '',
    underWay: true,
  },
}

const message = computed(() => MESSAGES[props.reason] || MESSAGES.connecting)
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
