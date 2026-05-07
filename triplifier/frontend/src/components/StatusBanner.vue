<script setup>
import { useStatusStore } from '@/stores/status'
import { storeToRefs } from 'pinia'

const status = useStatusStore()
const { messages } = storeToRefs(status)

function variant(level) {
  if (level === 'error') return 'alert-danger'
  if (level === 'warning') return 'alert-warning'
  if (level === 'success') return 'alert-success'
  return 'alert-info'
}
</script>

<template>
  <div v-if="messages.length" class="mt-3">
    <div
      v-for="m in messages"
      :key="m.id"
      :class="['alert', 'alert-dismissible', variant(m.level)]"
    >
      <i v-if="m.level === 'warning' || m.level === 'error'" class="fas fa-exclamation-triangle"></i>
      <i v-else-if="m.level === 'success'" class="fas fa-check-circle"></i>
      <i v-else class="fas fa-info-circle"></i>
      {{ ' ' }}{{ m.text }}
      <button type="button" class="close align-middle" @click="status.dismiss(m.id)">
        &times;
      </button>
    </div>
  </div>
</template>
