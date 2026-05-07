<script setup>
import { useStatusStore } from '@/stores/status'
import { storeToRefs } from 'pinia'

const status = useStatusStore()
const { message, level } = storeToRefs(status)
</script>

<template>
  <div v-if="message" :class="['status-banner', `status-${level}`]">
    {{ message }}
    <button class="status-close" @click="status.clear()">×</button>
  </div>
</template>

<style scoped>
.status-banner {
  padding: 0.75rem 1rem;
  margin: 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.status-info { background: #e7f3fe; color: #0c5460; }
.status-success { background: #d4edda; color: #155724; }
.status-warning { background: #fff3cd; color: #856404; }
.status-error { background: #f8d7da; color: #721c24; }
.status-close {
  background: transparent;
  border: 0;
  font-size: 1.25rem;
  cursor: pointer;
}
</style>
