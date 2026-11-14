<script setup>
import { computed, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useCyclingDots } from '@/composables/useCyclingDots'
import { useModalFocus } from '@/composables/useModalFocus'
import { errorMessage, useProjectStore } from '@/stores/project'
import { useWorkflowsStore } from '@/stores/workflows'

// Leaving a workflow for the home page asks whether to keep its store
// running, for the next visit and for other tools that use it, or to stop
// it, which frees its memory. Flyover stops a store only on this request.
// App.vue's navigation guard asks, and waits for the answer: "Stay" keeps
// the user where they were. Stopping happens here, so the dialog can say it
// is under way, and why it failed.

const workflows = useWorkflowsStore()
const projectStore = useProjectStore()
const { exitQuestion } = storeToRefs(workflows)

const dialog = ref(null)
const stopping = ref(false)
const error = ref('')
const isOpen = computed(() => exitQuestion.value !== null)
const workflowName = computed(() => exitQuestion.value?.workflow?.label || '')
const storeName = computed(() => exitQuestion.value?.workflow?.storeLabel || 'store')
const dots = useCyclingDots(() => stopping.value)

useModalFocus(dialog, isOpen, {
  onEscape: stay,
  onOpen: () => {
    stopping.value = false
    error.value = ''
  },
})

function keep() {
  if (!stopping.value) workflows.answerExit('keep')
}

function stay() {
  if (!stopping.value) workflows.answerExit('stay')
}

async function stop() {
  if (stopping.value) return
  error.value = ''
  stopping.value = true
  try {
    await projectStore.stopStore()
    workflows.answerExit('stopped')
  } catch (e) {
    error.value = `Flyover could not stop the ${storeName.value}: ${errorMessage(e)}`
  } finally {
    stopping.value = false
  }
}
</script>

<template>
  <div
    v-if="isOpen"
    class="exit-workflow"
    role="dialog"
    aria-modal="true"
    aria-labelledby="exit-workflow-title"
  >
    <div
      class="exit-workflow-backdrop"
      @click="stay"
    />
    <div
      ref="dialog"
      class="exit-workflow-dialog"
    >
      <div class="exit-workflow-header">
        <h5
          id="exit-workflow-title"
          class="exit-workflow-title"
        >
          <i
            class="fas fa-right-from-bracket"
            aria-hidden="true"
          /> Leave the {{ workflowName }} workflow?
        </h5>
        <button
          type="button"
          class="exit-workflow-close"
          aria-label="Close"
          :disabled="stopping"
          @click="stay"
        >
          &times;
        </button>
      </div>
      <div class="exit-workflow-body">
        <p class="mb-1">
          Do you want to keep the {{ storeName }} running, for your next visit and for other tools
          that use it, or stop it to free its memory?
        </p>
        <p class="small text-muted mb-0 exit-workflow-note">
          Once stopped, the {{ storeName }} cannot be queried, by Flyover or any other tool, until
          it starts again. Its data stays in it.
        </p>
        <div
          v-if="error"
          class="alert alert-danger alert-compact mt-3 mb-0"
          role="alert"
        >
          {{ error }}
        </div>
      </div>
      <div class="exit-workflow-footer">
        <button
          type="button"
          class="btn btn-link btn-sm"
          :disabled="stopping"
          @click="stay"
        >
          Stay
        </button>
        <button
          id="exit-workflow-stop"
          type="button"
          class="btn btn-outline-secondary btn-sm"
          :disabled="stopping"
          @click="stop"
        >
          <template v-if="stopping">
            <i
              class="fas fa-circle-stop"
              aria-hidden="true"
            /> Stopping<span
              class="exit-workflow-dots"
              aria-hidden="true"
            >{{ '.'.repeat(dots) }}</span>
          </template>
          <template v-else>
            <i
              class="fas fa-circle-stop"
              aria-hidden="true"
            /> Stop the {{ storeName }}
          </template>
        </button>
        <button
          id="exit-workflow-keep"
          type="button"
          class="btn btn-primary btn-sm"
          :disabled="stopping"
          @click="keep"
        >
          Keep it running
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.exit-workflow {
  position: fixed;
  inset: 0;
  z-index: 1095;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.exit-workflow-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
}

.exit-workflow-dialog {
  position: relative;
  display: flex;
  flex-direction: column;
  width: min(32rem, 100%);
  max-height: calc(100vh - 2rem);
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
  font-size: 0.9rem;
  overflow: hidden;
}

/* The app's own gradient, as on the step bar and the project pop-up. */
.exit-workflow-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.75rem 1rem;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: #fff;
}

.exit-workflow-title {
  margin: 0;
  font-size: 1.05rem;
}

.exit-workflow-close {
  padding: 0 0.25rem;
  border: 0;
  background: none;
  color: #fff;
  font-size: 1.4rem;
  line-height: 1;
  cursor: pointer;
}

.exit-workflow-body {
  overflow: auto;
  padding: 1rem;
}

.exit-workflow-footer {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 0.5rem;
  padding: 0.6rem 1rem;
  border-top: 1px solid #dee2e6;
}

/* Room for three dots, so the label does not shift as they count. */
.exit-workflow-dots {
  display: inline-block;
  width: 1.2em;
  text-align: left;
}
</style>
