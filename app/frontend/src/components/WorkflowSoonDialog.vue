<script setup>
import { computed, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useModalFocus } from '@/composables/useModalFocus'
import { useWorkflowsStore } from '@/stores/workflows'

// The pop-up a workflow that is not built yet shows (OMOP, CSV), in the
// style of the project pop-up.

const workflows = useWorkflowsStore()
const { soon } = storeToRefs(workflows)
const dialog = ref(null)
const isOpen = computed(() => soon.value !== null)

useModalFocus(dialog, isOpen, { onEscape: () => workflows.closeSoon() })
</script>

<template>
  <div
    v-if="soon"
    class="workflow-soon"
    role="dialog"
    aria-modal="true"
    aria-labelledby="workflow-soon-title"
  >
    <div
      class="workflow-soon-backdrop"
      @click="workflows.closeSoon()"
    />
    <div
      ref="dialog"
      class="workflow-soon-dialog"
    >
      <div class="workflow-soon-header">
        <h5
          id="workflow-soon-title"
          class="workflow-soon-title"
        >
          <i
            class="fas"
            :class="soon.icon"
            aria-hidden="true"
          /> The {{ soon.label }} workflow
        </h5>
      </div>
      <div class="workflow-soon-body">
        <p class="workflow-soon-note">
          <i
            class="fas fa-person-digging"
            aria-hidden="true"
          />
          <strong>This workflow is under development.</strong>
        </p>
        <p>{{ soon.description }}</p>
        <p class="mb-0 text-muted">
          Until it is ready, the RDF workflow is the one to use.
        </p>
      </div>
      <div class="workflow-soon-footer">
        <button
          type="button"
          class="btn btn-primary btn-sm"
          @click="workflows.closeSoon()"
        >
          Close
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.workflow-soon {
  position: fixed;
  inset: 0;
  z-index: 1095;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.workflow-soon-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
}

.workflow-soon-dialog {
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
.workflow-soon-header {
  padding: 0.75rem 1rem;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: #fff;
}

.workflow-soon-title {
  margin: 0;
  font-size: 1.05rem;
}

.workflow-soon-body {
  overflow: auto;
  padding: 1rem;
}

.workflow-soon-note {
  color: #764ba2;
}

.workflow-soon-footer {
  display: flex;
  justify-content: flex-end;
  padding: 0.6rem 1rem;
  border-top: 1px solid #dee2e6;
}
</style>
