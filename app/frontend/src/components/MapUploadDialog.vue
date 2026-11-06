<script setup>
import { computed, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useModalFocus } from '@/composables/useModalFocus'
import { projectName, projectVersion } from '@/lib/project'
import { useProjectStore } from '@/stores/project'
import { useStatusStore } from '@/stores/status'

// The question a map uploaded in a step raises (see planUpload in the
// project store): open it as the project, or keep what Flyover works on.
// Escape and the backdrop keep what is open, the choice that changes
// nothing.

const projectStore = useProjectStore()
const status = useStatusStore()
const { uploadQuestion: question, name: openName, configuredStore } = storeToRefs(projectStore)

const dialog = ref(null)
const isOpen = computed(() => Boolean(question.value))
const kind = computed(() => question.value?.kind)
const incoming = computed(() => question.value?.check?.project || {})
const incomingStore = computed(() => question.value?.check?.store || {})
const incomingLabel = computed(() =>
  [projectName(incoming.value), projectVersion(incoming.value)].filter(Boolean).join(' · ')
)
const fileName = computed(() => question.value?.fileName || 'This map')

const title = computed(
  () =>
    ({
      'no-project': `Open ${incomingLabel.value} as a project?`,
      'other-project': `Switch to ${incomingLabel.value}?`,
      'other-store': 'Switch to another RDF store?',
    })[kind.value] || "Replace the project's working copy?"
)

const openLabel = computed(
  () =>
    ({
      'no-project': 'Open as the project',
      'other-project': 'Switch project',
      'other-store': 'Switch store',
    })[kind.value] || 'Replace the working copy'
)

const keepLabel = computed(
  () =>
    ({
      'no-project': 'Keep going without a project',
      'other-project': `Keep going with ${openName.value}`,
      'other-store': 'Keep the current store',
    })[kind.value] || 'Keep the current copy'
)

function answer(choice) {
  projectStore.answerUpload(choice)
}

useModalFocus(dialog, isOpen, { onEscape: () => answer('keep') })

async function download() {
  if (!(await projectStore.downloadWorkingCopy())) {
    status.warning('There is no map in this browser to download.')
  }
}
</script>

<template>
  <div
    v-if="isOpen"
    class="upload-question"
    role="dialog"
    aria-modal="true"
    aria-labelledby="upload-question-title"
  >
    <div
      class="upload-question-backdrop"
      @click="answer('keep')"
    />
    <div
      ref="dialog"
      class="upload-question-dialog"
    >
      <div class="upload-question-header">
        <h5
          id="upload-question-title"
          class="upload-question-title"
        >
          <i
            class="fas fa-folder-open"
            aria-hidden="true"
          /> {{ title }}
        </h5>
      </div>

      <div class="upload-question-body">
        <template v-if="kind === 'no-project'">
          <p>
            <strong>{{ fileName }}</strong> belongs to the project
            <strong>{{ incomingLabel }}</strong>. Open it as the project, and Flyover works
            on the RDF store its map names.
          </p>
          <p>
            Or keep going without a project: the standard RDF flow on Flyover's configured
            RDF store<template v-if="configuredStore">
              (repository <strong>{{ configuredStore.repository }}</strong>)
            </template>, with this map, as before.
          </p>
        </template>
        <template v-else-if="kind === 'other-project'">
          <p>
            <strong>{{ fileName }}</strong> belongs to another project than the open one,
            <strong>{{ openName }}</strong>. Switching opens
            <strong>{{ incomingLabel }}</strong> and replaces the open project's working copy
            in this browser.
          </p>
          <p>Keeping going with {{ openName }} sets this upload aside.</p>
        </template>
        <template v-else-if="kind === 'other-store'">
          <p>
            <strong>{{ fileName }}</strong> belongs to the open project, but names another RDF
            store. Switching moves Flyover to that store, where the data in the current one
            stays, and replaces the project's working copy.
          </p>
          <p>Keeping the current store sets this upload aside.</p>
        </template>
        <template v-else>
          <p>
            <strong>{{ fileName }}</strong> belongs to the open project. It replaces the
            project's working copy, with the describe answers added since it was loaded.
          </p>
          <p>Keeping the current copy sets this upload aside.</p>
        </template>

        <p class="small text-muted upload-question-store">
          <i class="fas fa-database" /> Its RDF store: repository
          <strong>{{ incomingStore.repository }}</strong> ·
          <span class="upload-question-url">{{ incomingStore.url }}</span>
        </p>

        <div
          v-if="kind !== 'no-project'"
          class="alert alert-warning mb-0"
        >
          Download the current map first to keep it.
        </div>
      </div>

      <div class="upload-question-footer">
        <button
          v-if="kind !== 'no-project'"
          type="button"
          class="btn btn-outline-secondary"
          @click="download"
        >
          <i class="fas fa-download" /> Download the current map
        </button>
        <button
          type="button"
          class="btn btn-primary"
          @click="answer('open')"
        >
          <i class="fas fa-folder-open" /> {{ openLabel }}
        </button>
        <button
          type="button"
          class="btn btn-secondary"
          @click="answer('keep')"
        >
          {{ keepLabel }}
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.upload-question {
  position: fixed;
  inset: 0;
  z-index: 1095;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.upload-question-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
}

.upload-question-dialog {
  position: relative;
  display: flex;
  flex-direction: column;
  width: min(36rem, 100%);
  max-height: calc(100vh - 2rem);
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
  font-size: 0.9rem;
  overflow: hidden;
}

/* The app's own gradient, as on the step bar and the project pop-up. */
.upload-question-header {
  padding: 0.75rem 1rem;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: #fff;
}

.upload-question-title {
  margin: 0;
  font-size: 1.05rem;
}

.upload-question-body {
  overflow: auto;
  padding: 1rem;
}

.upload-question-url {
  overflow-wrap: anywhere;
}

.upload-question-footer {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 0.5rem;
  padding: 0.6rem 1rem;
  border-top: 1px solid #dee2e6;
}
</style>
